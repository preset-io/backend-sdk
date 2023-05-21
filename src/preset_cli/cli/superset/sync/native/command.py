"""
A command to sync Superset exports into a Superset instance.
"""

import getpass
import importlib.util
import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Iterator, Tuple
from zipfile import ZipFile

import backoff
import click
import requests
import yaml
from jinja2 import Template
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import make_url
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.exceptions import SupersetError
from preset_cli.lib import dict_merge

_logger = logging.getLogger(__name__)

YAML_EXTENSIONS = {".yaml", ".yml"}
ASSET_DIRECTORIES = {"databases", "datasets", "charts", "dashboards"}
OVERRIDES_SUFFIX = ".overrides"

# This should be identical to ``superset.models.core.PASSWORD_MASK``. It's duplicated here
# because we don't want to have the CLI to depend on the ``superset`` package.
PASSWORD_MASK = "X" * 10

AssetConfig = Dict[str, Any]


resource_types = {
    "chart": "Slice",
    "dashboard": "Dashboard",
    "database": "Database",
    "dataset": "SqlaTable",
}


def load_user_modules(root: Path) -> Dict[str, ModuleType]:
    """
    Load user-defined modules so they can be used with Jinja2.
    """
    modules = {}
    for path in root.glob("*.py"):
        spec = importlib.util.spec_from_file_location(path.stem, path)
        if spec and spec.loader:
            modules[path.stem] = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(modules[path.stem])  # type: ignore

    return modules


def raise_helper(message: str, *args: Any) -> None:
    """
    Macro for Jinja2 so users can raise exceptions.
    """
    raise Exception(message % args)


def is_yaml_config(path: Path) -> bool:
    """
    Is this a valid YAML config?
    """
    return (
        path.suffix.lower() in YAML_EXTENSIONS
        and path.parts[0] in ASSET_DIRECTORIES
        and path.suffixes[0].lower() != OVERRIDES_SUFFIX
    )


def load_yaml(path: Path) -> Dict[str, Any]:
    """
    Load a YAML file and returns it as a dictionary.
    """
    with open(path, encoding="utf-8") as input_:
        content = input_.read()

    return yaml.load(content, Loader=yaml.SafeLoader)


def render_yaml(path: Path, env: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load a YAML file as a template, render it, and deserialize it.
    """
    env["filepath"] = path

    with open(path, encoding="utf-8") as input_:
        template = Template(input_.read())

    content = template.render(**env)

    return yaml.load(content, Loader=yaml.SafeLoader)


@click.command()
@click.argument("directory", type=click.Path(exists=True, resolve_path=True))
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing resources",
)
@click.option(
    "--option",
    "-o",
    multiple=True,
    help="Custom values for templates (eg, country=BR)",
)
@click.option(
    "--disable-jinja-templating",
    is_flag=True,
    default=False,
    help="By default, the CLI supports Jinja templating. This flag disables it",
)
@click.option(
    "--disallow-edits",
    is_flag=True,
    default=False,
    help="Mark resources as manged externally to prevent edits",
)
@click.option("--external-url-prefix", default="", help="Base URL for resources")
@click.option(
    "--load-env",
    "-e",
    is_flag=True,
    default=False,
    help="Load environment variables to ``env[]`` template helper",
)
@click.option(
    "--split",
    "-s",
    is_flag=True,
    default=False,
    help="Split imports into individual assets",
)
@click.pass_context
def native(  # pylint: disable=too-many-locals, too-many-arguments, too-many-branches
    ctx: click.core.Context,
    directory: str,
    option: Tuple[str, ...],
    overwrite: bool = False,
    disable_jinja_templating: bool = False,
    disallow_edits: bool = True,  # pylint: disable=unused-argument
    external_url_prefix: str = "",
    load_env: bool = False,
    split: bool = False,
) -> None:
    """
    Sync exported DBs/datasets/charts/dashboards to Superset.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)
    root = Path(directory)

    base_url = URL(external_url_prefix) if external_url_prefix else None

    # collecting existing database UUIDs so we know if we're creating or updating
    existing_databases = {str(uuid) for uuid in client.get_uuids("database").values()}

    # env for Jinja2 templating
    env = dict(pair.split("=", 1) for pair in option if "=" in pair)  # type: ignore
    env["instance"] = url
    env["functions"] = load_user_modules(root / "functions")
    env["raise"] = raise_helper
    if load_env:
        env["env"] = os.environ

    # read all the YAML files
    configs: Dict[Path, AssetConfig] = {}
    queue = [root]
    while queue:
        path_name = queue.pop()
        relative_path = path_name.relative_to(root)

        if path_name.is_dir() and not path_name.stem.startswith("."):
            queue.extend(path_name.glob("*"))
        elif is_yaml_config(relative_path):
            config = (
                load_yaml(path_name)
                if disable_jinja_templating
                else render_yaml(path_name, env)
            )

            overrides_path = path_name.with_suffix(".overrides" + path_name.suffix)
            if overrides_path.exists():
                overrides = (
                    load_yaml(overrides_path)
                    if disable_jinja_templating
                    else render_yaml(overrides_path, env)
                )
                dict_merge(config, overrides)

            config["is_managed_externally"] = disallow_edits
            if base_url:
                config["external_url"] = str(
                    base_url / str(relative_path),
                )
            if (
                relative_path.parts[0] == "databases"
                and config["uuid"] not in existing_databases
            ):
                prompt_for_passwords(relative_path, config)
                verify_db_connectivity(config)
            if relative_path.parts[0] == "datasets" and isinstance(
                config.get("params"),
                str,
            ):
                config["params"] = json.loads(config["params"])

            configs["bundle" / relative_path] = config

    if split:
        import_resources_individually(configs, client, overwrite)
    else:
        contents = {str(k): yaml.dump(v) for k, v in configs.items()}
        import_resources(contents, client, overwrite)


def import_resources_individually(
    configs: Dict[Path, AssetConfig],
    client: SupersetClient,
    overwrite: bool,
) -> None:
    """
    Import contents individually.

    This will first import all the databases, then import each dataset (together with the
    database info, since it's needed), then charts, on so on. It helps troubleshoot
    problematic exports and large imports.
    """
    # store progress in case the import stops midway
    checkpoint_path = Path("checkpoint.log")
    if not checkpoint_path.exists():
        checkpoint_path.touch()

    with open(checkpoint_path, "r+", encoding="utf-8") as log:
        imported = {Path(path.strip()) for path in log.readlines()}
        asset_configs: Dict[Path, AssetConfig]
        imports = [
            ("databases", lambda config: []),
            ("datasets", lambda config: [config["database_uuid"]]),
            ("charts", lambda config: [config["dataset_uuid"]]),
            ("dashboards", get_charts_uuids),
        ]
        related_configs: Dict[str, Dict[Path, AssetConfig]] = {}
        for resource_name, get_related_uuids in imports:
            for path, config in configs.items():
                if path.parts[1] != resource_name:
                    continue

                asset_configs = {path: config}
                for uuid in get_related_uuids(config):
                    asset_configs.update(related_configs[uuid])

                _logger.info("Importing %s", path.relative_to("bundle"))
                contents = {str(k): yaml.dump(v) for k, v in asset_configs.items()}
                if path not in imported:
                    import_resources(contents, client, overwrite)
                    imported.add(path)
                    log.write(str(path) + "\n")
                    log.flush()

                related_configs[config["uuid"]] = asset_configs

    os.unlink(checkpoint_path)


def get_charts_uuids(config: AssetConfig) -> Iterator[str]:
    """
    Extract chart UUID from a dashboard config.
    """
    for child in config["position"].values():
        if (
            isinstance(child, dict)
            and child["type"] == "CHART"
            and "uuid" in child["meta"]
        ):
            yield child["meta"]["uuid"]


def verify_db_connectivity(config: Dict[str, Any]) -> None:
    """
    Test if we can connect to a given database.
    """
    uri = make_url(config["sqlalchemy_uri"])
    if config.get("password"):
        uri = uri.set(password=config["password"])

    try:
        engine = create_engine(uri)
        raw_connection = engine.raw_connection()
        engine.dialect.do_ping(raw_connection)
    except Exception as ex:  # pylint: disable=broad-except
        _logger.warning("Cannot connect to database %s", repr(uri))
        _logger.debug(ex)


def prompt_for_passwords(path: Path, config: Dict[str, Any]) -> None:
    """
    Prompt user for masked passwords.

    Modify the config in place.
    """
    uri = config["sqlalchemy_uri"]
    password = make_url(uri).password
    if password == PASSWORD_MASK and config.get("password") is None:
        config["password"] = getpass.getpass(
            f"Please provide the password for {path}: ",
        )


@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.ConnectionError, requests.exceptions.Timeout),
    max_time=60,
    max_tries=5,
    logger=__name__,
)
def import_resources(
    contents: Dict[str, str],
    client: SupersetClient,
    overwrite: bool,
) -> None:
    """
    Import a bundle of assets.
    """
    contents["bundle/metadata.yaml"] = yaml.dump(
        dict(
            version="1.0.0",
            type="assets",
            timestamp=datetime.now(tz=timezone.utc).isoformat(),
        ),
    )

    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        for file_path, file_content in contents.items():
            with bundle.open(file_path, "w") as output:
                output.write(file_content.encode())
    buf.seek(0)
    try:
        client.import_zip("assets", buf, overwrite=overwrite)
    except SupersetError as ex:
        click.echo(
            click.style(
                "\n".join(error["message"] for error in ex.errors),
                fg="bright_red",
            ),
        )

        # check if overwrite is needed:
        existing = [
            key
            for error in ex.errors
            for key, value in error.get("extra", {}).items()
            if "overwrite=true" in value
        ]
        if not existing:
            raise ex

        existing_list = "\n".join("- " + name for name in existing)
        click.echo(
            click.style(
                (
                    "The following file(s) already exist. Pass --overwrite to "
                    f"replace them.\n{existing_list}"
                ),
                fg="bright_red",
            ),
        )
