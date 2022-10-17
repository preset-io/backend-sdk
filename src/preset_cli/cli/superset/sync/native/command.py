"""
A command to sync Superset exports into a Superset instance.
"""

import getpass
import importlib.util
import logging
import os
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Tuple
from zipfile import ZipFile

import click
import yaml
from jinja2 import Template
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import make_url
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.exceptions import SupersetError

_logger = logging.getLogger(__name__)

YAML_EXTENSIONS = {".yaml", ".yml"}
ASSET_DIRECTORIES = {"databases", "datasets", "charts", "dashboards"}

# This should be identical to ``superset.models.core.PASSWORD_MASK``. It's duplicated here
# because we don't want to have the CLI to depend on the ``superset`` package.
PASSWORD_MASK = "X" * 10


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
@click.pass_context
def native(  # pylint: disable=too-many-locals, too-many-arguments
    ctx: click.core.Context,
    directory: str,
    option: Tuple[str, ...],
    overwrite: bool = False,
    disallow_edits: bool = True,  # pylint: disable=unused-argument
    external_url_prefix: str = "",
    load_env: bool = False,
) -> None:
    """
    Sync exported DBs/datasets/charts/dashboards to Superset.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)
    root = Path(directory)

    base_url = URL(external_url_prefix) if external_url_prefix else None

    # env for Jinja2 templating
    env = dict(pair.split("=", 1) for pair in option if "=" in pair)  # type: ignore
    env["instance"] = url
    env["functions"] = load_user_modules(root / "functions")
    env["raise"] = raise_helper
    if load_env:
        env["env"] = os.environ

    # read all the YAML files
    contents: Dict[str, str] = {}
    queue = [root]
    while queue:
        path_name = queue.pop()
        relative_path = path_name.relative_to(root)
        if path_name.is_dir() and not path_name.stem.startswith("."):
            queue.extend(path_name.glob("*"))
        elif (
            path_name.suffix.lower() in YAML_EXTENSIONS
            and relative_path.parts[0] in ASSET_DIRECTORIES
        ):
            with open(path_name, encoding="utf-8") as input_:
                env["filepath"] = path_name
                template = Template(input_.read())
                content = template.render(**env)
                config = yaml.load(content, Loader=yaml.SafeLoader)

                config["is_managed_externally"] = disallow_edits
                if base_url:
                    config["external_url"] = str(
                        base_url / str(relative_path),
                    )
                if relative_path.parts[0] == "databases":
                    prompt_for_passwords(relative_path, config)
                    verify_db_connectivity(config)

                contents[str("bundle" / relative_path)] = yaml.safe_dump(config)

    # TODO (betodealmeida): use endpoint from https://github.com/apache/superset/pull/19220
    for resource in ["database", "dataset", "chart", "dashboard"]:
        import_resource(resource, contents, client, overwrite)


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
        _logger.warning("Cannot connect to database %s", uri)
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


def import_resource(
    resource: str,
    contents: Dict[str, str],
    client: SupersetClient,
    overwrite: bool,
) -> None:
    """
    Import a given resource.
    """
    contents["bundle/metadata.yaml"] = yaml.dump(
        dict(
            version="1.0.0",
            type=resource_types[resource],
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
        client.import_zip(resource, buf, overwrite=overwrite)
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
