"""
A command to sync Superset exports into a Superset instance.
"""

# pylint: disable=too-many-lines

from __future__ import annotations

import getpass
import importlib.util
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from enum import Enum
from io import BytesIO
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple
from zipfile import ZipFile

import backoff
import click
import requests
import yaml
from jinja2 import Template
from jinja2.exceptions import TemplateSyntaxError
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import make_url
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.lib import (
    LogType,
    clean_logs,
    get_logs,
    write_logs_to_file,
)
from preset_cli.exceptions import CLIError, SupersetError
from preset_cli.lib import dict_merge

_logger = logging.getLogger(__name__)

YAML_EXTENSIONS = {".yaml", ".yml"}
ASSET_DIRECTORIES = {"databases", "datasets", "charts", "dashboards"}
OVERRIDES_SUFFIX = ".overrides"

# This should be identical to ``superset.models.core.PASSWORD_MASK``. It's duplicated here
# because we don't want to have the CLI to depend on the ``superset`` package.
PASSWORD_MASK = "X" * 10

AssetConfig = Dict[str, Any]


class ResourceType(Enum):
    """
    ResourceType Enum. Used to identify asset type (and corresponding metadata).
    """

    def __new__(
        cls,
        resource_name: str,
        metadata_type: str | None = None,
    ) -> "ResourceType":
        """
        ResourceType Constructor.
        """
        obj = object.__new__(cls)
        obj._value_ = resource_name
        obj._resource_name = resource_name  # type:ignore
        obj._metadata_type = metadata_type  # type:ignore
        return obj

    @property
    def resource_name(self) -> str:
        """
        Return the resource name for the asset type.
        """
        return self._resource_name  # type: ignore

    @property
    def metadata_type(self) -> str:
        """
        Return the metadata type for the asset type.
        """
        return self._metadata_type  # type: ignore

    ASSET = ("assets", "assets")
    CHART = ("chart", "Slice")
    DASHBOARD = ("dashboard", "Dashboard")
    DATABASE = ("database", "Database")
    DATASET = ("dataset", "SqlaTable")


def normalize_to_enum(  # pylint: disable=unused-argument
    ctx: click.core.Context,
    param: str,
    value: str | None,
):
    """
    Normalize the ``--asset-type`` option value and return the
    corresponding ResourceType Enum.
    """
    if value is None:
        return ResourceType.ASSET
    return ResourceType(value.lower())


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
        asset_content = input_.read()

    try:
        template = Template(asset_content)

    # For charts with a `query_context` -> ``str(JSON)``, templating the YAML structure directly
    # was failing. The route str(YAML) -> dict -> str(JSON) is more consistent.
    except TemplateSyntaxError:
        content = yaml.load(asset_content, Loader=yaml.SafeLoader)
        content = json.dumps(content)
        template = Template(content)

    content = template.render(**env)
    return yaml.load(content, Loader=yaml.SafeLoader)


def _is_bundle_root(path: Path) -> bool:
    return path.is_dir() and any((path / name).is_dir() for name in ASSET_DIRECTORIES)


def _safe_extract_zip(zip_path: Path, output_dir: Path) -> None:
    root = output_dir.resolve()
    with ZipFile(zip_path) as bundle:
        for member in bundle.infolist():
            member_path = Path(member.filename)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise click.ClickException(
                    f"Unsafe ZIP path detected: {member.filename}",
                )

            destination = (output_dir / member_path).resolve()
            if os.path.commonpath([str(root), str(destination)]) != str(root):
                raise click.ClickException(
                    f"ZIP entry escapes target directory: {member.filename}",
                )

            if member.is_dir():
                destination.mkdir(parents=True, exist_ok=True)
                continue

            destination.parent.mkdir(parents=True, exist_ok=True)
            with bundle.open(member) as src, open(destination, "wb") as dst:
                dst.write(src.read())


def _resolve_input_root(path: Path) -> Tuple[Path, tempfile.TemporaryDirectory | None]:
    if path.is_dir():
        return path, None

    if not path.is_file() or path.suffix.lower() != ".zip":
        raise click.ClickException(
            "Input must be a directory or a .zip bundle.",
        )

    temp_dir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
    extract_root = Path(temp_dir.name)
    _safe_extract_zip(path, extract_root)

    candidates = [extract_root, extract_root / "bundle"]
    for child in extract_root.iterdir():
        if child.is_dir():
            candidates.extend([child, child / "bundle"])

    seen: Set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if _is_bundle_root(candidate):
            return candidate, temp_dir

    temp_dir.cleanup()
    raise click.ClickException(
        "ZIP input does not contain a valid assets bundle.",
    )


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
@click.option(
    "--continue-on-error",
    "-c",
    is_flag=True,
    default=False,
    help="Continue the import if an asset fails to import (imports assets individually)",
)
@click.option(
    "--cascade/--no-cascade",
    default=True,
    help="When disabled, import dependencies without overwriting them",
)
@click.option(
    "--asset-type",
    type=click.Choice([rt.resource_name for rt in ResourceType], case_sensitive=False),
    callback=normalize_to_enum,
    help=(
        "Specify an asset type to import resources using the type's endpoint. "
        "This way other asset types included get created but not overwritten."
    ),
)
@click.option(
    "--db-password",
    multiple=True,
    help="Password for DB connections being imported (eg, uuid1=my_db_password)",
)
@click.pass_context
def native(  # pylint: disable=too-many-locals, too-many-arguments, too-many-branches
    ctx: click.core.Context,
    directory: str,
    option: Tuple[str, ...],
    asset_type: ResourceType,
    overwrite: bool = False,
    disable_jinja_templating: bool = False,
    disallow_edits: bool = True,  # pylint: disable=unused-argument
    external_url_prefix: str = "",
    load_env: bool = False,
    split: bool = False,
    continue_on_error: bool = False,
    cascade: bool = True,
    db_password: Tuple[str, ...] | None = None,
) -> None:
    """
    Sync exported DBs/datasets/charts/dashboards to Superset.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)
    root, temp_dir = _resolve_input_root(Path(directory))
    base_url = URL(external_url_prefix) if external_url_prefix else None

    try:
        # The ``--continue-on-error`` and ``--no-cascade`` flags force split mode.
        split = split or continue_on_error or not cascade

        # collecting existing database UUIDs so we know if we're creating or updating
        # newer versions expose the DB UUID in the API response,
        # olders only expose it via export
        try:
            existing_databases = {
                db_connection["uuid"] for db_connection in client.get_databases()
            }
        except KeyError:
            existing_databases = {
                str(uuid) for uuid in client.get_uuids("database").values()
            }

        # env for Jinja2 templating
        env = dict(pair.split("=", 1) for pair in option if "=" in pair)  # type: ignore
        env["instance"] = url
        env["functions"] = load_user_modules(root / "functions")  # type: ignore
        env["raise"] = raise_helper  # type: ignore
        if load_env:
            env["env"] = os.environ  # type: ignore

        pwds = dict(kv.split("=", 1) for kv in db_password or [])

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
                if relative_path.parts[0] == "databases":
                    new_conn = config["uuid"] not in existing_databases
                    add_password_to_config(relative_path, config, pwds, new_conn)
                if relative_path.parts[0] == "datasets" and isinstance(
                    config.get("params"),
                    str,
                ):
                    config["params"] = json.loads(config["params"])

                configs["bundle" / relative_path] = config

        if split:
            import_resources_individually(
                configs,
                client,
                overwrite,
                asset_type,
                continue_on_error,
                cascade=cascade,
                existing_databases=existing_databases,
            )
        else:
            contents = {str(k): yaml.dump(v) for k, v in configs.items()}
            import_resources(contents, client, overwrite, asset_type)
    finally:
        if temp_dir:
            temp_dir.cleanup()


def import_resources_individually(  # pylint: disable=too-many-locals, too-many-arguments, too-many-branches, too-many-statements
    configs: Dict[Path, AssetConfig],
    client: SupersetClient,
    overwrite: bool,
    asset_type: ResourceType,
    continue_on_error: bool = False,
    cascade: bool = True,
    existing_databases: Set[str] | None = None,
) -> None:
    """
    Import contents individually.

    This will first import all the databases, then import each dataset (together with the
    database info, since it's needed), then charts, and so on. It helps troubleshoot
    problematic exports and large imports.

    By default, the import logs all assets imported correctly to a checkpoint file so that
    if one fails, a future import continues from where it's left. If ``continue_on_error``
    is set to True, then only failures are logged to the file, and the import continues.
    """
    imports = [
        ("databases", lambda config: []),
        ("datasets", lambda config: [config["database_uuid"]]),
        ("charts", lambda config: [config["dataset_uuid"]]),
        ("dashboards", get_dashboard_related_uuids),
    ]
    resource_type_map = {
        "databases": ResourceType.DATABASE,
        "datasets": ResourceType.DATASET,
        "charts": ResourceType.CHART,
        "dashboards": ResourceType.DASHBOARD,
    }
    dependency_resource_map = {
        "dashboards": {
            "charts": "chart",
            "datasets": "dataset",
            "databases": "database",
        },
        "datasets": {
            "databases": "database",
        },
    }
    asset_configs: Dict[Path, AssetConfig]
    related_configs: Dict[str, Dict[Path, AssetConfig]] = {}

    log_file_path, logs = get_logs(LogType.ASSETS)
    assets_to_skip = {Path(log["path"]) for log in logs[LogType.ASSETS]}
    existing_databases = existing_databases or set()
    existing_uuid_cache: Dict[Tuple[str, str], bool] = {}

    def _resource_exists(resource_name: str, config: AssetConfig) -> bool:
        uuid_value = config.get("uuid")
        if not uuid_value:
            return False
        cache_key = (resource_name, str(uuid_value))
        if cache_key not in existing_uuid_cache:
            existing_uuid_cache[cache_key] = bool(
                _resolve_uuid_to_id(
                    client,
                    resource_name,
                    uuid_value,
                ),
            )
        return existing_uuid_cache[cache_key]

    with open(log_file_path, "w", encoding="utf-8") as log_file:
        for resource_name, get_related_uuids in imports:
            for path, config in configs.items():
                if path.parts[1] != resource_name:
                    continue

                asset_configs = {path: config}
                _logger.debug("Processing %s for import", path.relative_to("bundle"))
                asset_log = {
                    "uuid": config["uuid"],
                    "path": str(path),
                    "status": "SUCCESS",
                }
                try:
                    for uuid in get_related_uuids(config):
                        if uuid in related_configs:
                            asset_configs.update(related_configs[uuid])

                    skip_database_import = (
                        resource_name == "databases"
                        and asset_type != ResourceType.DATABASE
                        and config["uuid"] in existing_databases
                    )
                    if resource_name == "databases":
                        # Always keep database configs so dependent assets (datasets/charts)
                        # can include the database YAML even when the DB already exists.
                        related_configs[config["uuid"]] = asset_configs
                    else:
                        related_configs[config["uuid"]] = asset_configs

                    is_primary = asset_type == ResourceType.ASSET or (
                        asset_type.resource_name in resource_name
                    )
                    if path in assets_to_skip:
                        continue
                    if skip_database_import:
                        _logger.info(
                            "Skipping database import for existing database %s",
                            path.relative_to("bundle"),
                        )
                        continue

                    if not cascade and not is_primary:
                        resource_type = resource_type_map[resource_name].resource_name
                        if _resource_exists(resource_type, config):
                            _logger.info(
                                "Skipping existing %s %s (cascade disabled)",
                                resource_name[:-1],
                                config.get("uuid"),
                            )
                            continue

                    if (
                        not cascade
                        and is_primary
                        # Keep DB config in primary dataset no-cascade imports.
                        # Pruning it can skip dataset updates.
                        and resource_name != "datasets"
                    ):
                        _prune_existing_dependency_configs(
                            asset_configs,
                            path,
                            resource_name,
                            dependency_resource_map,
                            _resource_exists,
                        )

                    _logger.info("Importing %s", path.relative_to("bundle"))

                    if (
                        resource_name == "charts"
                        and asset_type == ResourceType.CHART
                        and not cascade
                    ):
                        _update_chart_no_cascade(
                            path,
                            config,
                            configs,
                            client,
                            overwrite,
                        )
                        continue

                    if (
                        resource_name == "dashboards"
                        and asset_type == ResourceType.DASHBOARD
                        and not cascade
                    ):
                        _update_dashboard_no_cascade(
                            path,
                            config,
                            configs,
                            client,
                            overwrite,
                        )
                        continue

                    contents = {str(k): yaml.dump(v) for k, v in asset_configs.items()}
                    effective_overwrite = (
                        overwrite if (cascade or is_primary) else False
                    )
                    if (
                        resource_name == "databases"
                        and asset_type != ResourceType.DATABASE
                    ):
                        effective_overwrite = False
                    import_resources(
                        contents,
                        client,
                        effective_overwrite,
                        resource_type_map[resource_name],
                    )
                except Exception:  # pylint: disable=broad-except
                    if not continue_on_error:
                        raise
                    asset_log["status"] = "FAILED"

                logs[LogType.ASSETS].append(asset_log)
                assets_to_skip.add(path)
                write_logs_to_file(log_file, logs)

    if not continue_on_error or not any(
        log["status"] == "FAILED" for log in logs[LogType.ASSETS]
    ):
        clean_logs(LogType.ASSETS, logs)


def get_dashboard_related_uuids(config: AssetConfig) -> Iterator[str]:
    """
    Extract dataset and chart UUID from a dashboard config.
    """
    for uuid in get_charts_uuids(config):
        yield uuid
    for uuid in get_dataset_filter_uuids(config):
        yield uuid


def _prune_existing_dependency_configs(
    asset_configs: Dict[Path, AssetConfig],
    primary_path: Path,
    resource_name: str,
    dependency_resource_map: Dict[str, Dict[str, str]],
    resource_exists: Callable[[str, AssetConfig], bool],
) -> None:
    """
    Remove dependency configs that already exist when cascade is disabled.
    """
    dependency_map = dependency_resource_map.get(resource_name)
    if not dependency_map:
        return

    for dep_path, dep_config in list(asset_configs.items()):
        if dep_path == primary_path or len(dep_path.parts) < 2:
            continue

        dep_resource = dependency_map.get(dep_path.parts[1])
        if dep_resource and resource_exists(dep_resource, dep_config):
            asset_configs.pop(dep_path, None)


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


def get_dataset_filter_uuids(config: AssetConfig) -> Set[str]:
    """
    Extract dataset UUID for datasets that are used in dashboard filters.
    """
    dataset_uuids = set()
    for filter_config in config["metadata"].get("native_filter_configuration", []):
        for target in filter_config.get("targets", {}):
            if uuid := target.get("datasetUuid"):
                if uuid not in dataset_uuids:
                    dataset_uuids.add(uuid)
    return dataset_uuids


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


def add_password_to_config(
    path: Path,
    config: Dict[str, Any],
    pwds: Dict[str, Any],
    new_conn: bool,
) -> None:
    """
    Add password passed in the command to the config.

    Prompt user for masked passwords for new connections if not provided. Modify
    the config in place.
    """
    uri = config["sqlalchemy_uri"]
    password = make_url(uri).password

    if config["uuid"] in pwds:
        config["password"] = pwds[config["uuid"]]
        verify_db_connectivity(config)
    elif password != PASSWORD_MASK or config.get("password"):
        verify_db_connectivity(config)
    elif new_conn:
        config["password"] = getpass.getpass(
            f"Please provide the password for {path}: ",
        )
        verify_db_connectivity(config)


def _resolve_uuid_to_id(
    client: SupersetClient,
    resource_name: str,
    uuid_value: Any,
    resources: List[Dict[str, Any]] | None = None,
) -> Optional[int]:
    """
    Resolve a UUID to a resource ID using the API, with fallbacks for older versions.
    """
    if not uuid_value:
        return None

    uuid_str = str(uuid_value)
    if resources is None:
        resources = client.get_resources(resource_name)
    for resource in resources:
        if str(resource.get("uuid")) == uuid_str:
            return resource["id"]

    # fallback for older versions that don't expose UUIDs in the API
    ids = {resource["id"] for resource in resources}
    if not ids:
        return None

    uuid_map = client.get_uuids(resource_name, ids)
    for resource_id, resource_uuid in uuid_map.items():
        if str(resource_uuid) == uuid_str:
            return resource_id

    return None


def _find_config_by_uuid(
    configs: Dict[Path, AssetConfig],
    resource_dir: str,
    uuid_value: Any,
) -> Optional[Tuple[Path, AssetConfig]]:
    """
    Find a config by UUID in the given resource directory.
    """
    if not uuid_value:
        return None
    uuid_str = str(uuid_value)
    for path, config in configs.items():
        if len(path.parts) > 1 and path.parts[1] == resource_dir:
            if str(config.get("uuid")) == uuid_str:
                return path, config
    return None


def _build_dataset_contents(
    configs: Dict[Path, AssetConfig],
    dataset_uuid: Any,
) -> Optional[Dict[str, str]]:
    """
    Build a minimal bundle for a dataset (dataset + database).
    """
    dataset_entry = _find_config_by_uuid(configs, "datasets", dataset_uuid)
    if not dataset_entry:
        return None
    dataset_path, dataset_config = dataset_entry
    database_uuid = dataset_config.get("database_uuid")
    database_entry = _find_config_by_uuid(configs, "databases", database_uuid)
    if not database_entry:
        return None
    database_path, database_config = database_entry

    return {
        str(dataset_path): yaml.dump(dataset_config),
        str(database_path): yaml.dump(database_config),
    }


def _build_chart_contents(
    configs: Dict[Path, AssetConfig],
    chart_uuid: Any,
) -> Optional[Dict[str, str]]:
    """
    Build a minimal bundle for a chart (chart + dataset + database).
    """
    chart_entry = _find_config_by_uuid(configs, "charts", chart_uuid)
    if not chart_entry:
        return None
    chart_path, chart_config = chart_entry

    dataset_uuid = chart_config.get("dataset_uuid")
    dataset_contents = _build_dataset_contents(configs, dataset_uuid)
    if not dataset_contents:
        return None

    contents = {str(chart_path): yaml.dump(chart_config)}
    contents.update(dataset_contents)
    return contents


def _build_dashboard_contents(
    configs: Dict[Path, AssetConfig],
    dashboard_uuid: Any,
    client: SupersetClient | None = None,
    missing_only: bool = False,
) -> Optional[Dict[str, str]]:
    """
    Build a minimal bundle for a dashboard and its dependencies.
    """
    dashboard_entry = _find_config_by_uuid(configs, "dashboards", dashboard_uuid)
    if not dashboard_entry:
        return None
    dashboard_path, dashboard_config = dashboard_entry

    def should_include(resource_name: str, uuid_value: Any) -> bool:
        if not missing_only:
            return True
        if client is None:
            return True
        return _resolve_uuid_to_id(client, resource_name, uuid_value) is None

    def add_dataset(dataset_uuid: Any, output: Dict[str, str]) -> None:
        dataset_entry = _find_config_by_uuid(configs, "datasets", dataset_uuid)
        if not dataset_entry:
            return
        dataset_path, dataset_config = dataset_entry
        if should_include("dataset", dataset_uuid):
            output[str(dataset_path)] = yaml.dump(dataset_config)

        database_uuid = dataset_config.get("database_uuid")
        database_entry = _find_config_by_uuid(configs, "databases", database_uuid)
        if not database_entry:
            return
        database_path, database_config = database_entry
        if should_include("database", database_uuid):
            output[str(database_path)] = yaml.dump(database_config)

    contents: Dict[str, str] = {
        str(dashboard_path): yaml.dump(dashboard_config),
    }

    for chart_uuid in get_charts_uuids(dashboard_config):
        chart_entry = _find_config_by_uuid(configs, "charts", chart_uuid)
        if not chart_entry:
            continue
        chart_path, chart_config = chart_entry
        if should_include("chart", chart_uuid):
            contents[str(chart_path)] = yaml.dump(chart_config)
        add_dataset(chart_config.get("dataset_uuid"), contents)

    for dataset_uuid in get_dataset_filter_uuids(dashboard_config):
        add_dataset(dataset_uuid, contents)

    return contents


def _safe_json_loads(value: Any, label: str) -> Optional[Dict[str, Any]]:
    """
    Safely parse JSON content that may be a dict or string.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            _logger.warning("Unable to parse %s as JSON", label)
            return None
    return None


def _update_chart_datasource_refs(
    params: Optional[Dict[str, Any]],
    query_context: Optional[Dict[str, Any]],
    datasource_id: int,
    datasource_type: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Update datasource references in chart params/query_context.
    """
    dataset_uid = f"{datasource_id}__{datasource_type}"

    if params is not None:
        params["datasource"] = dataset_uid

    if query_context is not None:
        query_context["datasource"] = {"id": datasource_id, "type": datasource_type}
        form_data = query_context.get("form_data")
        if isinstance(form_data, dict):
            form_data["datasource"] = dataset_uid
        queries = query_context.get("queries")
        if isinstance(queries, list):
            for query in queries:
                if isinstance(query, dict) and "datasource" in query:
                    query["datasource"] = query_context["datasource"]

    return params, query_context


def _filter_payload_to_schema(
    client: SupersetClient,
    resource_name: str,
    payload: Dict[str, Any],
    fallback_allowed: Set[str],
) -> Dict[str, Any]:
    """
    Filter payload keys based on API schema info when available.
    """
    try:
        info = client.get_resource_endpoint_info(resource_name, keys=["edit_columns"])
        allowed = {column["name"] for column in info.get("edit_columns", [])}
        if allowed:
            return {key: value for key, value in payload.items() if key in allowed}
    except SupersetError:
        pass

    return {key: value for key, value in payload.items() if key in fallback_allowed}


def _prepare_chart_update_payload(  # pylint: disable=too-many-branches
    config: AssetConfig,
    datasource_id: int,
    datasource_type: str,
    client: SupersetClient,
) -> Dict[str, Any]:
    """
    Build a chart update payload from export config.
    """
    payload: Dict[str, Any] = {}

    for key in [
        "slice_name",
        "description",
        "viz_type",
        "cache_timeout",
        "certified_by",
        "certification_details",
        "is_managed_externally",
        "external_url",
    ]:
        if key in config:
            payload[key] = config[key]

    owners = config.get("owners")
    if owners:
        if all(isinstance(owner, int) for owner in owners):
            payload["owners"] = owners
        else:
            _logger.warning("Skipping owners update for chart; owner IDs not available")

    tags = config.get("tags")
    if tags:
        if all(isinstance(tag, int) for tag in tags):
            payload["tags"] = tags
        else:
            _logger.warning("Skipping tags update for chart; tag IDs not available")

    params_value = config.get("params")
    params = _safe_json_loads(params_value, "chart params")

    query_context_value = config.get("query_context")
    query_context = _safe_json_loads(query_context_value, "chart query_context")

    if params is not None or query_context is not None:
        params, query_context = _update_chart_datasource_refs(
            params,
            query_context,
            datasource_id,
            datasource_type,
        )

    if params is not None:
        payload["params"] = json.dumps(params)
    elif isinstance(params_value, str):
        payload["params"] = params_value

    if query_context is not None:
        payload["query_context"] = json.dumps(query_context)
    elif isinstance(query_context_value, str):
        payload["query_context"] = query_context_value

    payload["datasource_id"] = datasource_id
    payload["datasource_type"] = datasource_type

    safe_allowlist = {
        "slice_name",
        "description",
        "viz_type",
        "owners",
        "params",
        "query_context",
        "cache_timeout",
        "datasource_id",
        "datasource_type",
        "dashboards",
        "certified_by",
        "certification_details",
        "is_managed_externally",
        "external_url",
        "tags",
    }
    return _filter_payload_to_schema(client, "chart", payload, safe_allowlist)


def _set_integer_list_payload_field(
    payload: Dict[str, Any],
    config: AssetConfig,
    field_name: str,
    warning_message: str,
) -> None:
    """
    Add a list-valued payload field only when all values are integer IDs.
    """
    values = config.get(field_name)
    if not values:
        return
    if all(isinstance(value, int) for value in values):
        payload[field_name] = values
    else:
        _logger.warning(warning_message)


def _set_json_payload_field(
    payload: Dict[str, Any],
    config: AssetConfig,
    preferred_field: str,
    fallback_field: str,
    payload_field: str,
) -> None:
    """
    Add a JSON payload field with support for dict and serialized string values.
    """
    preferred_value = config.get(preferred_field)
    fallback_value = config.get(fallback_field)
    if isinstance(preferred_value, dict):
        payload[payload_field] = json.dumps(preferred_value)
    elif isinstance(fallback_value, dict):
        payload[payload_field] = json.dumps(fallback_value)
    elif isinstance(fallback_value, str):
        payload[payload_field] = fallback_value


def _prepare_dashboard_update_payload(
    config: AssetConfig,
    client: SupersetClient,
) -> Dict[str, Any]:
    """
    Build a dashboard update payload from export config.
    """
    payload: Dict[str, Any] = {}

    for key in [
        "dashboard_title",
        "slug",
        "css",
        "published",
        "certified_by",
        "certification_details",
        "is_managed_externally",
        "external_url",
    ]:
        if key in config:
            payload[key] = config[key]

    _set_integer_list_payload_field(
        payload,
        config,
        "owners",
        "Skipping owners update for dashboard; owner IDs not available",
    )
    _set_integer_list_payload_field(
        payload,
        config,
        "roles",
        "Skipping roles update for dashboard; role IDs not available",
    )
    _set_json_payload_field(
        payload,
        config,
        preferred_field="position",
        fallback_field="position_json",
        payload_field="position_json",
    )
    _set_json_payload_field(
        payload,
        config,
        preferred_field="metadata",
        fallback_field="json_metadata",
        payload_field="json_metadata",
    )

    safe_allowlist = {
        "dashboard_title",
        "slug",
        "owners",
        "roles",
        "position_json",
        "css",
        "json_metadata",
        "published",
        "certified_by",
        "certification_details",
        "is_managed_externally",
        "external_url",
    }
    return _filter_payload_to_schema(client, "dashboard", payload, safe_allowlist)


def _update_chart_no_cascade(
    path: Path,
    config: AssetConfig,
    configs: Dict[Path, AssetConfig],
    client: SupersetClient,
    overwrite: bool,
) -> None:
    """
    Update a chart via API without cascading to dependencies.
    """
    chart_uuid = config.get("uuid")
    if not chart_uuid:
        raise CLIError(f"Chart config missing UUID: {path}", 1)

    chart_id = _resolve_uuid_to_id(client, "chart", chart_uuid)
    if chart_id is None:
        _logger.info("Chart %s not found; attempting to create it", chart_uuid)
        contents = _build_chart_contents(configs, chart_uuid)
        if not contents:
            raise CLIError(
                f"Chart {chart_uuid} not found and no dataset/database configs available.",
                1,
            )
        import_resources(
            contents,
            client,
            overwrite=False,
            asset_type=ResourceType.CHART,
        )
        chart_id = _resolve_uuid_to_id(client, "chart", chart_uuid)
        if chart_id is None:
            raise CLIError(f"Unable to create chart {chart_uuid}", 1)
        if not overwrite:
            _logger.info("Chart created; skipping update because overwrite=false")
            return

    if not overwrite:
        _logger.info("Skipping existing chart %s (overwrite=false)", chart_uuid)
        return

    dataset_uuid = config.get("dataset_uuid")
    if not dataset_uuid:
        raise CLIError(f"Chart {chart_uuid} missing dataset_uuid", 1)

    dataset_id = _resolve_uuid_to_id(client, "dataset", dataset_uuid)
    if dataset_id is None:
        _logger.info("Dataset %s not found; attempting to create it", dataset_uuid)
        contents = _build_dataset_contents(configs, dataset_uuid)
        if not contents:
            raise CLIError(
                f"Dataset {dataset_uuid} not found and no dataset/database configs available.",
                1,
            )
        import_resources(
            contents,
            client,
            overwrite=False,
            asset_type=ResourceType.DATASET,
        )
        dataset_id = _resolve_uuid_to_id(client, "dataset", dataset_uuid)
        if dataset_id is None:
            raise CLIError(f"Unable to create dataset {dataset_uuid}", 1)

    dataset = client.get_dataset(dataset_id)
    datasource_type = dataset.get("kind") or dataset.get("datasource_type") or "table"

    payload = _prepare_chart_update_payload(
        config,
        dataset_id,
        datasource_type,
        client,
    )
    _logger.info("Updating chart %s via API (no-cascade)", chart_uuid)
    client.update_chart(chart_id, **payload)


def _update_dashboard_no_cascade(
    path: Path,
    config: AssetConfig,
    configs: Dict[Path, AssetConfig],
    client: SupersetClient,
    overwrite: bool,
) -> None:
    """
    Update a dashboard via API without cascading to dependencies.
    """
    dashboard_uuid = config.get("uuid")
    if not dashboard_uuid:
        raise CLIError(f"Dashboard config missing UUID: {path}", 1)

    dashboard_id = _resolve_uuid_to_id(client, "dashboard", dashboard_uuid)
    if dashboard_id is None:
        _logger.info(
            "Dashboard %s not found; attempting to create it",
            dashboard_uuid,
        )
        contents = _build_dashboard_contents(
            configs,
            dashboard_uuid,
            client=client,
            missing_only=True,
        )
        if not contents:
            raise CLIError(
                "Dashboard not found and no dashboard config available for import.",
                1,
            )
        import_resources(
            contents,
            client,
            overwrite=False,
            asset_type=ResourceType.DASHBOARD,
        )
        dashboard_id = _resolve_uuid_to_id(client, "dashboard", dashboard_uuid)
        if dashboard_id is None:
            raise CLIError(f"Unable to create dashboard {dashboard_uuid}", 1)
        if not overwrite:
            _logger.info("Dashboard created; skipping update because overwrite=false")
            return

    if not overwrite:
        _logger.info("Skipping existing dashboard %s (overwrite=false)", dashboard_uuid)
        return

    payload = _prepare_dashboard_update_payload(config, client)
    _logger.info("Updating dashboard %s via API (no-cascade)", dashboard_uuid)
    client.update_dashboard(dashboard_id, **payload)


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
    asset_type: ResourceType,
) -> None:
    """
    Import a bundle of assets.
    """
    contents["bundle/metadata.yaml"] = yaml.dump(
        dict(
            version="1.0.0",
            type=asset_type.metadata_type,
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
        client.import_zip(asset_type.resource_name, buf, overwrite=overwrite)
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
                    "The following file(s) already exist. Pass ``--overwrite`` to "
                    f"replace them.\n{existing_list}"
                ),
                fg="bright_red",
            ),
        )
