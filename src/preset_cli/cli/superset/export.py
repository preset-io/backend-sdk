"""
A command to export Superset resources into a directory.
"""

import json
import re
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union
from zipfile import ZipFile

import click
import yaml
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.lib import (
    DASHBOARD_FILTER_KEYS,
    fetch_with_filter_fallback,
    parse_filters,
)
from preset_cli.lib import remove_root, split_comma

JINJA2_OPEN_MARKER = "__JINJA2_OPEN__"
JINJA2_CLOSE_MARKER = "__JINJA2_CLOSE__"
assert JINJA2_OPEN_MARKER != JINJA2_CLOSE_MARKER


def get_newline_char(force_unix_eol: bool = False) -> Union[str, None]:
    """Returns the newline character used by the open function"""
    return "\n" if force_unix_eol else None


def extract_uuid_from_asset(
    file_path: Optional[Path] = None,
    file_content: Optional[str] = None,
) -> Optional[str]:
    """
    Load YAML file and extract its UUID.
    """
    if file_path:
        with open(file_path, "r", encoding="utf-8") as content:
            file_content = content.read()

    if not file_content:
        return None

    data = yaml.load(file_content, Loader=yaml.SafeLoader)
    return data.get("uuid")


def simplify_filename(file_name: str) -> str:
    """
    Remove trailing numeric ID suffix from YAML filenames.
    """
    return re.sub(r"_\d+(?=\.ya?ml$)", "", file_name)


def _unique_simple_name(
    parent: Path,
    file_name: str,
    registry: Dict[Path, Set[str]],
) -> str:
    """
    Return a unique file name within the given directory.
    """
    used = registry.setdefault(parent, set())
    if file_name not in used:
        used.add(file_name)
        return file_name

    base, ext = (file_name.rsplit(".", 1) + [""])[:2]
    ext = f".{ext}" if ext else ""
    index = 2
    while True:
        candidate = f"{base}_{index}{ext}"
        if candidate not in used:
            used.add(candidate)
            return candidate
        index += 1


def apply_simple_filename(
    file_name: str,
    root: Path,
    registry: Dict[Path, Set[str]],
) -> str:
    """
    Apply simple filename rules and handle collisions.
    """
    path = Path(file_name)
    simple_name = simplify_filename(path.name)
    parent = root / path.parent
    unique_name = _unique_simple_name(parent, simple_name, registry)
    return str(path.parent / unique_name) if path.parent != Path(".") else unique_name


def _load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as input_:
        return yaml.load(input_, Loader=yaml.SafeLoader) or {}


def _get_dashboard_chart_uuids(config: Dict[str, Any]) -> Iterable[str]:
    for child in config.get("position", {}).values():
        if (
            isinstance(child, dict)
            and child.get("type") == "CHART"
            and "uuid" in child.get("meta", {})
        ):
            yield child["meta"]["uuid"]


def _get_dashboard_dataset_filter_uuids(config: Dict[str, Any]) -> Set[str]:
    dataset_uuids = set()
    for filter_config in config.get("metadata", {}).get(
        "native_filter_configuration",
        [],
    ):
        for target in filter_config.get("targets", {}):
            if uuid := target.get("datasetUuid"):
                dataset_uuids.add(uuid)
    return dataset_uuids


def _build_resource_uuid_map(
    resource_dir: Path,
    pattern: str,
) -> Dict[str, Path]:
    """
    Build UUID -> file map for resources under a directory.
    """
    resource_map: Dict[str, Path] = {}
    if not resource_dir.exists():
        return resource_map
    for resource_file in resource_dir.glob(pattern):
        if uuid := extract_uuid_from_asset(file_path=resource_file):
            resource_map[uuid] = resource_file
    return resource_map


def _copy_asset_relative_to_root(
    source: Path,
    root: Path,
    dashboard_dir: Path,
) -> None:
    """
    Copy an asset to the dashboard folder preserving export-relative path.
    """
    target = dashboard_dir / source.relative_to(root)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")


def _copy_chart_dependencies(
    root: Path,
    dashboard_dir: Path,
    chart_uuids: Set[str],
    chart_map: Dict[str, Path],
) -> Set[str]:
    """
    Copy chart files and return dataset UUIDs referenced by those charts.
    """
    dataset_uuids: Set[str] = set()
    for chart_uuid in chart_uuids:
        chart_path = chart_map.get(chart_uuid)
        if not chart_path:
            continue
        chart_config = _load_yaml(chart_path)
        if dataset_uuid := chart_config.get("dataset_uuid"):
            dataset_uuids.add(dataset_uuid)
        _copy_asset_relative_to_root(chart_path, root, dashboard_dir)
    return dataset_uuids


def _copy_dataset_dependencies(
    root: Path,
    dashboard_dir: Path,
    dataset_uuids: Set[str],
    dataset_map: Dict[str, Path],
) -> Set[str]:
    """
    Copy dataset files and return database UUIDs referenced by those datasets.
    """
    database_uuids: Set[str] = set()
    for dataset_uuid in dataset_uuids:
        dataset_path = dataset_map.get(dataset_uuid)
        if not dataset_path:
            continue
        dataset_config = _load_yaml(dataset_path)
        if database_uuid := dataset_config.get("database_uuid"):
            database_uuids.add(database_uuid)
        _copy_asset_relative_to_root(dataset_path, root, dashboard_dir)
    return database_uuids


def _copy_database_dependencies(
    root: Path,
    dashboard_dir: Path,
    database_uuids: Set[str],
    database_map: Dict[str, Path],
) -> None:
    """
    Copy database files to the dashboard folder.
    """
    for database_uuid in database_uuids:
        database_path = database_map.get(database_uuid)
        if not database_path:
            continue
        _copy_asset_relative_to_root(database_path, root, dashboard_dir)


def _cleanup_resource_directory(resource_dir: Path) -> None:
    """
    Remove all files/subdirs under a resource directory and then the directory itself.
    """
    if not resource_dir.exists():
        return
    for path in sorted(resource_dir.rglob("*"), reverse=True):
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            path.rmdir()
    resource_dir.rmdir()


def _restructure_single_dashboard(
    root: Path,
    dashboard_file: Path,
    chart_map: Dict[str, Path],
    dataset_map: Dict[str, Path],
    database_map: Dict[str, Path],
) -> None:
    """
    Move one dashboard and copy its dependencies into a dedicated folder.
    """
    config = _load_yaml(dashboard_file)
    dashboard_dir = dashboard_file.parent / dashboard_file.stem
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    dashboard_file.rename(dashboard_dir / "dashboard.yaml")

    chart_uuids = set(_get_dashboard_chart_uuids(config))
    dataset_uuids = _get_dashboard_dataset_filter_uuids(config)
    dataset_uuids.update(
        _copy_chart_dependencies(root, dashboard_dir, chart_uuids, chart_map),
    )
    database_uuids = _copy_dataset_dependencies(
        root,
        dashboard_dir,
        dataset_uuids,
        dataset_map,
    )
    _copy_database_dependencies(root, dashboard_dir, database_uuids, database_map)


def restructure_per_asset_folder(root: Path) -> None:
    """
    Reorganize flat export into per-dashboard subfolders.
    """
    dashboards_dir = root / "dashboards"
    if not dashboards_dir.exists():
        return

    charts_dir = root / "charts"
    datasets_dir = root / "datasets"
    databases_dir = root / "databases"
    chart_map = _build_resource_uuid_map(charts_dir, "*.yaml")
    dataset_map = _build_resource_uuid_map(datasets_dir, "**/*.yaml")
    database_map = _build_resource_uuid_map(databases_dir, "*.yaml")

    for dashboard_file in dashboards_dir.glob("*.yaml"):
        _restructure_single_dashboard(
            root,
            dashboard_file,
            chart_map,
            dataset_map,
            database_map,
        )

    for resource_dir in [charts_dir, datasets_dir, databases_dir]:
        _cleanup_resource_directory(resource_dir)


RESOURCE_NAMES = ("database", "dataset", "chart", "dashboard")


def _validate_export_destination_args(
    directory: Optional[str],
    output_zip: Optional[str],
) -> None:
    if directory and output_zip:
        raise click.UsageError(
            "Provide either a directory or --output-zip, not both.",
        )
    if not directory and not output_zip:
        raise click.UsageError(
            "Provide a directory or --output-zip.",
        )


def _build_requested_ids(
    database_ids: List[str],
    dataset_ids: List[str],
    chart_ids: List[str],
    dashboard_ids: List[str],
) -> Tuple[Dict[str, Set[int]], bool]:
    ids = {
        "database": {int(id_) for id_ in database_ids},
        "dataset": {int(id_) for id_ in dataset_ids},
        "chart": {int(id_) for id_ in chart_ids},
        "dashboard": {int(id_) for id_ in dashboard_ids},
    }
    ids_requested = any([database_ids, dataset_ids, chart_ids, dashboard_ids])
    return ids, ids_requested


@dataclass
class _DashboardFilterResult:
    dashboard_ids: Set[int]
    asset_types: Set[str]
    ids_requested: bool


@dataclass
class _ExportSelection:
    asset_types: Set[str]
    ids: Dict[str, Set[int]]
    ids_requested: bool
    overwrite: bool
    disable_jinja_escaping: bool
    force_unix_eol: bool
    simple_file_names: bool


def _apply_dashboard_filters(
    client: SupersetClient,
    asset_types: Set[str],
    ids_requested: bool,
    filters: Tuple[str, ...],
) -> Optional[_DashboardFilterResult]:
    if not filters:
        return None
    if asset_types and asset_types != {"dashboard"}:
        raise click.UsageError(
            "Filters are only supported for dashboard assets.",
        )
    parsed_filters = parse_filters(filters, DASHBOARD_FILTER_KEYS)
    dashboards = fetch_with_filter_fallback(
        client.get_dashboards,
        client.get_dashboards,
        parsed_filters,
        "dashboards",
    )
    return _DashboardFilterResult(
        dashboard_ids={dashboard["id"] for dashboard in dashboards},
        asset_types={"dashboard"},
        ids_requested=True if dashboards else ids_requested,
    )


def _resolve_directory_root(directory: str) -> Path:
    root = Path(directory)
    if not root.exists():
        raise click.UsageError(f"Directory does not exist: {root}")
    return root


def _export_selected_resources(
    root: Path,
    client: SupersetClient,
    selection: _ExportSelection,
    simple_name_registry: Dict[Path, Set[str]],
) -> None:
    for resource_name in RESOURCE_NAMES:
        if (not selection.asset_types or resource_name in selection.asset_types) and (
            selection.ids[resource_name] or not selection.ids_requested
        ):
            export_resource(
                resource_name,
                selection.ids[resource_name],
                root,
                client,
                selection.overwrite,
                selection.disable_jinja_escaping,
                skip_related=not selection.ids_requested,
                force_unix_eol=selection.force_unix_eol,
                simple_file_names=selection.simple_file_names,
                simple_name_registry=simple_name_registry,
            )


def _finalize_per_asset_folder_export(root: Path, per_asset_folder: bool) -> None:
    if per_asset_folder:
        restructure_per_asset_folder(root)


def zip_directory(source_dir: Path, output_path: Path) -> None:
    """
    Create a ZIP file from a directory tree.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output_path, "w") as bundle:
        for file_path in sorted(source_dir.rglob("*")):
            if file_path.is_file():
                bundle.write(file_path, file_path.relative_to(source_dir))


def build_local_uuid_mapping(root: Path) -> Dict[str, Dict[str, Path]]:
    """
    Build a mapping of UUIDs to file paths for existing resources in the
    target directory.
    """
    uuid_mapping: Dict[str, Dict[str, Path]] = {
        "dashboards": {},
        "charts": {},
        "datasets": {},
        "databases": {},
    }

    for resource_type, resource_map in uuid_mapping.items():
        resource_dir = root / resource_type
        if not resource_dir.exists():
            continue

        # For datasets, we need to handle subdirectories (database connections)
        if resource_type == "datasets":
            for db_dir in resource_dir.iterdir():
                for yaml_file in db_dir.glob("*.yaml"):
                    uuid = extract_uuid_from_asset(file_path=yaml_file)
                    if uuid:
                        resource_map[uuid] = yaml_file
        else:
            for yaml_file in resource_dir.glob("*.yaml"):
                uuid = extract_uuid_from_asset(file_path=yaml_file)
                if uuid:
                    resource_map[uuid] = yaml_file

    return uuid_mapping


def check_asset_uniqueness(  # pylint: disable=too-many-arguments
    overwrite: bool,
    file_content: str,
    file_name: str,
    file_path: Path,
    uuid_mapping: Dict[str, Dict[str, Path]],
    files_to_delete: List[Path],
) -> None:
    """
    Check if the asset is new to the directory or not.
    """
    # First validate by file name
    if file_path.exists():
        if not overwrite:
            raise Exception(
                f"File already exists and ``--overwrite`` was not specified: {file_path}",
            )
        return

    # Alternatively, validate by UUID
    incoming_uuid = extract_uuid_from_asset(file_content=file_content)
    if not incoming_uuid:
        return

    resource_type = None
    if file_name.startswith("dashboards/"):
        resource_type = "dashboards"
    elif file_name.startswith("charts/"):
        resource_type = "charts"
    elif file_name.startswith("datasets/"):
        resource_type = "datasets"
    elif file_name.startswith("databases/"):
        resource_type = "databases"

    if (
        resource_type
        and resource_type in uuid_mapping
        and (existing_file := uuid_mapping[resource_type].get(incoming_uuid))
    ):
        if not overwrite:
            raise Exception(
                f"Resource with UUID {incoming_uuid} already exists at {existing_file}. "
                f"Use --overwrite flag to replace it with {file_path}",
            )

        # Add the existing file to deletion queue
        files_to_delete.append(existing_file)
        del uuid_mapping[resource_type][incoming_uuid]


@click.command()
@click.argument(
    "directory",
    type=click.Path(resolve_path=True),
    required=False,
    default=None,
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing resources",
)
@click.option(
    "--disable-jinja-escaping",
    is_flag=True,
    default=False,
    help="Disable Jinja template escaping",
)
@click.option(
    "--force-unix-eol",
    is_flag=True,
    default=False,
    help="Force Unix end-of-line characters, otherwise use system default",
)
@click.option(
    "--asset-type",
    help="Asset type",
    multiple=True,
)
@click.option(
    "--database-ids",
    callback=split_comma,
    help="Comma separated list of database IDs to export",
)
@click.option(
    "--dataset-ids",
    callback=split_comma,
    help="Comma separated list of dataset IDs to export",
)
@click.option(
    "--chart-ids",
    callback=split_comma,
    help="Comma separated list of chart IDs to export",
)
@click.option(
    "--dashboard-ids",
    callback=split_comma,
    help="Comma separated list of dashboard IDs to export",
)
@click.option(
    "--filter",
    "-t",
    "filters",
    multiple=True,
    help="Filter key=value (repeatable, ANDed). Dashboard fields only.",
)
@click.option(
    "--output-zip",
    "-z",
    type=click.Path(resolve_path=True),
    default=None,
    help="Export to ZIP file instead of directory",
)
@click.option(
    "--per-asset-folder",
    is_flag=True,
    default=False,
    help="Create subfolder per exported dashboard with its dependencies",
)
@click.option(
    "--simple-file-names",
    "-s",
    is_flag=True,
    default=False,
    help="Remove numeric suffixes from exported YAML filenames",
)
@click.pass_context
def export_assets(  # pylint: disable=too-many-locals, too-many-arguments, too-many-branches, too-many-statements
    ctx: click.core.Context,
    directory: Optional[str],
    asset_type: Tuple[str, ...],
    database_ids: List[str],
    dataset_ids: List[str],
    chart_ids: List[str],
    dashboard_ids: List[str],
    filters: Tuple[str, ...],
    output_zip: Optional[str],
    per_asset_folder: bool,
    simple_file_names: bool,
    overwrite: bool = False,
    disable_jinja_escaping: bool = False,
    force_unix_eol: bool = False,
) -> None:
    """
    Export DBs/datasets/charts/dashboards to a directory.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)
    _validate_export_destination_args(directory, output_zip)
    asset_types = set(asset_type)
    ids, ids_requested = _build_requested_ids(
        database_ids,
        dataset_ids,
        chart_ids,
        dashboard_ids,
    )
    dashboard_filter_result = _apply_dashboard_filters(
        client,
        asset_types,
        ids_requested,
        filters,
    )
    if dashboard_filter_result is not None:
        if not dashboard_filter_result.dashboard_ids:
            click.echo("No dashboards match the specified filters.")
            return
        ids["dashboard"].update(dashboard_filter_result.dashboard_ids)
        asset_types = dashboard_filter_result.asset_types
        ids_requested = dashboard_filter_result.ids_requested

    selection = _ExportSelection(
        asset_types=asset_types,
        ids=ids,
        ids_requested=ids_requested,
        overwrite=overwrite,
        disable_jinja_escaping=disable_jinja_escaping,
        force_unix_eol=force_unix_eol,
        simple_file_names=simple_file_names,
    )

    simple_name_registry: Dict[Path, Set[str]] = {}

    if output_zip:
        with tempfile.TemporaryDirectory() as temp_dir:
            export_root = Path(temp_dir)
            _export_selected_resources(
                export_root,
                client,
                selection,
                simple_name_registry,
            )
            _finalize_per_asset_folder_export(export_root, per_asset_folder)
            zip_directory(export_root, Path(output_zip))
        return

    if directory is None:
        raise click.UsageError("Provide a directory or --output-zip.")
    root = _resolve_directory_root(directory)
    _export_selected_resources(
        root,
        client,
        selection,
        simple_name_registry,
    )
    _finalize_per_asset_folder_export(root, per_asset_folder)


def export_resource(  # pylint: disable=too-many-arguments, too-many-locals
    resource_name: str,
    requested_ids: Set[int],
    root: Path,
    client: SupersetClient,
    overwrite: bool,
    disable_jinja_escaping: bool,
    skip_related: bool = True,
    force_unix_eol: bool = False,
    simple_file_names: bool = False,
    simple_name_registry: Optional[Dict[Path, Set[str]]] = None,
) -> None:
    """
    Export a given resource and unzip it in a directory.
    """
    if requested_ids:
        ids = list(requested_ids)
    else:
        resources = client.get_resources(resource_name)
        ids = [resource["id"] for resource in resources]
    buf = client.export_zip(resource_name, ids)

    with ZipFile(buf) as bundle:
        contents = {
            remove_root(file_name): bundle.read(file_name).decode()
            for file_name in bundle.namelist()
        }

    # Build UUID mapping for existing files to validate uniqueness
    uuid_mapping = build_local_uuid_mapping(root)
    files_to_delete: List[Path] = []

    for file_name, file_content in contents.items():
        if skip_related and not file_name.startswith(resource_name):
            continue

        output_name = (
            apply_simple_filename(file_name, root, simple_name_registry)
            if simple_file_names and simple_name_registry is not None
            else file_name
        )
        target = root / output_name
        check_asset_uniqueness(
            overwrite,
            file_content,
            output_name,
            target,
            uuid_mapping,
            files_to_delete,
        )

        if not target.parent.exists():
            target.parent.mkdir(parents=True, exist_ok=True)

        # escape any pre-existing Jinja2 templates
        if not disable_jinja_escaping:
            asset_yaml = yaml.load(file_content, Loader=yaml.SafeLoader)
            for key, value in asset_yaml.items():
                asset_yaml[key] = traverse_data(value, handle_string)

            file_content = yaml.dump(asset_yaml, sort_keys=False)

        newline = get_newline_char(force_unix_eol)
        with open(target, "w", encoding="utf-8", newline=newline) as output:
            output.write(file_content)

    # Delete old files that have been replaced (only after successful write)
    failed_deletions = []
    for file_to_delete in files_to_delete:
        try:
            file_to_delete.unlink()
        except (IOError, OSError):
            failed_deletions.append(str(file_to_delete))

    if failed_deletions:
        error_msg = "Failed to delete the following files:\n" + "\n".join(
            failed_deletions,
        )
        click.echo(error_msg, err=True)
        raise SystemExit(1)


def traverse_data(value: Any, handler: Callable) -> Any:
    """
    Process value according to its data type
    """
    if isinstance(value, str):
        return handler(value)
    if isinstance(value, dict) and value:
        return {k: traverse_data(v, handler) for k, v in value.items()}
    if isinstance(value, list) and value:
        return [traverse_data(item, handler) for item in value]
    return value


def handle_string(value):
    """
    Try to load a string as JSON to traverse its content for proper Jinja templating escaping.
    Required for fields like ``query_context``
    """
    try:
        asset_dict = json.loads(value)
        return (
            json.dumps(traverse_data(asset_dict, jinja_escaper)) if asset_dict else "{}"
        )
    except json.JSONDecodeError:
        return jinja_escaper(value)


def jinja_escaper(value: str) -> str:
    """
    Escape Jinja macros and logical statements that shouldn't be handled by CLI
    """
    logical_statements_patterns = [
        r"(\{%-?\s*if)",  # {%if || {% if || {%-if || {%- if
        r"(\{%-?\s*elif)",  # {%elif || {% elif || {%-elif || {%- elif
        r"(\{%-?\s*else)",  # {%else || {% else || {%-else || {%- else
        r"(\{%-?\s*endif)",  # {%endif || {% endif || {%-endif || {%- endif
        r"(\{%-?\s*for)",  # {%for || {% for || {%-for || {%- for
        r"(\{%-?\s*endfor)",  # {%endfor || {% endfor || {%-endfor || {%- endfor
        r"(%})",  # %}
        r"(-%})",  # -%}
    ]

    for syntax in logical_statements_patterns:
        replacement = JINJA2_OPEN_MARKER + " '" + r"\1" + "' " + JINJA2_CLOSE_MARKER
        value = re.sub(syntax, replacement, value)

    # escaping macros
    value = value.replace(
        "{{",
        f"{JINJA2_OPEN_MARKER} '{{{{' {JINJA2_CLOSE_MARKER}",
    )
    value = value.replace(
        "}}",
        f"{JINJA2_OPEN_MARKER} '}}}}' {JINJA2_CLOSE_MARKER}",
    )
    value = value.replace(JINJA2_OPEN_MARKER, "{{")
    value = value.replace(JINJA2_CLOSE_MARKER, "}}")
    value = re.sub(r"' }} {{ '", " ", value)

    return value


@click.command()
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="users.yaml",
)
@click.option(
    "--force-unix-eol",
    is_flag=True,
    default=False,
    help="Force Unix end-of-line characters, otherwise use system default",
)
@click.pass_context
def export_users(
    ctx: click.core.Context,
    path: str,
    force_unix_eol: bool = False,
) -> None:
    """
    Export users and their roles to a YAML file.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    preset_baseurl = ctx.obj.get("MANAGER_URL")
    client = SupersetClient(url, auth, preset_baseurl)

    users = [
        {k: v for k, v in user.items() if k != "id"} for user in client.export_users()
    ]

    newline = get_newline_char(force_unix_eol)
    with open(path, "w", encoding="utf-8", newline=newline) as output:
        yaml.dump(users, output)


@click.command()
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="roles.yaml",
)
@click.option(
    "--force-unix-eol",
    is_flag=True,
    default=False,
    help="Force Unix end-of-line characters, otherwise use system default",
)
@click.pass_context
def export_roles(
    ctx: click.core.Context,
    path: str,
    force_unix_eol: bool = False,
) -> None:
    """
    Export roles to a YAML file.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    newline = get_newline_char(force_unix_eol)
    with open(path, "w", encoding="utf-8", newline=newline) as output:
        yaml.dump(list(client.export_roles()), output)


@click.command()
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="rls.yaml",
)
@click.option(
    "--force-unix-eol",
    is_flag=True,
    default=False,
    help="Force Unix end-of-line characters, otherwise use system default",
)
@click.pass_context
def export_rls(
    ctx: click.core.Context,
    path: str,
    force_unix_eol: bool = False,
) -> None:
    """
    Export RLS rules to a YAML file.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    newline = get_newline_char(force_unix_eol)
    with open(path, "w", encoding="utf-8", newline=newline) as output:
        yaml.dump(list(client.export_rls()), output, sort_keys=False)


@click.command()
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="ownership.yaml",
)
@click.option(
    "--asset-type",
    help="Asset type",
    multiple=True,
)
@click.option(
    "--dataset-ids",
    callback=split_comma,
    help="Comma separated list of dataset IDs to export ownership config",
)
@click.option(
    "--chart-ids",
    callback=split_comma,
    help="Comma separated list of chart IDs to export ownership config",
)
@click.option(
    "--dashboard-ids",
    callback=split_comma,
    help="Comma separated list of dashboard IDs to export ownership config",
)
@click.option(
    "--force-unix-eol",
    is_flag=True,
    default=False,
    help="Force Unix end-of-line characters, otherwise use system default",
)
@click.option(
    "--exclude-old-users",
    is_flag=True,
    default=False,
    help="Exclude users that are no longer a member in the team from the generated file",
)
@click.pass_context
def export_ownership(  # pylint: disable=too-many-locals, too-many-arguments
    ctx: click.core.Context,
    path: str,
    asset_type: Tuple[str, ...],
    dataset_ids: List[str],
    chart_ids: List[str],
    dashboard_ids: List[str],
    force_unix_eol: bool = False,
    exclude_old_users: bool = False,
) -> None:
    """
    Export DBs/datasets/charts/dashboards ownership to a YAML file.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    users = {user["id"]: user["email"] for user in client.export_users()}

    asset_types = set(asset_type)
    ids = {
        "dataset": {int(id_) for id_ in dataset_ids},
        "chart": {int(id_) for id_ in chart_ids},
        "dashboard": {int(id_) for id_ in dashboard_ids},
    }
    ids_requested = any([dataset_ids, chart_ids, dashboard_ids])

    ownership = defaultdict(list)
    for resource_name in ["dataset", "chart", "dashboard"]:
        if (not asset_types or resource_name in asset_types) and (
            ids[resource_name] or not ids_requested
        ):
            for resource in client.export_ownership(
                resource_name,
                ids[resource_name],
                users,
                exclude_old_users,
            ):
                ownership[resource_name].append(
                    {
                        "name": resource["name"],
                        "uuid": str(resource["uuid"]),
                        "owners": resource["owners"],
                    },
                )

    with open(
        path,
        "w",
        encoding="utf-8",
        newline=get_newline_char(force_unix_eol),
    ) as output:
        yaml.dump(dict(ownership), output)
