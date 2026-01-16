"""
A command to export Superset resources into a directory.
"""

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from zipfile import ZipFile

import click
import yaml
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
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
@click.argument("directory", type=click.Path(exists=True, resolve_path=True))
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
@click.pass_context
def export_assets(  # pylint: disable=too-many-locals, too-many-arguments
    ctx: click.core.Context,
    directory: str,
    asset_type: Tuple[str, ...],
    database_ids: List[str],
    dataset_ids: List[str],
    chart_ids: List[str],
    dashboard_ids: List[str],
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
    root = Path(directory)
    asset_types = set(asset_type)
    ids = {
        "database": {int(id_) for id_ in database_ids},
        "dataset": {int(id_) for id_ in dataset_ids},
        "chart": {int(id_) for id_ in chart_ids},
        "dashboard": {int(id_) for id_ in dashboard_ids},
    }
    ids_requested = any([database_ids, dataset_ids, chart_ids, dashboard_ids])

    for resource_name in ["database", "dataset", "chart", "dashboard"]:
        if (not asset_types or resource_name in asset_types) and (
            ids[resource_name] or not ids_requested
        ):
            export_resource(
                resource_name,
                ids[resource_name],
                root,
                client,
                overwrite,
                disable_jinja_escaping,
                skip_related=not ids_requested,
                force_unix_eol=force_unix_eol,
            )


def export_resource(  # pylint: disable=too-many-arguments, too-many-locals
    resource_name: str,
    requested_ids: Set[int],
    root: Path,
    client: SupersetClient,
    overwrite: bool,
    disable_jinja_escaping: bool,
    skip_related: bool = True,
    force_unix_eol: bool = False,
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

        target = root / file_name
        check_asset_uniqueness(
            overwrite,
            file_content,
            file_name,
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
