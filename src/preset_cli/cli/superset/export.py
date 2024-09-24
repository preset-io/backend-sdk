"""
A command to export Superset resources into a directory.
"""

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, List, Set, Tuple, Union
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
    resources = client.get_resources(resource_name)
    ids = [
        resource["id"]
        for resource in resources
        if resource["id"] in requested_ids or not requested_ids
    ]
    buf = client.export_zip(resource_name, ids)

    with ZipFile(buf) as bundle:
        contents = {
            remove_root(file_name): bundle.read(file_name).decode()
            for file_name in bundle.namelist()
        }

    for file_name, file_contents in contents.items():
        if skip_related and not file_name.startswith(resource_name):
            continue

        target = root / file_name
        if target.exists() and not overwrite:
            raise Exception(
                f"File already exists and ``--overwrite`` was not specified: {target}",
            )
        if not target.parent.exists():
            target.parent.mkdir(parents=True, exist_ok=True)

        # escape any pre-existing Jinja2 templates
        if not disable_jinja_escaping:
            asset_content = yaml.load(file_contents, Loader=yaml.SafeLoader)
            for key, value in asset_content.items():
                asset_content[key] = traverse_data(value, handle_string)

            file_contents = yaml.dump(asset_content, sort_keys=False)

        newline = get_newline_char(force_unix_eol)
        with open(target, "w", encoding="utf-8", newline=newline) as output:
            output.write(file_contents)


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
    client = SupersetClient(url, auth)

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
    "--force-unix-eol",
    is_flag=True,
    default=False,
    help="Force Unix end-of-line characters, otherwise use system default",
)
@click.pass_context
def export_ownership(
    ctx: click.core.Context,
    path: str,
    force_unix_eol: bool = False,
) -> None:
    """
    Export DBs/datasets/charts/dashboards ownership to a YAML file.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    ownership = defaultdict(list)
    for resource_name in ["dataset", "chart", "dashboard"]:
        for resource in client.export_ownership(resource_name):
            ownership[resource_name].append(
                {
                    "name": resource["name"],
                    "uuid": str(resource["uuid"]),
                    "owners": resource["owners"],
                },
            )

    newline = get_newline_char(force_unix_eol)
    with open(path, "w", encoding="utf-8", newline=newline) as output:
        yaml.dump(dict(ownership), output)
