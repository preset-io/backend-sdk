"""
A command to export Superset resources into a directory.
"""

from pathlib import Path
from zipfile import ZipFile

import click
import yaml
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.lib import remove_root

JINJA2_OPEN_MARKER = "__JINJA2_OPEN__"
JINJA2_CLOSE_MARKER = "__JINJA2_CLOSE__"
assert JINJA2_OPEN_MARKER != JINJA2_CLOSE_MARKER


@click.command()
@click.argument("directory", type=click.Path(exists=True, resolve_path=True))
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing resources",
)
@click.pass_context
def export_assets(  # pylint: disable=too-many-locals
    ctx: click.core.Context,
    directory: str,
    overwrite: bool = False,
) -> None:
    """
    Export DBs/datasets/charts/dashboards to a directory.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)
    root = Path(directory)

    for resource in ["database", "dataset", "chart", "dashboard"]:
        export_resource(resource, root, client, overwrite)


def export_resource(
    resource: str,
    root: Path,
    client: SupersetClient,
    overwrite: bool,
) -> None:
    """
    Export a given resource and unzip it in a directory.
    """
    resources = client.get_resources(resource)
    ids = [resource["id"] for resource in resources]
    buf = client.export_zip(resource, ids)

    with ZipFile(buf) as bundle:
        contents = {
            remove_root(file_name): bundle.read(file_name).decode()
            for file_name in bundle.namelist()
        }

    for file_name, file_contents in contents.items():
        # skip related files
        if not file_name.startswith(resource):
            continue

        target = root / file_name
        if target.exists() and not overwrite:
            raise Exception(
                f"File already exists and --overwrite was not specified: {target}",
            )
        if not target.parent.exists():
            target.parent.mkdir(parents=True, exist_ok=True)

        # escape any pre-existing Jinja2 templates
        file_contents = file_contents.replace(
            "{{",
            f"{JINJA2_OPEN_MARKER} '{{{{' {JINJA2_CLOSE_MARKER}",
        )
        file_contents = file_contents.replace(
            "}}",
            f"{JINJA2_OPEN_MARKER} '}}}}' {JINJA2_CLOSE_MARKER}",
        )
        file_contents = file_contents.replace(JINJA2_OPEN_MARKER, "{{")
        file_contents = file_contents.replace(JINJA2_CLOSE_MARKER, "}}")

        with open(target, "w", encoding="utf-8") as output:
            output.write(file_contents)


@click.command()
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="users.yaml",
)
@click.pass_context
def export_users(ctx: click.core.Context, path: str) -> None:
    """
    Export users and their roles to a YAML file.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    with open(path, "w", encoding="utf-8") as output:
        yaml.dump(list(client.export_users()), output)


@click.command()
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="rls.yaml",
)
@click.pass_context
def export_rls(ctx: click.core.Context, path: str) -> None:
    """
    Export RLS rules to a YAML file.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    with open(path, "w", encoding="utf-8") as output:
        yaml.dump(list(client.export_rls()), output)
