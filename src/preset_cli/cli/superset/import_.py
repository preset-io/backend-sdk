"""
Commands to import RLS rules, ownership, and more.
"""

import click
import yaml
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient


@click.command()
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="rls.yaml",
)
@click.pass_context
def import_rls(ctx: click.core.Context, path: str) -> None:
    """
    Import RLS rules from a YAML file.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    with open(path, encoding="utf-8") as input_:
        config = yaml.load(input_, Loader=yaml.SafeLoader)
        for rls in config:
            client.import_rls(rls)
