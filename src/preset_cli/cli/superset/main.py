"""
Main entry point for Superset commands.
"""

import click
from yarl import URL

from preset_cli.auth.main import UsernamePasswordAuth
from preset_cli.cli.superset.export import export
from preset_cli.cli.superset.sql import sql
from preset_cli.cli.superset.sync.main import sync


@click.group()
@click.argument("instance")
@click.option("-u", "--username", default="admin", help="Username")
@click.option(
    "-p",
    "--password",
    prompt=True,
    prompt_required=False,
    default="admin",
    hide_input=True,
    help="Password (leave empty for prompt)",
)
@click.pass_context
def superset(
    ctx: click.core.Context,
    instance: str,
    username: str = "admin",
    password: str = "admin",
):
    """
    An Apache Superset CLI.
    """
    ctx.ensure_object(dict)

    ctx.obj["INSTANCE"] = instance

    # allow a custom authenticator to be passed via the context
    if "AUTH" not in ctx.obj:
        ctx.obj["AUTH"] = UsernamePasswordAuth(URL(instance), username, password)


superset.add_command(sql)
superset.add_command(sync)
superset.add_command(export)
