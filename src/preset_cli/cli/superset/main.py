"""
Dispatcher for Superset commands.
"""

from typing import Any, Optional, Tuple

import click
from superset_sdk.cli.main import superset_cli


@click.group()
@click.option("--workspaces")
@click.pass_context
def superset(ctx: click.core.Context, workspaces: Tuple[str]) -> None:
    """
    Send commands to one or more Superset instances.
    """
    ctx.ensure_object(dict)
    # XXX map from workspace name to URLs
    # XXX prompt for workspaces
    ctx.obj["WORKSPACES"] = workspaces.split(",")


def add_superset_commands(group: click.core.Group) -> None:
    """
    Programmatically add Superset commands to the group.
    """
    for name, command in superset_cli.commands.items():

        @click.command()
        @click.pass_context
        def new_command(ctx: click.core.Context, *args: Any, **kwargs: Any) -> None:
            for instance in ctx.obj["WORKSPACES"]:
                ctx.invoke(command, instance=instance, *args, **kwargs)

        # remove INSTANCE argument from the command
        new_command.params = command.params[1:]

        group.add_command(new_command, name)


add_superset_commands(superset)
