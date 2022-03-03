"""
Dispatcher for Superset commands.
"""

from typing import Any

import click
from superset_sdk.cli.main import superset_cli


@click.group()
@click.pass_context
def superset(ctx: click.core.Context) -> None:
    """
    Send commands to one or more Superset instances.
    """
    ctx.ensure_object(dict)


def mutate_commands(source: click.core.Group, target: click.core.Group) -> None:
    """
    Programmatically modify commands so they work with workspaces.
    """
    for name, command in source.commands.items():

        if isinstance(command, click.core.Group):

            @click.group()
            @click.pass_context
            def new_group(
                ctx: click.core.Context, *args: Any, command=command, **kwargs: Any
            ) -> None:
                ctx.invoke(command, *args, **kwargs)

            mutate_commands(command, new_group)
            new_group.params = command.params[:]
            target.add_command(new_group, name)

        else:

            @click.command()
            @click.pass_context
            def new_command(
                ctx: click.core.Context, *args: Any, command=command, **kwargs: Any
            ) -> None:
                for instance in ctx.obj["WORKSPACES"]:
                    click.echo(f"\n{instance}")
                    ctx.obj["INSTANCE"] = instance
                    ctx.invoke(command, *args, **kwargs)

            new_command.params = command.params[:]
            target.add_command(new_command, name)


mutate_commands(superset_cli, superset)
