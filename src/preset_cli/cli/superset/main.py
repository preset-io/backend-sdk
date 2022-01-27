"""
Dispatcher for Superset commands.
"""

from typing import Any, List, Optional

import click
from superset_sdk.cli.main import superset_cli

from preset_cli.api.client import PresetClient


def split_comma(  # pylint: disable=unused-argument
    ctx: click.core.Context,
    param: str,
    value: Optional[str],
) -> List[str]:
    """
    Split CLI option into multiple values.
    """
    if value is None:
        return []

    return [option.strip() for option in value.split(",")]


def get_status_icon(status: str) -> str:
    """
    Return an icon (emoji) for a given status.
    """
    icons = {
        "READY": "✅",
        "LOADING_EXAMPLES": "📊",
        "CREATING_DB": "💾",
        "INITIALIZING_DB": "💾",
        "MIGRATING_DB": "🚧",
        "ROTATING_SECRETS": "🕵️",
        "UNKNOWN": "❓",
        "ERROR": "❗️",
        "UPGRADING": "⤴️",
    }
    return icons.get(status, "❓")


def parse_workspace_selection(selection: str, count: int) -> List[int]:
    """
    Parse a range of numbers.

        >>> parse_workspace_selection("1-4,7", 10)
        [1, 2, 3, 4, 7]

    """
    numbers = []
    for part in selection.split(","):
        if "-" in part:
            if part[0] == "-":
                part = "1" + part
            if part[-1] == "-":
                part = part + str(count)
            start, end = [int(number) for number in part.split("-", 1)]
            if end > count:
                raise Exception("Range {part} is greater than {count}")
            numbers.extend(range(start, end + 1))
        else:
            number = int(part)
            if number > count:
                raise Exception("Number {number} is greater than {count}")
            numbers.append(int(part))

    return numbers


@click.group()
@click.option("--workspaces", callback=split_comma)
@click.pass_context
def superset(ctx: click.core.Context, workspaces: List[str]) -> None:
    """
    Send commands to one or more Superset instances.
    """
    ctx.ensure_object(dict)

    if not workspaces:
        client = PresetClient(ctx.obj["MANAGER_URL"], ctx.obj["AUTH"])
        click.echo("Choose one or more workspaces (eg: 1-3,5,8-):")
        i = 1
        hostnames = {}
        for team in client.get_teams():
            click.echo(f'\n# {team["title"]} #')
            for workspace in client.get_workspaces(team_name=team["name"]):
                status = get_status_icon(workspace["workspace_status"])
                click.echo(f'{status} ({i}) {workspace["title"]}')
                hostnames[i] = "https://" + workspace["hostname"]
                i += 1

        if i == 1:
            click.echo("No workspaces available")
            return

        while not workspaces:
            try:
                choices = parse_workspace_selection(input("> "), i - 1)
                workspaces = [hostnames[choice] for choice in choices]
                break
            except Exception:  # pylint: disable=broad-except
                click.echo("Invalid choice")

    # store workspaces in order to invoke the command for each one
    ctx.obj["WORKSPACES"] = workspaces


def add_superset_commands(group: click.core.Group) -> None:
    """
    Programmatically add Superset commands to the group.
    """
    for name, command in superset_cli.commands.items():

        @click.command()
        @click.pass_context
        def new_command(
            ctx: click.core.Context, *args: Any, command=command, **kwargs: Any
        ) -> None:
            for instance in ctx.obj["WORKSPACES"]:
                click.echo(f"\n{instance}")
                ctx.obj["INSTANCE"] = instance
                ctx.invoke(command, *args, **kwargs)

        group.add_command(new_command, name)


add_superset_commands(superset)
