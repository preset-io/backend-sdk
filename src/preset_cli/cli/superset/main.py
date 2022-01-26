"""
Dispatcher for Superset commands.
"""

from typing import Any, List, Optional

import click
import requests
from superset_sdk.auth.jwt import JWTAuth
from superset_sdk.cli.main import superset_cli
from yarl import URL


def split_comma(ctx: click.core.Context, param: str, value: str) -> List[str]:
    """
    Split CLI option into multiple values.
    """
    return [option.strip() for option in value.split(",")]


def get_access_token(baseurl: URL, api_token: str, api_secret: str) -> str:
    """
    Fetch the JWT access token.
    """
    response = requests.post(
        baseurl / "api/v1/auth/",
        json={"name": api_token, "secret": api_secret},
        headers={"Content-Type": "application/json"},
    )
    payload = response.json()
    return payload["payload"]["access_token"]


@click.group()
@click.option("--baseurl", default="https://manage.app.preset.io/")
@click.option("--workspaces", callback=split_comma)
@click.option("--api-token")
@click.option("--api-secret")
@click.pass_context
def superset(
    ctx: click.core.Context,
    baseurl: URL,
    workspaces: List[str],
    api_token: str,
    api_secret: str,
) -> None:
    """
    Send commands to one or more Superset instances.
    """
    ctx.ensure_object(dict)
    # XXX map from workspace name to URLs
    # XXX prompt for workspaces

    # Store workspaces, since the Superset command will run for each workspace, instead
    # of a single instance.
    ctx.obj["WORKSPACES"] = workspaces

    # Store auth.
    access_token = get_access_token(URL(baseurl), api_token, api_secret)
    ctx.obj["AUTH"] = JWTAuth(access_token)


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
