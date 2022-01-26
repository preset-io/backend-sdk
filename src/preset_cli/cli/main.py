"""
Main entry point for the CLI.
"""

import webbrowser
from typing import Optional

import click
import requests
from superset_sdk.auth.jwt import JWTAuth
from yarl import URL

from preset_cli.cli.superset.main import superset


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
@click.option("--api-token", envvar="PRESET_API_TOKEN")
@click.option("--api-secret", envvar="PRESET_API_SECRET")
@click.option("--jwt-token", envvar="PRESET_JWT_TOKEN")
@click.pass_context
def preset_cli(
    ctx: click.core.Context,
    baseurl: str,
    api_token: Optional[str],
    api_secret: Optional[str],
    jwt_token: Optional[str],
):
    """
    A CLI for Preset.
    """
    ctx.ensure_object(dict)

    # store manager URL for other commands
    ctx.obj["MANAGER_URL"] = manager_url = URL(baseurl)

    # store auth in context so it's used by the Superset SDK
    if jwt_token is None:
        if api_token is None or api_secret is None:
            click.echo(
                "You need to specify a JWT token or an API key (name and secret)"
            )
            webbrowser.open(str(manager_url / "app/user"))
            ctx.exit()
        jwt_token = get_access_token(manager_url, api_token, api_secret)
    ctx.obj["AUTH"] = JWTAuth(jwt_token)


preset_cli.add_command(superset)
