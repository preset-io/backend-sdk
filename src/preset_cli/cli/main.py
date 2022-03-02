"""
Main entry point for the CLI.
"""

import getpass
import webbrowser
from pathlib import Path
from typing import Optional

import click
import requests
import yaml
from appdirs import user_config_dir
from superset_sdk.auth.jwt import JWTAuth
from yarl import URL

from preset_cli.cli.superset.main import superset

CREDENTIALS_FILE = "credentials.yaml"


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

    if jwt_token is None:
        if api_token is None or api_secret is None:
            # check for stored credentials
            config_dir = Path(user_config_dir("preset-cli", "Preset"))
            config_dir.mkdir(parents=True, exist_ok=True)
            credentials_path = config_dir / CREDENTIALS_FILE
            if credentials_path.exists():
                with open(credentials_path, encoding="utf-8") as input_:
                    credentials = yaml.load(input_, Loader=yaml.SafeLoader)
                api_token = credentials["api_token"]
                api_secret = credentials["api_secret"]
            else:
                click.echo(
                    "You need to specify a JWT token or an API key (name and secret)"
                )
                webbrowser.open(str(manager_url / "app/user"))

                # get the credentials from the user
                api_token = input("API token: ")
                api_secret = getpass.getpass("API secret: ")

                while True:
                    store = input(f"Store the credentials in {config_dir}? [y/N] ")
                    if store.strip().lower() == "y":
                        with open(credentials_path, "w", encoding="utf-8") as output:
                            yaml.safe_dump(
                                dict(api_token=api_token, api_secret=api_secret), output
                            )
                        credentials_path.chmod(0o600)
                        break

                    if store.strip().lower() in ("n", ""):
                        break

        jwt_token = get_access_token(manager_url, api_token, api_secret)

    # store auth in context so it's used by the Superset SDK
    ctx.obj["AUTH"] = JWTAuth(jwt_token)


preset_cli.add_command(superset)
