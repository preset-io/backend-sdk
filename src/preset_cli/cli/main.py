"""
Main entry point for the CLI.
"""

import getpass
import sys
import webbrowser
from pathlib import Path
from typing import List, Optional

import click
import requests
import yaml
from appdirs import user_config_dir
from superset_sdk.auth.jwt import JWTAuth
from yarl import URL

from preset_cli.api.client import PresetClient
from preset_cli.cli.superset.main import superset

CREDENTIALS_FILE = "credentials.yaml"


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
                raise Exception(f"End {end} is greater than {count}")
            numbers.extend(range(start, end + 1))
        else:
            number = int(part)
            if number > count:
                raise Exception(f"Number {number} is greater than {count}")
            numbers.append(int(part))

    return numbers


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


def get_credentials_path() -> Path:
    """
    Return the system-dependent location of the credentials.
    """
    config_dir = Path(user_config_dir("preset-cli", "Preset"))
    return config_dir / CREDENTIALS_FILE


def store_credentials(api_token: str, api_secret: str, credentials_path: Path) -> None:
    """
    Store credentials.
    """
    credentials_path.parent.mkdir(parents=True, exist_ok=True)

    while True:
        store = input(f"Store the credentials in {credentials_path}? [y/N] ")
        if store.strip().lower() == "y":
            with open(credentials_path, "w", encoding="utf-8") as output:
                yaml.safe_dump(dict(api_token=api_token, api_secret=api_secret), output)
            credentials_path.chmod(0o600)
            break

        if store.strip().lower() in ("n", ""):
            break


@click.group()
@click.option("--baseurl", default="https://manage.app.preset.io/")
@click.option("--api-token", envvar="PRESET_API_TOKEN")
@click.option("--api-secret", envvar="PRESET_API_SECRET")
@click.option("--jwt-token", envvar="PRESET_JWT_TOKEN")
@click.option("--workspaces", callback=split_comma)
@click.pass_context
def preset_cli(  # pylint: disable=too-many-branches, too-many-locals, too-many-arguments
    ctx: click.core.Context,
    baseurl: str,
    api_token: Optional[str],
    api_secret: Optional[str],
    jwt_token: Optional[str],
    workspaces: List[str],
) -> None:
    """
    A CLI for Preset.
    """
    ctx.ensure_object(dict)

    # store manager URL for other commands
    ctx.obj["MANAGER_URL"] = manager_url = URL(baseurl)

    if jwt_token is None:
        if api_token is None or api_secret is None:
            # check for stored credentials
            credentials_path = get_credentials_path()
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
                api_token = input("API token: ")
                api_secret = getpass.getpass("API secret: ")
                store_credentials(api_token, api_secret, credentials_path)

        jwt_token = get_access_token(manager_url, api_token, api_secret)

    # store auth in context so it's used by the Superset SDK
    ctx.obj["AUTH"] = JWTAuth(jwt_token)

    if not workspaces and ctx.invoked_subcommand == "superset":
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
            sys.exit(1)

        while not workspaces:
            try:
                choices = parse_workspace_selection(input("> "), i - 1)
                workspaces = [hostnames[choice] for choice in choices]
            except Exception:  # pylint: disable=broad-except
                click.echo("Invalid choice")

    # store workspaces in order to invoke the command for each one
    ctx.obj["WORKSPACES"] = workspaces


@click.command()
@click.option("--baseurl", default="https://manage.app.preset.io/")
@click.option(
    "--overwrite", is_flag=True, default=False, help="Overwrite existing credentials"
)
def auth(baseurl: str, overwrite: bool = False) -> None:
    """
    Store credentials for auth.
    """
    credentials_path = get_credentials_path()
    if credentials_path.exists() and not overwrite:
        click.echo(
            click.style(
                (
                    f"The file {credentials_path} already exists. "
                    "Pass --overwrite to replace it."
                ),
                fg="bright_red",
            )
        )
        sys.exit(1)

    manager_url = URL(baseurl)
    click.echo(
        f"Please generate a new token at {manager_url} if you don't have one already"
    )
    webbrowser.open(str(manager_url / "app/user"))
    api_token = input("API token: ")
    api_secret = getpass.getpass("API secret: ")

    store_credentials(api_token, api_secret, credentials_path)
    click.echo(f"Credentials stored in {credentials_path}")


preset_cli.add_command(auth)
preset_cli.add_command(superset)
