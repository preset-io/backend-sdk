"""
Main entry point for the CLI.
"""

import getpass
import logging
import sys
import webbrowser
from typing import List, Optional, cast

import click
import yaml
from yarl import URL

from preset_cli.api.clients.preset import PresetClient
from preset_cli.auth.jwt import JWTAuth
from preset_cli.auth.lib import (
    get_access_token,
    get_credentials_path,
    store_credentials,
)
from preset_cli.auth.main import Auth
from preset_cli.cli.superset.main import superset
from preset_cli.lib import setup_logging

_logger = logging.getLogger(__name__)


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


def parse_selection(selection: str, count: int) -> List[int]:
    """
    Parse a range of numbers.

        >>> parse_selection("1-4,7", 10)
        [1, 2, 3, 4, 7]

    """
    numbers: List[int] = []
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


@click.group()
@click.option("--baseurl", default="https://manage.app.preset.io/")
@click.option("--api-token", envvar="PRESET_API_TOKEN")
@click.option("--api-secret", envvar="PRESET_API_SECRET")
@click.option("--jwt-token", envvar="PRESET_JWT_TOKEN")
@click.option("--workspaces", callback=split_comma)
@click.option("--loglevel", default="INFO")
@click.pass_context
def preset_cli(  # pylint: disable=too-many-branches, too-many-locals, too-many-arguments
    ctx: click.core.Context,
    baseurl: str,
    api_token: Optional[str],
    api_secret: Optional[str],
    jwt_token: Optional[str],
    workspaces: List[str],
    loglevel: str,
) -> None:
    """
    A CLI for Preset.
    """
    setup_logging(loglevel)

    ctx.ensure_object(dict)

    # store manager URL for other commands
    ctx.obj["MANAGER_URL"] = manager_url = URL(baseurl)

    if jwt_token is None:
        if api_token is None or api_secret is None:
            # check for stored credentials
            credentials_path = get_credentials_path()
            if credentials_path.exists():
                try:
                    with open(credentials_path, encoding="utf-8") as input_:
                        credentials = yaml.load(input_, Loader=yaml.SafeLoader)
                    api_token = credentials["api_token"]
                    api_secret = credentials["api_secret"]
                except Exception:  # pylint: disable=broad-except
                    click.echo(
                        click.style(
                            "Couldn't read credentials",
                            fg="bright_red",
                        ),
                    )
                    sys.exit(1)
            else:
                click.echo(
                    "You need to specify a JWT token or an API key (name and secret)",
                )
                webbrowser.open(str(manager_url / "app/user"))
                api_token = input("API token: ")
                api_secret = getpass.getpass("API secret: ")
                store_credentials(api_token, api_secret, manager_url, credentials_path)

        api_token = cast(str, api_token)
        api_secret = cast(str, api_secret)
        try:
            jwt_token = get_access_token(manager_url, api_token, api_secret)
        except Exception:  # pylint: disable=broad-except
            jwt_token = None

    # store auth in context so it's used by the Superset SDK
    ctx.obj["AUTH"] = JWTAuth(jwt_token) if jwt_token else Auth()

    if not workspaces and ctx.invoked_subcommand == "superset":
        client = PresetClient(ctx.obj["MANAGER_URL"], ctx.obj["AUTH"])
        click.echo("Choose one or more workspaces (eg: 1-3,5,8-):")
        i = 0
        hostnames = []
        for team in client.get_teams():
            click.echo(f'\n# {team["title"]} #')
            for workspace in client.get_workspaces(team_name=team["name"]):
                status = get_status_icon(workspace["workspace_status"])
                click.echo(f'{status} ({i+1}) {workspace["title"]}')
                hostnames.append("https://" + workspace["hostname"])
                i += 1

        if i == 0:
            click.echo(
                click.style(
                    "No workspaces available",
                    fg="bright_red",
                ),
            )
            sys.exit(1)
        if i == 1:
            workspaces = hostnames

        while not workspaces:
            try:
                choices = parse_selection(input("> "), i)
                workspaces = [hostnames[choice - 1] for choice in choices]
            except Exception:  # pylint: disable=broad-except
                click.echo("Invalid choice")

    # store workspaces in order to invoke the command for each one
    ctx.obj["WORKSPACES"] = workspaces


@click.command()
@click.option("--baseurl", default="https://manage.app.preset.io/")
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite existing credentials",
)
@click.option(
    "--show",
    is_flag=True,
    default=False,
    help="Show existing credentials",
)
def auth(baseurl: str, overwrite: bool = False, show: bool = False) -> None:
    """
    Store credentials for auth.
    """
    credentials_path = get_credentials_path()

    if show:
        if not credentials_path.exists():
            click.echo(
                click.style(
                    (
                        f"The file {credentials_path} doesn't exist. "
                        "Run ``preset-cli auth`` to create it."
                    ),
                    fg="bright_red",
                ),
            )
            sys.exit(1)

        ruler = "=" * len(str(credentials_path))
        with open(credentials_path, encoding="utf-8") as input_:
            credentials = yaml.load(input_, Loader=yaml.SafeLoader)
            contents = yaml.dump(credentials)
        click.echo(f"{credentials_path}\n{ruler}\n{contents}")
        sys.exit(0)

    if credentials_path.exists() and not overwrite:
        click.echo(
            click.style(
                (
                    f"The file {credentials_path} already exists. "
                    "Pass --overwrite to replace it."
                ),
                fg="bright_red",
            ),
        )
        sys.exit(1)

    manager_url = URL(baseurl)
    click.echo(
        f"Please generate a new token at {manager_url} if you don't have one already",
    )
    webbrowser.open(str(manager_url / "app/user"))
    api_token = input("API token: ")
    api_secret = getpass.getpass("API secret: ")

    store_credentials(api_token, api_secret, manager_url, credentials_path)
    click.echo(f"Credentials stored in {credentials_path}")


@click.command()
@click.option("--teams", callback=split_comma)
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="users.yaml",
)
@click.pass_context
def invite_users(ctx: click.core.Context, teams: List[str], path: str) -> None:
    """
    Invite users to join Preset teams.
    """
    client = PresetClient(ctx.obj["MANAGER_URL"], ctx.obj["AUTH"])

    if not teams:
        click.echo("Choose one or more teams (eg: 1-3,5,8-):")
        i = 0
        all_teams = []
        for team in client.get_teams():
            click.echo(f'({i+1}) {team["title"]}')
            all_teams.append(team["name"])
            i += 1

        if i == 0:
            click.echo(
                click.style(
                    "No teams available",
                    fg="bright_red",
                ),
            )
            sys.exit(1)
        if i == 1:
            teams = all_teams

        while not teams:
            try:
                choices = parse_selection(input("> "), i)
                teams = [all_teams[choice - 1] for choice in choices]
            except Exception:  # pylint: disable=broad-except
                click.echo("Invalid choice")

    with open(path, encoding="utf-8") as input_:
        config = yaml.load(input_, Loader=yaml.SafeLoader)
        emails = [user["email"] for user in config]
        client.invite_users(teams, emails)


preset_cli.add_command(auth)
preset_cli.add_command(invite_users)
preset_cli.add_command(superset)
