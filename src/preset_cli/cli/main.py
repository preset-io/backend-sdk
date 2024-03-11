"""
Main entry point for the CLI.
"""

import csv
import getpass
import logging
import os.path
import sys
import webbrowser
from collections import defaultdict
from typing import Any, DefaultDict, Dict, List, Optional, Set, cast

import click
import yaml
from yarl import URL

from preset_cli.api.clients.preset import PresetClient
from preset_cli.api.clients.superset import SupersetClient
from preset_cli.auth.jwt import JWTAuth
from preset_cli.auth.lib import get_credentials_path, store_credentials
from preset_cli.auth.preset import JWTTokenError, PresetAuth
from preset_cli.cli.superset.main import superset
from preset_cli.exceptions import CLIError
from preset_cli.lib import raise_cli_errors, setup_logging, split_comma

_logger = logging.getLogger(__name__)


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
        "HIBERNATED": "💤",
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


def is_help() -> bool:
    """
    Are we running ``--help`` in a subcommand?

    This detects that ``--help`` was passed to a subcommand, and prevents prompting for
    workspaces and teams.
    """
    return "--help" in sys.argv[1:]


workspace_role_identifiers = {
    "workspace admin": "Admin",
    "primary contributor": "PresetAlpha",
    "secondary contributor": "PresetBeta",
    "limited contributor": "PresetGamma",
    "viewer": "PresetReportsOnly",
    "dashboard viewer": "PresetDashboardsOnly",
    "no access": "PresetNoAccess",
}


@click.group()
@click.option("--baseurl", default="https://api.app.preset.io/")
@click.option("--api-token", envvar="PRESET_API_TOKEN")
@click.option("--api-secret", envvar="PRESET_API_SECRET")
@click.option("--jwt-token", envvar="PRESET_JWT_TOKEN")
@click.option("--workspaces", envvar="PRESET_WORKSPACES", callback=split_comma)
@click.option("--loglevel", default="INFO")
@click.version_option()
@click.pass_context
@raise_cli_errors
def preset_cli(  # pylint: disable=too-many-branches, too-many-locals, too-many-arguments, too-many-statements
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
    ctx.obj["MANAGER_URL"] = manager_api_url = URL(baseurl)

    if ctx.invoked_subcommand == "auth":
        # The user is trying to auth themselves, so skip anything auth-related
        return

    if jwt_token:
        ctx.obj["AUTH"] = JWTAuth(jwt_token)
    else:
        if api_token is None or api_secret is None:
            # check for stored credentials
            credentials_path = get_credentials_path()
            if credentials_path.exists():
                try:
                    with open(credentials_path, encoding="utf-8") as input_:
                        credentials = yaml.load(input_, Loader=yaml.SafeLoader)
                    api_token = credentials["api_token"]
                    api_secret = credentials["api_secret"]
                except Exception as excinfo:  # pylint: disable=broad-except
                    raise CLIError("Couldn't read credentials", 1) from excinfo
            else:
                manager_url = URL(baseurl.replace("api.", "manage."))
                click.echo(
                    "You need to specify a JWT token or an API key (name and secret)",
                )
                webbrowser.open(str(manager_url / "app/user"))
                api_token = input("API token: ")
                api_secret = getpass.getpass("API secret: ")
                store_credentials(
                    api_token,
                    api_secret,
                    manager_api_url,
                    credentials_path,
                )

        api_token = cast(str, api_token)
        api_secret = cast(str, api_secret)
        try:
            ctx.obj["AUTH"] = PresetAuth(manager_api_url, api_token, api_secret)
        except JWTTokenError as excinfo:
            error_message = (
                "Failed to auth using the provided credentials."
                " Please run ``preset-cli auth``"
            )
            raise CLIError(error_message, 1) from excinfo

    if not workspaces and ctx.invoked_subcommand == "superset" and not is_help():
        client = PresetClient(ctx.obj["MANAGER_URL"], ctx.obj["AUTH"])
        click.echo("Choose one or more workspaces (eg: 1-3,5,8-):")
        i = 0
        hostnames = []
        for team in client.get_teams():
            click.echo(f'\n# {team["title"]} #')
            for workspace in client.get_workspaces(team_name=team["name"]):
                status = get_status_icon(workspace["workspace_status"])
                click.echo(f'{status} ({i + 1}) {workspace["title"]}')
                hostnames.append("https://" + workspace["hostname"])
                i += 1

        if i == 0:
            raise CLIError("No workspaces available", 1)
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
@click.option("--baseurl", default="https://api.app.preset.io/")
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
@raise_cli_errors
def auth(baseurl: str, overwrite: bool = False, show: bool = False) -> None:
    """
    Store credentials for auth.
    """
    credentials_path = get_credentials_path()

    if show:
        if not credentials_path.exists():
            error_message = (
                f"The file {credentials_path} doesn't exist. "
                "Run ``preset-cli auth`` to create it."
            )
            raise CLIError(error_message, 1)

        ruler = "=" * len(str(credentials_path))
        with open(credentials_path, encoding="utf-8") as input_:
            credentials = yaml.load(input_, Loader=yaml.SafeLoader)
            contents = yaml.dump(credentials)
        click.echo(f"{credentials_path}\n{ruler}\n{contents}")
        sys.exit(0)

    if credentials_path.exists() and not overwrite:
        error_message = (
            f"The file {credentials_path} already exists. "
            "Pass ``--overwrite`` to replace it."
        )
        raise CLIError(error_message, 1)

    manager_url = URL(baseurl.replace("api.", "manage."))
    manager_api_url = URL(baseurl)
    click.echo(
        f"Please generate a new token at {manager_url} if you don't have one already",
    )
    webbrowser.open(str(manager_url / "app/user"))
    api_token = input("API token: ")
    api_secret = getpass.getpass("API secret: ")

    store_credentials(api_token, api_secret, manager_api_url, credentials_path)
    click.echo(f"Credentials stored in {credentials_path}")


def get_teams(client: PresetClient) -> List[str]:
    """
    Prompt users for teams.
    """
    click.echo("Choose one or more teams (eg: 1-3,5,8-):")
    i = 0
    all_teams = []
    for team in client.get_teams():
        click.echo(f'({i + 1}) {team["title"]}')
        all_teams.append(team["name"])
        i += 1

    if i == 0:
        raise CLIError("No teams available", 1)
    if i == 1:
        return all_teams

    while True:
        try:
            choices = parse_selection(input("> "), i)
            return [all_teams[choice - 1] for choice in choices]
        except Exception:  # pylint: disable=broad-except
            click.echo("Invalid choice")


@click.command()
@click.option("--teams", callback=split_comma)
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="users.yaml",
)
@click.pass_context
@raise_cli_errors
def invite_users(ctx: click.core.Context, teams: List[str], path: str) -> None:
    """
    Invite users to join Preset teams.
    """
    client = PresetClient(ctx.obj["MANAGER_URL"], ctx.obj["AUTH"])

    if not teams:
        teams = get_teams(client)

    with open(path, encoding="utf-8") as input_:
        config = yaml.load(input_, Loader=yaml.SafeLoader)
        emails = [user["email"] for user in config]
        client.invite_users(teams, emails)


@click.command()
@click.option("--teams", callback=split_comma)
@click.option(
    "--save-report",
    help="Save results to a YAML or CSV file instead of priting on the terminal",
)
@click.pass_context
@raise_cli_errors
def list_group_membership(
    ctx: click.core.Context,
    teams: List[str],
    save_report: str,
) -> None:
    """
    List SCIM/user groups from Preset team(s)
    """
    client = PresetClient(ctx.obj["MANAGER_URL"], ctx.obj["AUTH"])
    if not teams:
        # prompt the user to specify the team(s), in case not specified via the `--teams` option
        teams = get_teams(client)

    # in case --save-report was used, confirm if a valid option was used before sending requests
    if save_report and save_report.casefold() not in {"yaml", "csv"}:
        raise CLIError(
            "Invalid option. Please use ``--save-report=csv`` or ``--save-report=yaml``",
            1,
        )

    for team in teams:
        # print the team name in case multiple teams were provided and it's not an export
        if not save_report and len(teams) > 1:
            click.echo(f"## Team {team} ##")

        # defining default start_at and group_count to execute it at least once
        start_at = 1
        group_count = 100

        # account for pagination
        while start_at <= group_count:
            groups = client.get_group_membership(team, start_at)
            group_count = groups["totalResults"]

            if group_count > 0:
                # print groups in console
                if not save_report:
                    print_group_membership(groups)

                # write report to a YAML file
                elif save_report.casefold() == "yaml":
                    export_group_membership_yaml(groups, team)

                # write report to a CSV file
                else:
                    export_group_membership_csv(groups, team)

            else:
                click.echo(f"Team {team} has no SCIM groups\n")

            # increment start_at in case a new page is needed
            start_at += 100


def print_group_membership(groups: Dict[str, Any]) -> None:
    """
    Print group membership on the terminal
    """
    for group in groups["Resources"]:
        click.echo(f'\nName: {group["displayName"]} ID: {group["id"]}')
        if group.get("members"):
            for member in group["members"]:
                click.echo(
                    f'# User: {member["display"]} Username: {member["value"]}',
                )
        else:
            click.echo("# Group with no users\n")


def export_group_membership_yaml(groups: Dict[str, Any], team: str) -> None:
    """
    Export group membership to a YAML file
    """
    yaml_name = team + "_user_group_membership.yaml"
    with open(
        yaml_name,
        "a+",
        encoding="UTF8",
    ) as yaml_creator:
        yaml.dump(groups, yaml_creator)


def export_group_membership_csv(groups: Dict[str, Any], team: str) -> None:
    """
    Export group membership to a CSV file
    """
    csv_name = team + "_user_group_membership.csv"
    for group in groups["Resources"]:
        # CSV report would include a group only in case it has members
        if group.get("members"):
            # Assure we just write headers once
            file_exists = os.path.isfile(csv_name)

            with open(csv_name, "a+", encoding="UTF8") as csv_writer:
                writer = csv.DictWriter(
                    csv_writer,
                    delimiter=",",
                    fieldnames=[
                        "Group Name",
                        "Group ID",
                        "User",
                        "Username",
                    ],
                )
                if not file_exists:
                    writer.writeheader()
                for member in group["members"]:
                    writer.writerow(
                        {
                            "Group Name": group["displayName"],
                            "Group ID": group["id"],
                            "User": member["display"],
                            "Username": member["value"],
                        },
                    )


@click.command()
@click.option("--teams", callback=split_comma)
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="users.yaml",
)
@click.pass_context
@raise_cli_errors
def import_users(ctx: click.core.Context, teams: List[str], path: str) -> None:
    """
    Import users by adding them via SCIM.
    """
    client = PresetClient(ctx.obj["MANAGER_URL"], ctx.obj["AUTH"])

    if not teams:
        teams = get_teams(client)

    with open(path, encoding="utf-8") as input_:
        users = yaml.load(input_, Loader=yaml.SafeLoader)
        client.import_users(teams, users)


@click.command()
@click.option("--teams", callback=split_comma)
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="user_roles.yaml",
)
@click.pass_context
def sync_roles(ctx: click.core.Context, teams: List[str], path: str) -> None:
    """
    Sync user roles (team, workspace, and data access).
    """
    client = PresetClient(ctx.obj["MANAGER_URL"], ctx.obj["AUTH"])

    if not teams:
        teams = get_teams(client)

    with open(path, encoding="utf-8") as input_:
        user_roles = yaml.load(input_, Loader=yaml.SafeLoader)

    for team_name in teams:
        workspaces = client.get_workspaces(team_name)
        sync_all_user_roles_to_team(client, team_name, user_roles, workspaces)


def sync_all_user_roles_to_team(  # pylint: disable=too-many-locals
    client: PresetClient,
    team_name: str,
    user_roles: List[Dict[str, Any]],
    workspaces: List[Dict[str, Any]],
) -> None:
    """
    Sync all user roles to a given team.
    """
    workspace_names = {
        workspace["title"]: workspace["name"] for workspace in workspaces
    }
    workspace_hostnames = {
        workspace["name"]: workspace["hostname"] for workspace in workspaces
    }

    users = client.get_team_members(team_name)
    user_ids = {user["user"]["email"]: user["user"]["id"] for user in users}

    for user in user_roles:
        user["id"] = user_ids[user["email"]]
        sync_user_roles_to_team(client, team_name, user, workspaces)

    # collect DAR roles so we can do a single request per workspace
    data_access_roles: DefaultDict[str, DefaultDict[str, Set]] = defaultdict(
        lambda: defaultdict(set),
    )
    for user in user_roles:
        for workspace_name, workspace_roles in user["workspaces"].items():
            # allow either a workspace name or title
            if workspace_name in workspace_names:
                workspace_name = workspace_names[workspace_name]
            workspace_hostname = workspace_hostnames[workspace_name]

            for data_access_role in workspace_roles.get("data_access_roles", []):
                data_access_roles[workspace_hostname][data_access_role].add(
                    user["email"],
                )

    for workspace_hostname, workspace_data_access_roles in data_access_roles.items():
        superset_client = SupersetClient(f"https://{workspace_hostname}/", client.auth)

        user_id_map = {
            user["email"]: user["id"] for user in superset_client.export_users()
        }
        for data_access_role, user_emails in workspace_data_access_roles.items():
            role_id = superset_client.get_role_id(data_access_role)
            workspace_user_ids = [user_id_map[email] for email in user_emails]
            superset_client.update_role(role_id, user=workspace_user_ids)


def sync_user_roles_to_team(
    client: PresetClient,
    team_name: str,
    user: Dict[str, Any],
    workspaces: List[Dict[str, Any]],
) -> None:
    """
    Sync roles from a single user to a given team.
    """
    workspace_names = {
        workspace["title"]: workspace["name"] for workspace in workspaces
    }
    workspace_ids = {workspace["name"]: workspace["id"] for workspace in workspaces}

    team_role = user["team_role"].lower()
    user_email = user["email"]
    user_id = user["id"]

    if team_role == "user":
        role_id = 2
    elif team_role == "admin":
        role_id = 1
    else:
        raise Exception(f"Invalid role {team_role.title()} for user {user_email}")
    _logger.info(
        "Setting team role of user %s to %s (%s) in team %s",
        user_email,
        role_id,
        team_role.title(),
        team_name,
    )
    client.change_team_role(team_name, user_id, role_id)

    for workspace_name, workspace_roles in user["workspaces"].items():
        # allow either a workspace name or title
        if workspace_name in workspace_names:
            workspace_name = workspace_names[workspace_name]
        workspace_id = workspace_ids[workspace_name]

        sync_user_role_to_workspace(
            client,
            team_name,
            user,
            workspace_id,
            workspace_roles,
        )


def sync_user_role_to_workspace(
    client: PresetClient,
    team_name: str,
    user: Dict[str, Any],
    workspace_id: int,
    workspace_roles: Dict[str, Any],
) -> None:
    """
    Sync user role to a given workspace.
    """
    workspace_role = workspace_roles["workspace_role"].lower()
    role_identifier = workspace_role_identifiers[workspace_role]

    _logger.info(
        "Setting workspace role of user %s to %s (%s)",
        user["email"],
        role_identifier,
        workspace_role.title(),
    )
    client.change_workspace_role(
        team_name,
        workspace_id,
        user["id"],
        role_identifier,
    )


preset_cli.add_command(auth, name="auth")
preset_cli.add_command(invite_users, name="invite-users")
preset_cli.add_command(import_users, name="import-users")
preset_cli.add_command(sync_roles)
preset_cli.add_command(superset)
preset_cli.add_command(list_group_membership, name="list-group-membership")
