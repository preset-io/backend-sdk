"""
Command to export user information for all workspaces.
"""

import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set

import click
import yaml

from preset_cli.api.clients.preset import PresetClient
from preset_cli.lib import raise_cli_errors

_logger = logging.getLogger(__name__)


def get_filtered_teams(client: PresetClient, teams: Set[str]) -> List[Dict[str, Any]]:
    """
    Get all teams or filter by specified teams.

    Args:
        client: PresetClient instance
        teams: Set of team names/titles to filter by

    Returns:
        List of filtered team dictionaries
    """
    return [
        team
        for team in client.get_teams()
        if not teams or team["name"] in teams or team["title"] in teams
    ]


def process_team_members(
    client: PresetClient,
    team_name: str,
    team_title: str,
    user_data: Dict[str, Dict[str, Any]],
    team_role_map: Dict[int, str],
) -> None:
    """
    Process team members and add their team roles to user data.

    Args:
        client: PresetClient instance
        team_name: Internal team name
        team_title: Display team title
        user_data: User data dictionary to update
        team_role_map: Mapping of role IDs to role names
    """
    try:
        team_members = client.get_team_members(team_name)

        for member in team_members:
            user_info = member["user"]
            email = user_info["email"]

            # Update user basic info if not already set
            if not user_data[email]["email"]:  # pragma: no cover
                user_data[email]["email"] = email
                user_data[email]["first_name"] = user_info.get("first_name", "")
                user_data[email]["last_name"] = user_info.get("last_name", "")
                user_data[email]["username"] = user_info.get("username", email)

            # Add team role
            team_role_id = member.get("team_role_id", 2)  # Default to user role
            user_data[email]["workspaces"][f"_team_{team_title}"] = {
                "team_role": team_role_map.get(team_role_id, "user"),
            }
    except Exception as exc:  # pylint: disable=broad-except
        _logger.warning("Failed to get team members for %s: %s", team_name, exc)
        click.echo(f"  Warning: Failed to get team members: {exc}")


# pylint: disable=too-many-arguments,too-many-locals
def _process_membership_page(
    client: PresetClient,
    team_name: str,
    workspace_id: int,
    page_number: int,
) -> Dict[str, Any]:
    """
    Fetch and return a single page of workspace memberships.

    Args:
        client: PresetClient instance
        team_name: Internal team name
        workspace_id: Workspace ID
        page_number: Page number to fetch

    Returns:
        API response payload containing memberships and metadata
    """
    params = {"page_number": page_number, "page_size": 250}
    url = (
        client.get_base_url()
        / "teams"
        / team_name
        / "workspaces"
        / str(workspace_id)
        / "memberships"
        % params
    )

    response = client.session.get(url)
    response.raise_for_status()
    return response.json()


def _process_membership_data(
    membership: Dict[str, Any],
    team_title: str,
    workspace_title: str,
    workspace_name: str,
    user_data: Dict[str, Dict[str, Any]],
    workspace_role_map: Dict[str, str],
) -> None:
    """
    Process a single membership record and update user data.

    Args:
        membership: Single membership record from API
        team_title: Display team title
        workspace_title: Display workspace title
        workspace_name: Internal workspace name
        user_data: User data dictionary to update
        workspace_role_map: Mapping of role identifiers to role names
    """
    user_info = membership["user"]
    email = user_info["email"]

    # Extract role_identifier from nested workspace_role object
    workspace_role_obj = membership.get("workspace_role", {})
    role_identifier = workspace_role_obj.get(
        "role_identifier",
        "PresetNoAccess",
    )
    role_name = workspace_role_obj.get("name", "")

    # Update user basic info if not already set
    if not user_data[email]["email"]:
        user_data[email]["email"] = email
        user_data[email]["first_name"] = user_info.get("first_name", "")
        user_data[email]["last_name"] = user_info.get("last_name", "")
        user_data[email]["username"] = user_info.get("username", email)

    # Add workspace role
    workspace_role = workspace_role_map.get(
        role_identifier,
        f"unknown:{role_identifier}",
    )
    _logger.debug(
        "User %s in %s/%s has role_identifier=%s (name=%s) -> workspace_role=%s",
        email,
        team_title,
        workspace_title,
        role_identifier,
        role_name,
        workspace_role,
    )

    # Skip only if explicitly no access roles, include everything else for now
    if role_identifier not in ("PresetNoAccess", "NoAccess"):
        workspace_key = f"{team_title}/{workspace_title}"
        user_data[email]["workspaces"][workspace_key] = {
            "workspace_role": workspace_role,
            "workspace_name": workspace_name,
            "team": team_title,
        }
    else:
        _logger.debug(
            "Skipping workspace %s/%s for user %s (no access: %s)",
            team_title,
            workspace_title,
            email,
            role_identifier,
        )


def process_workspace_memberships(
    client: PresetClient,
    team_name: str,
    team_title: str,
    workspace: Dict[str, Any],
    user_data: Dict[str, Dict[str, Any]],
    workspace_role_map: Dict[str, str],
) -> None:
    """
    Process workspace memberships with pagination.

    Args:
        client: PresetClient instance
        team_name: Internal team name
        team_title: Display team title
        workspace: Workspace dictionary with id, title, name
        user_data: User data dictionary to update
        workspace_role_map: Mapping of role identifiers to role names
    """
    workspace_id = workspace["id"]
    workspace_title = workspace["title"]
    workspace_name = workspace["name"]

    _logger.info("  Processing workspace: %s", workspace_title)
    click.echo(f"  Processing workspace: {workspace_title}")

    page_number = 1
    while True:
        try:
            payload = _process_membership_page(
                client,
                team_name,
                workspace_id,
                page_number,
            )

            for membership in payload.get("payload", []):
                _process_membership_data(
                    membership,
                    team_title,
                    workspace_title,
                    workspace_name,
                    user_data,
                    workspace_role_map,
                )

            # Check if there are more pages
            if payload["meta"]["count"] <= page_number * 250:
                break
            page_number += 1

        except Exception as exc:  # pylint: disable=broad-except
            _logger.warning(
                "Failed to get workspace memberships for %s/%s: %s",
                team_title,
                workspace_title,
                exc,
            )
            click.echo(f"    Warning: Failed to get workspace memberships: {exc}")
            break


def process_team_workspaces(
    client: PresetClient,
    team_name: str,
    team_title: str,
    user_data: Dict[str, Dict[str, Any]],
    workspace_role_map: Dict[str, str],
) -> None:
    """
    Process all workspaces for a team.

    Args:
        client: PresetClient instance
        team_name: Internal team name
        team_title: Display team title
        user_data: User data dictionary to update
        workspace_role_map: Mapping of role identifiers to role names
    """
    try:
        workspaces = client.get_workspaces(team_name)

        for workspace in workspaces:
            process_workspace_memberships(
                client,
                team_name,
                team_title,
                workspace,
                user_data,
                workspace_role_map,
            )

    except Exception as exc:  # pylint: disable=broad-except
        _logger.warning("Failed to get workspaces for %s: %s", team_name, exc)
        click.echo(f"  Warning: Failed to get workspaces: {exc}")


def convert_user_data_to_list(
    user_data: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Convert user data dictionary to a list and separate team/workspace roles.

    Args:
        user_data: Dictionary of user data indexed by email

    Returns:
        List of user dictionaries with separated teams and workspaces
    """
    users_list = []
    for data in sorted(user_data.values(), key=lambda user: user["email"]):
        # Separate team entries from workspace entries
        team_roles = {}
        workspace_roles = {}

        for key, value in data["workspaces"].items():
            if key.startswith("_team_"):
                team_name = key.replace("_team_", "")
                team_roles[team_name] = value["team_role"]
            else:
                workspace_roles[key] = value

        user_entry = {
            "email": data["email"],
            "first_name": data["first_name"],
            "last_name": data["last_name"],
            "username": data["username"],
        }

        # Only add teams/workspaces if they exist
        if team_roles:
            user_entry["teams"] = team_roles
        if workspace_roles:
            user_entry["workspaces"] = workspace_roles

        # Only include users with at least one role
        if "teams" in user_entry or "workspaces" in user_entry:  # pragma: no cover
            users_list.append(user_entry)

    return users_list


def write_users_to_file(users_list: List[Dict[str, Any]], path: str) -> None:
    """
    Write users list to YAML file.

    Args:
        users_list: List of user dictionaries
        path: Output file path
    """
    output_path = Path(path)
    with open(output_path, "w", encoding="utf-8") as output_file:
        yaml.dump(users_list, output_file, default_flow_style=False, sort_keys=False)

    click.echo(f"\nExported {len(users_list)} users to {output_path}")
    _logger.info("Exported %d users to %s", len(users_list), output_path)


@click.command()
@click.argument(
    "path",
    type=click.Path(resolve_path=True),
    default="users_workspace_roles.yaml",
)
@click.option("--teams", multiple=True, help="Specific teams to export (optional)")
@click.pass_context
@raise_cli_errors
def export_users(
    ctx: click.core.Context,
    path: str,
    teams: tuple,
) -> None:
    """
    Export users and their roles for all workspaces.

    This command exports a YAML file containing user information
    and their roles in each workspace across all teams.
    """
    auth = ctx.obj["AUTH"]
    manager_url = ctx.obj["MANAGER_URL"]
    client = PresetClient(manager_url, auth)

    # Get filtered teams
    filtered_teams = get_filtered_teams(client, set(teams))

    if not filtered_teams:
        click.echo("No teams found.")
        return

    # Initialize user data storage
    user_data: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "email": None,
            "first_name": None,
            "last_name": None,
            "username": None,
            "workspaces": {},
        },
    )

    # Role mapping dictionaries
    workspace_role_map = {
        "Admin": "workspace admin",
        "PresetAlpha": "primary contributor",
        "PresetBeta": "secondary contributor",
        "PresetGamma": "limited contributor",
        "PresetReportsOnly": "viewer",
        "PresetDashboardsOnly": "dashboard viewer",
        "PresetNoAccess": "no access",
        # Additional possible role identifiers
        "Alpha": "primary contributor",
        "Beta": "secondary contributor",
        "Gamma": "limited contributor",
        "ReportsOnly": "viewer",
        "DashboardsOnly": "dashboard viewer",
        "NoAccess": "no access",
    }

    team_role_map = {
        1: "admin",
        2: "user",
    }

    # Process each team
    for team in filtered_teams:
        team_name = team["name"]
        team_title = team["title"]

        _logger.info("Processing team: %s", team_title)
        click.echo(f"Processing team: {team_title}")

        # Process team members and their roles
        process_team_members(client, team_name, team_title, user_data, team_role_map)

        # Process workspaces and their memberships
        process_team_workspaces(
            client,
            team_name,
            team_title,
            user_data,
            workspace_role_map,
        )

    # Convert user data to final format and write to file
    users_list = convert_user_data_to_list(user_data)
    write_users_to_file(users_list, path)
