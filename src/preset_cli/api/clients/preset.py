"""
A simple client for interacting with the Preset API.
"""

from enum import Enum
from typing import Any, Iterator, List, Optional, Union

from bs4 import BeautifulSoup
from yarl import URL

from preset_cli import __version__
from preset_cli.auth.main import Auth
from preset_cli.lib import validate_response
from preset_cli.typing import UserType


class Role(int, Enum):
    """
    Roles for users.
    """

    ADMIN = 1
    USER = 2


class PresetClient:  # pylint: disable=too-few-public-methods

    """
    A client for the Preset API.
    """

    def __init__(self, baseurl: Union[str, URL], auth: Auth):
        # convert to URL if necessary
        self.baseurl = URL(baseurl)
        self.auth = auth
        self.auth.headers.update(
            {
                "User-Agent": "Preset CLI",
                "X-Client-Version": __version__,
            },
        )

    def get_teams(self) -> List[Any]:
        """
        Retrieve all teams based on membership.
        """
        session = self.auth.get_session()
        headers = self.auth.get_headers()
        response = session.get(self.baseurl / "api/v1/teams/", headers=headers)
        validate_response(response)

        payload = response.json()
        teams = payload["payload"]

        return teams

    def get_workspaces(self, team_name: str) -> List[Any]:
        """
        Retrieve all workspaces for a given team.
        """
        session = self.auth.get_session()
        headers = self.auth.get_headers()
        response = session.get(
            self.baseurl / "api/v1/teams" / team_name / "workspaces/",
            headers=headers,
        )
        validate_response(response)

        payload = response.json()
        workspaces = payload["payload"]

        return workspaces

    def invite_users(
        self,
        teams: List[str],
        emails: List[str],
        role_id=Role.USER,
    ) -> None:
        """
        Invite users to teams.
        """
        session = self.auth.get_session()
        headers = self.auth.get_headers()

        for team in teams:
            response = session.post(
                self.baseurl / "api/v1/teams" / team / "invites/many",
                headers=headers,
                json={
                    "invites": [
                        {"team_role_id": role_id, "email": email} for email in emails
                    ],
                },
            )
            validate_response(response)

    # pylint: disable=too-many-locals
    def export_users(self, workspace_url: URL) -> Iterator[UserType]:
        """
        Return all users from a given workspace.
        """
        session = self.auth.get_session()
        headers = self.auth.get_headers()

        team_name: Optional[str] = None
        workspace_id: Optional[int] = None

        for team in self.get_teams():
            for workspace in self.get_workspaces(team["name"]):
                if workspace_url.host == workspace["hostname"]:
                    team_name = team["name"]
                    workspace_id = workspace["id"]
                    break

        if team_name is None or workspace_id is None:
            raise Exception("Unable to find workspace and/or team")

        url = (
            self.baseurl
            / "api/v1/teams"
            / team_name
            / "workspaces"
            / str(workspace_id)
            / "memberships"
        )
        response = session.get(url, headers=headers)
        team_members: List[UserType] = [
            {
                "id": 0,
                "username": payload["user"]["username"],
                "role": [],  # TODO (betodealmeida)
                "first_name": payload["user"]["first_name"],
                "last_name": payload["user"]["last_name"],
                "email": payload["user"]["email"],
            }
            for payload in response.json()["payload"]
        ]

        # TODO (betodealmeida): improve this
        url = workspace_url / "roles/add"
        response = session.get(url, headers=headers)
        soup = BeautifulSoup(response.text, features="html.parser")
        select = soup.find("select", id="user")
        ids = {
            option.text: int(option.attrs["value"])
            for option in select.find_all("option")
        }

        for team_member in team_members:
            # pylint: disable=consider-using-f-string
            full_name = "{first_name} {last_name}".format(**team_member)
            if full_name in ids:
                team_member["id"] = ids[full_name]
                yield team_member
