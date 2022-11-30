"""
A simple client for interacting with the Preset API.
"""

import json
import logging
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional, Union

from bs4 import BeautifulSoup
from yarl import URL

from preset_cli import __version__
from preset_cli.auth.main import Auth
from preset_cli.lib import validate_response
from preset_cli.typing import UserType

_logger = logging.getLogger(__name__)


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

        self.session = auth.session
        self.session.headers.update(auth.get_headers())
        self.session.headers["User-Agent"] = "Preset CLI"
        self.session.headers["X-Client-Version"] = __version__

    def get_teams(self) -> List[Any]:
        """
        Retrieve all teams based on membership.
        """
        url = self.get_base_url() / "teams"
        _logger.debug("GET %s", url)
        response = self.session.get(url)
        validate_response(response)

        payload = response.json()
        teams = payload["payload"]

        return teams

    def get_team_members(self, team_name: str) -> List[Any]:
        """
        Retrieve all users for a given team.
        """
        url = self.get_base_url() / "teams" / team_name / "memberships"
        _logger.debug("GET %s", url)
        response = self.session.get(url)
        validate_response(response)

        payload = response.json()
        users = payload["payload"]

        return users

    def get_workspaces(self, team_name: str) -> List[Any]:
        """
        Retrieve all workspaces for a given team.
        """
        url = self.get_base_url() / "teams" / team_name / "workspaces"
        _logger.debug("GET %s", url)
        response = self.session.get(url)
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
        for team in teams:
            url = self.get_base_url() / "teams" / team / "invites/many"
            payload = {
                "invites": [
                    {"team_role_id": role_id, "email": email} for email in emails
                ],
            }
            _logger.debug("POST %s\n%s", url, json.dumps(payload, indent=4))
            response = self.session.post(url, json=payload)
            validate_response(response)

    # pylint: disable=too-many-locals
    def export_users(self, workspace_url: URL) -> Iterator[UserType]:
        """
        Return all users from a given workspace.
        """
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
            self.get_base_url()
            / "teams"
            / team_name
            / "workspaces"
            / str(workspace_id)
            / "memberships"
        )
        _logger.debug("GET %s", url)
        response = self.session.get(url)
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
        _logger.debug("GET %s", url)
        response = self.session.get(url)
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

    def import_users(self, teams: List[str], users: List[UserType]) -> None:
        """
        Import users by adding them via SCIM.
        """
        for team in teams:
            url = self.get_base_url() / "teams" / team / "scim/v2/Users"
            for user in users:
                payload = {
                    "schemas": [
                        "urn:ietf:params:scim:schemas:core:2.0:User",
                        "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User",
                    ],
                    "active": True,
                    "displayName": f'{user["first_name"]} {user["last_name"]}',
                    "emails": [
                        {
                            "primary": True,
                            "type": "work",
                            "value": user["email"],
                        },
                    ],
                    "meta": {"resourceType": "User"},
                    "userName": user["email"],
                    "name": {
                        "formatted": f'{user["first_name"]} {user["last_name"]}',
                        "familyName": user["last_name"],
                        "givenName": user["first_name"],
                    },
                }
                self.session.headers["Content-Type"] = "application/scim+json"
                self.session.headers["Accept"] = "application/scim+json"
                _logger.info("Importing %s", user["email"])
                _logger.debug("POST %s\n%s", url, json.dumps(payload, indent=4))
                response = self.session.post(url, json=payload)

                # ignore existing users
                if response.status_code == 409:
                    payload = response.json()
                    _logger.info(payload["detail"])
                    continue

                validate_response(response)

    def change_team_role(self, team_name: str, user_id: int, role_id: int) -> None:
        """
        Change the team role of a given user.
        """
        url = self.get_base_url() / "teams" / team_name / "memberships" / str(user_id)
        payload = {"team_role_id": role_id}
        _logger.debug("PATCH %s\n%s", url, json.dumps(payload, indent=4))
        self.session.patch(url, json=payload)

    def change_workspace_role(
        self,
        team_name: str,
        workspace_id: int,
        user_id: int,
        role_identifier: str,
    ) -> None:
        """
        Change the workspace role of a given user.
        """
        url = (
            self.get_base_url()
            / "teams"
            / team_name
            / "workspaces"
            / str(workspace_id)
            / "membership"
        )
        payload = {"role_identifier": role_identifier, "user_id": user_id}
        _logger.debug("PUT %s\n%s", url, json.dumps(payload, indent=4))
        self.session.put(url, json=payload)

    def get_base_url(self, version: Optional[str] = "v1") -> URL:
        """
        Return the base URL for API calls.
        """
        return self.baseurl / version

    def get_group_membership(
        self,
        team_name: str,
        page: int,
    ) -> Dict[str, Any]:
        """
        Lists all user/SCIM groups associated with a team
        """
        url = (
            self.get_base_url()
            / "teams"
            / team_name
            / "scim/v2/Groups"
            % {"startIndex": str(page)}
        )
        self.session.headers["Accept"] = "application/scim+json"
        _logger.debug("GET %s", url)
        response = self.session.get(url)
        return response.json()
