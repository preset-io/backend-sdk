"""
A simple client for interacting with the Preset API.
"""

from typing import Any, List, Union

from superset_sdk.auth.main import Auth
from yarl import URL

from preset_cli import __version__


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
        response.raise_for_status()

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
        response.raise_for_status()

        payload = response.json()
        workspaces = payload["payload"]

        return workspaces
