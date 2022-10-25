"""
Preset auth.
"""

from typing import Dict

import yaml
from yarl import URL

from preset_cli.auth.lib import get_access_token, get_credentials_path
from preset_cli.auth.main import Auth


class JWTTokenError(Exception):
    """
    Exception raised when fetching the JWT fails.
    """


class PresetAuth(Auth):  # pylint: disable=too-few-public-methods
    """
    Auth via Preset access token and secret.

    Automatically refreshes the JWT as needed.
    """

    def __init__(self, baseurl: URL, api_token: str, api_secret: str):
        super().__init__()

        self.baseurl = baseurl
        self.api_token = api_token
        self.api_secret = api_secret
        self.auth()

    def get_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def auth(self) -> None:
        """
        Fetch the JWT and store it.
        """
        try:
            self.token = get_access_token(self.baseurl, self.api_token, self.api_secret)
        except Exception as ex:  # pylint: disable=broad-except
            raise JWTTokenError("Unable to fetch JWT") from ex

    @classmethod
    def from_stored_credentials(cls) -> "PresetAuth":
        """
        Build auth from stored credentials.
        """
        credentials_path = get_credentials_path()
        if not credentials_path.exists():
            raise Exception(f"Could not load credentials from {credentials_path}")

        with open(credentials_path, encoding="utf-8") as input_:
            credentials = yaml.load(input_, Loader=yaml.SafeLoader)

        return PresetAuth(**credentials)
