"""
JWT auth.
"""

from typing import Dict

import yaml

from preset_cli.auth.lib import get_access_token, get_credentials_path
from preset_cli.auth.main import Auth


class JWTAuth(Auth):  # pylint: disable=too-few-public-methods
    """
    Auth via JWT.
    """

    def __init__(self, jwt_token: str):
        super().__init__()
        self.jwt_token = jwt_token

    def get_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.jwt_token}"}

    @classmethod
    def from_stored_credentials(cls) -> "JWTAuth":
        """
        Build auth from stored credentials.
        """
        credentials_path = get_credentials_path()
        if not credentials_path.exists():
            raise Exception(f"Could not load credentials from {credentials_path}")

        with open(credentials_path, encoding="utf-8") as input_:
            credentials = yaml.load(input_, Loader=yaml.SafeLoader)

        jwt_token = get_access_token(**credentials)

        return JWTAuth(jwt_token)
