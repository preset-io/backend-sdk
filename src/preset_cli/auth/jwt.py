"""
JWT auth.
"""

import yaml

from preset_cli.auth.lib import get_access_token, get_credentials_path
from preset_cli.auth.token import TokenAuth


class JWTAuth(TokenAuth):  # pylint: disable=too-few-public-methods
    """
    Auth via JWT.
    """

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

        token = get_access_token(**credentials)

        return JWTAuth(token)
