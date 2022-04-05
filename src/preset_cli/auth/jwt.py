"""
JWT auth.
"""

from typing import Dict

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
