"""
Token auth.
"""

from typing import Dict

from preset_cli.auth.main import Auth


class TokenAuth(Auth):  # pylint: disable=too-few-public-methods
    """
    Auth via a token.
    """

    def __init__(self, token: str):
        super().__init__()
        self.token = token

    def get_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}
