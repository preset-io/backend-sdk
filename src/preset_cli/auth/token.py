"""
Token auth.
"""

from typing import Dict, Optional

from yarl import URL

from preset_cli.auth.main import Auth


class TokenAuth(Auth):  # pylint: disable=too-few-public-methods, abstract-method
    """
    Auth via a token.
    """

    def __init__(self, token: str, baseurl: Optional[URL] = None):
        super().__init__()
        self.token = token
        self.baseurl = baseurl

    def get_csrf_token(self, jwt: str) -> str:
        """
        Get a CSRF token.
        """
        response = self.session.get(
            self.baseurl / "api/v1/security/csrf_token/",  # type: ignore
            headers={"Authorization": f"Bearer {jwt}"},
        )
        response.raise_for_status()
        payload = response.json()
        return payload["result"]

    def get_headers(self) -> Dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.token}",
        }
        if self.baseurl:
            headers["X-CSRFToken"] = self.get_csrf_token(self.token)
        return headers
