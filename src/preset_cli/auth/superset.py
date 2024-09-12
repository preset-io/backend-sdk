"""
Mechanisms for authentication and authorization for Superset instances.
"""

from typing import Dict, Optional

from yarl import URL

from preset_cli.auth.main import Auth
from preset_cli.auth.token import TokenAuth


class UsernamePasswordAuth(Auth):  # pylint: disable=too-few-public-methods
    """
    Auth to Superset via username/password.
    """

    def __init__(self, baseurl: URL, username: str, password: Optional[str] = None):
        super().__init__()

        self.csrf_token: Optional[str] = None
        self.baseurl = baseurl
        self.username = username
        self.password = password
        self.auth()

    def get_headers(self) -> Dict[str, str]:
        return {"X-CSRFToken": self.csrf_token} if self.csrf_token else {}

    def get_access_token(self):
        body = {"username": self.username, "password": self.password, "provider": "ldap"}
        response = self.session.post(self.baseurl / "api/v1/security/login", json=body)
        response.raise_for_status()
        return response.json()["access_token"]

    def get_csrf_token(self):
        response = self.session.get(self.baseurl / "api/v1/security/csrf_token/")
        response.raise_for_status()
        return response.json()["result"]

    def auth(self) -> None:
        """
        Login to get CSRF token and cookies.
        """
        self.session.headers = {"Authorization": f"Bearer {self.get_access_token()}"}
        csrf_token = self.get_csrf_token()

        if csrf_token:
            self.session.headers["X-CSRFToken"] = csrf_token
            self.session.headers["Referer"] = self.baseurl / "/api/v1/security/csrf_token/"
            self.csrf_token = csrf_token


class SupersetJWTAuth(TokenAuth):  # pylint: disable=abstract-method
    """
    Auth to Superset via JWT token.
    """

    def __init__(self, token: str, baseurl: URL):
        super().__init__(token)
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
        return {
            "Authorization": f"Bearer {self.token}",
            "X-CSRFToken": self.get_csrf_token(self.token),
        }
