"""
Mechanisms for authentication and authorization for Superset instances.
"""

from typing import Dict, Optional

from bs4 import BeautifulSoup
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

    def auth(self) -> None:
        """
        Login to get CSRF token and cookies.
        """
        data = {"username": self.username, "password": self.password}

        response = self.session.get(self.baseurl / "login/")
        soup = BeautifulSoup(response.text, "html.parser")
        input_ = soup.find("input", {"id": "csrf_token"})
        csrf_token = input_["value"] if input_ else None
        if csrf_token:
            self.session.headers["X-CSRFToken"] = csrf_token
            data["csrf_token"] = csrf_token
            self.csrf_token = csrf_token

        # set cookies
        self.session.post(self.baseurl / "login/", data=data)


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
        url = self.baseurl / "api/v1/security/csrf_token/"
        url = str(url) + "/"
        response = self.session.get(
            url,
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
