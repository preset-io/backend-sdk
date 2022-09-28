"""
Mechanisms for authentication and authorization.
"""

from typing import Optional

from bs4 import BeautifulSoup
from yarl import URL

from preset_cli.auth.main import Auth


class UsernamePasswordAuth(Auth):  # pylint: disable=too-few-public-methods
    """
    Auth via username/password.
    """

    def __init__(self, baseurl: URL, username: str, password: Optional[str] = None):
        super().__init__()
        self._do_login(baseurl, username, password)

    def _do_login(
        self,
        baseurl: URL,
        username: str,
        password: Optional[str] = None,
    ) -> None:
        """
        Login to get CSRF token and cookies.
        """
        response = self.session.get(baseurl / "login/")
        soup = BeautifulSoup(response.text, "html.parser")
        input_ = soup.find("input", {"id": "csrf_token"})
        csrf_token = input_["value"] if input_ else None

        data = {"username": username, "password": password}

        if csrf_token:
            self.headers["X-CSRFToken"] = csrf_token
            data["csrf_token"] = csrf_token

        # set cookies
        self.session.post(baseurl / "login/", data=data)
