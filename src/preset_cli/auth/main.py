"""
Mechanisms for authentication and authorization.
"""

from typing import Dict, Optional

import requests
from bs4 import BeautifulSoup
from yarl import URL


class Auth:  # pylint: disable=too-few-public-methods
    """
    An authentication/authorization mechanism.
    """

    def __init__(self):
        self.session = requests.Session()
        self.headers = {}

    def get_session(self) -> requests.Session:
        """
        Return a session.
        """
        return self.session

    def get_headers(self) -> Dict[str, str]:  # pylint: disable=no-self-use
        """
        Return headers for auth.
        """
        return self.headers


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
        csrf_token = soup.find("input", {"id": "csrf_token"})["value"]

        # update headers
        self.headers["X-CSRFToken"] = csrf_token

        # set cookies
        self.session.post(
            baseurl / "login/",
            data=dict(username=username, password=password, csrf_token=csrf_token),
        )
