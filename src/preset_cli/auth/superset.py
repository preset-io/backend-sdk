"""
Mechanisms for authentication and authorization for Superset instances.
"""

import logging
from typing import Dict, Optional

from bs4 import BeautifulSoup
from requests.exceptions import RequestException
from yarl import URL

from preset_cli.auth.main import Auth
from preset_cli.auth.token import TokenAuth

_logger = logging.getLogger(__name__)


class UsernamePasswordAuth(Auth):  # pylint: disable=too-few-public-methods
    """
    Auth to Superset via username/password.
    """

    def __init__(
        self,
        baseurl: URL,
        username: str,
        password: Optional[str] = None,
        provider: Optional[str] = "db",
    ):
        super().__init__()

        self.csrf_token: Optional[str] = None
        self.baseurl = baseurl
        self.username = username
        self.password = password
        self.provider = provider or "db"
        self.auth()

    def get_headers(self) -> Dict[str, str]:
        return {"X-CSRFToken": self.csrf_token} if self.csrf_token else {}

    def get_access_token(self):
        """
        Get an access token from superset API: api/v1/security/login.
        """
        body = {
            "username": self.username,
            "password": self.password,
            "provider": self.provider,
        }
        if "Referer" in self.session.headers:
            del self.session.headers["Referer"]
        response = self.session.post(self.baseurl / "api/v1/security/login", json=body)
        response.raise_for_status()
        return response.json()["access_token"]

    def get_csrf_token(self):
        """
        Get a CSRF token from superset API: api/v1/security/csrf_token .
        """
        response = self.session.get(self.baseurl / "api/v1/security/csrf_token/")
        response.raise_for_status()
        return response.json()["result"]

    def auth(self) -> None:
        """
        Login to get CSRF token and cookies.

        Try the documented REST API first; fall back to the legacy HTML-scraping
        flow if the API is unavailable (e.g. on older Superset versions that do
        not expose ``/api/v1/security/login``).
        """
        try:
            self.session.headers["Authorization"] = f"Bearer {self.get_access_token()}"
            csrf_token = self.get_csrf_token()
        except (RequestException, KeyError, ValueError) as ex:
            _logger.warning(
                "API authentication failed (%s); falling back to legacy "
                "HTML-based login flow.",
                ex,
            )
            self.session.headers.pop("Authorization", None)
            self._legacy_auth()
            return

        if csrf_token:
            self.session.headers["X-CSRFToken"] = csrf_token
            self.session.headers["Referer"] = str(
                self.baseurl / "api/v1/security/csrf_token/",
            )
            self.csrf_token = csrf_token

    def _legacy_auth(self) -> None:
        """
        Legacy login flow: scrape the CSRF token from ``/login/`` and POST
        credentials as form data. Kept as a fallback for older Superset
        instances that don't expose the security API.
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
