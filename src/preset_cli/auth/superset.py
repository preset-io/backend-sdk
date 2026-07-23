"""
Mechanisms for authentication and authorization for Superset instances.
"""

from __future__ import annotations

import logging

from bs4 import BeautifulSoup
from requests import Session
from requests.exceptions import RequestException
from yarl import URL

from preset_cli.auth.main import Auth
from preset_cli.auth.token import TokenAuth

_logger = logging.getLogger(__name__)


def get_access_token(
    session: Session,
    baseurl: URL,
    username: str,
    password: str | None,
    provider: str,
) -> str:
    """
    Fetch a JWT access token from Superset's security API.
    """
    session.headers.pop("Referer", None)
    response = session.post(
        baseurl / "api/v1/security/login",
        json={
            "username": username,
            "password": password,
            "provider": provider,
        },
    )
    response.raise_for_status()
    return response.json()["access_token"]


def get_csrf_token(
    session: Session,
    baseurl: URL,
    token: str | None = None,
) -> str:
    """
    Fetch a CSRF token from Superset's security API.
    """
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    response = session.get(
        baseurl / "api/v1/security/csrf_token/",
        headers=headers,
    )
    response.raise_for_status()
    return response.json()["result"]


class UsernamePasswordAuth(Auth):  # pylint: disable=too-few-public-methods
    """
    Auth to Superset via username/password.

    Authenticates against the documented security API, obtaining a JWT access
    token and a CSRF token. Falls back to the legacy HTML-scraping login flow
    for older Superset instances that do not expose ``/api/v1/security/login``.
    """

    def __init__(
        self,
        baseurl: URL,
        username: str,
        password: str | None = None,
        provider: str = "db",
    ):
        super().__init__()

        self.baseurl = baseurl
        self.username = username
        self.password = password
        self.provider = provider or "db"

        self.token: str | None = None
        self.csrf_token: str | None = None
        self.auth()

    def get_headers(self) -> dict[str, str]:
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if self.csrf_token:
            headers["X-CSRFToken"] = self.csrf_token
        return headers

    def auth(self) -> None:
        """
        Authenticate via the security API, falling back to the legacy flow.
        """
        try:
            self.token = get_access_token(
                self.session,
                self.baseurl,
                self.username,
                self.password,
                self.provider,
            )
            self.csrf_token = get_csrf_token(
                self.session,
                self.baseurl,
                self.token,
            )
        except (RequestException, KeyError, ValueError) as ex:
            _logger.warning(
                "API authentication failed (%s); falling back to legacy "
                "HTML-based login flow.",
                ex,
            )
            self.token = None
            self._legacy_auth()

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

    def get_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "X-CSRFToken": get_csrf_token(self.session, self.baseurl, self.token),
        }
