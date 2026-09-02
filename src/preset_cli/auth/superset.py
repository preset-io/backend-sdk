"""
Mechanisms for authentication and authorization for Superset instances.
"""

from __future__ import annotations

import logging
from typing import Any, cast

from bs4 import BeautifulSoup
from requests import Response, Session
from requests.exceptions import RequestException
from yarl import URL

from preset_cli.auth.main import Auth
from preset_cli.auth.token import TokenAuth

_logger = logging.getLogger(__name__)


def _is_reauth_hook(hook: Any) -> bool:
    """Return whether ``hook`` is the Auth response hook."""
    return (
        isinstance(getattr(hook, "__self__", None), Auth)
        and getattr(
            hook,
            "__name__",
            None,
        )
        == "reauth"
    )


def _request_without_reauth(
    session: Session,
    method: str,
    url: URL,
    **kwargs: Any,
) -> Response:
    """Send one auth subrequest without recursively invoking Auth.reauth."""
    hooks = session.hooks["response"]
    session.hooks["response"] = [hook for hook in hooks if not _is_reauth_hook(hook)]
    try:
        return getattr(session, method.lower())(url, **kwargs)
    finally:
        session.hooks["response"] = hooks


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
    # ``None`` removes the inherited session header for this request only.
    headers: dict[str, Any] = {"Referer": None}
    response = _request_without_reauth(
        session,
        "POST",
        baseurl / "api/v1/security/login",
        headers=headers,
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
    response = _request_without_reauth(
        session,
        "GET",
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
        self._using_legacy_auth = False
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
        self._clear_api_auth_state()
        self._using_legacy_auth = False
        try:
            self.token = self.get_access_token()
            self.csrf_token = self.get_csrf_token()
        except (RequestException, KeyError, ValueError) as ex:
            _logger.warning(
                "API authentication failed (%s); falling back to legacy "
                "HTML-based login flow.",
                ex,
            )
            self._clear_api_auth_state()
            self._using_legacy_auth = True
            self._legacy_auth()

    def get_access_token(self) -> str:
        """Get an access token using this authenticator's credentials."""
        return get_access_token(
            self.session,
            self.baseurl,
            self.username,
            self.password,
            self.provider,
        )

    def get_csrf_token(self) -> str:
        """Get a CSRF token for this authenticator's current JWT."""
        return get_csrf_token(self.session, self.baseurl, self.token)

    def _clear_api_auth_state(self) -> None:
        """Clear API credentials and the headers owned by this authenticator."""
        self.token = None
        self.csrf_token = None
        self.session.headers.pop("Authorization", None)
        self.session.headers.pop("X-CSRFToken", None)

    def _legacy_auth(self) -> None:
        """
        Legacy login flow: scrape the CSRF token from ``/login/`` and POST
        credentials as form data. Kept as a fallback for older Superset
        instances that don't expose the security API.
        """
        data = {"username": self.username, "password": self.password}

        response = _request_without_reauth(self.session, "GET", self.baseurl / "login/")
        soup = BeautifulSoup(response.text, "html.parser")
        input_ = soup.find("input", {"id": "csrf_token"})
        csrf_token = cast(str, input_["value"]) if input_ else None
        if csrf_token:
            self.session.headers["X-CSRFToken"] = csrf_token
            data["csrf_token"] = csrf_token
            self.csrf_token = csrf_token

        _request_without_reauth(
            self.session,
            "POST",
            self.baseurl / "login/",
            data=data,
        )


class SupersetJWTAuth(TokenAuth):  # pylint: disable=abstract-method
    """
    Auth to Superset via JWT token.
    """

    def __init__(self, token: str, baseurl: URL):
        super().__init__(token)
        self.baseurl = baseurl

    def get_csrf_token(self, jwt: str) -> str:
        """Get a CSRF token."""
        return get_csrf_token(self.session, self.baseurl, jwt)

    def get_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "X-CSRFToken": self.get_csrf_token(self.token),
        }
