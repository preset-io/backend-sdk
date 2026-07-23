"""
Test username:password authentication mechanism.
"""

from pytest_mock import MockerFixture
from requests import Session
from requests_mock.mocker import Mocker
from yarl import URL

from preset_cli.auth.superset import (
    SupersetJWTAuth,
    UsernamePasswordAuth,
    get_access_token,
    get_csrf_token,
)


def test_username_password_auth(requests_mock: Mocker) -> None:
    """
    Tests for the username/password authentication mechanism.
    """
    csrf_token = "CSFR_TOKEN"
    access_token = "ACCESS_TOKEN"
    requests_mock.get(
        "https://superset.example.org/api/v1/security/csrf_token/",
        json={"result": csrf_token},
    )
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        json={"access_token": access_token},
    )

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    assert auth.get_headers() == {
        "Authorization": f"Bearer {access_token}",
        "X-CSRFToken": csrf_token,
    }


def test_username_password_auth_no_csrf(requests_mock: Mocker) -> None:
    """
    When the security API returns no CSRF token, only the Bearer header is set.
    """
    access_token = "ACCESS_TOKEN"
    requests_mock.get(
        "https://superset.example.org/api/v1/security/csrf_token/",
        json={"result": None},
    )
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        json={"access_token": access_token},
    )

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    assert auth.get_headers() == {"Authorization": f"Bearer {access_token}"}


def test_username_password_auth_legacy_fallback(requests_mock: Mocker) -> None:
    """
    When the security API is unavailable, fall back to the legacy
    HTML-scraping login flow.
    """
    csrf_token = "LEGACY_CSRF"
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        status_code=404,
    )
    requests_mock.get(
        "https://superset.example.org/login/",
        text=(
            f'<html><body><input id="csrf_token" name="csrf_token" '
            f'value="{csrf_token}" /></body></html>'
        ),
    )
    requests_mock.post("https://superset.example.org/login/")

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    # no JWT is obtained in legacy mode; auth is carried by cookies + CSRF
    assert auth.token is None
    assert auth.get_headers() == {"X-CSRFToken": csrf_token}
    assert "Authorization" not in auth.session.headers


def test_username_password_auth_legacy_fallback_no_csrf(
    requests_mock: Mocker,
) -> None:
    """
    In the legacy flow, when the login page exposes no CSRF token input, the
    credentials are still posted (relying on cookies) and no CSRF header is set.
    """
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        status_code=404,
    )
    requests_mock.get(
        "https://superset.example.org/login/",
        text="<html><body>no csrf here</body></html>",
    )
    requests_mock.post("https://superset.example.org/login/")

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    # pylint: disable=use-implicit-booleaness-not-comparison
    assert auth.token is None
    assert auth.csrf_token is None
    assert auth.get_headers() == {}
    assert "X-CSRFToken" not in auth.session.headers


def test_username_password_auth_reauth(requests_mock: Mocker) -> None:
    """
    On a 401, ``reauth`` re-runs the login flow and the retried request
    carries a fresh Bearer token.
    """
    csrf_token = "CSFR_TOKEN"
    requests_mock.get(
        "https://superset.example.org/api/v1/security/csrf_token/",
        json={"result": csrf_token},
    )
    # the login endpoint hands out a new token on each call
    requests_mock.post(
        "https://superset.example.org/api/v1/security/login",
        [
            {"json": {"access_token": "TOKEN_1"}},
            {"json": {"access_token": "TOKEN_2"}},
        ],
    )
    # the protected resource is expired once, then succeeds
    requests_mock.get(
        "https://superset.example.org/api/v1/me/",
        [
            {"status_code": 401},
            {"status_code": 200, "json": {"result": "ok"}},
        ],
    )

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    assert auth.token == "TOKEN_1"

    response = auth.session.get(
        "https://superset.example.org/api/v1/me/",
        headers=auth.get_headers(),
    )
    assert response.status_code == 200
    # the token was refreshed and the retried request used it
    assert auth.token == "TOKEN_2"
    assert requests_mock.last_request.headers["Authorization"] == "Bearer TOKEN_2"


def test_jwt_auth_superset(mocker: MockerFixture) -> None:
    """
    Test the ``SupersetJWTAuth`` authentication mechanism for a Superset tenant.
    """
    mocker.patch(
        "preset_cli.auth.superset.get_csrf_token",
        return_value="myCSRFToken",
    )
    auth = SupersetJWTAuth("my-token", URL("https://example.org/"))

    assert auth.get_headers() == {
        "Authorization": "Bearer my-token",
        "X-CSRFToken": "myCSRFToken",
    }


def test_get_access_token(requests_mock: Mocker) -> None:
    """
    Test the ``get_access_token`` helper.
    """
    requests_mock.post(
        "https://example.org/api/v1/security/login",
        json={"access_token": "my-token"},
    )

    session = Session()
    token = get_access_token(
        session,
        URL("https://example.org/"),
        "admin",
        "password123",
        "db",
    )
    assert token == "my-token"
    assert requests_mock.last_request.json() == {
        "username": "admin",
        "password": "password123",
        "provider": "db",
    }


def test_get_csrf_token(requests_mock: Mocker) -> None:
    """
    Test the ``get_csrf_token`` helper.
    """
    requests_mock.get(
        "https://example.org/api/v1/security/csrf_token/",
        json={"result": "myCSRFToken"},
    )

    session = Session()
    csrf_token = get_csrf_token(session, URL("https://example.org/"), "my-token")
    assert csrf_token == "myCSRFToken"
    assert requests_mock.last_request.headers["Authorization"] == "Bearer my-token"
