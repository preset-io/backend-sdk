"""
Test username:password authentication mechanism.
"""

from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker
from yarl import URL

from preset_cli.auth.superset import SupersetJWTAuth, UsernamePasswordAuth


def test_username_password_auth(requests_mock: Mocker) -> None:
    """
    Tests for the username/password authentication mechanism.
    """
    csrf_token = "CSFR_TOKEN"
    requests_mock.get(
        "https://superset.example.org/login/",
        text=f'<html><body><input id="csrf_token" value="{csrf_token}"></body></html>',
    )
    requests_mock.post("https://superset.example.org/login/")

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    assert auth.get_headers() == {
        "X-CSRFToken": csrf_token,
    }

    assert (
        requests_mock.last_request.text
        == "username=admin&password=password123&csrf_token=CSFR_TOKEN"
    )


def test_username_password_auth_no_csrf(requests_mock: Mocker) -> None:
    """
    Tests for the username/password authentication mechanism.
    """
    requests_mock.get(
        "https://superset.example.org/login/",
        text="<html><body>WTF_CSRF_ENABLED = False</body></html>",
    )
    requests_mock.post("https://superset.example.org/login/")

    auth = UsernamePasswordAuth(
        URL("https://superset.example.org/"),
        "admin",
        "password123",
    )
    # pylint: disable=use-implicit-booleaness-not-comparison
    assert auth.get_headers() == {}

    assert requests_mock.last_request.text == "username=admin&password=password123"


def test_jwt_auth_superset(mocker: MockerFixture) -> None:
    """
    Test the ``JWTAuth`` authentication mechanism for Superset tenant.
    """
    auth = SupersetJWTAuth("my-token", URL("https://example.org/"))
    mocker.patch.object(auth, "get_csrf_token", return_value="myCSRFToken")

    assert auth.get_headers() == {
        "Authorization": "Bearer my-token",
        "X-CSRFToken": "myCSRFToken",
    }


def test_get_csrf_token(requests_mock: Mocker) -> None:
    """
    Test the get_csrf_token method.
    """
    auth = SupersetJWTAuth("my-token", URL("https://example.org/"))
    requests_mock.get(
        "https://example.org/api/v1/security/csrf_token/",
        json={"result": "myCSRFToken"},
    )

    assert auth.get_csrf_token("my-token") == "myCSRFToken"
