"""
Test authentication mechanisms.
"""

from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker
from yarl import URL

from preset_cli.auth.main import Auth, UsernamePasswordAuth


def test_auth(mocker: MockerFixture) -> None:
    """
    Tests for the base class ``Auth``.
    """
    requests = mocker.patch("preset_cli.auth.main.requests")

    auth = Auth()
    assert auth.get_session() == requests.Session()


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
