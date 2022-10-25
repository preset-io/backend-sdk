"""
Test authentication mechanisms.
"""

from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker

from preset_cli.auth.main import Auth


def test_auth(mocker: MockerFixture) -> None:
    """
    Tests for the base class ``Auth``.
    """
    # pylint: disable=invalid-name
    Session = mocker.patch("preset_cli.auth.main.Session")

    auth = Auth()
    assert auth.session == Session()


def test_reauth(requests_mock: Mocker) -> None:
    """
    Test the ``reauth`` hook when authentication fails.
    """
    requests_mock.get("http://example.org/", status_code=401)

    # the base class has no reauth
    auth = Auth()
    response = auth.session.get("http://example.org/")
    assert response.status_code == 401
