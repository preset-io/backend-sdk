"""
Test authentication mechanisms.
"""

from pytest_mock import MockerFixture

from preset_cli.auth.main import Auth


def test_auth(mocker: MockerFixture) -> None:
    """
    Tests for the base class ``Auth``.
    """
    requests = mocker.patch("preset_cli.auth.main.requests")

    auth = Auth()
    assert auth.get_session() == requests.Session()
