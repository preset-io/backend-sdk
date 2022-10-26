"""
Test Preset auth.
"""

import pytest
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker
from yarl import URL

from preset_cli.auth.preset import PresetAuth


def test_preset_auth(mocker: MockerFixture) -> None:
    """
    Test the ``PresetAuth`` authentication mechanism.
    """
    mocker.patch("preset_cli.auth.preset.get_access_token", return_value="JWT_TOKEN")

    auth = PresetAuth(URL("http:/api.app.preset.io/"), "TOKEN", "SECRET")
    assert auth.get_headers() == {"Authorization": "Bearer JWT_TOKEN"}


def test_preset_auth_reauth(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test reauthorizing on a 401.
    """
    mocker.patch(
        "preset_cli.auth.preset.get_access_token",
        side_effect=["JWT_TOKEN1", "JWT_TOKEN2"],
    )
    requests_mock.get(
        "https://api.app.preset.io/",
        status_code=401,
    )
    requests_mock.get(
        "https://api.app.preset.io/",
        request_headers={"Authorization": "Bearer JWT_TOKEN1"},
        status_code=401,
    )
    requests_mock.get(
        "https://api.app.preset.io/",
        request_headers={"Authorization": "Bearer JWT_TOKEN2"},
        status_code=200,
    )

    auth = PresetAuth(URL("https:/api.app.preset.io/"), "TOKEN", "SECRET")
    assert auth.get_headers() == {"Authorization": "Bearer JWT_TOKEN1"}
    response = auth.session.get("https://api.app.preset.io/")
    assert response.status_code == 200
    assert auth.get_headers() == {"Authorization": "Bearer JWT_TOKEN2"}


def test_preset_auth_from_stored_credentials(mocker: MockerFixture) -> None:
    """
    Test instantiating the object from stored credentials
    """
    mocker.patch("preset_cli.auth.preset.open")

    get_credentials_path = mocker.patch("preset_cli.auth.preset.get_credentials_path")
    get_credentials_path().exists.return_value = True
    get_credentials_path().__str__.return_value = "/path/to/credentials.yaml"

    yaml = mocker.patch("preset_cli.auth.preset.yaml")
    yaml.load.return_value = {
        "api_token": "TOKEN",
        "api_secret": "SECRET",
        "baseurl": "https://api.app.preset.io/",
    }

    get_access_token = mocker.patch("preset_cli.auth.preset.get_access_token")
    get_access_token.return_value = "JWT_TOKEN"

    auth = PresetAuth.from_stored_credentials()
    assert auth.token == "JWT_TOKEN"
    get_access_token.assert_called_with(
        "https://api.app.preset.io/",
        "TOKEN",
        "SECRET",
    )

    # test for error
    get_credentials_path().exists.return_value = False
    with pytest.raises(Exception) as excinfo:
        PresetAuth.from_stored_credentials()
    assert (
        str(excinfo.value)
        == "Could not load credentials from /path/to/credentials.yaml"
    )
