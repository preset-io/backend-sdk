"""
Test JWT auth.
"""

import pytest
from pytest_mock import MockerFixture

from preset_cli.auth.jwt import JWTAuth


def test_jwt_auth() -> None:
    """
    Test the ``JWTAuth`` authentication mechanism.
    """
    auth = JWTAuth("my-token")
    assert auth.get_headers() == {"Authorization": "Bearer my-token"}


def test_jwt_auth_from_stored_credentials(mocker: MockerFixture) -> None:
    """
    Test instantiating the object from stored credentials
    """
    mocker.patch("preset_cli.auth.jwt.open")

    get_credentials_path = mocker.patch("preset_cli.auth.jwt.get_credentials_path")
    get_credentials_path().exists.return_value = True
    get_credentials_path().__str__.return_value = "/path/to/credentials.yaml"

    yaml = mocker.patch("preset_cli.auth.jwt.yaml")
    yaml.load.return_value = {
        "api_token": "TOKEN",
        "api_secret": "SECRET",
        "baseurl": "https://manage.app.preset.io/",
    }

    get_access_token = mocker.patch("preset_cli.auth.jwt.get_access_token")
    get_access_token.return_value = "JWT_TOKEN"

    auth = JWTAuth.from_stored_credentials()
    assert auth.token == "JWT_TOKEN"
    get_access_token.assert_called_with(
        baseurl="https://manage.app.preset.io/",
        api_token="TOKEN",
        api_secret="SECRET",
    )

    # can also pass a URL
    auth = JWTAuth.from_stored_credentials()
    assert auth.token == "JWT_TOKEN"

    # test for error
    get_credentials_path().exists.return_value = False
    with pytest.raises(Exception) as excinfo:
        JWTAuth.from_stored_credentials()
    assert (
        str(excinfo.value)
        == "Could not load credentials from /path/to/credentials.yaml"
    )
