"""
Tests for ``preset_cli.auth.lib``.
"""

from pathlib import Path

import yaml
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker
from yarl import URL

from preset_cli.auth.lib import (
    get_access_token,
    get_credentials_path,
    store_credentials,
)


def test_get_access_token(requests_mock: Mocker) -> None:
    """
    Test ``get_access_token``.
    """
    requests_mock.post(
        "https://manage.app.preset.io/api/v1/auth/",
        json={"payload": {"access_token": "TOKEN"}},
    )

    access_token = get_access_token(
        URL("https://manage.app.preset.io/"),
        "API_TOKEN",
        "API_SECRET",
    )
    assert access_token == "TOKEN"

    access_token = get_access_token(
        "https://manage.app.preset.io/",
        "API_TOKEN",
        "API_SECRET",
    )
    assert access_token == "TOKEN"


def test_get_credentials_path(mocker: MockerFixture) -> None:
    """
    Test ``get_credentials_path``.
    """
    mocker.patch("preset_cli.auth.lib.user_config_dir", return_value="/path/to/config")
    assert get_credentials_path() == Path("/path/to/config/credentials.yaml")


# pylint: disable=unused-argument, invalid-name
def test_store_credentials(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``store_credentials``.
    """
    credentials_path = Path("/path/to/config/credentials.yaml")

    mocker.patch("preset_cli.auth.lib.input", side_effect=["invalid", "n"])
    store_credentials(
        "API_TOKEN",
        "API_SECRET",
        URL("https://manage.app.preset.io/"),
        credentials_path,
    )
    assert not credentials_path.exists()

    mocker.patch("preset_cli.auth.lib.input", return_value="y")
    store_credentials(
        "API_TOKEN",
        "API_SECRET",
        URL("https://manage.app.preset.io/"),
        credentials_path,
    )
    assert credentials_path.exists()
    with open(credentials_path, encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == {
        "api_secret": "API_SECRET",
        "api_token": "API_TOKEN",
        "baseurl": "https://manage.app.preset.io/",
    }
