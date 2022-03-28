"""
Tests for ``preset_cli.cli.main``.
"""
# pylint: disable=unused-argument, invalid-name, redefined-outer-name

from pathlib import Path
from typing import Any, Dict

import pytest
import yaml
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker
from yarl import URL

from preset_cli.cli.main import (
    get_access_token,
    get_credentials_path,
    get_status_icon,
    parse_workspace_selection,
    preset_cli,
    split_comma,
    store_credentials,
)


def test_split_comma(mocker: MockerFixture) -> None:
    """
    Test ``split_comma``.

    This is used to split workspaces passed in the CLI.
    """
    ctx = mocker.MagicMock()
    assert split_comma(
        ctx,
        "workspaces",
        "https://ws1.preset.io/,https://ws3.preset.io/, https://ws2.preset.io/",
    ) == [
        "https://ws1.preset.io/",
        "https://ws3.preset.io/",
        "https://ws2.preset.io/",
    ]
    assert split_comma(ctx, "workspaces", None) == []


def test_get_status_icon() -> None:
    """
    Test ``get_status_icon``.
    """
    assert get_status_icon("READY") == "✅"
    assert get_status_icon("LOADING_EXAMPLES") == "📊"
    assert get_status_icon("CREATING_DB") == "💾"
    assert get_status_icon("INITIALIZING_DB") == "💾"
    assert get_status_icon("MIGRATING_DB") == "🚧"
    assert get_status_icon("ROTATING_SECRETS") == "🕵️"
    assert get_status_icon("UNKNOWN") == "❓"
    assert get_status_icon("ERROR") == "❗️"
    assert get_status_icon("UPGRADING") == "⤴️"
    assert get_status_icon("INVALID") == "❓"


def test_parse_workspace_selection() -> None:
    """
    Test ``parse_workspace_selection``.
    """
    assert parse_workspace_selection("1-4,7", 10) == [1, 2, 3, 4, 7]
    assert parse_workspace_selection("-4", 10) == [1, 2, 3, 4]
    assert parse_workspace_selection("4-", 10) == [4, 5, 6, 7, 8, 9, 10]

    with pytest.raises(Exception) as excinfo:
        parse_workspace_selection("1-20", 10)
    assert str(excinfo.value) == "End 20 is greater than 10"
    with pytest.raises(Exception) as excinfo:
        parse_workspace_selection("20", 10)
    assert str(excinfo.value) == "Number 20 is greater than 10"


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


def test_get_credentials_path(mocker: MockerFixture) -> None:
    """
    Test ``get_credentials_path``.
    """
    mocker.patch("preset_cli.cli.main.user_config_dir", return_value="/path/to/config")
    assert get_credentials_path() == Path("/path/to/config/credentials.yaml")


def test_store_credentials(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``store_credentials``.
    """
    credentials_path = Path("/path/to/config/credentials.yaml")

    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "n"])
    store_credentials("API_TOKEN", "API_SECRET", credentials_path)
    assert not credentials_path.exists()

    mocker.patch("preset_cli.cli.main.input", return_value="y")
    store_credentials("API_TOKEN", "API_SECRET", credentials_path)
    assert credentials_path.exists()
    with open(credentials_path, encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == {"api_secret": "API_SECRET", "api_token": "API_TOKEN"}


def test_auth(mocker: MockerFixture) -> None:
    """
    Test the ``auth`` command.
    """
    credentials_path = Path("/path/to/config/credentials.yaml")
    mocker.patch(
        "preset_cli.cli.main.get_credentials_path",
        return_value=credentials_path,
    )
    webbrowser = mocker.patch("preset_cli.cli.main.webbrowser")
    mocker.patch("preset_cli.cli.main.input", return_value="API_TOKEN")
    getpass = mocker.patch("preset_cli.cli.main.getpass")
    getpass.getpass.return_value = "API_SECRET"
    store_credentials = mocker.patch("preset_cli.cli.main.store_credentials")

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token", "JWT_TOKEN", "auth"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    webbrowser.open.assert_called_with("https://manage.app.preset.io/app/user")
    store_credentials.assert_called_with("API_TOKEN", "API_SECRET", credentials_path)


def test_auth_overwrite(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``auth`` command when credentials already exist.
    """
    credentials_path = Path("/path/to/config/credentials.yaml")
    fs.create_file(credentials_path)
    mocker.patch(
        "preset_cli.cli.main.get_credentials_path",
        return_value=credentials_path,
    )

    runner = CliRunner()

    result = runner.invoke(
        preset_cli,
        ["--jwt-token", "JWT_TOKEN", "auth"],
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    mocker.patch("preset_cli.cli.main.webbrowser")
    mocker.patch("preset_cli.cli.main.input", return_value="API_TOKEN")
    getpass = mocker.patch("preset_cli.cli.main.getpass")
    getpass.getpass.return_value = "API_SECRET"
    mocker.patch("preset_cli.cli.main.store_credentials")
    result = runner.invoke(
        preset_cli,
        ["--jwt-token", "JWT_TOKEN", "auth", "--overwrite"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_jwt_token_credentials_exist(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the command when the credentials are stored.
    """
    credentials_path = Path("/path/to/config/credentials.yaml")
    fs.create_file(
        credentials_path,
        contents=yaml.dump({"api_secret": "API_SECRET", "api_token": "API_TOKEN"}),
    )
    mocker.patch(
        "preset_cli.cli.main.get_credentials_path",
        return_value=credentials_path,
    )
    mocker.patch("preset_cli.cli.main.get_access_token", return_value="JWT_TOKEN")
    JWTAuth = mocker.patch("preset_cli.cli.main.JWTAuth")

    runner = CliRunner()
    result = runner.invoke(preset_cli, ["auth", "--help"], catch_exceptions=False)
    assert result.exit_code == 0
    JWTAuth.assert_called_with("JWT_TOKEN")


def test_jwt_token_invalid_credentials(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the command when the credentials are stored.
    """
    credentials_path = Path("/path/to/config/credentials.yaml")
    fs.create_file(
        credentials_path,
        contents=yaml.dump({"api_token": "API_TOKEN"}),
    )
    mocker.patch(
        "preset_cli.cli.main.get_credentials_path",
        return_value=credentials_path,
    )
    mocker.patch("preset_cli.cli.main.get_access_token", return_value="JWT_TOKEN")

    runner = CliRunner()
    result = runner.invoke(preset_cli, ["auth", "--help"], catch_exceptions=False)
    assert result.exit_code == 1


def test_jwt_token_prompt_for_credentials(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the command when the credentials are stored.
    """
    credentials_path = Path("/path/to/config/credentials.yaml")
    mocker.patch(
        "preset_cli.cli.main.get_credentials_path",
        return_value=credentials_path,
    )
    mocker.patch("preset_cli.cli.main.webbrowser")
    mocker.patch("preset_cli.cli.main.input", return_value="API_TOKEN")
    getpass = mocker.patch("preset_cli.cli.main.getpass")
    getpass.getpass.return_value = "API_SECRET"
    mocker.patch("preset_cli.cli.main.store_credentials")
    mocker.patch("preset_cli.cli.main.get_access_token", return_value="JWT_TOKEN")
    JWTAuth = mocker.patch("preset_cli.cli.main.JWTAuth")

    runner = CliRunner()
    result = runner.invoke(preset_cli, ["auth", "--help"], catch_exceptions=False)
    assert result.exit_code == 0
    JWTAuth.assert_called_with("JWT_TOKEN")


def test_jwt_token_credentials_passed(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the command when the credentials are stored.
    """
    mocker.patch("preset_cli.cli.main.get_access_token", return_value="JWT_TOKEN")
    JWTAuth = mocker.patch("preset_cli.cli.main.JWTAuth")

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--api-token", "API_TOKEN", "--api-secret", "API_SECRET", "auth", "--help"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    JWTAuth.assert_called_with("JWT_TOKEN")


def test_workspaces(mocker: MockerFixture) -> None:
    """
    Test that we prompt user for their workspaces if not specified.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    client = PresetClient()
    client.get_teams.return_value = [{"name": "botafogo", "title": "Alvinegro"}]
    client.get_workspaces.return_value = [
        {"workspace_status": "READY", "title": "My Workspace", "hostname": "ws1"},
        {"workspace_status": "READY", "title": "My Other Workspace", "hostname": "ws2"},
    ]
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])

    runner = CliRunner()
    obj: Dict[str, Any] = {}
    result = runner.invoke(
        preset_cli,
        ["--jwt-token", "JWT_TOKEN", "superset", "--help"],
        catch_exceptions=False,
        obj=obj,
    )
    assert result.exit_code == 0
    assert obj["WORKSPACES"] == ["https://ws1", "https://ws2"]


def test_workspaces_no_workspaces(mocker: MockerFixture) -> None:
    """
    Test when no workspaces are available.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    client = PresetClient()
    client.get_teams.return_value = [{"name": "botafogo", "title": "Alvinegro"}]
    client.get_workspaces.return_value = []
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])

    runner = CliRunner()
    obj: Dict[str, Any] = {}
    result = runner.invoke(
        preset_cli,
        ["--jwt-token", "JWT_TOKEN", "superset", "--help"],
        catch_exceptions=False,
        obj=obj,
    )
    assert result.exit_code == 1
