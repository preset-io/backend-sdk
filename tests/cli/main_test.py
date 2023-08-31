"""
Tests for ``preset_cli.cli.main``.
"""
# pylint: disable=unused-argument, invalid-name, redefined-outer-name, too-many-lines

import csv
import os
from pathlib import Path
from typing import Any, Dict
from unittest.mock import call

import pytest
import yaml
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from yarl import URL

from preset_cli.cli.main import (
    export_group_membership_csv,
    export_group_membership_yaml,
    get_status_icon,
    parse_selection,
    preset_cli,
    print_group_membership,
    sync_all_user_roles_to_team,
    sync_user_role_to_workspace,
    sync_user_roles_to_team,
)
from preset_cli.lib import split_comma


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


def test_parse_selection() -> None:
    """
    Test ``parse_selection``.
    """
    assert parse_selection("1-4,7", 10) == [1, 2, 3, 4, 7]
    assert parse_selection("-4", 10) == [1, 2, 3, 4]
    assert parse_selection("4-", 10) == [4, 5, 6, 7, 8, 9, 10]

    with pytest.raises(Exception) as excinfo:
        parse_selection("1-20", 10)
    assert str(excinfo.value) == "End 20 is greater than 10"
    with pytest.raises(Exception) as excinfo:
        parse_selection("20", 10)
    assert str(excinfo.value) == "Number 20 is greater than 10"


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
    store_credentials.assert_called_with(
        "API_TOKEN",
        "API_SECRET",
        URL("https://api.app.preset.io/"),
        credentials_path,
    )


def test_auth_show(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``auth --show`` command.
    """
    credentials_path = Path("/path/to/config/credentials.yaml")
    fs.create_file(
        credentials_path,
        contents=yaml.dump(
            {
                "baseurl": "https://api.app.preset.io/",
                "api_secret": "XXX",
                "api_token": "abc",
            },
        ),
    )
    mocker.patch(
        "preset_cli.cli.main.get_credentials_path",
        return_value=credentials_path,
    )

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token", "JWT_TOKEN", "auth", "--show"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert (
        result.output
        == """/path/to/config/credentials.yaml
================================
api_secret: XXX
api_token: abc
baseurl: https://api.app.preset.io/

"""
    )


def test_auth_show_no_credentials(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``auth --show`` command when there are no credentials.
    """
    credentials_path = Path("/path/to/config/credentials.yaml")
    mocker.patch(
        "preset_cli.cli.main.get_credentials_path",
        return_value=credentials_path,
    )

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token", "JWT_TOKEN", "auth", "--show"],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert result.output == (
        "The file /path/to/config/credentials.yaml doesn't exist. "
        "Run ``preset-cli auth`` to create it.\n"
    )


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


def test_auth_overwrite_expired_credentials(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``auth`` command when overwriting expired credentials.
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
    get_access_token = mocker.patch(
        "preset_cli.auth.preset.get_access_token",
        side_effect=Exception("Unable to get access token"),
    )

    runner = CliRunner()

    mocker.patch("preset_cli.cli.main.webbrowser")
    mocker.patch("preset_cli.cli.main.input", return_value="API_TOKEN")
    getpass = mocker.patch("preset_cli.cli.main.getpass")
    getpass.getpass.return_value = "API_SECRET"
    mocker.patch("preset_cli.cli.main.store_credentials")
    result = runner.invoke(
        preset_cli,
        ["auth", "--overwrite"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    get_access_token.assert_not_called()


def test_cmd_handling_failed_creds(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``superset`` command when overwriting expired credentials.
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
    get_access_token = mocker.patch(
        "preset_cli.auth.preset.get_access_token",
        side_effect=Exception("Unable to get access token"),
    )

    runner = CliRunner()

    mocker.patch("preset_cli.cli.main.webbrowser")
    mocker.patch("preset_cli.cli.main.input", return_value="API_TOKEN")
    getpass = mocker.patch("preset_cli.cli.main.getpass")
    getpass.getpass.return_value = "API_SECRET"
    mocker.patch("preset_cli.cli.main.store_credentials")
    result = runner.invoke(
        preset_cli,
        ["superset"],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    get_access_token.assert_called_with(
        URL("https://api.app.preset.io/"),
        "API_TOKEN",
        "API_SECRET",
    )


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
    mocker.patch("preset_cli.auth.preset.get_access_token", return_value="JWT_TOKEN")
    PresetAuth = mocker.patch("preset_cli.cli.main.PresetAuth")

    runner = CliRunner()
    result = runner.invoke(preset_cli, ["superset", "--help"], catch_exceptions=False)
    assert result.exit_code == 1
    PresetAuth.assert_called_with(
        URL("https://api.app.preset.io/"),
        "API_TOKEN",
        "API_SECRET",
    )


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
    mocker.patch("preset_cli.auth.preset.get_access_token", return_value="JWT_TOKEN")

    runner = CliRunner()
    result = runner.invoke(preset_cli, ["superset", "--help"], catch_exceptions=False)
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
    mocker.patch("preset_cli.auth.preset.get_access_token", return_value="JWT_TOKEN")
    PresetAuth = mocker.patch("preset_cli.cli.main.PresetAuth")

    runner = CliRunner()
    result = runner.invoke(preset_cli, ["superset", "--help"], catch_exceptions=False)
    assert result.exit_code == 1
    PresetAuth.assert_called_with(
        URL("https://api.app.preset.io/"),
        "API_TOKEN",
        "API_SECRET",
    )


def test_jwt_token_credentials_passed(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the command when the credentials are stored.
    """
    mocker.patch("preset_cli.auth.preset.get_access_token", return_value="JWT_TOKEN")
    PresetAuth = mocker.patch("preset_cli.cli.main.PresetAuth")

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        [
            "--api-token",
            "API_TOKEN",
            "--api-secret",
            "API_SECRET",
            "superset",
            "--help",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    PresetAuth.assert_called_with(
        URL("https://api.app.preset.io/"),
        "API_TOKEN",
        "API_SECRET",
    )


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


def test_workspaces_from_env(
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test that we don't prompt user for their workspaces if defined in the environment.
    """
    monkeypatch.setenv("PRESET_WORKSPACES", "https://ws1,https://ws2")

    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    client = PresetClient()

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
    client.get_workspaces.assert_not_called()


def test_workspaces_help(mocker: MockerFixture) -> None:
    """
    Test that we don't prompt user for their workspaces if ``--help`` is passed.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    client = PresetClient()
    sys = mocker.patch("preset_cli.cli.main.sys")
    sys.argv = ["preset-cli", "--jwt-token", "JWT_TOKEN", "superset", "--help"]

    runner = CliRunner()
    obj: Dict[str, Any] = {}
    runner.invoke(
        preset_cli,
        ["--jwt-token", "JWT_TOKEN", "superset", "--help"],
        catch_exceptions=False,
        obj=obj,
    )
    client.get_teams.assert_not_called()
    client.get_workspaces.assert_not_called()


def test_workspaces_single_workspace(mocker: MockerFixture) -> None:
    """
    Test that we don't prompt user for their workspaces if they have only one.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    client = PresetClient()
    client.get_teams.return_value = [{"name": "botafogo", "title": "Alvinegro"}]
    client.get_workspaces.return_value = [
        {"workspace_status": "READY", "title": "My Workspace", "hostname": "ws1"},
    ]
    parse_selection = mocker.patch(
        "preset_cli.cli.main.parse_selection",
    )

    runner = CliRunner()
    obj: Dict[str, Any] = {}
    result = runner.invoke(
        preset_cli,
        ["--jwt-token", "JWT_TOKEN", "superset", "--help"],
        catch_exceptions=False,
        obj=obj,
    )
    assert result.exit_code == 0
    assert obj["WORKSPACES"] == ["https://ws1"]
    parse_selection.assert_not_called()


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


def test_invite_users(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``invite_users`` command.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    client = PresetClient()
    users = [
        {"email": "adoe@example.com"},
        {"email": "bdoe@example.com"},
    ]
    fs.create_file("users.yaml", contents=yaml.dump(users))

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "invite-users", "--teams=team1"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    client.invite_users.assert_called_with(
        ["team1"],
        ["adoe@example.com", "bdoe@example.com"],
    )


def test_invite_users_choose_teams(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``invite_users`` command when no teams are passed.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    client = PresetClient()
    client.get_teams.return_value = [
        {"name": "botafogo", "title": "Alvinegro"},
        {"name": "flamengo", "title": "Rubro-Negro"},
    ]
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])
    users = [
        {"email": "adoe@example.com"},
        {"email": "bdoe@example.com"},
    ]
    fs.create_file("users.yaml", contents=yaml.dump(users))

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "invite-users"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    client.invite_users.assert_called_with(
        ["botafogo", "flamengo"],
        ["adoe@example.com", "bdoe@example.com"],
    )


def test_invite_users_no_teams(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``invite_users`` command when no teams are available.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    client = PresetClient()
    client.get_teams.return_value = []
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "invite-users"],
        catch_exceptions=False,
    )
    assert result.exit_code == 1


def test_invite_users_single_team(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``invite_users`` command doesn't prompt for teams when only one is available.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    client = PresetClient()
    client.get_teams.return_value = [
        {"name": "botafogo", "title": "Alvinegro"},
    ]
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])
    parse_selection = mocker.patch(
        "preset_cli.cli.main.parse_selection",
    )
    users = [
        {"email": "adoe@example.com"},
        {"email": "bdoe@example.com"},
    ]
    fs.create_file("users.yaml", contents=yaml.dump(users))

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "invite-users"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    client.invite_users.assert_called_with(
        ["botafogo"],
        ["adoe@example.com", "bdoe@example.com"],
    )
    parse_selection.assert_not_called()


def test_import_users(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``import_users`` command.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    client = PresetClient()
    users = [
        {"first_name": "Alice", "last_name": "Doe", "email": "adoe@example.com"},
        {"first_name": "Bob", "last_name": "Doe", "email": "bdoe@example.com"},
    ]
    fs.create_file("users.yaml", contents=yaml.dump(users))

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "import-users", "--teams=team1"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    client.import_users.assert_called_with(
        ["team1"],
        [
            {"first_name": "Alice", "last_name": "Doe", "email": "adoe@example.com"},
            {"first_name": "Bob", "last_name": "Doe", "email": "bdoe@example.com"},
        ],
    )


def test_import_users_choose_teams(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``import_users`` command when no teams are passed.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    client = PresetClient()
    client.get_teams.return_value = [
        {"name": "botafogo", "title": "Alvinegro"},
        {"name": "flamengo", "title": "Rubro-Negro"},
    ]
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])
    users = [
        {"first_name": "Alice", "last_name": "Doe", "email": "adoe@example.com"},
        {"first_name": "Bob", "last_name": "Doe", "email": "bdoe@example.com"},
    ]
    fs.create_file("users.yaml", contents=yaml.dump(users))

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "import-users"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    client.import_users.assert_called_with(
        ["botafogo", "flamengo"],
        [
            {"first_name": "Alice", "last_name": "Doe", "email": "adoe@example.com"},
            {"first_name": "Bob", "last_name": "Doe", "email": "bdoe@example.com"},
        ],
    )


def test_sync_roles(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``sync_roles`` command.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    client = PresetClient()
    client.get_workspaces.return_value = [
        {"workspace_status": "READY", "title": "My Workspace", "hostname": "ws1"},
        {"workspace_status": "READY", "title": "My Other Workspace", "hostname": "ws2"},
    ]
    sync_all_user_roles_to_team = mocker.patch(
        "preset_cli.cli.main.sync_all_user_roles_to_team",
    )

    user_roles = [
        {
            "email": "adoe@example.com",
            "team_role": "Admin",
            "workspaces": {
                "My Workspace": {
                    "workspace_role": "Limited Contributor",
                    "data_access_roles": ["Database access on A Postgres database"],
                },
            },
        },
    ]
    fs.create_file("user_roles.yaml", contents=yaml.dump(user_roles))

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "sync-roles", "--teams=team1"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    client.get_workspaces.assert_called_with("team1")
    sync_all_user_roles_to_team.assert_called_with(
        client,
        "team1",
        user_roles,
        [
            {"workspace_status": "READY", "title": "My Workspace", "hostname": "ws1"},
            {
                "workspace_status": "READY",
                "title": "My Other Workspace",
                "hostname": "ws2",
            },
        ],
    )

    mocker.patch("preset_cli.cli.main.get_teams", return_value=["team1"])
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "sync-roles"],
        catch_exceptions=False,
    )
    client.get_workspaces.assert_called_with("team1")


def test_sync_all_user_roles_to_team(mocker: MockerFixture) -> None:
    """
    Test the ``sync_all_user_roles_to_team`` helper.
    """
    sync_user_roles_to_team = mocker.patch(
        "preset_cli.cli.main.sync_user_roles_to_team",
    )
    client = mocker.MagicMock()
    client.get_team_members.return_value = [
        {"user": {"email": "adoe@example.com", "id": 1001}},
        {"user": {"email": "bdoe@example.com", "id": 1002}},
    ]
    SupersetClient = mocker.patch("preset_cli.cli.main.SupersetClient")
    superset_client = SupersetClient()
    superset_client.export_users.return_value = [
        {"email": "adoe@example.com", "id": 1},
        {"email": "bdoe@example.com", "id": 2},
    ]
    superset_client.get_role_id.return_value = 42
    workspaces = [
        {"name": "ws1", "title": "My Workspace", "hostname": "ws1.example.org"},
        {"name": "ws2", "title": "My Other Workspace", "hostname": "ws2.example.org"},
    ]
    user_roles = [
        {
            "email": "adoe@example.com",
            "team_role": "Admin",
            "workspaces": {
                "ws1": {
                    "workspace_role": "Limited Contributor",
                    "data_access_roles": ["Database access on A Postgres database"],
                },
            },
        },
    ]

    sync_all_user_roles_to_team(client, "team1", user_roles, workspaces)

    sync_user_roles_to_team.assert_called_with(
        client,
        "team1",
        {
            "id": 1001,
            "email": "adoe@example.com",
            "team_role": "Admin",
            "workspaces": {
                "ws1": {
                    "workspace_role": "Limited Contributor",
                    "data_access_roles": ["Database access on A Postgres database"],
                },
            },
        },
        workspaces,
    )
    SupersetClient.assert_called_with("https://ws1.example.org/", client.auth)
    superset_client.update_role.assert_called_with(42, user=[1])


def test_sync_all_user_roles_to_team_workspace_title(mocker: MockerFixture) -> None:
    """
    Test the ``sync_all_user_roles_to_team`` helper.

    Here the config uses the workspace title instead of the name.
    """
    sync_user_roles_to_team = mocker.patch(
        "preset_cli.cli.main.sync_user_roles_to_team",
    )
    client = mocker.MagicMock()
    client.get_team_members.return_value = [
        {"user": {"email": "adoe@example.com", "id": 1001}},
        {"user": {"email": "bdoe@example.com", "id": 1002}},
    ]
    SupersetClient = mocker.patch("preset_cli.cli.main.SupersetClient")
    superset_client = SupersetClient()
    superset_client.export_users.return_value = [
        {"email": "adoe@example.com", "id": 1},
        {"email": "bdoe@example.com", "id": 2},
    ]
    superset_client.get_role_id.return_value = 42
    workspaces = [
        {"name": "ws1", "title": "My Workspace", "hostname": "ws1.example.org"},
        {"name": "ws2", "title": "My Other Workspace", "hostname": "ws2.example.org"},
    ]
    user_roles = [
        {
            "email": "adoe@example.com",
            "team_role": "Admin",
            "workspaces": {
                "My Workspace": {
                    "workspace_role": "Limited Contributor",
                    "data_access_roles": ["Database access on A Postgres database"],
                },
            },
        },
    ]

    sync_all_user_roles_to_team(client, "team1", user_roles, workspaces)

    sync_user_roles_to_team.assert_called_with(
        client,
        "team1",
        {
            "id": 1001,
            "email": "adoe@example.com",
            "team_role": "Admin",
            "workspaces": {
                "My Workspace": {
                    "workspace_role": "Limited Contributor",
                    "data_access_roles": ["Database access on A Postgres database"],
                },
            },
        },
        workspaces,
    )
    SupersetClient.assert_called_with("https://ws1.example.org/", client.auth)
    superset_client.update_role.assert_called_with(42, user=[1])


def test_sync_user_roles_to_team(mocker: MockerFixture) -> None:
    """
    Test the ``sync_user_roles_to_team`` helper.
    """
    sync_user_role_to_workspace = mocker.patch(
        "preset_cli.cli.main.sync_user_role_to_workspace",
    )
    client = mocker.MagicMock()
    user = {
        "id": 1001,
        "email": "adoe@example.com",
        "team_role": "Admin",
        "workspaces": {
            "My Workspace": {
                "workspace_role": "Limited Contributor",
                "data_access_roles": ["Database access on A Postgres database"],
            },
        },
    }
    workspaces = [
        {
            "id": 1,
            "name": "ws1",
            "title": "My Workspace",
            "hostname": "ws1.example.org",
        },
        {
            "id": 2,
            "name": "ws2",
            "title": "My Other Workspace",
            "hostname": "ws2.example.org",
        },
    ]

    sync_user_roles_to_team(client, "team1", user, workspaces)

    client.change_team_role.assert_called_with("team1", 1001, 1)
    sync_user_role_to_workspace.assert_called_with(
        client,
        "team1",
        user,
        1,
        {
            "data_access_roles": ["Database access on A Postgres database"],
            "workspace_role": "Limited Contributor",
        },
    )

    user = {
        "id": 1001,
        "email": "adoe@example.com",
        "team_role": "User",
        "workspaces": {
            "My Workspace": {
                "workspace_role": "Limited Contributor",
                "data_access_roles": ["Database access on A Postgres database"],
            },
        },
    }
    sync_user_roles_to_team(client, "team1", user, workspaces)
    client.change_team_role.assert_called_with("team1", 1001, 2)

    user = {
        "id": 1001,
        "email": "adoe@example.com",
        "team_role": "Super Mega Admin",
        "workspaces": {
            "My Workspace": {
                "workspace_role": "Limited Contributor",
                "data_access_roles": ["Database access on A Postgres database"],
            },
        },
    }
    with pytest.raises(Exception) as excinfo:
        sync_user_roles_to_team(client, "team1", user, workspaces)
    assert (
        str(excinfo.value) == "Invalid role Super Mega Admin for user adoe@example.com"
    )

    user = {
        "id": 1001,
        "email": "adoe@example.com",
        "team_role": "User",
        "workspaces": {
            "ws1": {
                "workspace_role": "Limited Contributor",
                "data_access_roles": ["Database access on A Postgres database"],
            },
        },
    }
    sync_user_roles_to_team(client, "team1", user, workspaces)
    sync_user_role_to_workspace.assert_called_with(
        client,
        "team1",
        user,
        1,
        {
            "data_access_roles": ["Database access on A Postgres database"],
            "workspace_role": "Limited Contributor",
        },
    )


def test_sync_user_role_to_workspace(mocker: MockerFixture) -> None:
    """
    Test the ``sync_user_role_to_workspace`` helper.
    """
    client = mocker.MagicMock()
    user = {
        "id": 1001,
        "email": "adoe@example.com",
        "team_role": "User",
        "workspaces": {
            "ws1": {
                "workspace_role": "Limited Contributor",
                "data_access_roles": ["Database access on A Postgres database"],
            },
        },
    }

    sync_user_role_to_workspace(
        client,
        "team1",
        user,
        1,
        {
            "data_access_roles": ["Database access on A Postgres database"],
            "workspace_role": "Limited Contributor",
        },
    )

    client.change_workspace_role.assert_called_with(
        "team1",
        1,
        1001,
        "PresetGamma",
    )


def test_list_group_membership_specified_team(mocker: MockerFixture) -> None:
    """
    Test the ``list_group_membership`` command when a team is specified.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])

    client = PresetClient()
    client.get_teams.assert_not_called()
    client.get_group_membership.return_value = {
        "Resources": [],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 0,
    }

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "list-group-membership", "--teams=team1"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    client.get_group_membership.assert_called_with("team1", 1)
    assert result.output == "Team team1 has no SCIM groups\n\n"


def test_list_group_membership_multiple_teams(mocker: MockerFixture) -> None:
    """
    Test the ``list_group_membership`` command when specifying two teams.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])

    client = PresetClient()
    client.get_teams.assert_not_called()
    client.get_group_membership.side_effect = [
        {
            "Resources": [],
            "itemsPerPage": 100,
            "schemas": [
                "urn:ietf:params:scim:api:messages:2.0:ListResponse",
            ],
            "startIndex": 1,
            "totalResults": 0,
        },
        {
            "Resources": [],
            "itemsPerPage": 100,
            "schemas": [
                "urn:ietf:params:scim:api:messages:2.0:ListResponse",
            ],
            "startIndex": 1,
            "totalResults": 0,
        },
    ]

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "list-group-membership", "--teams=team1,team2"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    expected_calls = [call("team1", 1), call("team2", 1)]

    client.get_group_membership.assert_has_calls(expected_calls, any_order=False)

    assert (
        result.output
        == """## Team team1 ##
Team team1 has no SCIM groups

## Team team2 ##
Team team2 has no SCIM groups

"""
    )


def test_list_group_membership_no_team_available(mocker: MockerFixture) -> None:
    """
    Test the ``list_group_membership`` command when no teams are available.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])

    client = PresetClient()
    client.get_teams.return_value = []

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "list-group-membership"],
        catch_exceptions=False,
    )
    assert result.exit_code == 1


def test_list_group_membership_team_with_no_groups(mocker: MockerFixture) -> None:
    """
    Test the ``list_group_membership`` command when specifying a team with no groups available.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])

    client = PresetClient()
    client.get_teams.assert_not_called()
    client.get_group_membership.return_value = {
        "Resources": [],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 0,
    }

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "list-group-membership", "--teams=team1"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    client.get_group_membership.assert_called_with("team1", 1)

    assert result.output == "Team team1 has no SCIM groups\n\n"


def test_list_group_membership_group_with_no_members(mocker: MockerFixture) -> None:
    """
    Test the ``list_group_membership`` command when the specified team has a group with no members.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])
    print_group_membership = mocker.patch(
        "preset_cli.cli.main.print_group_membership",
    )

    client = PresetClient()
    client.get_teams.assert_not_called()
    client.get_group_membership.return_value = {
        "Resources": [
            {
                "displayName": "SCIM Group",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                "members": [],
            },
        ],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 1,
    }

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        ["--jwt-token=XXX", "list-group-membership", "--teams=team1"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    client.get_group_membership.assert_called_with("team1", 1)

    print_group_membership.assert_called_with = {
        "Resources": [
            {
                "displayName": "SCIM Group",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                "members": [],
            },
        ],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 1,
    }


def test_list_group_membership_incorrect_export(mocker: MockerFixture) -> None:
    """
    Test the ``list_group_membership`` command with an incorrect ``--export-report`` parameter.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])

    client = PresetClient()
    client.get_teams.assert_not_called()
    client.get_group_membership.not_called = ()

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        [
            "--jwt-token=XXX",
            "list-group-membership",
            "--teams=team1",
            "--save-report=invalid",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1


def test_list_group_membership_export_yaml(mocker: MockerFixture) -> None:
    """
    Test the ``list_group_membership`` command setting ``--export-report=yaml``.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])
    export_group_membership_yaml = mocker.patch(
        "preset_cli.cli.main.export_group_membership_yaml",
    )

    client = PresetClient()
    client.get_teams.assert_not_called()
    client.get_group_membership.return_value = {
        "Resources": [
            {
                "displayName": "SCIM Test Group",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                "members": [
                    {
                        "display": "Test Account 01",
                        "value": "samlp|example|testaccount01@example.com",
                    },
                ],
                "meta": {
                    "resourceType": "Group",
                },
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:Group",
                ],
            },
        ],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 1,
    }

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        [
            "--jwt-token=XXX",
            "list-group-membership",
            "--teams=team1",
            "--save-report=yaml",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    client.get_group_membership.assert_called_with("team1", 1)

    export_group_membership_yaml.assert_called_with = (
        {
            "Resources": [
                {
                    "displayName": "SCIM Test Group",
                    "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                    "members": [
                        {
                            "display": "Test Account 01",
                            "value": "samlp|example|testaccount01@example.com",
                        },
                    ],
                    "meta": {
                        "resourceType": "Group",
                    },
                    "schemas": [
                        "urn:ietf:params:scim:schemas:core:2.0:Group",
                    ],
                },
            ],
            "itemsPerPage": 100,
            "schemas": [
                "urn:ietf:params:scim:api:messages:2.0:ListResponse",
            ],
            "startIndex": 1,
            "totalResults": 1,
        },
        "team1",
    )


def test_list_group_membership_export_csv(mocker: MockerFixture) -> None:
    """
    Test the ``list_group_membership`` setting ``--export-report=csv``.
    """
    PresetClient = mocker.patch("preset_cli.cli.main.PresetClient")
    mocker.patch("preset_cli.cli.main.input", side_effect=["invalid", "-"])
    export_group_membership_csv = mocker.patch(
        "preset_cli.cli.main.export_group_membership_csv",
    )

    client = PresetClient()
    client.get_teams.assert_not_called()
    client.get_group_membership.return_value = {
        "Resources": [
            {
                "displayName": "SCIM Test Group",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                "members": [
                    {
                        "display": "Test Account 01",
                        "value": "samlp|example|testaccount01@example.com",
                    },
                ],
                "meta": {
                    "resourceType": "Group",
                },
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:Group",
                ],
            },
        ],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 1,
    }

    runner = CliRunner()
    result = runner.invoke(
        preset_cli,
        [
            "--jwt-token=XXX",
            "list-group-membership",
            "--teams=team1",
            "--save-report=csv",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    client.get_group_membership.assert_called_with(
        "team1",
        1,
    )

    export_group_membership_csv.assert_called_with = (
        {
            "Resources": [
                {
                    "displayName": "SCIM Test Group",
                    "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                    "members": [
                        {
                            "display": "Test Account 01",
                            "value": "samlp|example|testaccount01@example.com",
                        },
                    ],
                    "meta": {
                        "resourceType": "Group",
                    },
                    "schemas": [
                        "urn:ietf:params:scim:schemas:core:2.0:Group",
                    ],
                },
            ],
            "itemsPerPage": 100,
            "schemas": [
                "urn:ietf:params:scim:api:messages:2.0:ListResponse",
            ],
            "startIndex": 1,
            "totalResults": 1,
        },
        "team1",
    )


def test_print_group_membership_group_with_no_members(capfd) -> None:
    """
    Test the ``print_group_membership`` helper with a group with no members.
    """

    groups = {
        "Resources": [
            {
                "displayName": "SCIM Group",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                "members": [],
            },
        ],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 1,
    }

    print_group_membership(groups)
    out = capfd.readouterr().out
    assert (
        out
        == """
Name: SCIM Group ID: b2a691ca-0ef8-464c-9601-9c50158c5426
# Group with no users

"""
    )


def test_print_group_membership_group_with_members(capfd) -> None:
    """
    Test the ``print_group_membership`` helper.
    """

    groups = {
        "Resources": [
            {
                "displayName": "SCIM Group",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                "members": [
                    {
                        "display": "Test Account 01",
                        "value": "samlp|example|testaccount01@example.com",
                    },
                ],
            },
        ],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 1,
    }

    print_group_membership(groups)
    out = capfd.readouterr().out
    assert (
        out
        == """
Name: SCIM Group ID: b2a691ca-0ef8-464c-9601-9c50158c5426
# User: Test Account 01 Username: samlp|example|testaccount01@example.com
"""
    )


def test_export_group_membership_yaml() -> None:
    """
    Test the ``export_group_membership_yaml`` helper.
    """

    groups = {
        "Resources": [
            {
                "displayName": "SCIM Group",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                "members": [
                    {
                        "display": "Test Account 01",
                        "value": "samlp|example|testaccount01@example.com",
                    },
                ],
            },
        ],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 1,
    }

    export_group_membership_yaml(groups, "team1")
    with open("team1_user_group_membership.yaml", encoding="utf-8") as yaml_test_output:
        assert yaml.load(yaml_test_output.read(), Loader=yaml.SafeLoader) == {
            "Resources": [
                {
                    "displayName": "SCIM Group",
                    "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                    "members": [
                        {
                            "display": "Test Account 01",
                            "value": "samlp|example|testaccount01@example.com",
                        },
                    ],
                },
            ],
            "itemsPerPage": 100,
            "schemas": [
                "urn:ietf:params:scim:api:messages:2.0:ListResponse",
            ],
            "startIndex": 1,
            "totalResults": 1,
        }

    os.remove("team1_user_group_membership.yaml")


def test_export_group_membership_csv() -> None:
    """
    Test the ``export_group_membership_csv`` helper.
    """

    groups = {
        "Resources": [
            {
                "displayName": "SCIM Test Group",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                "members": [
                    {
                        "display": "Test Account 01",
                        "value": "samlp|example|testaccount01@example.com",
                    },
                ],
                "meta": {
                    "resourceType": "Group",
                },
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:Group",
                ],
            },
            {
                "displayName": "SCIM Test Group 02",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5537",
                "members": [
                    {
                        "display": "Test Account 02",
                        "value": "samlp|example|testaccount02@example.com",
                    },
                ],
                "meta": {
                    "resourceType": "Group",
                },
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:Group",
                ],
            },
        ],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 2,
    }

    data = [
        [
            "SCIM Test Group",
            "b2a691ca-0ef8-464c-9601-9c50158c5426",
            "Test Account 01",
            "samlp|example|testaccount01@example.com",
        ],
        [
            "SCIM Test Group 02",
            "b2a691ca-0ef8-464c-9601-9c50158c5537",
            "Test Account 02",
            "samlp|example|testaccount02@example.com",
        ],
    ]
    i = 0

    export_group_membership_csv(groups, "team1")
    with open(
        "team1_user_group_membership.csv",
        "r",
        encoding="utf-8",
    ) as csv_test_output:
        file_content = csv.reader(csv_test_output)
        assert next(file_content) == ["Group Name", "Group ID", "User", "Username"]
        for row in file_content:
            assert row == data[i]
            i += 1

        os.remove("team1_user_group_membership.csv")


def test_export_group_membership_csv_empty_group() -> None:
    """
    Test the ``export_group_membership_csv`` helper with an empty group.
    """
    groups = {
        "Resources": [
            {
                "displayName": "SCIM Test Group",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                "members": [],
                "meta": {
                    "resourceType": "Group",
                },
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:Group",
                ],
            },
        ],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 1,
    }

    export_group_membership_csv(groups, "team1")

    file_exists = os.path.isfile("team1_user_group_membership.csv")
    assert not file_exists


def test_export_group_membership_csv_pagination() -> None:
    """
    Test the ``export_group_membership_csv`` when pagination is needed.
    """

    groups = {
        "Resources": [
            {
                "displayName": "SCIM Test Group",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5426",
                "members": [
                    {
                        "display": "Test Account 01",
                        "value": "samlp|example|testaccount01@example.com",
                    },
                ],
                "meta": {
                    "resourceType": "Group",
                },
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:Group",
                ],
            },
            {
                "displayName": "SCIM Test Group 02",
                "id": "b2a691ca-0ef8-464c-9601-9c50158c5537",
                "members": [
                    {
                        "display": "Test Account 02",
                        "value": "samlp|example|testaccount02@example.com",
                    },
                ],
                "meta": {
                    "resourceType": "Group",
                },
                "schemas": [
                    "urn:ietf:params:scim:schemas:core:2.0:Group",
                ],
            },
        ],
        "itemsPerPage": 100,
        "schemas": [
            "urn:ietf:params:scim:api:messages:2.0:ListResponse",
        ],
        "startIndex": 1,
        "totalResults": 102,
    }

    data = [
        [
            "SCIM Test Group",
            "b2a691ca-0ef8-464c-9601-9c50158c5426",
            "Test Account 01",
            "samlp|example|testaccount01@example.com",
        ],
        [
            "SCIM Test Group 02",
            "b2a691ca-0ef8-464c-9601-9c50158c5537",
            "Test Account 02",
            "samlp|example|testaccount02@example.com",
        ],
        [
            "SCIM Test Group",
            "b2a691ca-0ef8-464c-9601-9c50158c5426",
            "Test Account 01",
            "samlp|example|testaccount01@example.com",
        ],
        [
            "SCIM Test Group 02",
            "b2a691ca-0ef8-464c-9601-9c50158c5537",
            "Test Account 02",
            "samlp|example|testaccount02@example.com",
        ],
    ]
    i = 0

    export_group_membership_csv(groups, "team1")
    with open(
        "team1_user_group_membership.csv",
        "r",
        encoding="utf-8",
    ) as csv_test_output:
        file_content = csv.reader(csv_test_output)
        assert next(file_content) == ["Group Name", "Group ID", "User", "Username"]
        for row in file_content:
            assert row == data[i]
            i += 1

        os.remove("team1_user_group_membership.csv")
