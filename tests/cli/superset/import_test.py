"""
Tests for the import commands.
"""

# pylint: disable=invalid-name

from pathlib import Path
from unittest import mock
from uuid import UUID

import pytest
import yaml
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.cli.superset.main import superset_cli


def test_import_rls(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``import_rls`` command.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.import_.SupersetClient")
    client = SupersetClient()
    rls = [
        {
            "clause": "client_id = 9",
            "description": "Rule description",
            "filter_type": "Regular",
            "group_key": "department",
            "name": "Rule name",
            "roles": ["Gamma"],
            "tables": ["main.test_table"],
        },
    ]
    fs.create_file("rls.yaml", contents=yaml.dump(rls))

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "import-rls"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    client.import_rls.assert_called_with(rls[0])


def test_import_roles(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``import_roles`` command.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.import_.SupersetClient")
    client = SupersetClient()
    roles = [
        {
            "name": "Role name",
            "permissions": ["can do this", "can do that"],
        },
    ]
    fs.create_file("roles.yaml", contents=yaml.dump(roles))

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "import-roles"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    client.import_role.assert_called_with(roles[0])


def test_import_ownership(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``import_ownership`` command.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.import_.SupersetClient")
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    client = SupersetClient()
    client.export_users.return_value = [{"id": 1, "email": "admin@example.com"}]
    client.get_uuids.return_value = {1: UUID("e4e6a14b-c3e8-4fdf-a850-183ba6ce15e0")}
    ownership = {
        "dataset": [
            {
                "name": "test_table",
                "owners": ["admin@example.com"],
                "uuid": "e4e6a14b-c3e8-4fdf-a850-183ba6ce15e0",
            },
        ],
    }
    fs.create_file("ownership.yaml", contents=yaml.dump(ownership))

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "import-ownership"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    client.import_ownership.assert_called_once_with(
        "dataset",
        ownership["dataset"][0],
        {"admin@example.com": 1},
        {"e4e6a14b-c3e8-4fdf-a850-183ba6ce15e0": 1},
    )


def test_import_ownership_progress_log(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``import_ownership`` command with an existing log file.
    """
    logs_content = {
        "assets": [
            {
                "path": "/path/to/root/first_path",
                "status": "SUCCESS",
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e6",
            },
            {
                "path": "/path/to/root/second_path",
                "status": "FAILED",
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e7",
            },
        ],
        "ownership": [
            {
                "status": "SUCCESS",
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e8",
            },
            {
                "status": "FAILED",
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e9",
            },
        ],
    }
    fs.create_file("progress.log", contents=yaml.dump(logs_content))
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.import_.SupersetClient")
    client = SupersetClient()
    client.export_users.return_value = [
        {"id": 1, "email": "admin@example.com"},
        {"id": 2, "email": "viewer@example.com"},
    ]
    users = {
        "admin@example.com": 1,
        "viewer@example.com": 2,
    }
    client.get_uuids.return_value = {
        1: UUID("18ddf8ab-68f9-4c15-ba9f-c75921b019e6"),
        2: UUID("18ddf8ab-68f9-4c15-ba9f-c75921b019e7"),
        3: UUID("18ddf8ab-68f9-4c15-ba9f-c75921b019e8"),
        4: UUID("18ddf8ab-68f9-4c15-ba9f-c75921b019e9"),
        5: UUID("e4e6a14b-c3e8-4fdf-a850-183ba6ce15e0"),
    }
    uuids = {
        "18ddf8ab-68f9-4c15-ba9f-c75921b019e6": 1,
        "18ddf8ab-68f9-4c15-ba9f-c75921b019e7": 2,
        "18ddf8ab-68f9-4c15-ba9f-c75921b019e8": 3,
        "18ddf8ab-68f9-4c15-ba9f-c75921b019e9": 4,
        "e4e6a14b-c3e8-4fdf-a850-183ba6ce15e0": 5,
    }
    ownership = {
        "dataset": [
            {
                "name": "test_table",
                "owners": ["admin@example.com"],
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e8",
            },
            {
                "name": "other_table",
                "owners": ["viewer@example.com"],
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e9",
            },
            {
                "name": "yet_another_table",
                "owners": ["admin@example.com", "viewer@example.com"],
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e7",
            },
            {
                "name": "just_another_test_table",
                "owners": ["viewer@example.com", "admin@example.com"],
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e6",
            },
            {
                "name": "last_table",
                "owners": ["viewer@example.com"],
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e0",
            },
        ],
    }
    fs.create_file("ownership.yaml", contents=yaml.dump(ownership))

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "import-ownership"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Should skip `18ddf8ab-68f9-4c15-ba9f-c75921b019e7` as its import
    # failed. Should also skip `18ddf8ab-68f9-4c15-ba9f-c75921b019e8`
    # because its ownership was sucessfully imported, but retry
    # `18ddf8ab-68f9-4c15-ba9f-c75921b019e9`.
    client.import_ownership.assert_has_calls(
        [
            mock.call("dataset", ownership["dataset"][1], users, uuids),
            mock.call("dataset", ownership["dataset"][3], users, uuids),
            mock.call("dataset", ownership["dataset"][4], users, uuids),
        ],
    )


def test_import_ownership_failure(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``import_ownership`` command when a failure happens without
    the ``continue-on-error`` flag.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.import_.SupersetClient")
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    client = SupersetClient()
    client.export_users.return_value = [{"id": 1, "email": "admin@example.com"}]
    client.get_uuids.return_value = {
        1: UUID("18ddf8ab-68f9-4c15-ba9f-c75921b019e6"),
        2: UUID("18ddf8ab-68f9-4c15-ba9f-c75921b019e7"),
    }
    ownership = {
        "dataset": [
            {
                "name": "test_table",
                "owners": ["admin@example.com"],
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e6",
            },
            {
                "name": "test_table_two",
                "owners": ["admin@example.com"],
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e7",
            },
        ],
    }
    fs.create_file("ownership.yaml", contents=yaml.dump(ownership))
    client.import_ownership.side_effect = [
        None,
        Exception("An error occurred!"),
    ]

    assert not Path("progress.log").exists()

    runner = CliRunner()
    with pytest.raises(Exception):
        runner.invoke(
            superset_cli,
            ["https://superset.example.org/", "import-ownership"],
            catch_exceptions=False,
        )

    assert Path("progress.log").exists()
    with open("progress.log", encoding="utf-8") as log:
        content = yaml.load(log, Loader=yaml.SafeLoader)

    assert content == {
        "assets": [],
        "ownership": [
            {
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e6",
                "status": "SUCCESS",
            },
        ],
    }


def test_import_ownership_failure_continue(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``import_ownership`` command when a failure happens
    with the ``continue-on-error`` flag.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.import_.SupersetClient")
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    client = SupersetClient()
    client.export_users.return_value = [{"id": 1, "email": "admin@example.com"}]
    client.get_uuids.return_value = {
        1: UUID("18ddf8ab-68f9-4c15-ba9f-c75921b019e6"),
        2: UUID("18ddf8ab-68f9-4c15-ba9f-c75921b019e7"),
    }
    ownership = {
        "dataset": [
            {
                "name": "test_table",
                "owners": ["admin@example.com"],
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e6",
            },
            {
                "name": "test_table_two",
                "owners": ["admin@example.com"],
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e7",
            },
        ],
    }
    fs.create_file("ownership.yaml", contents=yaml.dump(ownership))
    client.import_ownership.side_effect = [
        Exception("An error occurred!"),
        None,
    ]

    assert not Path("progress.log").exists()

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "import-ownership", "--continue-on-error"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    assert Path("progress.log").exists()
    with open("progress.log", encoding="utf-8") as log:
        content = yaml.load(log, Loader=yaml.SafeLoader)

    assert content == {
        "assets": [],
        "ownership": [
            {
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e6",
                "status": "FAILED",
            },
            {
                "uuid": "18ddf8ab-68f9-4c15-ba9f-c75921b019e7",
                "status": "SUCCESS",
            },
        ],
    }


def test_import_ownership_continue_on_errors(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``import_ownership`` command with the ``continue-on-error`` flag.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    mocker.patch("preset_cli.cli.superset.import_.SupersetClient")
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    ownership = {
        "dataset": [
            {
                "name": "test_table",
                "owners": ["admin@example.com"],
                "uuid": "uuid1",
            },
            {
                "name": "test_table_two",
                "owners": ["admin@example.com"],
                "uuid": "uuid2",
            },
        ],
    }
    fs.create_file("ownership.yaml", contents=yaml.dump(ownership))

    assert not Path("progress.log").exists()

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "import-ownership", "--continue-on-error"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert not Path("progress.log").exists()
