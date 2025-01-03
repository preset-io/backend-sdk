"""
Tests for the import commands.
"""

# pylint: disable=invalid-name

from pathlib import Path
from unittest import mock

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

    client.import_ownership.assert_called_with("dataset", ownership["dataset"][0])


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
                "uuid": "uuid1",
            },
            {
                "path": "/path/to/root/second_path",
                "status": "FAILED",
                "uuid": "uuid2",
            },
        ],
        "ownership": [
            {
                "status": "SUCCESS",
                "uuid": "uuid3",
            },
            {
                "status": "FAILED",
                "uuid": "uuid4",
            },
        ],
    }
    fs.create_file("progress.log", contents=yaml.dump(logs_content))
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.import_.SupersetClient")
    client = SupersetClient()
    ownership = {
        "dataset": [
            {
                "name": "test_table",
                "owners": ["admin@example.com"],
                "uuid": "uuid1",
            },
            {
                "name": "other_table",
                "owners": ["admin@example.com"],
                "uuid": "uuid2",
            },
            {
                "name": "yet_another_table",
                "owners": ["admin@example.com"],
                "uuid": "uuid3",
            },
            {
                "name": "just_another_test_table",
                "owners": ["admin@example.com"],
                "uuid": "uuid4",
            },
            {
                "name": "last_table",
                "owners": ["admin@example.com"],
                "uuid": "uuid5",
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

    # Should skip `uuid2` as its import failed. Should also skip `uuid3`
    # because its ownership was sucessfully imported, but retry `uuid4`.
    client.import_ownership.assert_has_calls(
        [
            mock.call("dataset", ownership["dataset"][0]),
            mock.call("dataset", ownership["dataset"][3]),
            mock.call("dataset", ownership["dataset"][4]),
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
        "ownership": [
            {
                "uuid": "uuid1",
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
        "ownership": [
            {
                "uuid": "uuid1",
                "status": "FAILED",
            },
            {
                "uuid": "uuid2",
                "status": "SUCCESS",
            },
        ],
    }


def test_import_ownership_continue_no_errors(
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
