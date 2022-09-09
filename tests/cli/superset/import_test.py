"""
Tests for the import commands.
"""

# pylint: disable=invalid-name

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


def test_import_ownership(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``import_ownership`` command.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.import_.SupersetClient")
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

    client.import_ownership.assert_called_with("dataset", ownership["dataset"])
