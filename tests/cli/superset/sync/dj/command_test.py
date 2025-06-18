"""
Tests for the DJ sync command.
"""

# pylint: disable=invalid-name

from uuid import UUID

from click.testing import CliRunner
from pytest_mock import MockerFixture
from yarl import URL

from preset_cli.cli.superset.main import superset_cli


def test_dj_command(mocker: MockerFixture) -> None:
    """
    Tests for the sync command.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dj.command.SupersetClient",
    )
    UsernamePasswordAuth = mocker.patch(
        "preset_cli.cli.superset.main.UsernamePasswordAuth",
    )
    DJClient = mocker.patch("preset_cli.cli.superset.sync.dj.command.DJClient")
    sync_cube = mocker.patch("preset_cli.cli.superset.sync.dj.command.sync_cube")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dj",
            "--cubes",
            "default.repair_orders_cube",
            "--database-uuid",
            "a1ad7bd5-b1a3-4d64-afb1-a84c2f4d7715",
            "--schema",
            "schema",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    SupersetClient.assert_called_once_with(
        URL("https://superset.example.org/"),
        UsernamePasswordAuth(),
    )
    DJClient.assert_called_once_with("http://localhost:8000")
    DJClient().basic_login.assert_called_once_with("dj", "dj")

    sync_cube.assert_called_once_with(
        UUID("a1ad7bd5-b1a3-4d64-afb1-a84c2f4d7715"),
        "schema",
        DJClient(),
        SupersetClient(),
        "default.repair_orders_cube",
        None,
    )
