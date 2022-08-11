"""
Tests for the dbt import command.
"""
# pylint: disable=invalid-name

import os
from pathlib import Path

from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.cli.superset.main import superset_cli
from preset_cli.exceptions import DatabaseNotFoundError

dirname, _ = os.path.split(os.path.abspath(__file__))
with open(os.path.join(dirname, "manifest.json"), encoding="utf-8") as fp:
    manifest_contents = fp.read()


def test_dbt(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt`` command.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
    )
    sync_exposures = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_exposures",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt",
            str(manifest),
            "--profiles",
            str(profiles),
            "--exposures",
            str(exposures),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "default",
        "dev",
        False,
        False,
        "",
    )
    models = [
        {
            "database": "examples_dev",
            "description": "",
            "meta": {},
            "name": "messages_channels",
            "schema": "public",
            "unique_id": "model.superset_examples.messages_channels",
        },
    ]
    metrics = [
        {
            "depends_on": ["model.superset_examples.messages_channels"],
            "description": "",
            "filters": [],
            "label": "",
            "meta": {},
            "name": "cnt",
            "sql": "*",
            "type": "count",
        },
    ]
    sync_datasets.assert_called_with(
        client,
        models,
        metrics,
        sync_database(),
        False,
        "",
    )
    sync_exposures.assert_called_with(client, exposures, sync_datasets())


def test_dbt_no_exposures(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt`` command when no exposures file is passed.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles)

    mocker.patch("preset_cli.cli.superset.sync.dbt.command.SupersetClient")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    mocker.patch("preset_cli.cli.superset.sync.dbt.command.sync_database")
    mocker.patch("preset_cli.cli.superset.sync.dbt.command.sync_datasets")
    sync_exposures = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_exposures",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt",
            str(manifest),
            "--profiles",
            str(profiles),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_exposures.assert_not_called()


def test_dbt_default_profile(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt`` command when the profile is not passed
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    mocker.patch("preset_cli.cli.superset.sync.dbt.command.sync_datasets")
    mocker.patch("preset_cli.cli.superset.sync.dbt.command.sync_exposures")
    # pylint: disable=redefined-outer-name
    os = mocker.patch("preset_cli.cli.superset.sync.dbt.command.os")
    os.path.expanduser.return_value = str(profiles)

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt",
            str(manifest),
            "--exposures",
            str(exposures),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "default",
        "dev",
        False,
        False,
        "",
    )


def test_dbt_no_database(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt`` command when no database is found and ``--import-db`` not passed.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    mocker.patch("preset_cli.cli.superset.sync.dbt.command.SupersetClient")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
        side_effect=DatabaseNotFoundError(),
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt",
            str(manifest),
            "--profiles",
            str(profiles),
            "--exposures",
            str(exposures),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.output == "No database was found, pass --import-db to create\n"
