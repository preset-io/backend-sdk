"""
Tests for the dbt import command.
"""
# pylint: disable=invalid-name, too-many-lines

import os
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.cli.superset.main import superset_cli
from preset_cli.cli.superset.sync.dbt.command import (
    get_account_id,
    get_job_id,
    get_project_id,
)
from preset_cli.exceptions import DatabaseNotFoundError

dirname, _ = os.path.split(os.path.abspath(__file__))
with open(os.path.join(dirname, "manifest.json"), encoding="utf-8") as fp:
    manifest_contents = fp.read()


def test_dbt_core(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command.
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
            "dbt-core",
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
        "default",
        None,
        False,
        False,
        "",
    )
    models = [
        {
            "database": "examples_dev",
            "columns": [],
            "meta": {},
            "description": "",
            "name": "messages_channels",
            "tags": [],
            "schema": "public",
            "unique_id": "model.superset_examples.messages_channels",
            "created_at": 1642628933.004452,
            "children": ["metric.superset_examples.cnt"],
            "depends_on": {
                "macros": [],
                "nodes": [
                    "source.superset_examples.public.channels",
                    "source.superset_examples.public.messages",
                ],
            },
            "unrendered_config": {"materialized": "view"},
            "resource_type": "model",
            "path": "slack/messages_channels.sql",
            "extra_ctes": [],
            "package_name": "superset_examples",
            "alias": "messages_channels",
            "relation_name": '"examples_dev"."public"."messages_channels"',
            "config": {
                "enabled": True,
                "alias": None,
                "schema": None,
                "database": None,
                "tags": [],
                "meta": {},
                "materialized": "view",
                "persist_docs": {},
                "quoting": {},
                "column_types": {},
                "full_refresh": None,
                "on_schema_change": "ignore",
                "post-hook": [],
                "pre-hook": [],
            },
            "patch_path": None,
            "compiled_sql": (
                "SELECT messages.ts, channels.name, messages.text "
                'FROM "examples_dev"."public"."messages" messages '
                'JOIN "examples_dev"."public"."channels" channels '
                "ON messages.channel_id = channels.id"
            ),
            "extra_ctes_injected": True,
            "deferred": False,
            "root_path": "/Users/beto/Projects/dbt-examples/superset_examples",
            "original_file_path": "models/slack/messages_channels.sql",
            "refs": [],
            "fqn": ["superset_examples", "slack", "messages_channels"],
            "raw_sql": (
                "SELECT messages.ts, channels.name, messages.text "
                "FROM {{ source ('public', 'messages') }} messages "
                "JOIN {{ source ('public', 'channels') }} channels "
                "ON messages.channel_id = channels.id"
            ),
            "build_path": None,
            "sources": [["public", "channels"], ["public", "messages"]],
            "checksum": {
                "name": "sha256",
                "checksum": "b4ce232b28280daa522b37e12c36b67911e2a98456b8a3b99440075ec5564609",
            },
            "docs": {"show": True},
            "compiled_path": "target/compiled/superset_examples/models/slack/messages_channels.sql",
            "compiled": True,
        },
    ]
    metrics = [
        {
            "label": "",
            "sql": "*",
            "depends_on": ["model.superset_examples.messages_channels"],
            "meta": {},
            "description": "",
            "name": "cnt",
            "type": "count",
            "filters": [],
            "unique_id": "metric.superset_examples.cnt",
            "created_at": 1642630986.1942852,
            "package_name": "superset_examples",
            "sources": [],
            "root_path": "/Users/beto/Projects/dbt-examples/superset_examples",
            "path": "slack/schema.yml",
            "resource_type": "metric",
            "original_file_path": "models/slack/schema.yml",
            "model": "ref('messages_channels')",
            "timestamp": None,
            "fqn": ["superset_examples", "slack", "cnt"],
            "time_grains": [],
            "tags": [],
            "refs": [["messages_channels"]],
            "dimensions": [],
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
    sync_exposures.assert_called_with(
        client,
        exposures,
        sync_datasets(),
        {("public", "messages_channels"): "ref(messages_channels)"},
    )


def test_dbt_core_dbt_project(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command with a ``dbt_project.yml`` file.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    dbt_project = root / "default/dbt_project.yml"
    fs.create_file(
        dbt_project,
        contents=yaml.dump(
            {
                "name": "my_project",
                "profile": "default",
                "target-path": "target",
            },
        ),
    )
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(dbt_project),
            "--profiles",
            str(profiles),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "my_project",
        "default",
        None,
        False,
        False,
        "",
    )


def test_dbt_core_invalid_argument(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command with an invalid argument.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles)
    wrong = root / "wrong"
    fs.create_file(wrong)

    mocker.patch("preset_cli.cli.superset.sync.dbt.command.SupersetClient")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(wrong),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert result.output == "FILE should be either manifest.json or dbt_project.yml\n"


def test_dbt(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt`` command.

    Initially ``dbt-core`` was just ``dbt``. This aliases was added for backwards
    compatibility.
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
        "default",
        None,
        False,
        False,
        "",
    )
    models = [
        {
            "meta": {},
            "tags": [],
            "columns": [],
            "schema": "public",
            "name": "messages_channels",
            "database": "examples_dev",
            "description": "",
            "unique_id": "model.superset_examples.messages_channels",
            "extra_ctes": [],
            "compiled_path": "target/compiled/superset_examples/models/slack/messages_channels.sql",
            "build_path": None,
            "path": "slack/messages_channels.sql",
            "docs": {"show": True},
            "relation_name": '"examples_dev"."public"."messages_channels"',
            "depends_on": {
                "macros": [],
                "nodes": [
                    "source.superset_examples.public.channels",
                    "source.superset_examples.public.messages",
                ],
            },
            "children": ["metric.superset_examples.cnt"],
            "original_file_path": "models/slack/messages_channels.sql",
            "sources": [["public", "channels"], ["public", "messages"]],
            "resource_type": "model",
            "compiled_sql": (
                "SELECT messages.ts, channels.name, messages.text "
                'FROM "examples_dev"."public"."messages" messages '
                'JOIN "examples_dev"."public"."channels" channels '
                "ON messages.channel_id = channels.id"
            ),
            "config": {
                "enabled": True,
                "alias": None,
                "schema": None,
                "database": None,
                "tags": [],
                "meta": {},
                "materialized": "view",
                "persist_docs": {},
                "quoting": {},
                "column_types": {},
                "full_refresh": None,
                "on_schema_change": "ignore",
                "post-hook": [],
                "pre-hook": [],
            },
            "compiled": True,
            "fqn": ["superset_examples", "slack", "messages_channels"],
            "deferred": False,
            "alias": "messages_channels",
            "checksum": {
                "name": "sha256",
                "checksum": "b4ce232b28280daa522b37e12c36b67911e2a98456b8a3b99440075ec5564609",
            },
            "created_at": 1642628933.004452,
            "raw_sql": (
                "SELECT messages.ts, channels.name, messages.text "
                "FROM {{ source ('public', 'messages') }} messages "
                "JOIN {{ source ('public', 'channels') }} channels "
                "ON messages.channel_id = channels.id"
            ),
            "patch_path": None,
            "root_path": "/Users/beto/Projects/dbt-examples/superset_examples",
            "extra_ctes_injected": True,
            "package_name": "superset_examples",
            "unrendered_config": {"materialized": "view"},
            "refs": [],
        },
    ]
    metrics = [
        {
            "meta": {},
            "depends_on": ["model.superset_examples.messages_channels"],
            "unique_id": "metric.superset_examples.cnt",
            "label": "",
            "sql": "*",
            "type": "count",
            "name": "cnt",
            "description": "",
            "filters": [],
            "model": "ref('messages_channels')",
            "sources": [],
            "original_file_path": "models/slack/schema.yml",
            "resource_type": "metric",
            "tags": [],
            "path": "slack/schema.yml",
            "created_at": 1642630986.1942852,
            "fqn": ["superset_examples", "slack", "cnt"],
            "package_name": "superset_examples",
            "timestamp": None,
            "root_path": "/Users/beto/Projects/dbt-examples/superset_examples",
            "time_grains": [],
            "refs": [["messages_channels"]],
            "dimensions": [],
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
    sync_exposures.assert_called_with(
        client,
        exposures,
        sync_datasets(),
        {("public", "messages_channels"): "ref(messages_channels)"},
    )


def test_dbt_core_no_exposures(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command when no exposures file is passed.
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
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_exposures.assert_not_called()


def test_dbt_core_default_profile(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command when the profile is not passed
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
            "dbt-core",
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
        "default",
        None,
        False,
        False,
        "",
    )


def test_dbt_core_no_database(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command when no database is found and ``--import-db`` not passed.
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
            "dbt-core",
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


def test_dbt_cloud(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
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
    dbt_client.get_models.return_value = models
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
            "unique_id": "metric.superset_examples.cnt",
        },
    ]
    dbt_client.get_metrics.return_value = metrics
    database = mocker.MagicMock()
    superset_client.get_databases.return_value = [database]
    superset_client.get_database.return_value = database

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "123",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_datasets.assert_called_with(
        superset_client,
        models,
        metrics,
        database,
        False,
        "",
    )


def test_dbt_cloud_no_job_id(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command when no job ID is specified.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
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
    dbt_client.get_models.return_value = models
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
            "unique_id": "metric.superset_examples.cnt",
        },
    ]
    dbt_client.get_metrics.return_value = metrics
    dbt_client.get_accounts.return_value = [{"id": 1, "name": "My account"}]
    dbt_client.get_projects.return_value = [{"id": 1000, "name": "My project"}]
    dbt_client.get_jobs.return_value = [{"id": 123, "name": "My job"}]
    database = mocker.MagicMock()
    superset_client.get_databases.return_value = [database]
    superset_client.get_database.return_value = database

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    dbt_client.get_database_name.assert_called_with(123)
    dbt_client.get_models.assert_called_with(123)
    dbt_client.get_metrics.assert_called_with(123)
    sync_datasets.assert_called_with(
        superset_client,
        models,
        metrics,
        database,
        False,
        "",
    )


def test_get_account_id(mocker: MockerFixture) -> None:
    """
    Test the ``get_account_id`` helper.
    """
    client = mocker.MagicMock()

    client.get_accounts.return_value = []
    with pytest.raises(SystemExit) as excinfo:
        get_account_id(client)
    assert excinfo.type == SystemExit
    assert excinfo.value.code == 1

    client.get_accounts.return_value = [
        {"id": 1, "name": "My account"},
    ]
    assert get_account_id(client) == 1

    client.get_accounts.return_value = [
        {"id": 1, "name": "My account"},
        {"id": 3, "name": "My other account"},
    ]
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.input",
        side_effect=["invalid", "2"],
    )
    assert get_account_id(client) == 3


def test_get_project_id(mocker: MockerFixture) -> None:
    """
    Test the ``get_project_id`` helper.
    """
    client = mocker.MagicMock()

    client.get_projects.return_value = []
    with pytest.raises(SystemExit) as excinfo:
        get_project_id(client, account_id=42)
    assert excinfo.type == SystemExit
    assert excinfo.value.code == 1

    client.get_projects.return_value = [
        {"id": 1, "name": "My project"},
    ]
    assert get_project_id(client, account_id=42) == 1

    client.get_projects.return_value = [
        {"id": 1, "name": "My project"},
        {"id": 3, "name": "My other project"},
    ]
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.input",
        side_effect=["invalid", "2"],
    )
    assert get_project_id(client, account_id=42) == 3

    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_account_id",
        return_value=42,
    )
    client.get_projects.return_value = [
        {"id": 1, "name": "My project"},
    ]
    assert get_project_id(client) == 1
    client.get_projects.assert_called_with(42)


def test_get_job_id(mocker: MockerFixture) -> None:
    """
    Test the ``get_job_id`` helper.
    """
    client = mocker.MagicMock()

    client.get_jobs.return_value = []
    with pytest.raises(SystemExit) as excinfo:
        get_job_id(client, account_id=42, project_id=43)
    assert excinfo.type == SystemExit
    assert excinfo.value.code == 1

    client.get_jobs.return_value = [
        {"id": 1, "name": "My job"},
    ]
    assert get_job_id(client, account_id=42, project_id=43) == 1

    client.get_jobs.return_value = [
        {"id": 1, "name": "My job"},
        {"id": 3, "name": "My other job"},
    ]
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.input",
        side_effect=["invalid", "2"],
    )
    assert get_job_id(client, account_id=42, project_id=43) == 3

    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_account_id",
        return_value=42,
    )
    client.get_jobs.return_value = [
        {"id": 1, "name": "My job"},
    ]
    assert get_job_id(client, project_id=43) == 1
    client.get_jobs.assert_called_with(42, 43)


def test_dbt_cloud_no_database(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command when no database is found.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    dbt_client.get_database_name.return_value = "my_db"
    superset_client.get_databases.return_value = []

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "123",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.output == 'No database named "my_db" was found\n'


def test_dbt_cloud_multiple_databases(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command when multiple databases are found.

    This should never happen, since Supersret has a uniqueness contraint on the table
    name. Nevertheless, test this for completeness.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    dbt_client.get_database_name.return_value = "my_db"
    superset_client.get_databases.return_value = [
        mocker.MagicMock(),
        mocker.MagicMock(),
    ]

    runner = CliRunner()
    with pytest.raises(Exception) as excinfo:
        runner.invoke(
            superset_cli,
            [
                "https://superset.example.org/",
                "sync",
                "dbt-cloud",
                "XXX",
                "123",
            ],
            catch_exceptions=False,
        )
    assert str(excinfo.value) == "More than one database with the same name found"


def test_dbt_core_exposures_only(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``--exposures-only`` option with dbt core.
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
    client.get_datasets.return_value = [
        {"schema": "public", "table_name": "messages_channels"},
        {"schema": "public", "table_name": "some_other_table"},
    ]
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
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--exposures",
            str(exposures),
            "--exposures-only",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_not_called()
    sync_datasets.assert_not_called()
    sync_exposures.assert_called_with(
        client,
        exposures,
        [
            {"schema": "public", "table_name": "messages_channels"},
        ],
        {("public", "messages_channels"): "ref(messages_channels)"},
    )


def test_dbt_cloud_exposures_only(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``--exposures-only`` option with dbt cloud.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    superset_client.get_datasets.return_value = [
        {"schema": "public", "table_name": "messages_channels"},
        {"schema": "public", "table_name": "some_other_table"},
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
    )
    sync_exposures = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_exposures",
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
    dbt_client.get_models.return_value = models
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
            "unique_id": "metric.superset_examples.cnt",
        },
    ]
    dbt_client.get_metrics.return_value = metrics
    database = mocker.MagicMock()
    superset_client.get_databases.return_value = [database]
    superset_client.get_database.return_value = database

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "123",
            "--exposures",
            str(exposures),
            "--exposures-only",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_datasets.assert_not_called()
    sync_exposures.assert_called_with(
        superset_client,
        exposures,
        [
            {"schema": "public", "table_name": "messages_channels"},
        ],
        {("public", "messages_channels"): "ref(messages_channels)"},
    )
