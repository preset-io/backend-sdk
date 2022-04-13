"""
Tests for ``preset_cli.cli.superset.sync.dbt.datasets``.
"""
# pylint: disable=invalid-name

import json
from pathlib import Path
from unittest import mock

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.cli.superset.sync.dbt.datasets import sync_datasets

manifest_config = {
    "metrics": {
        "metric.superset_examples.cnt": {
            "fqn": ["superset_examples", "slack", "cnt"],
            "unique_id": "metric.superset_examples.cnt",
            "package_name": "superset_examples",
            "root_path": "/Users/beto/Projects/dbt-examples/superset_examples",
            "path": "slack/schema.yml",
            "original_file_path": "models/slack/schema.yml",
            "model": "ref('messages_channels')",
            "name": "cnt",
            "description": "",
            "label": "",
            "type": "count",
            "sql": "*",
            "timestamp": None,
            "filters": [],
            "time_grains": [],
            "dimensions": [],
            "resource_type": "metric",
            "meta": {},
            "tags": [],
            "sources": [],
            "depends_on": {
                "macros": [],
                "nodes": ["model.superset_examples.messages_channels"],
            },
            "refs": [["messages_channels"]],
            "created_at": 1642630986.1942852,
        },
    },
    "sources": {
        "source.superset_examples.public.messages": {
            "fqn": ["superset_examples", "slack", "public", "messages"],
            "database": "examples_dev",
            "schema": "public",
            "unique_id": "source.superset_examples.public.messages",
            "package_name": "superset_examples",
            "root_path": "/Users/beto/Projects/dbt-examples/superset_examples",
            "path": "models/slack/schema.yml",
            "original_file_path": "models/slack/schema.yml",
            "name": "messages",
            "source_name": "public",
            "source_description": "",
            "loader": "",
            "identifier": "messages",
            "resource_type": "source",
            "quoting": {
                "database": None,
                "schema": None,
                "identifier": None,
                "column": None,
            },
            "loaded_at_field": None,
            "freshness": {
                "warn_after": {"count": None, "period": None},
                "error_after": {"count": None, "period": None},
                "filter": None,
            },
            "external": None,
            "description": "Messages in the Slack channel",
            "columns": {},
            "meta": {},
            "source_meta": {},
            "tags": [],
            "config": {"enabled": True},
            "patch_path": None,
            "unrendered_config": {},
            "relation_name": '"examples_dev"."public"."messages"',
            "created_at": 1642628933.0432189,
        },
        "source.superset_examples.public.channels": {
            "fqn": ["superset_examples", "slack", "public", "channels"],
            "database": "examples_dev",
            "schema": "public",
            "unique_id": "source.superset_examples.public.channels",
            "package_name": "superset_examples",
            "root_path": "/Users/beto/Projects/dbt-examples/superset_examples",
            "path": "models/slack/schema.yml",
            "original_file_path": "models/slack/schema.yml",
            "name": "channels",
            "source_name": "public",
            "source_description": "",
            "loader": "",
            "identifier": "channels",
            "resource_type": "source",
            "quoting": {
                "database": None,
                "schema": None,
                "identifier": None,
                "column": None,
            },
            "loaded_at_field": None,
            "freshness": {
                "warn_after": {"count": None, "period": None},
                "error_after": {"count": None, "period": None},
                "filter": None,
            },
            "external": None,
            "description": "Information about Slack channels",
            "columns": {},
            "meta": {},
            "source_meta": {},
            "tags": [],
            "config": {"enabled": True},
            "patch_path": None,
            "unrendered_config": {},
            "relation_name": '"examples_dev"."public"."channels"',
            "created_at": 1642628933.043388,
        },
    },
    "nodes": {
        "model.superset_examples.messages_channels": {
            "raw_sql": """SELECT
  messages.ts,
  channels.name,
  messages.text
FROM
  {{ source ('public', 'messages') }} messages
  JOIN {{ source ('public', 'channels') }} channels ON messages.channel_id = channels.id""",
            "compiled": True,
            "resource_type": "model",
            "depends_on": {
                "macros": [],
                "nodes": [
                    "source.superset_examples.public.channels",
                    "source.superset_examples.public.messages",
                ],
            },
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
            "database": "examples_dev",
            "schema": "public",
            "fqn": ["superset_examples", "slack", "messages_channels"],
            "unique_id": "model.superset_examples.messages_channels",
            "package_name": "superset_examples",
            "root_path": "/Users/beto/Projects/dbt-examples/superset_examples",
            "path": "slack/messages_channels.sql",
            "original_file_path": "models/slack/messages_channels.sql",
            "name": "messages_channels",
            "alias": "messages_channels",
            "checksum": {
                "name": "sha256",
                "checksum": "b4ce232b28280daa522b37e12c36b67911e2a98456b8a3b99440075ec5564609",
            },
            "tags": [],
            "refs": [],
            "sources": [["public", "channels"], ["public", "messages"]],
            "description": "",
            "columns": {},
            "meta": {},
            "docs": {"show": True},
            "patch_path": None,
            "compiled_path": "target/compiled/superset_examples/models/slack/messages_channels.sql",
            "build_path": None,
            "deferred": False,
            "unrendered_config": {"materialized": "view"},
            "created_at": 1642628933.004452,
            "compiled_sql": """SELECT
  messages.ts,
  channels.name,
  messages.text
FROM
  "examples_dev"."public"."messages" messages
  JOIN "examples_dev"."public"."channels" channels ON messages.channel_id = channels.id""",
            "extra_ctes_injected": True,
            "extra_ctes": [],
            "relation_name": '"examples_dev"."public"."messages_channels"',
        },
    },
}


def test_sync_datasets_new(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``sync_datasets`` when no datasets exist yet.
    """
    manifest = Path("/path/to/root/default/target/manifest.json")
    fs.create_file(manifest, contents=json.dumps(manifest_config))
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}, {"id": 2}, {"id": 3}]

    sync_datasets(
        client=client,
        manifest_path=manifest,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
    )
    client.create_dataset.assert_has_calls(
        [
            mock.call(database=1, schema="public", table_name="messages"),
            mock.call(database=1, schema="public", table_name="channels"),
            mock.call(database=1, schema="public", table_name="messages_channels"),
        ],
    )
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                description="Messages in the Slack channel",
                extra=json.dumps(
                    {
                        "resource_type": "source",
                        "unique_id": "source.superset_examples.public.messages",
                        "depends_on": "source('public', 'messages')",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                2,
                description="Information about Slack channels",
                extra=json.dumps(
                    {
                        "resource_type": "source",
                        "unique_id": "source.superset_examples.public.channels",
                        "depends_on": "source('public', 'channels')",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                3,
                description="",
                extra=json.dumps(
                    {
                        "resource_type": "model",
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                3,
                metrics=[
                    {
                        "expression": "count(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "count(*)",
                        "description": "",
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_new_bq_error(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``sync_datasets`` when one of the sources is in a different BQ project.

    When that happens we can't add the dataset, since Superset can't read the metadata.
    """
    manifest = Path("/path/to/root/default/target/manifest.json")
    fs.create_file(manifest, contents=json.dumps(manifest_config))
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [
        Exception("Table is in a different project"),
        {"id": 2},
        {"id": 3},
    ]

    sync_datasets(
        client=client,
        manifest_path=manifest,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
    )
    client.create_dataset.assert_has_calls(
        [
            mock.call(database=1, schema="public", table_name="messages"),
            mock.call(database=1, schema="public", table_name="channels"),
            mock.call(database=1, schema="public", table_name="messages_channels"),
        ],
    )
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                2,
                description="Information about Slack channels",
                extra=json.dumps(
                    {
                        "resource_type": "source",
                        "unique_id": "source.superset_examples.public.channels",
                        "depends_on": "source('public', 'channels')",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                3,
                description="",
                extra=json.dumps(
                    {
                        "resource_type": "model",
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                3,
                metrics=[
                    {
                        "expression": "count(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "count(*)",
                        "description": "",
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_existing(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``sync_datasets`` when datasets already exist.
    """
    manifest = Path("/path/to/root/default/target/manifest.json")
    fs.create_file(manifest, contents=json.dumps(manifest_config))
    client = mocker.MagicMock()
    client.get_datasets.side_effect = [[{"id": 1}], [{"id": 2}], [{"id": 3}]]

    sync_datasets(
        client=client,
        manifest_path=manifest,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
    )
    client.create_dataset.assert_not_called()
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                description="Messages in the Slack channel",
                extra=json.dumps(
                    {
                        "resource_type": "source",
                        "unique_id": "source.superset_examples.public.messages",
                        "depends_on": "source('public', 'messages')",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                2,
                description="Information about Slack channels",
                extra=json.dumps(
                    {
                        "resource_type": "source",
                        "unique_id": "source.superset_examples.public.channels",
                        "depends_on": "source('public', 'channels')",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                3,
                description="",
                extra=json.dumps(
                    {
                        "resource_type": "model",
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                3,
                metrics=[
                    {
                        "expression": "count(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "count(*)",
                        "description": "",
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_multiple_existing(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``sync_datasets`` when multiple datasets are found to exist.
    """
    manifest = Path("/path/to/root/default/target/manifest.json")
    fs.create_file(manifest, contents=json.dumps(manifest_config))
    client = mocker.MagicMock()
    client.get_datasets.return_value = [{"id": 1}, {"id": 2}]

    with pytest.raises(Exception) as excinfo:
        sync_datasets(
            client=client,
            manifest_path=manifest,
            database={"id": 1},
            disallow_edits=False,
            external_url_prefix="",
        )
    assert str(excinfo.value) == "More than one dataset found"


def test_sync_datasets_external_url(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``sync_datasets`` when passing external URL prefix.
    """
    manifest = Path("/path/to/root/default/target/manifest.json")
    fs.create_file(manifest, contents=json.dumps(manifest_config))
    client = mocker.MagicMock()
    client.get_datasets.side_effect = [[{"id": 1}], [{"id": 2}], [{"id": 3}]]

    sync_datasets(
        client=client,
        manifest_path=manifest,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="https://dbt.example.org/",
    )
    client.create_dataset.assert_not_called()
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                description="Messages in the Slack channel",
                extra=json.dumps(
                    {
                        "resource_type": "source",
                        "unique_id": "source.superset_examples.public.messages",
                        "depends_on": "source('public', 'messages')",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
                external_url=(
                    "https://dbt.example.org/#!/source/"
                    "source.superset_examples.public.messages"
                ),
            ),
            mock.call(
                2,
                description="Information about Slack channels",
                extra=json.dumps(
                    {
                        "resource_type": "source",
                        "unique_id": "source.superset_examples.public.channels",
                        "depends_on": "source('public', 'channels')",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
                external_url=(
                    "https://dbt.example.org/#!/source/"
                    "source.superset_examples.public.channels"
                ),
            ),
            mock.call(
                3,
                description="",
                extra=json.dumps(
                    {
                        "resource_type": "model",
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
                external_url=(
                    "https://dbt.example.org/#!/model/"
                    "model.superset_examples.messages_channels"
                ),
            ),
            mock.call(
                3,
                metrics=[
                    {
                        "expression": "count(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "count(*)",
                        "description": "",
                    },
                ],
            ),
        ],
    )
