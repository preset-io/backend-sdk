"""
Tests for ``preset_cli.cli.superset.sync.dbt.datasets``.
"""
# pylint: disable=invalid-name

import json
from typing import List
from unittest import mock

import pytest
from pytest_mock import MockerFixture

from preset_cli.api.clients.dbt import MetricSchema, ModelSchema
from preset_cli.cli.superset.sync.dbt.datasets import sync_datasets

metric_schema = MetricSchema()
metrics: List[MetricSchema] = [
    metric_schema.load(
        {
            "depends_on": ["model.superset_examples.messages_channels"],
            "description": "",
            "filters": [],
            "meta": {},
            "name": "cnt",
            "label": "",
            "sql": "*",
            "type": "count",
            "unique_id": "metric.superset_examples.cnt",
        },
    ),
]

model_schema = ModelSchema()
models: List[ModelSchema] = [
    model_schema.load(
        {
            "database": "examples_dev",
            "schema": "public",
            "description": "",
            "meta": {},
            "name": "messages_channels",
            "unique_id": "model.superset_examples.messages_channels",
        },
    ),
]


def test_sync_datasets_new(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when no datasets exist yet.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}, {"id": 2}, {"id": 3}]

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
    )
    client.create_dataset.assert_has_calls(
        [
            mock.call(database=1, schema="public", table_name="messages_channels"),
        ],
    )
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                description="",
                extra=json.dumps(
                    {
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                        "certification": {"details": "This table is produced by dbt"},
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                1,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "cnt",
                        "description": "",
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_no_metrics(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when no datasets exist yet.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}, {"id": 2}, {"id": 3}]

    sync_datasets(
        client=client,
        models=models,
        metrics=[],
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
    )
    client.create_dataset.assert_has_calls(
        [
            mock.call(database=1, schema="public", table_name="messages_channels"),
        ],
    )
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                description="",
                extra=json.dumps(
                    {
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                        "certification": {"details": "This table is produced by dbt"},
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
        ],
    )


def test_sync_datasets_new_bq_error(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when one of the sources is in a different BQ project.

    When that happens we can't add the dataset, since Superset can't read the metadata.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [
        Exception("Table is in a different project"),
    ]

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
    )
    client.create_dataset.assert_has_calls(
        [
            mock.call(database=1, schema="public", table_name="messages_channels"),
        ],
    )
    client.update_dataset.assert_has_calls([])


def test_sync_datasets_existing(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when datasets already exist.
    """
    client = mocker.MagicMock()
    client.get_datasets.side_effect = [[{"id": 1}], [{"id": 2}], [{"id": 3}]]

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
    )
    client.create_dataset.assert_not_called()
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                description="",
                extra=json.dumps(
                    {
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                        "certification": {"details": "This table is produced by dbt"},
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                1,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "cnt",
                        "description": "",
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_multiple_existing(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when multiple datasets are found to exist.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = [{"id": 1}, {"id": 2}]

    with pytest.raises(Exception) as excinfo:
        sync_datasets(
            client=client,
            models=models,
            metrics=metrics,
            database={"id": 1},
            disallow_edits=False,
            external_url_prefix="",
        )
    assert str(excinfo.value) == "More than one dataset found"


def test_sync_datasets_external_url(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when passing external URL prefix.
    """
    client = mocker.MagicMock()
    client.get_datasets.side_effect = [[{"id": 1}], [{"id": 2}], [{"id": 3}]]

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="https://dbt.example.org/",
    )
    client.create_dataset.assert_not_called()
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                description="",
                extra=json.dumps(
                    {
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                        "certification": {"details": "This table is produced by dbt"},
                    },
                ),
                is_managed_externally=False,
                metrics=[],
                external_url=(
                    "https://dbt.example.org/#!"
                    "/model/model.superset_examples.messages_channels"
                ),
            ),
            mock.call(
                1,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "cnt",
                        "description": "",
                    },
                ],
            ),
        ],
    )
