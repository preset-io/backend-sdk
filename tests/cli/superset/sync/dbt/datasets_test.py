"""
Tests for ``preset_cli.cli.superset.sync.dbt.datasets``.
"""
# pylint: disable=invalid-name, too-many-lines

import copy
import json
from typing import Dict, List, cast
from unittest import mock

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine.url import make_url

from preset_cli.api.clients.dbt import MetricSchema, ModelSchema
from preset_cli.api.clients.superset import SupersetMetricDefinition
from preset_cli.cli.superset.sync.dbt.datasets import (
    create_dataset,
    model_in_database,
    sync_datasets,
)
from preset_cli.exceptions import ErrorLevel, ErrorPayload, SupersetError

metric_schema = MetricSchema()

metrics: Dict[str, List[SupersetMetricDefinition]] = {
    "model.superset_examples.messages_channels": [
        {
            "description": "",
            "expression": "COUNT(*)",
            "extra": "{}",
            "metric_name": "cnt",
            "metric_type": "count",
            "verbose_name": "",
        },
    ],
}

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
            "columns": [{"name": "id", "description": "Primary key"}],
        },
    ),
]

error = ErrorPayload(message="error", level=ErrorLevel.ERROR, extra={})


def test_sync_datasets_new(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when no datasets exist yet.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}, {"id": 2}, {"id": 3}]
    client.get_dataset.return_value = {
        "columns": [
            {"column_name": "id", "is_dttm": False, "type_generic": "INTEGER"},
            {"column_name": "ts", "is_dttm": True, "type_generic": "TIMESTAMP"},
        ],
    }

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1, "sqlalchemy_uri": "postgresql://user@host/examples_dev"},
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
                override_columns=True,
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
                override_columns=False,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
            ),
            mock.call(
                1,
                override_columns=True,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "is_dttm": False,
                        "verbose_name": "id",
                    },
                    {
                        "column_name": "ts",
                        "is_dttm": True,
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_first_update_fails(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with ``update_dataset`` failing in the first execution.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}]
    client.update_dataset.side_effect = [SupersetError([error])]
    client.get_dataset.return_value = {
        "columns": [
            {"column_name": "id", "is_dttm": False, "type_generic": "INTEGER"},
            {"column_name": "ts", "is_dttm": True, "type_generic": "TIMESTAMP"},
        ],
    }

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1, "sqlalchemy_uri": "postgresql://user@host/examples_dev"},
        disallow_edits=False,
        external_url_prefix="",
    )
    client.create_dataset.assert_called_with(
        database=1,
        schema="public",
        table_name="messages_channels",
    )
    client.update_dataset.assert_called_with(
        1,
        override_columns=True,
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
    )


def test_sync_datasets_second_update_fails(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with ``update_dataset`` failing in the second execution.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}]
    client.update_dataset.side_effect = [mocker.MagicMock(), SupersetError([error])]
    client.get_dataset.return_value = {
        "columns": [
            {"column_name": "id", "is_dttm": False, "type_generic": "INTEGER"},
            {"column_name": "ts", "is_dttm": True, "type_generic": "TIMESTAMP"},
        ],
    }

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1, "sqlalchemy_uri": "postgresql://user@host/examples_dev"},
        disallow_edits=False,
        external_url_prefix="",
    )
    client.create_dataset.assert_called_with(
        database=1,
        schema="public",
        table_name="messages_channels",
    )
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                override_columns=True,
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
                override_columns=False,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_third_update_fails(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with ``update_dataset`` failing in the third execution.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}]
    client.update_dataset.side_effect = [
        mocker.MagicMock(),
        mocker.MagicMock(),
        SupersetError([error]),
    ]
    client.get_dataset.return_value = {
        "columns": [
            {"column_name": "id", "is_dttm": False, "type_generic": "INTEGER"},
            {"column_name": "ts", "is_dttm": True, "type_generic": "TIMESTAMP"},
        ],
    }

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1, "sqlalchemy_uri": "postgresql://user@host/examples_dev"},
        disallow_edits=False,
        external_url_prefix="",
    )
    client.create_dataset.assert_called_with(
        database=1,
        schema="public",
        table_name="messages_channels",
    )
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                override_columns=True,
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
                override_columns=False,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
            ),
            mock.call(
                1,
                override_columns=True,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "is_dttm": False,
                        "verbose_name": "id",
                    },
                    {
                        "column_name": "ts",
                        "is_dttm": True,
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_with_alias(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when datasets has an alias.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}, {"id": 2}, {"id": 3}]
    client.get_dataset.return_value = {
        "columns": [
            {"column_name": "id", "is_dttm": False, "type_generic": "INTEGER"},
            {"column_name": "ts", "is_dttm": True, "type_generic": "TIMESTAMP"},
        ],
    }

    models_with_alias: List[ModelSchema] = [
        model_schema.load(
            {
                "alias": "model_alias",
                "database": "examples_dev",
                "schema": "public",
                "description": "",
                "meta": {},
                "name": "messages_channels",
                "unique_id": "model.superset_examples.messages_channels",
                "columns": [{"name": "id", "description": "Primary key"}],
            },
        ),
    ]
    sync_datasets(
        client=client,
        models=models_with_alias,
        metrics=metrics,
        database={"id": 1, "sqlalchemy_uri": "postgresql://user@host/examples_dev"},
        disallow_edits=False,
        external_url_prefix="",
    )
    client.create_dataset.assert_has_calls(
        [
            mock.call(database=1, schema="public", table_name="model_alias"),
        ],
    )
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                override_columns=True,
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
                override_columns=False,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
            ),
            mock.call(
                1,
                override_columns=True,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "is_dttm": False,
                        "verbose_name": "id",
                    },
                    {
                        "column_name": "ts",
                        "is_dttm": True,
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
    client.get_dataset.return_value = {
        "columns": [{"column_name": "id", "is_dttm": False}],
    }

    sync_datasets(
        client=client,
        models=models,
        metrics={},
        database={"id": 1, "sqlalchemy_uri": "postgresql://user@host/examples_dev"},
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
                override_columns=True,
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
                override_columns=True,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "is_dttm": False,
                        "verbose_name": "id",
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_custom_certification(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with a custom certification.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}, {"id": 2}, {"id": 3}]
    client.get_dataset.return_value = {
        "columns": [{"column_name": "id", "is_dttm": False}],
    }

    sync_datasets(
        client=client,
        models=models,
        metrics={},
        database={"id": 1, "sqlalchemy_uri": "postgresql://user@host/examples_dev"},
        disallow_edits=False,
        external_url_prefix="",
        certification={"details": "This dataset is synced from dbt Cloud"},
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
                override_columns=True,
                description="",
                extra=json.dumps(
                    {
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                        "certification": {
                            "details": "This dataset is synced from dbt Cloud",
                        },
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                1,
                override_columns=True,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "is_dttm": False,
                        "verbose_name": "id",
                    },
                ],
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
        database={"id": 1, "sqlalchemy_uri": "postgresql://user@host/examples_dev"},
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
    client.get_dataset.return_value = {
        "columns": [{"column_name": "id", "is_dttm": False, "is_active": None}],
    }

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
                override_columns=True,
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
                override_columns=False,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
            ),
            mock.call(
                1,
                override_columns=True,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "is_dttm": False,
                        "verbose_name": "id",
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


def test_sync_datasets_external_url_disallow_edits(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when passing external URL prefix and disallow-edits.
    """
    client = mocker.MagicMock()
    client.get_datasets.side_effect = [[{"id": 1}], [{"id": 2}], [{"id": 3}]]
    client.get_dataset.return_value = {
        "columns": [{"column_name": "id", "is_dttm": False}],
    }

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=True,
        external_url_prefix="https://dbt.example.org/",
    )
    client.create_dataset.assert_not_called()
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                override_columns=True,
                description="",
                extra=json.dumps(
                    {
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                        "certification": {"details": "This table is produced by dbt"},
                    },
                ),
                is_managed_externally=True,
                metrics=[],
                external_url=(
                    "https://dbt.example.org/"
                    "#!/model/model.superset_examples.messages_channels"
                ),
            ),
            mock.call(
                1,
                override_columns=False,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
            ),
            mock.call(
                1,
                override_columns=True,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "is_dttm": False,
                        "verbose_name": "id",
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_preserve_metadata(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with preserve_metadata set to True.
    Metrics should be merged (Preset as the source of truth).
    """
    client = mocker.MagicMock()
    metrics_ = copy.deepcopy(metrics)
    metrics_["model.superset_examples.messages_channels"].append(
        metric_schema.load(
            {
                "description": "",
                "expression": "MAX(id)",
                "extra": "{}",
                "metric_name": "max_id",
                "metric_type": "max",
                "verbose_name": "",
            },
        ),
    )
    client.get_datasets.side_effect = [[{"id": 1}], [{"id": 2}], [{"id": 3}]]
    client.get_dataset.return_value = {
        "columns": [
            {
                "column_name": "id",
                "is_dttm": False,
                "filterable": False,
                "groupby": False,
            },
        ],
        "metrics": [
            {
                "changed_on": "2023-08-28T18:01:24.190211",
                "created_on": "2023-08-28T18:01:24.190208",
                "expression": "count(*)/1",
                "id": 190,
                "metric_name": "cnt",
            },
            {
                "changed_on": "2023-08-27T18:01:24.190211",
                "created_on": "2023-08-27T18:01:24.190208",
                "expression": "COUNT (DISTINCT id)",
                "id": 191,
                "metric_name": "unique_ids",
            },
        ],
        "id": 1,
    }

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics_,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="https://dbt.example.org/",
        reload_columns=False,
    )
    client.create_dataset.assert_not_called()
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                override_columns=False,
                description="",
                extra=json.dumps(
                    {
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                        "certification": {"details": "This table is produced by dbt"},
                    },
                ),
                is_managed_externally=False,
                metrics=[
                    {
                        "expression": "count(*)/1",
                        "id": 190,
                        "metric_name": "cnt",
                    },
                    {
                        "expression": "COUNT (DISTINCT id)",
                        "id": 191,
                        "metric_name": "unique_ids",
                    },
                    {
                        "expression": "MAX(id)",
                        "metric_name": "max_id",
                        "metric_type": "max",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
                external_url=(
                    "https://dbt.example.org/"
                    "#!/model/model.superset_examples.messages_channels"
                ),
            ),
            mock.call(
                1,
                override_columns=False,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "filterable": False,
                        "groupby": False,
                        "is_dttm": False,
                        "verbose_name": "id",
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_merge_metadata(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with merge_metadata set to True.
    Metrics should be merged (dbt as the source of truth).
    """
    client = mocker.MagicMock()
    metrics_ = copy.deepcopy(metrics)
    metrics_["model.superset_examples.messages_channels"].append(
        metric_schema.load(
            {
                "description": "",
                "expression": "MAX(id)",
                "extra": "{}",
                "metric_name": "max_id",
                "metric_type": "max",
                "verbose_name": "",
            },
        ),
    )
    client.get_datasets.side_effect = [[{"id": 1}], [{"id": 2}], [{"id": 3}]]
    client.get_dataset.return_value = {
        "columns": [
            {
                "column_name": "id",
                "is_dttm": False,
                "filterable": False,
                "groupby": False,
            },
        ],
        "metrics": [
            {
                "changed_on": "2023-08-28T18:01:24.190211",
                "created_on": "2023-08-28T18:01:24.190208",
                "expression": "count(*)/1",
                "id": 190,
                "metric_name": "cnt",
            },
            {
                "changed_on": "2023-08-27T18:01:24.190211",
                "created_on": "2023-08-27T18:01:24.190208",
                "expression": "COUNT (DISTINCT id)",
                "id": 191,
                "metric_name": "unique_ids",
            },
        ],
        "id": 1,
    }

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics_,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="https://dbt.example.org/",
        reload_columns=False,
        merge_metadata=True,
    )
    client.create_dataset.assert_not_called()
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                override_columns=False,
                description="",
                extra=json.dumps(
                    {
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                        "certification": {"details": "This table is produced by dbt"},
                    },
                ),
                is_managed_externally=False,
                metrics=[
                    {
                        "expression": "COUNT (DISTINCT id)",
                        "id": 191,
                        "metric_name": "unique_ids",
                    },
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                        "id": 190,
                    },
                    {
                        "expression": "MAX(id)",
                        "metric_name": "max_id",
                        "metric_type": "max",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
                external_url=(
                    "https://dbt.example.org/"
                    "#!/model/model.superset_examples.messages_channels"
                ),
            ),
            mock.call(
                1,
                override_columns=False,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "filterable": False,
                        "groupby": False,
                        "is_dttm": False,
                        "verbose_name": "id",
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_no_columns(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when passing external URL prefix.
    """
    client = mocker.MagicMock()
    client.get_datasets.side_effect = [[{"id": 1}], [{"id": 2}], [{"id": 3}]]

    modified_models = [models[0].copy()]
    modified_models[0]["columns"] = {}

    sync_datasets(
        client=client,
        models=modified_models,
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
                override_columns=True,
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
                override_columns=False,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
            ),
        ],
    )


def test_create_dataset_physical(mocker: MockerFixture) -> None:
    """
    Test ``create_dataset`` for physical datasets.
    """
    client = mocker.MagicMock()

    create_dataset(
        client,
        {
            "id": 1,
            "schema": "public",
            "name": "Database",
            "sqlalchemy_uri": "postgresql://user@host/examples_dev",
        },
        models[0],
    )
    client.create_dataset.assert_called_with(
        database=1,
        schema="public",
        table_name="messages_channels",
    )


def test_create_dataset_virtual(mocker: MockerFixture) -> None:
    """
    Test ``create_dataset`` for virtual datasets.
    """
    create_engine = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.create_engine",
    )
    create_engine().dialect.identifier_preparer.quote = lambda token: token
    client = mocker.MagicMock()

    create_dataset(
        client,
        {
            "id": 1,
            "schema": "public",
            "name": "Database",
            "sqlalchemy_uri": "postgresql://user@host/examples",
        },
        models[0],
    )
    client.create_dataset.assert_called_with(
        database=1,
        schema="public",
        table_name="messages_channels",
        sql="SELECT * FROM examples_dev.public.messages_channels",
    )


def test_model_in_database() -> None:
    """
    Test the ``model_in_database`` helper.
    """
    url = make_url("bigquery://project-1")
    assert model_in_database(cast(ModelSchema, {"database": "project-1"}), url)
    assert not model_in_database(cast(ModelSchema, {"database": "project-2"}), url)

    url = make_url("snowflake://user:password@host/db1")
    assert model_in_database(cast(ModelSchema, {"database": "db1"}), url)
    assert not model_in_database(cast(ModelSchema, {"database": "db2"}), url)


def test_sync_datasets_null_certification(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with no certification info
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}, {"id": 2}, {"id": 3}]
    client.get_dataset.return_value = {
        "columns": [
            {"column_name": "id", "is_dttm": False, "type_generic": "INTEGER"},
            {"column_name": "ts", "is_dttm": True, "type_generic": "TIMESTAMP"},
        ],
    }

    models_with_null_certification: List[ModelSchema] = [
        model_schema.load(
            {
                "database": "examples_dev",
                "schema": "public",
                "description": "",
                "meta": {"superset": {"extra": {"certification": None}}},
                "name": "messages_channels",
                "unique_id": "model.superset_examples.messages_channels",
                "columns": [{"name": "id", "description": "Primary key"}],
            },
        ),
    ]
    sync_datasets(
        client=client,
        models=models_with_null_certification,
        metrics=metrics,
        database={"id": 1, "sqlalchemy_uri": "postgresql://user@host/examples_dev"},
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
                override_columns=True,
                description="",
                extra=json.dumps(
                    {
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                1,
                override_columns=False,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
            ),
            mock.call(
                1,
                override_columns=True,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "is_dttm": False,
                        "verbose_name": "id",
                    },
                    {
                        "column_name": "ts",
                        "is_dttm": True,
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_model_certification(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with certification from model definition
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}, {"id": 2}, {"id": 3}]
    client.get_dataset.return_value = {
        "columns": [
            {"column_name": "id", "is_dttm": False, "type_generic": "INTEGER"},
            {"column_name": "ts", "is_dttm": True, "type_generic": "TIMESTAMP"},
        ],
    }

    models_with_model_certification: List[ModelSchema] = [
        model_schema.load(
            {
                "database": "examples_dev",
                "schema": "public",
                "description": "",
                "meta": {
                    "superset": {
                        "extra": {
                            "certification": {
                                "details": "I declare this dataset certified",
                                "certified_by": "Myself",
                            },
                        },
                    },
                },
                "name": "messages_channels",
                "unique_id": "model.superset_examples.messages_channels",
                "columns": [{"name": "id", "description": "Primary key"}],
            },
        ),
    ]
    sync_datasets(
        client=client,
        models=models_with_model_certification,
        metrics=metrics,
        database={"id": 1, "sqlalchemy_uri": "postgresql://user@host/examples_dev"},
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
                override_columns=True,
                description="",
                extra=json.dumps(
                    {
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                        "certification": {
                            "details": "I declare this dataset certified",
                            "certified_by": "Myself",
                        },
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                1,
                override_columns=False,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
            ),
            mock.call(
                1,
                override_columns=True,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "is_dttm": False,
                        "verbose_name": "id",
                    },
                    {
                        "column_name": "ts",
                        "is_dttm": True,
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_warning(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with warning information
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}, {"id": 2}, {"id": 3}]
    client.get_dataset.return_value = {
        "columns": [
            {"column_name": "id", "is_dttm": False, "type_generic": "INTEGER"},
            {"column_name": "ts", "is_dttm": True, "type_generic": "TIMESTAMP"},
        ],
    }

    models_with_warning: List[ModelSchema] = [
        model_schema.load(
            {
                "database": "examples_dev",
                "schema": "public",
                "description": "",
                "meta": {
                    "superset": {"extra": {"warning_markdown": "Under Construction"}},
                },
                "name": "messages_channels",
                "unique_id": "model.superset_examples.messages_channels",
                "columns": [{"name": "id", "description": "Primary key"}],
            },
        ),
    ]
    sync_datasets(
        client=client,
        models=models_with_warning,
        metrics=metrics,
        database={"id": 1, "sqlalchemy_uri": "postgresql://user@host/examples_dev"},
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
                override_columns=True,
                description="",
                extra=json.dumps(
                    {
                        "unique_id": "model.superset_examples.messages_channels",
                        "depends_on": "ref('messages_channels')",
                        "certification": {"details": "This table is produced by dbt"},
                        "warning_markdown": "Under Construction",
                    },
                ),
                is_managed_externally=False,
                metrics=[],
            ),
            mock.call(
                1,
                override_columns=False,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
            ),
            mock.call(
                1,
                override_columns=True,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "is_dttm": False,
                        "verbose_name": "id",
                    },
                    {
                        "column_name": "ts",
                        "is_dttm": True,
                    },
                ],
            ),
        ],
    )


def test_sync_datasets_meta_test(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with an additional field set through meta.superset
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = [{"id": 1}, {"id": 2}, {"id": 3}]
    client.get_dataset.return_value = {
        "columns": [
            {"column_name": "id", "is_dttm": False, "type_generic": "INTEGER"},
            {"column_name": "ts", "is_dttm": True, "type_generic": "TIMESTAMP"},
        ],
    }

    models_with_meta_info: List[ModelSchema] = [
        model_schema.load(
            {
                "database": "examples_dev",
                "schema": "public",
                "description": "",
                "meta": {"superset": {"cache_timeout": 250}},
                "name": "messages_channels",
                "unique_id": "model.superset_examples.messages_channels",
                "columns": [{"name": "id", "description": "Primary key"}],
            },
        ),
    ]
    sync_datasets(
        client=client,
        models=models_with_meta_info,
        metrics=metrics,
        database={"id": 1, "sqlalchemy_uri": "postgresql://user@host/examples_dev"},
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
                override_columns=True,
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
                cache_timeout=250,
            ),
            mock.call(
                1,
                override_columns=False,
                metrics=[
                    {
                        "expression": "COUNT(*)",
                        "metric_name": "cnt",
                        "metric_type": "count",
                        "verbose_name": "",
                        "description": "",
                        "extra": "{}",
                    },
                ],
            ),
            mock.call(
                1,
                override_columns=True,
                columns=[
                    {
                        "column_name": "id",
                        "description": "Primary key",
                        "is_dttm": False,
                        "verbose_name": "id",
                    },
                    {
                        "column_name": "ts",
                        "is_dttm": True,
                    },
                ],
            ),
        ],
    )
