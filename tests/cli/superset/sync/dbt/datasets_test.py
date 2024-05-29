"""
Tests for ``preset_cli.cli.superset.sync.dbt.datasets``.
"""
# pylint: disable=invalid-name, too-many-lines, redefined-outer-name

import json
from typing import Any, Dict, List, cast
from unittest import mock

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine.url import make_url
from yarl import URL

from preset_cli.api.clients.dbt import MetricSchema, ModelSchema
from preset_cli.api.clients.superset import SupersetMetricDefinition
from preset_cli.cli.superset.sync.dbt.datasets import (
    DEFAULT_CERTIFICATION,
    clean_metadata,
    compute_columns,
    compute_columns_metadata,
    compute_dataset_metadata,
    compute_metrics,
    create_dataset,
    get_certification_info,
    get_or_create_dataset,
    model_in_database,
    no_catalog_support,
    sync_datasets,
)
from preset_cli.exceptions import (
    CLIError,
    DatabaseNotFoundError,
    ErrorLevel,
    ErrorPayload,
    SupersetError,
)

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

superset_metrics: List[SupersetMetricDefinition] = [
    {
        "description": "Superset desc",
        "expression": "COUNT(*)",
        "metric_name": "cnt",
        "id": 1,
        "verbose_name": "Count from Superset",
    },
    {
        "description": "Superset desc",
        "expression": "SUM(price_each)",
        "metric_name": "revenue",
        "id": 2,
        "verbose_name": "SUM from Superset",
    },
]

dataset_columns = [
    {
        "advanced_data_type": None,
        "changed_on": "2024-01-23T20:29:33.945074",
        "column_name": "product_line",
        "created_on": "2024-01-23T20:29:33.945070",
        "description": "Description for Product Line",
        "expression": None,
        "extra": "{}",
        "filterable": True,
        "groupby": True,
        "id": 1,
        "is_active": True,
        "is_dttm": False,
        "python_date_format": None,
        "type": None,
        "type_generic": None,
        "uuid": "5b0dc14a-8c1a-4fcb-8791-3a955d8609d3",
        "verbose_name": None,
    },
    {
        "advanced_data_type": None,
        "changed_on": "2024-01-03T13:30:19.139128",
        "column_name": "id",
        "created_on": "2021-12-22T16:59:38.825689",
        "description": "Description for ID",
        "expression": None,
        "extra": "{}",
        "filterable": True,
        "groupby": False,
        "id": 2,
        "is_active": None,
        "is_dttm": False,
        "python_date_format": None,
        "type": "INTEGER",
        "type_generic": None,
        "uuid": "a2952680-2671-4a97-b608-3483cf7f11d2",
        "verbose_name": None,
    },
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
    client.create_dataset.return_value = {
        "id": 1,
        "data": {"metrics": [], "columns": []},
    }
    client.get_dataset.return_value = {"id": 1, "metrics": [], "columns": []}
    compute_dataset_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_dataset_metadata",
    )
    compute_columns_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns_metadata",
    )
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
        catalog="examples_dev",
        schema="public",
        table_name="messages_channels",
    )
    client.get_dataset.assert_called_with(client.create_dataset()["id"])
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                override_columns=True,
                **compute_dataset_metadata_mock(),
            ),
            mock.call(
                1,
                override_columns=True,
                columns=compute_columns_metadata_mock(),
            ),
        ],
    )


def test_sync_datasets_first_update_fails(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with ``update_dataset`` failing in the first execution.
    """
    client = mocker.MagicMock()
    client.update_dataset.side_effect = [SupersetError([error])]
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.get_or_create_dataset",
        return_value={"id": 1, "metrics": [], "columns": []},
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_metrics",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns",
    )
    compute_dataset_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_dataset_metadata",
    )
    working, failed = sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
    )

    client.update_dataset.assert_called_with(
        1,
        override_columns=True,
        **compute_dataset_metadata_mock(),
    )
    assert working == []
    assert failed == [models[0]["unique_id"]]


def test_sync_datasets_second_update_fails(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with ``update_dataset`` failing in the second execution.
    """
    client = mocker.MagicMock()
    client.update_dataset.side_effect = [mocker.MagicMock(), SupersetError([error])]
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.get_or_create_dataset",
        return_value={"id": 1, "metrics": [], "columns": []},
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_metrics",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns",
    )
    compute_dataset_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_dataset_metadata",
    )
    compute_columns_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns_metadata",
    )
    working, failed = sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
    )

    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                override_columns=True,
                **compute_dataset_metadata_mock(),
            ),
            mock.call(
                1,
                override_columns=True,
                columns=compute_columns_metadata_mock(),
            ),
        ],
    )
    assert working == []
    assert failed == [models[0]["unique_id"]]


def test_sync_datasets_with_alias(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when the model has an alias.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_metrics",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_dataset_metadata",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns_metadata",
    )
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
    client.get_datasets.assert_called_with(
        database=mock.ANY,
        schema="public",
        table_name="model_alias",
    )
    client.get_datasets.return_value = [{"id": 1}]
    client.get_dataset.return_value = {"id": 1}


def test_sync_datasets_custom_certification(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with a custom certification.
    """
    client = mocker.MagicMock()
    get_or_create_dataset_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.get_or_create_dataset",
        return_value={"id": 1, "metrics": [], "columns": []},
    )
    compute_metrics_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_metrics",
    )
    compute_columns_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns",
    )
    compute_dataset_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_dataset_metadata",
    )
    compute_columns_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns_metadata",
    )
    model_id = models[0]["unique_id"]

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
        certification={"details": "This dataset is synced from dbt Cloud"},
    )

    get_or_create_dataset_mock.assert_called_with(client, models[0], {"id": 1})
    compute_metrics_mock.assert_called_with(
        get_or_create_dataset_mock()["metrics"],
        metrics[model_id],
        True,
        False,
    )
    compute_columns_metadata_mock.assert_called_with(
        models[0]["columns"],
        client.get_dataset()["columns"],
        True,
        False,
    )
    compute_columns_mock.assert_not_called()
    compute_dataset_metadata_mock.assert_called_with(
        models[0],
        {"details": "This dataset is synced from dbt Cloud"},
        False,
        compute_metrics_mock(),
        None,
        [],
    )
    client.create_dataset.assert_not_called()
    client.get_refreshed_dataset_columns.assert_not_called()
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                override_columns=True,
                **compute_dataset_metadata_mock(),
            ),
            mock.call(
                1,
                override_columns=True,
                columns=compute_columns_metadata_mock(),
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
            mock.call(
                database=1,
                catalog="examples_dev",
                schema="public",
                table_name="messages_channels",
            ),
        ],
    )
    client.update_dataset.assert_has_calls([])


def test_sync_datasets_existing(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when datasets already exist.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_metrics",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_dataset_metadata",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns_metadata",
    )

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
    )
    client.create_dataset.assert_not_called()


def test_sync_datasets_multiple_existing(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when multiple datasets are found to exist.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = [{"id": 1}, {"id": 2}]

    working, failed = sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
    )
    assert working == []
    assert failed == [models[0]["unique_id"]]


def test_sync_datasets_external_url_disallow_edits(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when passing external URL prefix and disallow-edits.
    """
    client = mocker.MagicMock()
    get_or_create_dataset_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.get_or_create_dataset",
        return_value={"id": 1, "metrics": [], "columns": []},
    )
    compute_metrics_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_metrics",
    )
    compute_columns_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns",
    )
    compute_dataset_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_dataset_metadata",
    )
    compute_columns_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns_metadata",
    )
    model_id = models[0]["unique_id"]

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=True,
        external_url_prefix="https://dbt.example.org/",
    )

    get_or_create_dataset_mock.assert_called_with(client, models[0], {"id": 1})
    compute_metrics_mock.assert_called_with(
        get_or_create_dataset_mock()["metrics"],
        metrics[model_id],
        True,
        False,
    )
    compute_columns_metadata_mock.assert_called_with(
        models[0]["columns"],
        client.get_dataset()["columns"],
        True,
        False,
    )
    compute_columns_mock.assert_not_called()
    compute_dataset_metadata_mock.assert_called_with(
        models[0],
        None,
        True,
        compute_metrics_mock(),
        URL("https://dbt.example.org/"),
        [],
    )
    client.create_dataset.assert_not_called()
    client.get_refreshed_dataset_columns.assert_not_called()
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                override_columns=True,
                **compute_dataset_metadata_mock(),
            ),
            mock.call(
                1,
                override_columns=True,
                columns=compute_columns_metadata_mock(),
            ),
        ],
    )


def test_sync_datasets_preserve_metadata(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with preserve_metadata set to True.
    """
    client = mocker.MagicMock()
    get_or_create_dataset_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.get_or_create_dataset",
        return_value={"id": 1, "metrics": [], "columns": []},
    )
    compute_metrics_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_metrics",
    )
    compute_columns_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns",
    )
    compute_dataset_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_dataset_metadata",
    )
    compute_columns_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns_metadata",
    )
    model_id = models[0]["unique_id"]

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
        reload_columns=False,
        merge_metadata=False,
    )

    get_or_create_dataset_mock.assert_called_with(client, models[0], {"id": 1})
    compute_metrics_mock.assert_called_with(
        get_or_create_dataset_mock()["metrics"],
        metrics[model_id],
        False,
        False,
    )
    compute_columns_metadata_mock.assert_called_with(
        models[0]["columns"],
        client.get_dataset()["columns"],
        False,
        False,
    )
    compute_columns_mock.assert_called_with(
        get_or_create_dataset_mock()["columns"],
        client.get_refreshed_dataset_columns(get_or_create_dataset_mock()["id"]),
    )
    compute_dataset_metadata_mock.assert_called_with(
        models[0],
        None,
        False,
        compute_metrics_mock(),
        None,
        compute_columns_mock(),
    )
    client.create_dataset.assert_not_called()
    client.get_refreshed_dataset_columns.assert_called_with(
        get_or_create_dataset_mock()["id"],
    )
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                override_columns=False,
                **compute_dataset_metadata_mock(),
            ),
            mock.call(
                1,
                override_columns=False,
                columns=compute_columns_metadata_mock(),
            ),
        ],
    )


def test_sync_datasets_merge_metadata(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` with merge_metadata set to True.
    """
    client = mocker.MagicMock()
    get_or_create_dataset_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.get_or_create_dataset",
        return_value={"id": 1, "metrics": [], "columns": []},
    )
    compute_metrics_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_metrics",
    )
    compute_columns_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns",
    )
    compute_dataset_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_dataset_metadata",
    )
    compute_columns_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns_metadata",
    )
    model_id = models[0]["unique_id"]

    sync_datasets(
        client=client,
        models=models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
        reload_columns=False,
        merge_metadata=True,
    )

    get_or_create_dataset_mock.assert_called_with(client, models[0], {"id": 1})
    compute_metrics_mock.assert_called_with(
        get_or_create_dataset_mock()["metrics"],
        metrics[model_id],
        False,
        True,
    )
    compute_columns_metadata_mock.assert_called_with(
        models[0]["columns"],
        client.get_dataset()["columns"],
        False,
        True,
    )
    compute_columns_mock.assert_called_with(
        get_or_create_dataset_mock()["columns"],
        client.get_refreshed_dataset_columns(get_or_create_dataset_mock()["id"]),
    )
    compute_dataset_metadata_mock.assert_called_with(
        models[0],
        None,
        False,
        compute_metrics_mock(),
        None,
        compute_columns_mock(),
    )
    client.create_dataset.assert_not_called()
    client.get_refreshed_dataset_columns.assert_called_with(
        get_or_create_dataset_mock()["id"],
    )
    client.update_dataset.assert_has_calls(
        [
            mock.call(
                1,
                override_columns=False,
                **compute_dataset_metadata_mock(),
            ),
            mock.call(
                1,
                override_columns=False,
                columns=compute_columns_metadata_mock(),
            ),
        ],
    )


def test_sync_datasets_no_columns(mocker: MockerFixture) -> None:
    """
    Test ``sync_datasets`` when there's no dbt metadata for columns.
    """
    client = mocker.MagicMock()
    modified_models = [models[0].copy()]
    modified_models[0]["columns"] = {}
    model_id = modified_models[0]["unique_id"]

    get_or_create_dataset_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.get_or_create_dataset",
        return_value={"id": 1, "metrics": [], "columns": []},
    )
    compute_metrics_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_metrics",
    )
    compute_columns_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns",
    )
    compute_dataset_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_dataset_metadata",
    )
    compute_columns_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.compute_columns_metadata",
    )

    sync_datasets(
        client=client,
        models=modified_models,
        metrics=metrics,
        database={"id": 1},
        disallow_edits=False,
        external_url_prefix="",
    )

    get_or_create_dataset_mock.assert_called_with(client, modified_models[0], {"id": 1})
    compute_metrics_mock.assert_called_with(
        get_or_create_dataset_mock()["metrics"],
        metrics[model_id],
        True,
        False,
    )
    compute_columns_mock.assert_not_called()
    compute_dataset_metadata_mock.assert_called_with(
        modified_models[0],
        None,
        False,
        compute_metrics_mock(),
        None,
        [],
    )
    compute_columns_metadata_mock.assert_not_called()
    client.create_dataset.assert_not_called()
    client.get_refreshed_dataset_columns.assert_not_called()
    client.update_dataset.assert_called_with(
        1,
        override_columns=True,
        **compute_dataset_metadata_mock,
    )


@pytest.fixture()
def no_catalog_support_client(mocker: MockerFixture) -> Any:
    """
    Fixture to return a mocked client with no catalog support.
    """
    client = mocker.MagicMock()
    client.create_dataset.side_effect = [
        SupersetError(
            errors=[
                {
                    "message": json.dumps({"message": {"catalog": ["Unknown field."]}}),
                    "error_type": "UNKNOWN_ERROR",
                    "level": ErrorLevel.ERROR,
                },
            ],
        ),
        None,
    ]
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.no_catalog_support",
        return_value=True,
    )
    return client


def test_create_dataset_physical_no_catalog(
    no_catalog_support_client: MockerFixture,
) -> None:
    """
    Test ``create_dataset`` for physical datasets.
    """
    create_dataset(
        no_catalog_support_client,
        {
            "id": 1,
            "catalog": "examples_dev",
            "schema": "public",
            "name": "Database",
            "sqlalchemy_uri": "postgresql://user@host/examples_dev",
        },
        models[0],
    )
    no_catalog_support_client.create_dataset.assert_called_with(
        database=1,
        schema="public",
        table_name="messages_channels",
    )


def test_create_dataset_virtual(
    mocker: MockerFixture,
    no_catalog_support_client: MockerFixture,
) -> None:
    """
    Test ``create_dataset`` for virtual datasets.
    """
    create_engine = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.lib.create_engine",
    )
    create_engine().dialect.identifier_preparer.quote = lambda token: token

    create_dataset(
        no_catalog_support_client,
        {
            "id": 1,
            "schema": "public",
            "name": "Database",
            "sqlalchemy_uri": "postgresql://user@host/examples",
        },
        models[0],
    )
    no_catalog_support_client.create_dataset.assert_called_with(
        database=1,
        schema="public",
        table_name="messages_channels",
        sql="SELECT * FROM examples_dev.public.messages_channels",
    )


def test_create_dataset_virtual_missing_dependency(
    no_catalog_support_client: MockerFixture,
) -> None:
    """
    Test ``create_dataset`` for virtual datasets when the DB connection requires
    an additional package.
    """
    with pytest.raises(NotImplementedError):
        create_dataset(
            no_catalog_support_client,
            {
                "id": 1,
                "schema": "public",
                "name": "Database",
                "sqlalchemy_uri": "blah://user@host/examples",
            },
            models[0],
        )


def test_create_dataset_virtual_missing_dependency_snowflake(
    capsys: pytest.CaptureFixture[str],
    no_catalog_support_client: MockerFixture,
) -> None:
    """
    Test ``create_dataset`` for virtual datasets when the DB connection requires
    an additional package.
    """
    with pytest.raises(SystemExit) as excinfo:
        create_dataset(
            no_catalog_support_client,
            {
                "id": 1,
                "schema": "public",
                "name": "other_db",
                "sqlalchemy_uri": "snowflake://user@host/examples",
            },
            models[0],
        )
    output_content = capsys.readouterr()
    assert excinfo.type == SystemExit
    assert excinfo.value.code == 1
    assert "preset-cli[snowflake]" in output_content.out


def test_get_or_create_dataset_existing_dataset(mocker: MockerFixture) -> None:
    """
    Test the ``get_or_create_dataset`` helper when it finds a dataset.
    """
    client = mocker.MagicMock()
    # create_dataset = mocker.patch("preset_cli.cli.superset.sync.dbt.datasets.create_engine")
    client.get_datasets.return_value = [{"id": 1}]
    client.get_dataset.return_value = {"id": 1}
    database = {"id": 1}
    result = get_or_create_dataset(client, models[0], database)
    assert result == client.get_dataset()
    client.create_dataset.assert_not_called()


def test_get_or_create_dataset_multiple_datasets(mocker: MockerFixture) -> None:
    """
    Test the ``get_or_create_dataset`` helper when it finds many datasets.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = [{"id": 1}, {"id": 2}]
    database = {"id": 1}
    with pytest.raises(CLIError) as excinfo:
        get_or_create_dataset(client, models[0], database)
    assert excinfo.type == CLIError
    assert excinfo.value.exit_code == 1
    assert "More than one dataset found" in str(excinfo.value)
    client.get_dataset.assert_not_called()
    client.create_dataset.assert_not_called()


def test_get_or_create_dataset_no_datasets(mocker: MockerFixture) -> None:
    """
    Test the ``get_or_create_dataset`` helper when the dataset doesn't exist.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.get_dataset.return_value = {"id": 1}
    create_dataset_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.create_dataset",
    )
    database = {"id": 1}
    result = get_or_create_dataset(client, models[0], database)
    assert result == client.get_dataset()
    create_dataset_mock.assert_called_with(client, database, models[0])


def test_get_or_create_dataset_creation_failure(mocker: MockerFixture) -> None:
    """
    Test the ``get_or_create_dataset`` helper when it fails to create a dataset.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    create_dataset_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.create_dataset",
    )
    create_dataset_mock.side_effect = Exception("Error")
    database = {"id": 1}
    with pytest.raises(CLIError) as excinfo:
        get_or_create_dataset(client, models[0], database)
    assert excinfo.type == CLIError
    assert excinfo.value.exit_code == 1
    assert "Unable to create dataset" in str(excinfo.value)
    client.get_dataset.assert_not_called()
    create_dataset_mock.assert_called_with(client, database, models[0])


def test_get_certification_info_from_model() -> None:
    """
    Test the ``get_certification_info`` helper when the model contains certification info.
    """
    model = models[0].copy()
    model["meta"] = {
        "extra": {
            "certification": {
                "details": "I declare it certified",
                "certified_by": "Myself",
            },
        },
    }
    result = get_certification_info(model["meta"], None)
    assert result == {"details": "I declare it certified", "certified_by": "Myself"}


def test_get_certification_info_from_model_None() -> None:
    """
    Test the ``get_certification_info`` helper when the model sets certification to None.
    """
    model = models[0].copy()
    model["meta"] = {"extra": {"certification": None}}
    result = get_certification_info(model["meta"], None)
    assert result is None


def test_get_certification_info_from_arg() -> None:
    """
    Test the ``get_certification_info`` helper when certification is passed via parameter.
    """
    certification = {"details": "Trust it", "certified_by": "Me"}
    result = get_certification_info(models[0], certification)
    assert result == certification


def test_get_certification_info_default() -> None:
    """
    Test the ``get_certification_info`` helper when using default certification value.
    """
    result = get_certification_info(models[0], None)
    assert result == DEFAULT_CERTIFICATION


def test_compute_metrics_reload_not_merge() -> None:
    """
    Test the ``compute_metrics`` helper with the default flow.
    """
    result = compute_metrics(
        [],
        metrics["model.superset_examples.messages_channels"],
        True,
        False,
    )
    assert result == metrics["model.superset_examples.messages_channels"]


def test_compute_metrics_reload_not_merge_with_superset_metrics() -> None:
    """
    Test the ``compute_metrics`` helper with the default flow + existing metrics.
    """
    result = compute_metrics(
        superset_metrics,
        metrics["model.superset_examples.messages_channels"],
        True,
        False,
    )
    assert result == metrics["model.superset_examples.messages_channels"]


def test_compute_metrics_reload_not_merge_with_no_dbt_metrics() -> None:
    """
    Test the ``compute_metrics`` helper with the default flow + no dbt metrics.
    """
    result = compute_metrics(
        superset_metrics,
        [],
        True,
        False,
    )
    assert result == []


def test_compute_metrics_reload_not_merge_with_conflict() -> None:
    """
    Test the ``compute_metrics`` helper with the default flow + conflict.
    """
    result = compute_metrics(
        superset_metrics,
        metrics["model.superset_examples.messages_channels"],
        True,
        False,
    )
    assert result == metrics["model.superset_examples.messages_channels"]


def test_compute_metrics_with_merge_without_reload() -> None:
    """
    Test the ``compute_metrics`` helper with merge without reload.
    """
    result = compute_metrics(
        superset_metrics,
        metrics["model.superset_examples.messages_channels"],
        False,
        True,
    )
    final = metrics["model.superset_examples.messages_channels"].copy()

    for dbt_metric in final:
        for superset_metric in superset_metrics:
            if dbt_metric["metric_name"] == superset_metric["metric_name"]:
                dbt_metric["id"] = superset_metric["id"]

    for superset_metric in superset_metrics:
        keep = True
        for dbt_metric in final:
            if superset_metric["metric_name"] == dbt_metric["metric_name"]:
                keep = False
        if keep:
            final.append(superset_metric)

    assert result == final


def test_compute_metrics_without_merge_and_reload() -> None:
    """
    Test the ``compute_metrics`` helper without reload and merge.
    """
    result = compute_metrics(
        superset_metrics,
        metrics["model.superset_examples.messages_channels"],
        False,
        False,
    )
    final = superset_metrics.copy()
    additions = []
    for superset_metric in final:
        for dbt_metric in metrics["model.superset_examples.messages_channels"]:
            if superset_metric["metric_name"] == dbt_metric["metric_name"]:
                keep = False
        if keep:
            additions.append(dbt_metric)
    for add in additions:
        final.append(add)
    assert result == final


def test_compute_metrics_without_merge_and_reload_no_metrics() -> None:
    """
    Test the ``compute_metrics`` helper without reload and merge without Superset metrics.
    """
    result = compute_metrics(
        [],
        metrics["model.superset_examples.messages_channels"],
        False,
        False,
    )
    assert result == metrics["model.superset_examples.messages_channels"]


def test_compute_columns_no_new_columns(mocker: MockerFixture) -> None:
    """
    Test the ``compute_columns`` helper when no new columns are returned.
    """
    clean_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.clean_metadata",
    )
    result = compute_columns(dataset_columns, dataset_columns)
    assert result == [
        clean_metadata_mock(dataset_columns[0]),
        clean_metadata_mock(dataset_columns[1]),
    ]
    clean_metadata_mock.assert_has_calls(
        [
            mock.call(dataset_columns[0]),
            mock.call(dataset_columns[1]),
        ],
    )


def test_compute_columns_new_columns(mocker: MockerFixture) -> None:
    """
    Test the ``compute_columns`` helper when new columns are returned.
    """
    clean_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.clean_metadata",
    )
    refreshed_list = dataset_columns.copy()
    refreshed_list.append(
        {
            "advanced_data_type": None,
            "changed_on": "2024-01-03T13:30:19.012896",
            "column_name": "price_each",
            "created_on": "2021-12-22T16:59:38.420192",
            "description": None,
            "expression": None,
            "extra": "{}",
            "filterable": True,
            "groupby": True,
            "id": 341,
            "is_active": None,
            "is_dttm": False,
            "python_date_format": None,
            "type": "DOUBLE PRECISION",
            "type_generic": 0,
            "uuid": "309aed93-6b56-4519-b9f4-0271ec0d2d60",
            "verbose_name": None,
        },
    )
    result = compute_columns(dataset_columns, refreshed_list)
    assert result == [
        clean_metadata_mock(dataset_columns[0]),
        clean_metadata_mock(dataset_columns[1]),
        clean_metadata_mock(refreshed_list[2]),
    ]
    clean_metadata_mock.assert_has_calls(
        [
            mock.call(dataset_columns[0]),
            mock.call(dataset_columns[1]),
            mock.call(refreshed_list[2]),
        ],
    )


def test_compute_columns_column_removed(mocker: MockerFixture) -> None:
    """
    Test the ``compute_columns`` helper when a column is removed.
    """
    clean_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.clean_metadata",
    )
    refreshed_list = dataset_columns.copy()
    refreshed_list.pop(1)
    result = compute_columns(dataset_columns, refreshed_list)
    assert result == [
        clean_metadata_mock(dataset_columns[0]),
    ]
    clean_metadata_mock.assert_called_with(dataset_columns[0])


def test_compute_columns_no_columns(mocker: MockerFixture) -> None:
    """
    Test the ``compute_columns`` helper when no column is returned.
    """
    clean_metadata_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.datasets.clean_metadata",
    )
    result = compute_columns(dataset_columns, [])
    assert result == []
    clean_metadata_mock.assert_not_called()


def test_compute_columns_metadata_with_reload_no_merge() -> None:
    """
    Test the ``compute_columns_metadata`` helper with reload without merge.
    """
    result = compute_columns_metadata(
        models[0]["columns"],
        dataset_columns,
        True,
        False,
    )

    final = dataset_columns.copy()

    for column in final:
        for dbt_column in models[0]["columns"]:
            if column["column_name"] == dbt_column["name"]:
                column["verbose_name"] = dbt_column["name"]
                column["description"] = dbt_column["description"]

    assert result == final


def test_compute_columns_metadata_without_reload_with_merge() -> None:
    """
    Test the ``compute_columns_metadata`` helper without reload with merge.
    """
    result = compute_columns_metadata(
        models[0]["columns"],
        dataset_columns,
        False,
        True,
    )

    final = dataset_columns.copy()

    for column in final:
        for dbt_column in models[0]["columns"]:
            if column["column_name"] == dbt_column["name"]:
                column["verbose_name"] = dbt_column["name"]
                column["description"] = dbt_column["description"]

    assert result == final


def test_compute_columns_metadata_without_reload_and_merge() -> None:
    """
    Test the ``compute_columns_metadata`` helper without reload and merge.
    """
    result = compute_columns_metadata(
        models[0]["columns"],
        dataset_columns,
        False,
        False,
    )
    assert result == dataset_columns


def test_compute_columns_metadata_without_reload_and_merge_other() -> None:
    """
    Test the ``compute_columns_metadata`` helper without reload and merge
    validating that dbt-specific metadata is still synced.
    """
    modified_columns = dataset_columns.copy()
    modified_columns[1]["description"] = None
    result = compute_columns_metadata(
        models[0]["columns"],
        modified_columns,
        False,
        False,
    )
    modified_columns[1]["description"] = models[0]["columns"][0]["description"]
    assert result == modified_columns


def test_compute_columns_metadata_dbt_column_meta() -> None:
    """
    Test the ``compute_columns_metadata`` helper when inferring additional metadata
    from dbt.
    """
    modified_columns = models.copy()
    modified_columns[0]["columns"] = [
        {
            "name": "id",
            "description": "Primary key",
            "meta": {
                "superset": {
                    "groupby": True,
                },
            },
        },
    ]
    result = compute_columns_metadata(
        modified_columns[0]["columns"],
        dataset_columns,
        False,
        False,
    )
    expected = dataset_columns.copy()
    expected[1]["groupby"] = True
    assert result == expected


def test_compute_dataset_metadata_with_dbt_metadata() -> None:
    """
    Test the ``compute_dataset_metadata`` helper with metadata from dbt.
    """
    model = {
        "database": "examples_dev",
        "schema": "public",
        "description": "",
        "meta": {
            "superset": {
                "extra": {"warning_markdown": "Under Construction"},
                "cache_timeout": 300,
            },
        },
        "name": "messages_channels",
        "unique_id": "model.superset_examples.messages_channels",
        "columns": [{"name": "id", "description": "Primary key"}],
    }
    model_name = model["name"]
    result = compute_dataset_metadata(
        model,
        {},
        False,
        superset_metrics,
        None,
        [],
    )

    assert result == {
        "description": model["description"],
        "extra": json.dumps(
            {
                "unique_id": model["unique_id"],
                "depends_on": f"ref('{model_name}')",
                "warning_markdown": "Under Construction",
                "certification": DEFAULT_CERTIFICATION,
            },
        ),
        "is_managed_externally": False,
        "metrics": superset_metrics,
        "cache_timeout": 300,
    }


def test_compute_dataset_metadata_with_certification_and_columns() -> None:
    """
    Test the ``compute_dataset_metadata`` helper with certification info and columns.
    """
    model: Dict[str, Any] = {
        "database": "examples_dev",
        "schema": "public",
        "description": "",
        "meta": {"superset": {"extra": {"certification": {"details": "Trust this"}}}},
        "name": "messages_channels",
        "unique_id": "model.superset_examples.messages_channels",
        "columns": [{"name": "id", "description": "Primary key"}],
    }
    model_name = model["name"]
    certification_details = model["meta"]["superset"]["extra"]["certification"]
    result = compute_dataset_metadata(
        model,
        {},
        False,
        superset_metrics,
        None,
        dataset_columns,
    )
    assert result == {
        "description": model["description"],
        "extra": json.dumps(
            {
                "unique_id": model["unique_id"],
                "depends_on": f"ref('{model_name}')",
                "certification": certification_details,
            },
        ),
        "is_managed_externally": False,
        "metrics": superset_metrics,
        "columns": dataset_columns,
    }


def test_compute_dataset_metadata_with_url_disallow_edits_cert_arg() -> None:
    """
    Test the ``compute_dataset_metadata`` helper with base URL, disallow edits and
    a certification argument.
    """
    model = {
        "database": "examples_dev",
        "schema": "public",
        "description": "",
        "meta": {},
        "name": "messages_channels",
        "unique_id": "model.superset_examples.messages_channels",
        "columns": [{"name": "id", "description": "Primary key"}],
    }
    model_name = model["name"]
    certification = {"details": "This dataset is synced from dbt Cloud"}
    result = compute_dataset_metadata(
        model,
        certification,
        True,
        superset_metrics,
        URL("https://dbt.example.org/"),
        [],
    )
    assert result == {
        "description": model["description"],
        "extra": json.dumps(
            {
                "unique_id": model["unique_id"],
                "depends_on": f"ref('{model_name}')",
                "certification": certification,
            },
        ),
        "is_managed_externally": True,
        "metrics": superset_metrics,
        "external_url": (
            "https://dbt.example.org/#!/model/model.superset_examples.messages_channels"
        ),
    }


def test_compute_dataset_metadata_null_certification() -> None:
    """
    Test the ``compute_dataset_metadata`` helper with certification set to `None`.
    """
    model = models[0].copy()
    model["meta"] = {"superset": {"extra": {"certification": None}}}
    model_name = model["name"]
    result = compute_dataset_metadata(
        model,
        None,
        False,
        superset_metrics,
        None,
        [],
    )
    assert result == {
        "description": model["description"],
        "extra": json.dumps(
            {
                "unique_id": model["unique_id"],
                "depends_on": f"ref('{model_name}')",
            },
        ),
        "is_managed_externally": False,
        "metrics": superset_metrics,
    }


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


def test_clean_metadata() -> None:
    """
    Test the ``clean_metadata`` helper.
    """
    test_data = {
        "autoincrement": "Auto Increment",
        "changed_on": "Changed On",
        "comment": "Comment",
        "to_be_kept": "To Be Kept",
        "created_on": "Created On",
        "default": "Default",
        "name": "Name",
        "nullable": "Nullable",
        "type_generic": "Type Generic",
        "yeah": "sure",
        "precision": "very precise",
        "scale": "to the max",
        "max_length": 9000,
        "info": {
            "custom_info": "blah",
        },
    }
    result = clean_metadata(test_data)
    assert result == {
        "to_be_kept": "To Be Kept",
        "yeah": "sure",
    }


def test_no_catalog_support() -> None:
    """
    Test the ``no_catalog_support`` helper.
    """
    assert no_catalog_support(DatabaseNotFoundError()) is False
    assert (
        no_catalog_support(
            SupersetError(
                errors=[
                    {
                        "message": json.dumps({"message": "Error"}),
                        "error_type": "UNKNOWN_ERROR",
                        "level": ErrorLevel.ERROR,
                    },
                    {
                        "message": json.dumps(
                            {"message": {"catalog": ["Cannot contain spaces."]}},
                        ),
                        "error_type": "UNKNOWN_ERROR",
                        "level": ErrorLevel.ERROR,
                    },
                    {
                        "message": json.dumps(
                            {"message": {"catalog": ["Unknown field."]}},
                        ),
                        "error_type": "UNKNOWN_ERROR",
                        "level": ErrorLevel.ERROR,
                    },
                ],
            ),
        )
        is True
    )


def test_create_dataset_error(mocker: MockerFixture) -> None:
    """
    Test that ``create_dataset`` surfaces errors.
    """
    client = mocker.MagicMock()
    client.create_dataset.side_effect = DatabaseNotFoundError()

    with pytest.raises(DatabaseNotFoundError):
        create_dataset(
            client,
            {
                "id": 1,
                "catalog": "examples_dev",
                "schema": "public",
                "name": "Database",
                "sqlalchemy_uri": "postgresql://user@host/examples_dev",
            },
            models[0],
        )
