"""
Tests for the ``export-metric-view`` command.
"""

# pylint: disable=redefined-outer-name, invalid-name, unused-argument

from pathlib import Path
from typing import Any, Dict

import yaml
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.cli.superset.main import superset_cli
from preset_cli.cli.superset.metric_view import (
    _translate_d3_format,
    convert_dataset_to_metric_view,
)

PHYSICAL_DATASET = {
    "id": 27,
    "table_name": "messages_channels",
    "schema": "public",
    "sql": None,
    "description": "Channels and messages joined together",
    "main_dttm_col": "ts",
    "columns": [
        {
            "column_name": "ts",
            "expression": None,
            "is_dttm": True,
            "is_active": True,
            "type": "TIMESTAMP WITHOUT TIME ZONE",
            "verbose_name": None,
            "description": None,
        },
        {
            "column_name": "name",
            "expression": None,
            "is_dttm": False,
            "is_active": True,
            "type": "VARCHAR(255)",
            "verbose_name": "Channel Name",
            "description": "Display name of the channel",
        },
        {
            "column_name": "is_recent",
            "expression": "ts >= NOW() - INTERVAL '7 days'",
            "is_dttm": False,
            "is_active": True,
            "type": "BOOLEAN",
            "verbose_name": None,
            "description": None,
        },
        {
            "column_name": "legacy",
            "expression": None,
            "is_dttm": False,
            "is_active": False,
            "type": "TEXT",
            "verbose_name": None,
            "description": None,
        },
    ],
    "metrics": [
        {
            "metric_name": "cnt",
            "expression": "count(*)",
            "metric_type": "count",
            "verbose_name": "Row count",
            "description": "Total number of rows",
            "d3format": ",d",
            "currency": None,
        },
        {
            "metric_name": "total_revenue",
            "expression": "SUM(price)",
            "metric_type": "sum",
            "verbose_name": "Total revenue",
            "description": "",
            "d3format": "$,.2f",
            "currency": None,
        },
    ],
}


def test_convert_dataset_to_metric_view_physical() -> None:
    """
    Physical datasets emit a ``schema.table`` source plus dimensions and
    measures derived from the dataset's columns and metrics.
    """
    result = convert_dataset_to_metric_view(PHYSICAL_DATASET)

    assert result == {
        "version": "0.1",
        "source": "public.messages_channels",
        "comment": "Channels and messages joined together",
        "dimensions": [
            {"name": "ts", "expr": "ts"},
            {
                "name": "name",
                "expr": "name",
                "display_name": "Channel Name",
                "comment": "Display name of the channel",
            },
            {
                "name": "is_recent",
                "expr": "ts >= NOW() - INTERVAL '7 days'",
            },
        ],
        "measures": [
            {
                "name": "cnt",
                "expr": "count(*)",
                "display_name": "Row count",
                "comment": "Total number of rows",
                "format": {"type": "number", "decimal_places": 0},
            },
            {
                "name": "total_revenue",
                "expr": "SUM(price)",
                "display_name": "Total revenue",
                "format": {
                    "type": "currency",
                    "currency_code": "USD",
                    "decimal_places": 2,
                },
            },
        ],
    }


def test_convert_dataset_to_metric_view_virtual() -> None:
    """
    Virtual datasets emit the dataset's SQL verbatim as the source.
    """
    virtual = {
        "table_name": "active_users",
        "schema": None,
        "sql": "SELECT id, last_seen FROM users WHERE active = true",
        "columns": [
            {"column_name": "id", "expression": None, "is_active": True},
        ],
        "metrics": [],
    }
    result = convert_dataset_to_metric_view(virtual)
    assert result["source"] == "SELECT id, last_seen FROM users WHERE active = true"
    assert "comment" not in result
    assert result["dimensions"] == [{"name": "id", "expr": "id"}]
    assert "measures" not in result


def test_convert_dataset_with_catalog() -> None:
    """
    A dataset that carries a catalog produces a fully qualified source.
    """
    dataset: Dict[str, Any] = {
        "table_name": "orders",
        "schema": "sales",
        "catalog": "prod",
        "sql": None,
        "columns": [],
        "metrics": [],
    }
    assert convert_dataset_to_metric_view(dataset)["source"] == "prod.sales.orders"


def test_translate_d3_format() -> None:
    """
    Only well-known d3 patterns are translated; everything else returns
    ``None`` so the caller can skip emitting a format block.
    """
    assert _translate_d3_format(",d") == {"type": "number", "decimal_places": 0}
    assert _translate_d3_format(",.2f") == {"type": "number", "decimal_places": 2}
    assert _translate_d3_format("$,.2f") == {
        "type": "currency",
        "currency_code": "USD",
        "decimal_places": 2,
    }
    assert _translate_d3_format(".1%") == {"type": "percentage", "decimal_places": 1}
    assert _translate_d3_format(None) is None
    assert _translate_d3_format("") is None
    assert _translate_d3_format("totally bogus") is None


def test_measure_falls_back_to_currency_field() -> None:
    """
    If d3format is unrecognized but ``currency`` is set, emit a currency
    format using that currency code.
    """
    dataset = {
        "table_name": "t",
        "schema": "s",
        "sql": None,
        "columns": [],
        "metrics": [
            {
                "metric_name": "m",
                "expression": "SUM(x)",
                "d3format": "weird",
                "currency": "EUR",
            },
        ],
    }
    result = convert_dataset_to_metric_view(dataset)
    assert result["measures"][0]["format"] == {
        "type": "currency",
        "currency_code": "EUR",
    }


def test_measure_without_format_omits_format_block() -> None:
    """
    Without a d3format or currency, the measure has no ``format`` field.
    """
    dataset: Dict[str, Any] = {
        "table_name": "t",
        "schema": "s",
        "sql": None,
        "columns": [],
        "metrics": [{"metric_name": "m", "expression": "COUNT(*)"}],
    }
    result = convert_dataset_to_metric_view(dataset)
    assert "format" not in result["measures"][0]


def test_export_metric_view_command(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    The CLI command writes one YAML file per dataset to the target directory.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.metric_view.SupersetClient",
    )
    client = SupersetClient()
    client.get_datasets.return_value = [{"id": 27}]
    client.get_dataset.return_value = PHYSICAL_DATASET

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export-metric-view",
            "out",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    client.get_dataset.assert_called_once_with(27)

    expected = Path("out") / "public__messages_channels.yaml"
    assert expected.exists()
    with open(expected, encoding="utf-8") as input_:
        contents = yaml.safe_load(input_)
    assert contents["source"] == "public.messages_channels"
    assert contents["measures"][0]["name"] == "cnt"


def test_export_metric_view_command_with_explicit_ids(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Passing ``--dataset-ids`` skips the listing call and exports only those.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.metric_view.SupersetClient",
    )
    client = SupersetClient()
    second_dataset = dict(PHYSICAL_DATASET, table_name="orders")
    client.get_dataset.side_effect = [PHYSICAL_DATASET, second_dataset]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export-metric-view",
            "out",
            "--dataset-ids",
            "27,42",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    client.get_datasets.assert_not_called()
    assert client.get_dataset.call_count == 2
    assert (Path("out") / "public__messages_channels.yaml").exists()
    assert (Path("out") / "public__orders.yaml").exists()


def test_export_metric_view_command_overwrite(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Without ``--overwrite``, re-running against the same directory errors.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.metric_view.SupersetClient",
    )
    client = SupersetClient()
    client.get_datasets.return_value = [{"id": 27}]
    client.get_dataset.return_value = PHYSICAL_DATASET

    runner = CliRunner()
    first = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export-metric-view", "out"],
        catch_exceptions=False,
    )
    assert first.exit_code == 0

    second = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export-metric-view", "out"],
    )
    assert second.exit_code != 0
    assert "already exists" in second.output

    third = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "export-metric-view",
            "out",
            "--overwrite",
        ],
        catch_exceptions=False,
    )
    assert third.exit_code == 0
