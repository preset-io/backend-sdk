"""
Tests for the OSI sync command.
"""

# pylint: disable=invalid-name

import os
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.cli.superset.main import superset_cli
from preset_cli.cli.superset.sync.osi.lib import (
    build_join_sql,
    build_metric_for_physical_dataset,
    build_metrics,
    detect_dialect,
    get_database_id,
    get_metrics_for_dataset,
    get_osi_expression,
    get_referenced_datasets,
    parse_osi_file,
    parse_source,
    translate_expression,
)
from preset_cli.exceptions import CLIError


# Sample OSI model for testing
SAMPLE_OSI_MODEL = {
    "semantic_model": [
        {
            "name": "test_model",
            "description": "A test semantic model",
            "datasets": [
                {
                    "name": "orders",
                    "source": "db.public.orders",
                    "primary_key": ["order_id"],
                    "fields": [
                        {
                            "name": "order_id",
                            "expression": {
                                "dialects": [
                                    {"dialect": "ANSI_SQL", "expression": "order_id"}
                                ]
                            },
                        },
                        {
                            "name": "customer_id",
                            "expression": {
                                "dialects": [
                                    {"dialect": "ANSI_SQL", "expression": "customer_id"}
                                ]
                            },
                        },
                        {
                            "name": "order_date",
                            "expression": {
                                "dialects": [
                                    {"dialect": "ANSI_SQL", "expression": "order_date"}
                                ]
                            },
                            "dimension": {"is_time": True},
                        },
                        {
                            "name": "amount",
                            "expression": {
                                "dialects": [
                                    {"dialect": "ANSI_SQL", "expression": "amount"}
                                ]
                            },
                        },
                    ],
                },
                {
                    "name": "customers",
                    "source": "db.public.customers",
                    "primary_key": ["id"],
                    "fields": [
                        {
                            "name": "id",
                            "expression": {
                                "dialects": [{"dialect": "ANSI_SQL", "expression": "id"}]
                            },
                        },
                        {
                            "name": "email",
                            "expression": {
                                "dialects": [
                                    {"dialect": "ANSI_SQL", "expression": "email"}
                                ]
                            },
                        },
                    ],
                },
            ],
            "relationships": [
                {
                    "name": "orders_to_customers",
                    "from": "orders",
                    "to": "customers",
                    "from_columns": ["customer_id"],
                    "to_columns": ["id"],
                }
            ],
            "metrics": [
                {
                    "name": "total_revenue",
                    "expression": {
                        "dialects": [
                            {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}
                        ]
                    },
                    "description": "Total revenue from all orders",
                },
                {
                    "name": "customer_count",
                    "expression": {
                        "dialects": [
                            {
                                "dialect": "ANSI_SQL",
                                "expression": "COUNT(DISTINCT customers.id)",
                            }
                        ]
                    },
                    "description": "Total number of customers",
                },
            ],
        }
    ]
}


def test_parse_osi_file(fs: FakeFilesystem) -> None:
    """
    Test parsing an OSI YAML file.
    """
    fs.create_file("/test/model.yaml", contents=yaml.dump(SAMPLE_OSI_MODEL))

    model = parse_osi_file("/test/model.yaml")
    assert model["name"] == "test_model"
    assert len(model["datasets"]) == 2
    assert len(model["relationships"]) == 1
    assert len(model["metrics"]) == 2


def test_parse_osi_file_missing_semantic_model(fs: FakeFilesystem) -> None:
    """
    Test parsing an OSI file without semantic_model key.
    """
    fs.create_file("/test/invalid.yaml", contents=yaml.dump({"datasets": []}))

    with pytest.raises(CLIError) as excinfo:
        parse_osi_file("/test/invalid.yaml")
    assert "missing 'semantic_model' key" in str(excinfo.value)


def test_parse_osi_file_empty_semantic_model(fs: FakeFilesystem) -> None:
    """
    Test parsing an OSI file with empty semantic_model list.
    """
    fs.create_file("/test/empty.yaml", contents=yaml.dump({"semantic_model": []}))

    with pytest.raises(CLIError) as excinfo:
        parse_osi_file("/test/empty.yaml")
    assert "empty 'semantic_model' list" in str(excinfo.value)


def test_get_database_id(mocker: MockerFixture) -> None:
    """
    Test the get_database_id helper.
    """
    client = mocker.MagicMock()

    # Test no databases available
    client.get_databases.return_value = []
    with pytest.raises(CLIError) as excinfo:
        get_database_id(client)
    assert "No databases available" in str(excinfo.value)

    # Test single database auto-selection
    client.get_databases.return_value = [
        {"id": 1, "database_name": "My database"},
    ]
    assert get_database_id(client) == 1

    # Test multiple databases with prompting
    client.get_databases.return_value = [
        {"id": 1, "database_name": "My database"},
        {"id": 3, "database_name": "My other database"},
    ]
    mocker.patch(
        "preset_cli.cli.superset.sync.osi.lib.input",
        side_effect=["invalid", "2"],
    )
    assert get_database_id(client) == 3


def test_detect_dialect() -> None:
    """
    Test dialect detection from sqlalchemy URIs.
    """
    assert detect_dialect("postgresql+psycopg2://user:pass@host/db") == "postgres"
    assert detect_dialect("snowflake://user:pass@account/db") == "snowflake"
    assert detect_dialect("bigquery://project") == "bigquery"
    assert detect_dialect("mysql+pymysql://user:pass@host/db") == "mysql"
    assert detect_dialect("redshift+psycopg2://user:pass@host/db") == "redshift"
    assert detect_dialect("trino://user@host/catalog") == "trino"
    assert detect_dialect("duckdb:///path/to/db") == "duckdb"
    assert detect_dialect("unknown://host/db") is None


def test_parse_source() -> None:
    """
    Test parsing OSI source strings.
    """
    # Full path: database.schema.table
    result = parse_source("mydb.public.orders")
    assert result == {"catalog": "mydb", "schema": "public", "table": "orders"}

    # Schema.table
    result = parse_source("public.orders")
    assert result == {"catalog": None, "schema": "public", "table": "orders"}

    # Table only
    result = parse_source("orders")
    assert result == {"catalog": None, "schema": None, "table": "orders"}


def test_get_osi_expression() -> None:
    """
    Test extracting expressions from OSI field/metric definitions.
    """
    # Standard format with dialects
    field = {
        "expression": {
            "dialects": [
                {"dialect": "ANSI_SQL", "expression": "customer_id"},
                {"dialect": "SNOWFLAKE", "expression": "CUSTOMER_ID"},
            ]
        }
    }
    assert get_osi_expression(field) == "customer_id"

    # Non-ANSI_SQL first
    field = {
        "expression": {
            "dialects": [
                {"dialect": "SNOWFLAKE", "expression": "CUSTOMER_ID"},
            ]
        }
    }
    assert get_osi_expression(field) == "CUSTOMER_ID"

    # Empty expression
    field = {"expression": {}}
    assert get_osi_expression(field) is None

    # No expression key
    field = {}
    assert get_osi_expression(field) is None


def test_translate_expression() -> None:
    """
    Test SQL expression translation with sqlglot.
    """
    # Basic translation
    result = translate_expression("SUM(amount)", "postgres", {})
    assert result == "SUM(amount)"

    # With dataset alias replacement
    aliases = {"orders": "orders", "customers": "customers"}
    result = translate_expression("SUM(orders.amount)", "postgres", aliases)
    assert "SUM(orders.amount)" in result

    # Handles invalid expressions gracefully
    result = translate_expression("INVALID EXPRESSION ###", "postgres", {})
    assert result == "INVALID EXPRESSION ###"  # Returns original on failure


def test_build_join_sql() -> None:
    """
    Test building JOIN SQL from OSI datasets and relationships.
    """
    datasets = [
        {"name": "orders", "source": "db.public.orders"},
        {"name": "customers", "source": "db.public.customers"},
    ]
    relationships = [
        {
            "name": "orders_to_customers",
            "from": "orders",
            "to": "customers",
            "from_columns": ["customer_id"],
            "to_columns": ["id"],
        }
    ]

    sql = build_join_sql(datasets, relationships)

    # Should contain SELECT with all tables
    assert "orders.*" in sql
    assert "customers.*" in sql

    # Should have FROM clause with fact table
    assert "FROM" in sql

    # Should have LEFT JOIN
    assert "LEFT JOIN" in sql
    assert "ON" in sql


def test_build_join_sql_composite_key() -> None:
    """
    Test JOIN SQL generation with composite foreign keys.
    """
    datasets = [
        {"name": "order_lines", "source": "db.public.order_lines"},
        {"name": "products", "source": "db.public.products"},
    ]
    relationships = [
        {
            "name": "order_lines_to_products",
            "from": "order_lines",
            "to": "products",
            "from_columns": ["product_id", "variant_id"],
            "to_columns": ["id", "variant_id"],
        }
    ]

    sql = build_join_sql(datasets, relationships)

    # Should have both join conditions
    assert "product_id" in sql
    assert "variant_id" in sql
    assert "AND" in sql


def test_build_join_sql_no_datasets() -> None:
    """
    Test JOIN SQL generation with no datasets raises error.
    """
    with pytest.raises(CLIError) as excinfo:
        build_join_sql([], [])
    assert "No datasets defined" in str(excinfo.value)


def test_build_metrics() -> None:
    """
    Test converting OSI metrics to Superset format.
    """
    osi_metrics = [
        {
            "name": "total_revenue",
            "expression": {
                "dialects": [
                    {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}
                ]
            },
            "description": "Total revenue",
        },
        {
            "name": "customer_count",
            "expression": {
                "dialects": [
                    {"dialect": "ANSI_SQL", "expression": "COUNT(DISTINCT customers.id)"}
                ]
            },
        },
    ]
    aliases = {"orders": "orders", "customers": "customers"}

    metrics = build_metrics(osi_metrics, "postgres", aliases)

    assert len(metrics) == 2
    assert metrics[0]["metric_name"] == "total_revenue"
    assert metrics[0]["metric_type"] == "sql"
    assert metrics[0]["description"] == "Total revenue"
    assert metrics[1]["metric_name"] == "customer_count"


def test_get_referenced_datasets() -> None:
    """
    Test extracting dataset references from metric expressions.
    """
    dataset_names = ["orders", "customers", "products"]

    # Single dataset reference
    expr1 = "SUM(orders.amount)"
    assert get_referenced_datasets(expr1, dataset_names) == ["orders"]

    # Multiple dataset references
    expr2 = "SUM(orders.amount) / COUNT(DISTINCT customers.id)"
    refs = get_referenced_datasets(expr2, dataset_names)
    assert set(refs) == {"orders", "customers"}

    # No dataset references (just column names)
    expr3 = "SUM(amount)"
    assert get_referenced_datasets(expr3, dataset_names) == []

    # Dataset name that's a substring of another
    dataset_names2 = ["order", "order_lines"]
    expr4 = "SUM(order_lines.quantity)"
    assert get_referenced_datasets(expr4, dataset_names2) == ["order_lines"]


def test_get_metrics_for_dataset() -> None:
    """
    Test filtering metrics that reference only a single dataset.
    """
    osi_metrics = [
        {
            "name": "total_sales",
            "expression": {
                "dialects": [
                    {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}
                ]
            },
        },
        {
            "name": "total_profit",
            "expression": {
                "dialects": [
                    {"dialect": "ANSI_SQL", "expression": "SUM(orders.profit)"}
                ]
            },
        },
        {
            "name": "avg_order_per_customer",
            "expression": {
                "dialects": [
                    {
                        "dialect": "ANSI_SQL",
                        "expression": "SUM(orders.amount) / COUNT(DISTINCT customers.id)",
                    }
                ]
            },
        },
    ]
    all_datasets = ["orders", "customers"]

    # Should only get metrics that reference only 'orders'
    orders_metrics = get_metrics_for_dataset(osi_metrics, "orders", all_datasets)
    assert len(orders_metrics) == 2
    assert orders_metrics[0]["name"] == "total_sales"
    assert orders_metrics[1]["name"] == "total_profit"

    # Should get no metrics for 'customers' (no single-dataset metrics)
    customers_metrics = get_metrics_for_dataset(osi_metrics, "customers", all_datasets)
    assert len(customers_metrics) == 0


def test_build_metric_for_physical_dataset() -> None:
    """
    Test building a metric for a physical dataset (strips dataset prefix).
    """
    metric = {
        "name": "total_sales",
        "expression": {
            "dialects": [
                {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}
            ]
        },
        "description": "Total sales amount",
    }

    result = build_metric_for_physical_dataset(metric, "orders", "postgres")

    assert result["metric_name"] == "total_sales"
    assert result["expression"] == "SUM(amount)"  # Prefix stripped
    assert result["description"] == "Total sales amount"
    assert result["metric_type"] == "sql"


def test_osi_command(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the full OSI sync command.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    fs.create_file(root / "model.yaml", contents=yaml.dump(SAMPLE_OSI_MODEL))

    mocker.patch(
        "preset_cli.cli.superset.main.UsernamePasswordAuth",
    )

    superset_client = mocker.MagicMock()
    superset_client.get_databases.return_value = [
        {"id": 1, "database_name": "test_db"},
    ]
    superset_client.get_database.return_value = {
        "id": 1,
        "database_name": "test_db",
        "sqlalchemy_uri": "postgresql+psycopg2://user:pass@host/db",
    }
    superset_client.get_datasets.return_value = []
    superset_client.create_dataset.return_value = {"id": 100}
    superset_client.get_dataset.return_value = {
        "id": 100,
        "table_name": "test_model_denormalized",
        "metrics": [],
    }
    superset_client.update_dataset.return_value = {"id": 100}

    mocker.patch(
        "preset_cli.cli.superset.sync.osi.command.SupersetClient",
        return_value=superset_client,
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "http://localhost:8088",
            "sync",
            "osi",
            "--database-id",
            "1",
            str(root / "model.yaml"),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Parsing OSI file" in result.output
    assert "Found semantic model: test_model" in result.output
    assert "Sync complete!" in result.output


def test_osi_command_with_prompting(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test OSI sync command with database prompting.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    fs.create_file(root / "model.yaml", contents=yaml.dump(SAMPLE_OSI_MODEL))

    mocker.patch(
        "preset_cli.cli.superset.main.UsernamePasswordAuth",
    )

    superset_client = mocker.MagicMock()
    superset_client.get_databases.return_value = [
        {"id": 1, "database_name": "test_db"},
        {"id": 2, "database_name": "other_db"},
    ]
    superset_client.get_database.return_value = {
        "id": 2,
        "database_name": "other_db",
        "sqlalchemy_uri": "postgresql+psycopg2://user:pass@host/db",
    }
    superset_client.get_datasets.return_value = []
    superset_client.create_dataset.return_value = {"id": 100}
    superset_client.get_dataset.return_value = {
        "id": 100,
        "table_name": "test_model_denormalized",
        "metrics": [],
    }
    superset_client.update_dataset.return_value = {"id": 100}

    mocker.patch(
        "preset_cli.cli.superset.sync.osi.command.SupersetClient",
        return_value=superset_client,
    )

    # Patch input to select second database
    mocker.patch(
        "preset_cli.cli.superset.sync.osi.lib.input",
        return_value="2",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "http://localhost:8088",
            "sync",
            "osi",
            str(root / "model.yaml"),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "Choose a database:" in result.output


def test_osi_command_file_not_found(mocker: MockerFixture) -> None:
    """
    Test OSI sync command with non-existent file.
    """
    mocker.patch(
        "preset_cli.cli.superset.main.UsernamePasswordAuth",
    )

    superset_client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.osi.command.SupersetClient",
        return_value=superset_client,
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "http://localhost:8088",
            "sync",
            "osi",
            "--database-id",
            "1",
            "/nonexistent/model.yaml",
        ],
    )

    assert result.exit_code != 0
    assert "does not exist" in result.output.lower() or "no such file" in result.output.lower()
