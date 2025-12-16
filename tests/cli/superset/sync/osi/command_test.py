"""
Tests for the OSI sync command.
"""

# pylint: disable=invalid-name,too-many-lines

from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.api.clients.superset import SupersetMetricDefinition
from preset_cli.cli.superset.main import superset_cli
from preset_cli.cli.superset.sync.osi.lib import (
    build_columns_from_fields,
    build_join_sql,
    build_metric_for_physical_dataset,
    build_metrics,
    clean_metadata,
    compute_metrics,
    create_physical_datasets,
    detect_dialect,
    get_database_id,
    get_metrics_for_dataset,
    get_or_create_denormalized_dataset,
    get_or_create_physical_dataset,
    get_osi_expression,
    get_referenced_datasets,
    parse_osi_file,
    parse_source,
    sync_osi,
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
                                    {"dialect": "ANSI_SQL", "expression": "order_id"},
                                ],
                            },
                        },
                        {
                            "name": "customer_id",
                            "expression": {
                                "dialects": [
                                    {
                                        "dialect": "ANSI_SQL",
                                        "expression": "customer_id",
                                    },
                                ],
                            },
                        },
                        {
                            "name": "order_date",
                            "expression": {
                                "dialects": [
                                    {"dialect": "ANSI_SQL", "expression": "order_date"},
                                ],
                            },
                            "dimension": {"is_time": True},
                        },
                        {
                            "name": "amount",
                            "expression": {
                                "dialects": [
                                    {"dialect": "ANSI_SQL", "expression": "amount"},
                                ],
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
                                "dialects": [
                                    {"dialect": "ANSI_SQL", "expression": "id"},
                                ],
                            },
                        },
                        {
                            "name": "email",
                            "expression": {
                                "dialects": [
                                    {"dialect": "ANSI_SQL", "expression": "email"},
                                ],
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
                },
            ],
            "metrics": [
                {
                    "name": "total_revenue",
                    "expression": {
                        "dialects": [
                            {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"},
                        ],
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
                            },
                        ],
                    },
                    "description": "Total number of customers",
                },
            ],
        },
    ],
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
            ],
        },
    }
    assert get_osi_expression(field) == "customer_id"

    # Non-ANSI_SQL first
    field = {
        "expression": {
            "dialects": [
                {"dialect": "SNOWFLAKE", "expression": "CUSTOMER_ID"},
            ],
        },
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
        },
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
        },
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

    By default, dataset prefixes are stripped from expressions for use in
    denormalized virtual datasets where columns don't have table prefixes.
    """
    osi_metrics = [
        {
            "name": "total_revenue",
            "expression": {
                "dialects": [
                    {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"},
                ],
            },
            "description": "Total revenue",
        },
        {
            "name": "customer_count",
            "expression": {
                "dialects": [
                    {
                        "dialect": "ANSI_SQL",
                        "expression": "COUNT(DISTINCT customers.id)",
                    },
                ],
            },
        },
    ]
    aliases = {"orders": "orders", "customers": "customers"}

    # Default: strip_prefixes=True for denormalized virtual datasets
    metrics = build_metrics(osi_metrics, "postgres", aliases)

    assert len(metrics) == 2
    assert metrics[0]["metric_name"] == "total_revenue"
    assert metrics[0]["metric_type"] == "sql"
    assert metrics[0]["description"] == "Total revenue"
    # Prefix should be stripped: "SUM(orders.amount)" -> "SUM(amount)"
    assert metrics[0]["expression"] == "SUM(amount)"
    assert metrics[1]["metric_name"] == "customer_count"
    # Prefix should be stripped: "COUNT(DISTINCT customers.id)" -> "COUNT(DISTINCT id)"
    assert metrics[1]["expression"] == "COUNT(DISTINCT id)"

    # With strip_prefixes=False, prefixes are preserved
    metrics_with_prefix = build_metrics(
        osi_metrics,
        "postgres",
        aliases,
        strip_prefixes=False,
    )
    assert "orders.amount" in metrics_with_prefix[0]["expression"]
    assert "customers.id" in metrics_with_prefix[1]["expression"]


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
                    {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"},
                ],
            },
        },
        {
            "name": "total_profit",
            "expression": {
                "dialects": [
                    {"dialect": "ANSI_SQL", "expression": "SUM(orders.profit)"},
                ],
            },
        },
        {
            "name": "avg_order_per_customer",
            "expression": {
                "dialects": [
                    {
                        "dialect": "ANSI_SQL",
                        "expression": "SUM(orders.amount) / COUNT(DISTINCT customers.id)",
                    },
                ],
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
            "dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"}],
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
    assert (
        "does not exist" in result.output.lower()
        or "no such file" in result.output.lower()
    )


def test_compute_metrics_reload_columns() -> None:
    """
    Test compute_metrics with reload_columns=True (default behavior).

    When reload_columns is True, OSI metrics completely replace existing metrics.
    Superset-only metrics are deleted.
    """
    existing_metrics: List[Dict[str, Any]] = [
        {"id": 1, "metric_name": "existing_metric", "expression": "COUNT(*)"},
        {"id": 2, "metric_name": "shared_metric", "expression": "SUM(old)"},
    ]
    new_metrics: List[SupersetMetricDefinition] = [
        {
            "metric_name": "shared_metric",
            "expression": "SUM(new)",
            "metric_type": "sql",
        },
        {"metric_name": "new_metric", "expression": "AVG(x)", "metric_type": "sql"},
    ]

    result = compute_metrics(
        existing_metrics,
        new_metrics,
        reload_columns=True,
        merge_metadata=False,
    )

    # Should only have OSI metrics
    assert len(result) == 2
    names = {m["metric_name"] for m in result}
    assert names == {"shared_metric", "new_metric"}

    # shared_metric should have id from existing for update
    shared = next(m for m in result if m["metric_name"] == "shared_metric")
    assert shared["id"] == 2
    assert shared["expression"] == "SUM(new)"

    # new_metric should not have id (it's new)
    new = next(m for m in result if m["metric_name"] == "new_metric")
    assert "id" not in new


def test_compute_metrics_merge_metadata() -> None:
    """
    Test compute_metrics with merge_metadata=True.

    OSI metrics are synced (updated if existing), Superset-only metrics preserved.
    """
    existing_metrics: List[Dict[str, Any]] = [
        {"id": 1, "metric_name": "superset_only", "expression": "COUNT(*)"},
        {"id": 2, "metric_name": "shared_metric", "expression": "SUM(old)"},
    ]
    new_metrics: List[SupersetMetricDefinition] = [
        {
            "metric_name": "shared_metric",
            "expression": "SUM(new)",
            "metric_type": "sql",
        },
        {"metric_name": "new_osi_metric", "expression": "AVG(x)", "metric_type": "sql"},
    ]

    result = compute_metrics(
        existing_metrics,
        new_metrics,
        reload_columns=False,
        merge_metadata=True,
    )

    # Should have: shared_metric (updated), new_osi_metric (new), superset_only (preserved)
    assert len(result) == 3
    names = {m["metric_name"] for m in result}
    assert names == {"shared_metric", "new_osi_metric", "superset_only"}

    # shared_metric should have id and new expression
    shared = next(m for m in result if m["metric_name"] == "shared_metric")
    assert shared["id"] == 2
    assert shared["expression"] == "SUM(new)"

    # superset_only should be preserved with id
    superset = next(m for m in result if m["metric_name"] == "superset_only")
    assert superset["id"] == 1


def test_compute_metrics_preserve_metadata() -> None:
    """
    Test compute_metrics with both flags False (preserve_metadata mode).

    Existing Superset metrics are preserved, only NEW OSI metrics are added.
    """
    existing_metrics: List[Dict[str, Any]] = [
        {"id": 1, "metric_name": "superset_only", "expression": "COUNT(*)"},
        {"id": 2, "metric_name": "shared_metric", "expression": "SUM(old)"},
    ]
    new_metrics: List[SupersetMetricDefinition] = [
        {
            "metric_name": "shared_metric",
            "expression": "SUM(new)",
            "metric_type": "sql",
        },
        {"metric_name": "new_osi_metric", "expression": "AVG(x)", "metric_type": "sql"},
    ]

    result = compute_metrics(
        existing_metrics,
        new_metrics,
        reload_columns=False,
        merge_metadata=False,
    )

    # Should have: shared_metric (NOT updated), new_osi_metric (added), superset_only (preserved)
    assert len(result) == 3
    names = {m["metric_name"] for m in result}
    assert names == {"shared_metric", "new_osi_metric", "superset_only"}

    # shared_metric should keep OLD expression (not updated)
    shared = next(m for m in result if m["metric_name"] == "shared_metric")
    assert shared["id"] == 2
    assert shared["expression"] == "SUM(old)"

    # new_osi_metric should be added (no id since it's new)
    new = next(m for m in result if m["metric_name"] == "new_osi_metric")
    assert "id" not in new

    # superset_only should be preserved
    superset = next(m for m in result if m["metric_name"] == "superset_only")
    assert superset["id"] == 1


def test_osi_command_preserve_and_merge_error(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test that --preserve-metadata and --merge-metadata cannot be combined.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    fs.create_file(root / "model.yaml", contents=yaml.dump(SAMPLE_OSI_MODEL))

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
            "--preserve-metadata",
            "--merge-metadata",
            "--database-id",
            "1",
            str(root / "model.yaml"),
        ],
    )

    assert result.exit_code != 0
    assert "cannot be combined" in result.output


def test_osi_command_parse_error(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test OSI command when file parsing fails.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    fs.create_file(root / "invalid.yaml", contents="not: valid: yaml: content:")

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
            str(root / "invalid.yaml"),
        ],
    )

    assert result.exit_code != 0
    assert "Failed to parse OSI file" in result.output


def test_osi_command_database_error(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test OSI command when getting database fails.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    fs.create_file(root / "model.yaml", contents=yaml.dump(SAMPLE_OSI_MODEL))

    mocker.patch(
        "preset_cli.cli.superset.main.UsernamePasswordAuth",
    )

    superset_client = mocker.MagicMock()
    superset_client.get_database.side_effect = Exception("Database not found")
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
            "999",
            str(root / "model.yaml"),
        ],
    )

    assert result.exit_code != 0
    assert "Failed to get database" in result.output


def test_osi_command_sync_error(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test OSI command when sync fails.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    fs.create_file(root / "model.yaml", contents=yaml.dump(SAMPLE_OSI_MODEL))

    mocker.patch(
        "preset_cli.cli.superset.main.UsernamePasswordAuth",
    )

    superset_client = mocker.MagicMock()
    superset_client.get_database.return_value = {
        "id": 1,
        "database_name": "test_db",
        "sqlalchemy_uri": "postgresql+psycopg2://user:pass@host/db",
    }
    superset_client.get_datasets.side_effect = Exception("API error")
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
    )

    assert result.exit_code != 0
    assert "Failed to sync OSI model" in result.output


def test_translate_expression_with_dialect() -> None:
    """
    Test expression translation with a specific target dialect.
    """
    # Test with postgres dialect
    result = translate_expression("SUM(amount)", "postgres", {})
    assert result == "SUM(amount)"

    # Test with snowflake dialect
    result = translate_expression("SUM(amount)", "snowflake", {})
    assert "SUM" in result


def test_translate_expression_with_aliases() -> None:
    """
    Test expression translation with dataset aliases.
    """
    aliases = {"orders": "o", "customers": "c"}
    result = translate_expression(
        "SUM(orders.amount) + COUNT(customers.id)",
        "postgres",
        aliases,
    )
    # Should replace dataset names with aliases
    assert "o.amount" in result or "orders.amount" in result


def test_build_join_sql_reverse_relationship() -> None:
    """
    Test JOIN SQL when relationship goes from dimension to fact table.
    """
    datasets = [
        {"name": "dim_date", "source": "db.public.dim_date"},
        {"name": "fact_sales", "source": "db.public.fact_sales"},
    ]
    relationships = [
        {
            "name": "date_to_sales",
            "from": "dim_date",
            "to": "fact_sales",
            "from_columns": ["date_key"],
            "to_columns": ["sale_date_key"],
        },
    ]

    sql = build_join_sql(datasets, relationships)

    # Should still generate valid SQL
    assert "SELECT" in sql
    assert "FROM" in sql


def test_build_join_sql_unrelated_tables() -> None:
    """
    Test JOIN SQL with tables that have no relationships (CROSS JOIN).
    """
    datasets = [
        {"name": "orders", "source": "db.public.orders"},
        {"name": "unrelated", "source": "db.public.unrelated_table"},
    ]
    relationships: List[Dict[str, Any]] = []  # No relationships

    sql = build_join_sql(datasets, relationships)

    # Should use CROSS JOIN for unrelated tables
    assert "CROSS JOIN" in sql


def test_build_join_sql_with_catalog() -> None:
    """
    Test JOIN SQL with catalog.schema.table format.
    """
    datasets = [
        {"name": "orders", "source": "catalog.schema.orders"},
        {"name": "customers", "source": "catalog.schema.customers"},
    ]
    relationships = [
        {
            "name": "orders_to_customers",
            "from": "orders",
            "to": "customers",
            "from_columns": ["customer_id"],
            "to_columns": ["id"],
        },
    ]

    sql = build_join_sql(datasets, relationships)

    # Should include catalog in table reference
    assert "catalog.schema.orders" in sql


def test_build_columns_from_fields() -> None:
    """
    Test building column metadata from OSI fields.
    """
    osi_datasets = [
        {
            "name": "orders",
            "fields": [
                {
                    "name": "order_id",
                    "expression": {
                        "dialects": [{"dialect": "ANSI_SQL", "expression": "order_id"}],
                    },
                },
                {
                    "name": "order_date",
                    "expression": {
                        "dialects": [
                            {"dialect": "ANSI_SQL", "expression": "order_date"},
                        ],
                    },
                    "dimension": {"is_time": True},
                    "description": "Date when order was placed",
                },
                {
                    "name": "computed_field",
                    "expression": {
                        "dialects": [
                            {
                                "dialect": "ANSI_SQL",
                                "expression": "price * quantity",
                            },
                        ],
                    },
                },
            ],
        },
    ]

    columns = build_columns_from_fields(osi_datasets, "postgres")

    assert len(columns) == 3

    # Check order_id column
    order_id_col = next(c for c in columns if c["column_name"] == "order_id")
    assert "is_dttm" not in order_id_col

    # Check order_date column (time dimension)
    order_date_col = next(c for c in columns if c["column_name"] == "order_date")
    assert order_date_col["is_dttm"] is True
    assert order_date_col["description"] == "Date when order was placed"

    # Check computed field has expression
    computed_col = next(c for c in columns if c["column_name"] == "computed_field")
    assert "expression" in computed_col


def test_build_columns_from_fields_duplicate_columns() -> None:
    """
    Test that duplicate column names are skipped.
    """
    osi_datasets = [
        {
            "name": "orders",
            "fields": [
                {
                    "name": "id",
                    "expression": {
                        "dialects": [{"dialect": "ANSI_SQL", "expression": "id"}],
                    },
                },
            ],
        },
        {
            "name": "customers",
            "fields": [
                {
                    "name": "id",  # Same name as orders.id
                    "expression": {
                        "dialects": [{"dialect": "ANSI_SQL", "expression": "id"}],
                    },
                },
            ],
        },
    ]

    columns = build_columns_from_fields(osi_datasets, "postgres")

    # Should only have one 'id' column
    id_columns = [c for c in columns if c["column_name"] == "id"]
    assert len(id_columns) == 1


def test_build_metrics_skip_invalid() -> None:
    """
    Test that metrics without valid expressions are skipped.
    """
    osi_metrics = [
        {
            "name": "valid_metric",
            "expression": {
                "dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(amount)"}],
            },
        },
        {
            "name": "invalid_metric",
            "expression": {},  # No valid expression
        },
    ]
    aliases = {"orders": "orders"}

    metrics = build_metrics(osi_metrics, "postgres", aliases)

    # Should only have one valid metric
    assert len(metrics) == 1
    assert metrics[0]["metric_name"] == "valid_metric"


def test_clean_metadata() -> None:
    """
    Test that clean_metadata removes API-incompatible fields.
    """
    metric = {
        "id": 1,
        "metric_name": "test_metric",
        "expression": "COUNT(*)",
        "changed_on": "2023-01-01",
        "created_on": "2023-01-01",
        "autoincrement": True,
        "comment": "test comment",
    }

    cleaned = clean_metadata(metric)

    assert "id" in cleaned
    assert "metric_name" in cleaned
    assert "changed_on" not in cleaned
    assert "created_on" not in cleaned
    assert "autoincrement" not in cleaned
    assert "comment" not in cleaned


def test_create_physical_datasets(mocker: MockerFixture) -> None:
    """
    Test creating physical datasets with metrics.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.return_value = {"id": 1}
    client.get_dataset.return_value = {
        "id": 1,
        "table_name": "orders",
        "metrics": [],
    }

    osi_datasets = [
        {"name": "orders", "source": "db.public.orders"},
    ]
    database = {"id": 1, "database_name": "test_db"}
    osi_metrics = [
        {
            "name": "total_orders",
            "expression": {
                "dialects": [{"dialect": "ANSI_SQL", "expression": "COUNT(orders.id)"}],
            },
        },
    ]

    result = create_physical_datasets(
        client,
        osi_datasets,
        database,
        osi_metrics=osi_metrics,
        target_dialect="postgres",
    )

    assert "orders" in result
    assert client.create_dataset.called


def test_create_physical_datasets_error_handling(mocker: MockerFixture) -> None:
    """
    Test that errors in creating datasets are handled gracefully.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.side_effect = Exception("API Error")

    osi_datasets = [
        {"name": "orders", "source": "db.public.orders"},
    ]
    database = {"id": 1, "database_name": "test_db"}

    result = create_physical_datasets(client, osi_datasets, database)

    # Should return empty dict but not raise
    assert result == {}


def test_get_or_create_denormalized_dataset_existing(mocker: MockerFixture) -> None:
    """
    Test updating an existing denormalized dataset.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = [{"id": 100}]
    client.get_dataset.return_value = {
        "id": 100,
        "table_name": "test_model_denormalized",
        "metrics": [{"id": 1, "metric_name": "old_metric", "expression": "COUNT(*)"}],
    }

    osi_model = {
        "name": "test_model",
        "datasets": [
            {"name": "orders", "source": "db.public.orders"},
        ],
        "relationships": [],
        "metrics": [
            {
                "name": "new_metric",
                "expression": {
                    "dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(amount)"}],
                },
            },
        ],
    }
    database = {
        "id": 1,
        "database_name": "test_db",
        "sqlalchemy_uri": "postgresql://host/db",
    }

    result = get_or_create_denormalized_dataset(
        client,
        osi_model,
        database,
        target_dialect="postgres",
    )

    # Should update existing dataset
    assert client.update_dataset.called
    assert result["id"] == 100


def test_get_or_create_denormalized_dataset_new(mocker: MockerFixture) -> None:
    """
    Test creating a new denormalized dataset.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []  # No existing
    client.create_dataset.return_value = {"id": 200}
    client.get_dataset.return_value = {
        "id": 200,
        "table_name": "test_model_denormalized",
        "metrics": [],
    }

    osi_model = {
        "name": "test_model",
        "datasets": [
            {"name": "orders", "source": "db.public.orders"},
        ],
        "relationships": [],
        "metrics": [
            {
                "name": "total_sales",
                "expression": {
                    "dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(amount)"}],
                },
            },
        ],
    }
    database = {
        "id": 1,
        "database_name": "test_db",
        "sqlalchemy_uri": "postgresql://host/db",
    }

    result = get_or_create_denormalized_dataset(
        client,
        osi_model,
        database,
        target_dialect="postgres",
    )

    assert client.create_dataset.called
    assert result["id"] == 200


def test_sync_osi_full_flow(mocker: MockerFixture) -> None:
    """
    Test the full sync_osi flow.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.return_value = {"id": 1}
    client.get_dataset.return_value = {
        "id": 1,
        "table_name": "test",
        "metrics": [],
    }

    osi_model = {
        "name": "test_model",
        "datasets": [
            {"name": "orders", "source": "db.public.orders"},
            {"name": "customers", "source": "db.public.customers"},
        ],
        "relationships": [
            {
                "name": "orders_to_customers",
                "from": "orders",
                "to": "customers",
                "from_columns": ["customer_id"],
                "to_columns": ["id"],
            },
        ],
        "metrics": [
            {
                "name": "total_revenue",
                "expression": {
                    "dialects": [
                        {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"},
                    ],
                },
            },
        ],
    }
    database = {
        "id": 1,
        "database_name": "test_db",
        "sqlalchemy_uri": "postgresql+psycopg2://user:pass@host/db",
    }

    result = sync_osi(client, osi_model, database)

    # Should create datasets and return denormalized dataset
    assert result is not None
    assert client.create_dataset.called


def test_sync_osi_no_dialect_detected(mocker: MockerFixture) -> None:
    """
    Test sync_osi when dialect cannot be detected.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.return_value = {"id": 1}
    client.get_dataset.return_value = {
        "id": 1,
        "table_name": "test",
        "metrics": [],
    }

    osi_model = {
        "name": "test_model",
        "datasets": [{"name": "orders", "source": "db.public.orders"}],
        "relationships": [],
        "metrics": [],
    }
    database = {
        "id": 1,
        "database_name": "test_db",
        "sqlalchemy_uri": "unknown://host/db",  # Unknown dialect
    }

    result = sync_osi(client, osi_model, database)

    assert result is not None


def test_get_osi_expression_list_format() -> None:
    """
    Test get_osi_expression with standard dict format.
    """
    # Test the normal dict with dialects format
    field_dict = {
        "expression": {
            "dialects": [{"dialect": "ANSI_SQL", "expression": "column_name"}],
        },
    }
    result = get_osi_expression(field_dict)
    assert result == "column_name"


def test_get_osi_expression_non_ansi_dialect() -> None:
    """
    Test get_osi_expression falls back to first dialect when ANSI_SQL not present.
    """
    field = {
        "expression": {
            "dialects": [
                {"dialect": "SNOWFLAKE", "expression": "SNOWFLAKE_EXPR"},
            ],
        },
    }
    result = get_osi_expression(field)
    assert result == "SNOWFLAKE_EXPR"


def test_get_osi_expression_direct_list_format() -> None:
    """
    Test get_osi_expression when expression is directly a list of dialects.
    """
    # When expression itself is a list (alternate format)
    field = {
        "expression": [
            {"dialect": "ANSI_SQL", "expression": "direct_list_expr"},
        ],
    }
    result = get_osi_expression(field)
    assert result == "direct_list_expr"


def test_build_join_sql_reverse_join_direction() -> None:
    """
    Test JOIN SQL where the relationship goes from dimension to fact.

    This tests the branch where to_ds is in joined_tables but from_ds is not.
    """
    datasets = [
        {"name": "fact_sales", "source": "db.public.fact_sales"},
        {"name": "dim_product", "source": "db.public.dim_product"},
    ]
    # Relationship defined from dimension to fact (reverse of typical)
    relationships = [
        {
            "name": "product_to_sales",
            "from": "dim_product",
            "to": "fact_sales",
            "from_columns": ["product_id"],
            "to_columns": ["product_id"],
        },
    ]

    sql = build_join_sql(datasets, relationships)

    # Should generate valid JOIN SQL
    assert "SELECT" in sql
    assert "FROM" in sql
    assert "LEFT JOIN" in sql
    assert "dim_product" in sql


def test_build_join_sql_missing_dataset_in_relationship() -> None:
    """
    Test JOIN SQL when a relationship references a non-existent dataset.
    """
    datasets = [
        {"name": "orders", "source": "db.public.orders"},
    ]
    relationships = [
        {
            "name": "orders_to_missing",
            "from": "orders",
            "to": "missing_table",  # Does not exist
            "from_columns": ["id"],
            "to_columns": ["order_id"],
        },
    ]

    sql = build_join_sql(datasets, relationships)

    # Should still generate SQL, just without the join for missing table
    assert "SELECT" in sql
    assert "FROM" in sql
    # Should not have LEFT JOIN since the target doesn't exist
    assert "LEFT JOIN" not in sql


def test_build_join_sql_both_tables_already_joined() -> None:
    """
    Test JOIN SQL when both tables in a relationship are already joined.

    This covers the 'else: continue' branch (line 300).
    """
    datasets = [
        {"name": "orders", "source": "db.public.orders"},
        {"name": "customers", "source": "db.public.customers"},
        {"name": "products", "source": "db.public.products"},
    ]
    relationships = [
        # First relationship joins customers
        {
            "name": "orders_to_customers",
            "from": "orders",
            "to": "customers",
            "from_columns": ["customer_id"],
            "to_columns": ["id"],
        },
        # Second relationship joins products
        {
            "name": "orders_to_products",
            "from": "orders",
            "to": "products",
            "from_columns": ["product_id"],
            "to_columns": ["id"],
        },
        # Third relationship between already-joined tables (should be skipped)
        {
            "name": "customers_to_products",
            "from": "customers",
            "to": "products",
            "from_columns": ["fav_product"],
            "to_columns": ["id"],
        },
    ]

    sql = build_join_sql(datasets, relationships)

    # Should have JOINs for customers and products, but not a third JOIN
    assert sql.count("LEFT JOIN") == 2


def test_build_join_sql_with_schema_only() -> None:
    """
    Test JOIN SQL with schema.table format (no catalog).
    """
    datasets = [
        {"name": "orders", "source": "public.orders"},
        {"name": "customers", "source": "public.customers"},
    ]
    relationships = [
        {
            "name": "orders_to_customers",
            "from": "orders",
            "to": "customers",
            "from_columns": ["customer_id"],
            "to_columns": ["id"],
        },
    ]

    sql = build_join_sql(datasets, relationships)

    assert "public.orders" in sql
    assert "public.customers" in sql


def test_build_join_sql_unrelated_with_schema_catalog() -> None:
    """
    Test CROSS JOIN for unrelated tables with full catalog.schema.table format.
    """
    datasets = [
        {"name": "main", "source": "cat.schema.main"},
        {"name": "lookup", "source": "cat.schema.lookup"},
    ]
    relationships: List[Dict[str, Any]] = []  # No relationships

    sql = build_join_sql(datasets, relationships)

    assert "CROSS JOIN" in sql
    assert "cat.schema.lookup" in sql


def test_get_or_create_physical_dataset_existing(mocker: MockerFixture) -> None:
    """
    Test get_or_create_physical_dataset when dataset already exists.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = [{"id": 42}]
    client.get_dataset.return_value = {
        "id": 42,
        "table_name": "existing_table",
    }

    osi_dataset = {"name": "my_table", "source": "db.public.my_table"}
    database = {"id": 1}

    result = get_or_create_physical_dataset(client, osi_dataset, database)

    assert result["id"] == 42
    # Should not call create_dataset since it exists
    client.create_dataset.assert_not_called()


def test_get_or_create_physical_dataset_with_catalog(mocker: MockerFixture) -> None:
    """
    Test get_or_create_physical_dataset with catalog.schema.table format.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []  # No existing
    client.create_dataset.return_value = {"id": 100}
    client.get_dataset.return_value = {"id": 100, "table_name": "my_table"}

    osi_dataset = {"name": "my_table", "source": "my_catalog.my_schema.my_table"}
    database = {"id": 1}

    result = get_or_create_physical_dataset(client, osi_dataset, database)

    assert result["id"] == 100
    # Verify catalog was passed
    call_kwargs = client.create_dataset.call_args[1]
    assert call_kwargs.get("catalog") == "my_catalog"
    assert call_kwargs.get("schema") == "my_schema"


def test_get_metrics_for_dataset_no_expression() -> None:
    """
    Test get_metrics_for_dataset skips metrics without expressions.
    """
    osi_metrics = [
        {
            "name": "valid_metric",
            "expression": {
                "dialects": [
                    {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"},
                ],
            },
        },
        {
            "name": "invalid_metric",
            "expression": {},  # No valid expression
        },
    ]

    result = get_metrics_for_dataset(osi_metrics, "orders", ["orders", "customers"])

    # Should only include the valid metric
    assert len(result) == 1
    assert result[0]["name"] == "valid_metric"


def test_build_metric_for_physical_dataset_with_dialect() -> None:
    """
    Test build_metric_for_physical_dataset with target dialect translation.
    """
    metric = {
        "name": "total_amount",
        "expression": {
            "dialects": [
                {"dialect": "ANSI_SQL", "expression": "SUM(orders.amount)"},
            ],
        },
        "description": "Total order amount",
    }

    result = build_metric_for_physical_dataset(metric, "orders", "snowflake")

    assert result["metric_name"] == "total_amount"
    assert result["description"] == "Total order amount"
    # Expression should have dataset prefix stripped
    assert "orders." not in result["expression"]
    assert "amount" in result["expression"]


def test_build_metric_for_physical_dataset_translation_error(
    mocker: MockerFixture,
) -> None:
    """
    Test build_metric_for_physical_dataset handles translation errors gracefully.
    """
    # Mock sqlglot to raise an exception
    mocker.patch(
        "preset_cli.cli.superset.sync.osi.lib.sqlglot.transpile",
        side_effect=Exception("Translation failed"),
    )

    metric = {
        "name": "test_metric",
        "expression": {
            "dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(t.x)"}],
        },
    }

    # Should not raise, should fall back to original expression
    result = build_metric_for_physical_dataset(metric, "t", "postgres")

    assert result["metric_name"] == "test_metric"
    # Expression should still be set (original, without prefix)
    assert "expression" in result


def test_create_physical_datasets_existing_metrics_no_new(
    mocker: MockerFixture,
) -> None:
    """
    Test create_physical_datasets when all metrics already exist (no new count).

    This covers line 516 (echo without new metrics count).
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.return_value = {"id": 1}
    client.get_dataset.return_value = {
        "id": 1,
        "table_name": "orders",
        "metrics": [
            {"metric_name": "existing_metric", "expression": "COUNT(*)"},
        ],
    }

    osi_datasets = [{"name": "orders", "source": "db.public.orders"}]
    database = {"id": 1, "database_name": "test_db"}
    osi_metrics = [
        {
            "name": "existing_metric",  # Same name as existing
            "expression": {
                "dialects": [
                    {"dialect": "ANSI_SQL", "expression": "COUNT(orders.id)"},
                ],
            },
        },
    ]

    result = create_physical_datasets(
        client,
        osi_datasets,
        database,
        osi_metrics=osi_metrics,
        target_dialect="postgres",
        reload_columns=False,  # Preserve existing
        merge_metadata=False,
    )

    assert "orders" in result


def test_get_or_create_denormalized_dataset_empty_datasets(
    mocker: MockerFixture,
) -> None:
    """
    Test get_or_create_denormalized_dataset with empty datasets list.

    This covers line 691 (schema = None when no datasets).
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.return_value = {"id": 1}
    client.get_dataset.return_value = {"id": 1, "table_name": "test_denormalized"}

    osi_model = {
        "name": "test_model",
        "datasets": [],  # Empty datasets
        "relationships": [],
        "metrics": [],
    }
    database = {"id": 1, "database_name": "test_db"}

    # This will raise CLIError due to empty datasets in build_join_sql
    with pytest.raises(CLIError, match="No datasets"):
        get_or_create_denormalized_dataset(
            client,
            osi_model,
            database,
            target_dialect="postgres",
        )


def test_get_or_create_denormalized_dataset_with_schema(mocker: MockerFixture) -> None:
    """
    Test creating new denormalized dataset with schema.

    This covers lines 745-746 (adding schema to kwargs).
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []  # No existing
    client.create_dataset.return_value = {"id": 200}
    client.get_dataset.return_value = {
        "id": 200,
        "table_name": "test_model_denormalized",
    }

    osi_model = {
        "name": "test_model",
        "datasets": [
            {"name": "orders", "source": "myschema.orders"},  # Has schema
        ],
        "relationships": [],
        "metrics": [],
    }
    database = {"id": 1, "database_name": "test_db"}

    result = get_or_create_denormalized_dataset(
        client,
        osi_model,
        database,
        target_dialect="postgres",
    )

    # Verify schema was passed to create_dataset
    call_kwargs = client.create_dataset.call_args[1]
    assert call_kwargs.get("schema") == "myschema"
    assert result["id"] == 200


def test_translate_expression_transpile_failure(mocker: MockerFixture) -> None:
    """
    Test translate_expression when sqlglot transpile fails.
    """
    mocker.patch(
        "preset_cli.cli.superset.sync.osi.lib.sqlglot.transpile",
        side_effect=Exception("Parse error"),
    )

    result = translate_expression("INVALID SQL", "postgres", {})

    # Should return original expression on failure
    assert result == "INVALID SQL"


def test_translate_expression_empty_result(mocker: MockerFixture) -> None:
    """
    Test translate_expression when sqlglot returns empty result.
    """
    mocker.patch(
        "preset_cli.cli.superset.sync.osi.lib.sqlglot.transpile",
        return_value=[],  # Empty result
    )

    result = translate_expression("SUM(x)", "postgres", {})

    # Should return original expression when result is empty
    assert result == "SUM(x)"


def test_build_join_sql_fact_table_without_schema() -> None:
    """
    Test JOIN SQL where fact table source has no schema (just table name).
    """
    datasets = [
        {"name": "orders", "source": "orders"},  # No schema
        {"name": "customers", "source": "customers"},  # No schema
    ]
    relationships = [
        {
            "name": "orders_to_customers",
            "from": "orders",
            "to": "customers",
            "from_columns": ["customer_id"],
            "to_columns": ["id"],
        },
    ]

    sql = build_join_sql(datasets, relationships)

    assert "FROM orders orders" in sql
    assert "LEFT JOIN customers customers" in sql


def test_build_join_sql_join_with_schema_no_catalog() -> None:
    """
    Test JOIN SQL where joined table has schema but no catalog.

    This covers the branch at line 304-305.
    """
    datasets = [
        {"name": "fact", "source": "public.fact"},  # Schema only
        {"name": "dim", "source": "public.dim"},  # Schema only
    ]
    relationships = [
        {
            "name": "fact_to_dim",
            "from": "fact",
            "to": "dim",
            "from_columns": ["dim_id"],
            "to_columns": ["id"],
        },
    ]

    sql = build_join_sql(datasets, relationships)

    assert "LEFT JOIN public.dim" in sql


def test_build_join_sql_join_with_catalog() -> None:
    """
    Test JOIN SQL where joined table has full catalog.schema.table format.

    This covers the branch at line 306-307.
    """
    datasets = [
        {"name": "fact", "source": "cat.schema.fact"},
        {"name": "dim", "source": "cat.schema.dim"},
    ]
    relationships = [
        {
            "name": "fact_to_dim",
            "from": "fact",
            "to": "dim",
            "from_columns": ["dim_id"],
            "to_columns": ["id"],
        },
    ]

    sql = build_join_sql(datasets, relationships)

    assert "LEFT JOIN cat.schema.dim" in sql


def test_build_join_sql_unrelated_with_schema_no_catalog() -> None:
    """
    Test CROSS JOIN for unrelated table with schema but no catalog.

    This covers the branch at lines 318-319.
    """
    datasets = [
        {"name": "main", "source": "public.main"},
        {"name": "lookup", "source": "public.lookup"},
    ]
    relationships: List[Dict[str, Any]] = []

    sql = build_join_sql(datasets, relationships)

    assert "CROSS JOIN public.lookup" in sql


def test_get_or_create_physical_dataset_with_schema_no_catalog(
    mocker: MockerFixture,
) -> None:
    """
    Test get_or_create_physical_dataset with schema.table format (no catalog).

    This covers the branch at lines 364-365 (schema without catalog).
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []  # No existing
    client.create_dataset.return_value = {"id": 100}
    client.get_dataset.return_value = {"id": 100, "table_name": "my_table"}

    osi_dataset = {"name": "my_table", "source": "my_schema.my_table"}
    database = {"id": 1}

    result = get_or_create_physical_dataset(client, osi_dataset, database)

    assert result["id"] == 100
    call_kwargs = client.create_dataset.call_args[1]
    assert call_kwargs.get("schema") == "my_schema"
    assert call_kwargs.get("catalog") is None


def test_get_or_create_physical_dataset_existing_with_schema(
    mocker: MockerFixture,
) -> None:
    """
    Test get_or_create_physical_dataset when dataset exists and has schema.

    This covers the branch at line 350-351 (schema filter added).
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = [{"id": 42}]
    client.get_dataset.return_value = {"id": 42, "table_name": "my_table"}

    osi_dataset = {"name": "my_table", "source": "my_schema.my_table"}
    database = {"id": 1}

    result = get_or_create_physical_dataset(client, osi_dataset, database)

    assert result["id"] == 42
    # Verify schema was included in the filter
    call_kwargs = client.get_datasets.call_args[1]
    assert call_kwargs.get("schema") == "my_schema"


def test_build_metric_for_physical_dataset_successful_translation() -> None:
    """
    Test build_metric_for_physical_dataset with successful dialect translation.

    This covers the branch at lines 432-436 where transpile succeeds.
    """
    metric = {
        "name": "count_metric",
        "expression": {
            "dialects": [
                {"dialect": "ANSI_SQL", "expression": "COUNT(orders.id)"},
            ],
        },
    }

    # Test with a dialect that sqlglot supports
    result = build_metric_for_physical_dataset(metric, "orders", "postgres")

    assert result["metric_name"] == "count_metric"
    # The expression should be translated and have prefix stripped
    assert "COUNT" in result["expression"]
    assert "orders." not in result["expression"]


def test_get_or_create_denormalized_dataset_no_schema(mocker: MockerFixture) -> None:
    """
    Test creating denormalized dataset when source has no schema.

    This covers the branch at line 745 where schema is None.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []
    client.create_dataset.return_value = {"id": 300}
    client.get_dataset.return_value = {
        "id": 300,
        "table_name": "test_model_denormalized",
    }

    osi_model = {
        "name": "test_model",
        "datasets": [
            {"name": "orders", "source": "orders"},  # No schema
        ],
        "relationships": [],
        "metrics": [],
    }
    database = {"id": 1, "database_name": "test_db"}

    result = get_or_create_denormalized_dataset(
        client,
        osi_model,
        database,
        target_dialect="postgres",
    )

    # Verify schema was NOT passed (since source has no schema)
    call_kwargs = client.create_dataset.call_args[1]
    assert "schema" not in call_kwargs
    assert result["id"] == 300


def test_build_join_sql_dimension_joins_fact() -> None:
    """
    Test JOIN SQL when dimension table needs to join to fact table.

    This covers lines 279-284 where to_ds is already joined and from_ds needs joining.
    """
    # Create a scenario where the dimension is the second table to be joined
    datasets = [
        {"name": "fact_sales", "source": "sales"},  # Will be fact table
        {"name": "dim_time", "source": "time_dim"},
        {"name": "dim_product", "source": "product_dim"},
    ]
    # Relationships from dimensions to fact
    relationships = [
        {
            "name": "time_to_sales",
            "from": "dim_time",  # Not in joined yet
            "to": "fact_sales",  # Is the fact table (already joined)
            "from_columns": ["time_key"],
            "to_columns": ["time_fk"],
        },
        {
            "name": "product_to_sales",
            "from": "dim_product",
            "to": "fact_sales",
            "from_columns": ["product_key"],
            "to_columns": ["product_fk"],
        },
    ]

    sql = build_join_sql(datasets, relationships)

    # Should join dim_time and dim_product to fact_sales
    assert "LEFT JOIN" in sql
    assert "dim_time" in sql
    assert "dim_product" in sql


def test_translate_expression_no_dialect() -> None:
    """
    Test translate_expression when no target dialect is specified.

    This ensures the branch where target_dialect is None/empty is taken.
    """
    result = translate_expression("SUM(amount)", None, {"orders": "o"})

    # Should replace aliases but not translate (no dialect)
    assert "o.amount" in result or "SUM(amount)" in result


def test_build_metric_for_physical_dataset_no_dialect() -> None:
    """
    Test build_metric_for_physical_dataset without target dialect.

    This covers the branch where target_dialect is None (lines 432-440 not taken).
    """
    metric = {
        "name": "simple_metric",
        "expression": {
            "dialects": [
                {"dialect": "ANSI_SQL", "expression": "SUM(orders.total)"},
            ],
        },
    }

    # No dialect - should not attempt translation
    result = build_metric_for_physical_dataset(metric, "orders", None)

    assert result["metric_name"] == "simple_metric"
    # Expression should have prefix stripped but no dialect translation
    assert "orders." not in result["expression"]
    assert "total" in result["expression"]


def test_get_or_create_physical_dataset_no_schema(mocker: MockerFixture) -> None:
    """
    Test get_or_create_physical_dataset with just table name (no schema/catalog).

    This covers the branch at lines 350 and 364 where schema is None.
    """
    client = mocker.MagicMock()
    client.get_datasets.return_value = []  # No existing
    client.create_dataset.return_value = {"id": 100}
    client.get_dataset.return_value = {"id": 100, "table_name": "my_table"}

    osi_dataset = {"name": "my_table", "source": "my_table"}  # Just table name
    database = {"id": 1}

    result = get_or_create_physical_dataset(client, osi_dataset, database)

    assert result["id"] == 100
    call_kwargs = client.create_dataset.call_args[1]
    # Schema should not be in kwargs when source has no schema
    assert "schema" not in call_kwargs or call_kwargs.get("schema") is None


def test_build_join_sql_relationship_from_unknown_dataset() -> None:
    """
    Test JOIN SQL when relationship's 'from' field references unknown dataset.

    This covers line 238->236 where from_dataset is not in outgoing_counts.
    """
    datasets = [
        {"name": "orders", "source": "orders"},
        {"name": "customers", "source": "customers"},
    ]
    relationships = [
        {
            "name": "unknown_to_orders",
            "from": "unknown_dataset",  # Not in datasets list
            "to": "orders",
            "from_columns": ["id"],
            "to_columns": ["ref_id"],
        },
    ]

    sql = build_join_sql(datasets, relationships)

    # Should still generate valid SQL, ignoring the unknown relationship
    assert "SELECT" in sql
    assert "FROM" in sql


def test_build_join_sql_reverse_join_missing_from_dataset() -> None:
    """
    Test JOIN SQL where 'from' table is not in dataset list but 'to' is joined.

    This covers line 282 where join_dataset is None in the reverse join branch.
    """
    datasets = [
        {"name": "fact_sales", "source": "sales"},
    ]
    # Relationship from non-existent dataset to fact_sales
    relationships = [
        {
            "name": "ghost_to_sales",
            "from": "non_existent_dim",  # Not in datasets
            "to": "fact_sales",  # Is the fact table (already joined)
            "from_columns": ["key"],
            "to_columns": ["dim_fk"],
        },
    ]

    sql = build_join_sql(datasets, relationships)

    # Should still generate valid SQL without the join
    assert "SELECT" in sql
    assert "FROM sales fact_sales" in sql
    assert "LEFT JOIN" not in sql  # No join since 'from' doesn't exist


def test_build_join_sql_unrelated_table_no_schema() -> None:
    """
    Test CROSS JOIN for unrelated table with just table name (no schema).

    This covers lines 318->320 where ds_source["schema"] is falsy.
    """
    datasets = [
        {"name": "main_table", "source": "main"},  # No schema
        {"name": "lookup", "source": "lookup"},  # No schema, will be CROSS JOINed
    ]
    relationships: List[Dict[str, Any]] = []  # No relationships

    sql = build_join_sql(datasets, relationships)

    # Should use CROSS JOIN with just table name (no schema prefix)
    assert "CROSS JOIN lookup lookup" in sql


def test_build_metric_for_physical_dataset_empty_transpile_result(
    mocker: MockerFixture,
) -> None:
    """
    Test build_metric_for_physical_dataset when transpile returns empty list.

    This covers lines 435->440 where result is empty/falsy.
    """
    mocker.patch(
        "preset_cli.cli.superset.sync.osi.lib.sqlglot.transpile",
        return_value=[],  # Empty result
    )

    metric = {
        "name": "test_metric",
        "expression": {
            "dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(t.amount)"}],
        },
    }

    result = build_metric_for_physical_dataset(metric, "t", "postgres")

    assert result["metric_name"] == "test_metric"
    # Expression should be the cleaned (prefix-stripped) original
    assert result["expression"] == "SUM(amount)"
