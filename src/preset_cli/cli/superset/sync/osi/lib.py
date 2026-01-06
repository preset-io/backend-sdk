"""
Core logic for OSI (Open Semantic Interchange) sync.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

import click
import sqlglot
import yaml
from sqlalchemy.engine.url import make_url

from preset_cli.api.clients.superset import SupersetClient, SupersetMetricDefinition
from preset_cli.api.operators import OneToMany
from preset_cli.exceptions import CLIError

_logger = logging.getLogger(__name__)

# Fields to remove from metrics/columns when updating (not accepted by Superset API)
FIELDS_TO_CLEAN = (
    "autoincrement",
    "changed_on",
    "comment",
    "created_on",
    "default",
    "name",
    "nullable",
    "type_generic",
    "precision",
    "scale",
    "max_length",
    "info",
)


def clean_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove incompatible fields from metadata for create/update operations.

    Superset's API doesn't accept certain read-only fields like changed_on, created_on, etc.
    """
    cleaned = metadata.copy()
    for key in FIELDS_TO_CLEAN:
        cleaned.pop(key, None)
    return cleaned


# Map sqlalchemy driver names to sqlglot dialects
DIALECT_MAP = {
    "postgresql": "postgres",
    "postgresql+psycopg2": "postgres",
    "postgresql+asyncpg": "postgres",
    "redshift": "redshift",
    "redshift+psycopg2": "redshift",
    "redshift+redshift_connector": "redshift",
    "bigquery": "bigquery",
    "snowflake": "snowflake",
    "mysql": "mysql",
    "mysql+pymysql": "mysql",
    "mysql+mysqldb": "mysql",
    "trino": "trino",
    "presto": "presto",
    "databricks": "databricks",
    "duckdb": "duckdb",
    "sqlite": "sqlite",
    "clickhouse": "clickhouse",
    "clickhouse+native": "clickhouse",
    "athena": "presto",  # Athena uses Presto dialect
    "awsathena+rest": "presto",
}

# OSI dialect names to sqlglot dialect names
OSI_DIALECT_MAP = {
    "ANSI_SQL": None,  # sqlglot default
    "SNOWFLAKE": "snowflake",
    "MDX": None,  # Not supported by sqlglot
    "TABLEAU": None,  # Not supported by sqlglot
}


def parse_osi_file(file_path: str) -> Dict[str, Any]:
    """
    Parse and validate an OSI YAML file.

    Returns the first semantic model from the file.
    """
    with open(file_path, encoding="utf-8") as file_handle:
        content = yaml.safe_load(file_handle)

    if "semantic_model" not in content:
        raise CLIError("Invalid OSI file: missing 'semantic_model' key", 1)

    models = content["semantic_model"]
    if not models:
        raise CLIError("Invalid OSI file: empty 'semantic_model' list", 1)

    return models[0]


def get_database_id(client: SupersetClient) -> int:
    """
    Prompt user to select a database.

    If there's only one database, auto-selects it.
    """
    databases = client.get_databases()
    if not databases:
        raise CLIError("No databases available", 1)

    if len(databases) == 1:
        database = databases[0]
        click.echo(
            f'Using database {database["database_name"]} [id={database["id"]}] '
            "since it's the only one",
        )
        return database["id"]

    click.echo("Choose a database:")
    for i, database in enumerate(databases):
        click.echo(f'({i+1}) {database["database_name"]} [id={database["id"]}]')

    while True:
        try:
            choice = int(input("> "))
        except Exception:  # pylint: disable=broad-except
            choice = -1
        if 0 < choice <= len(databases):
            return databases[choice - 1]["id"]
        click.echo("Invalid choice")


def detect_dialect(sqlalchemy_uri: str) -> Optional[str]:
    """
    Detect sqlglot dialect from a sqlalchemy URI.

    Returns None if dialect cannot be determined (will use sqlglot default).
    """
    url = make_url(sqlalchemy_uri)
    drivername = url.drivername

    return DIALECT_MAP.get(drivername)


def get_osi_expression(field_or_metric: Dict[str, Any]) -> Optional[str]:
    """
    Extract the best expression from an OSI field or metric.

    Prefers ANSI_SQL dialect, falls back to first available.
    """
    expression_obj = field_or_metric.get("expression", {})

    # Handle different expression formats in the spec
    # Check if expression is directly a list (alternate format)
    if isinstance(expression_obj, list):
        dialects = expression_obj
    else:
        dialects = expression_obj.get("dialects", [])

    if not dialects:
        return None

    # Prefer ANSI_SQL
    for dialect_entry in dialects:
        if dialect_entry.get("dialect") == "ANSI_SQL":
            return dialect_entry.get("expression")

    # Fall back to first available
    return dialects[0].get("expression") if dialects else None


def translate_expression(
    expression: str,
    target_dialect: Optional[str],
    dataset_aliases: Optional[Dict[str, str]] = None,
) -> str:
    """
    Translate SQL expression to target dialect using sqlglot.

    Also replaces dataset.column references with just column names
    (since the denormalized view will have all columns).
    """
    if dataset_aliases:
        # Replace dataset.column with alias.column for proper JOIN references
        for dataset_name, alias in dataset_aliases.items():
            # Match dataset_name.column_name pattern
            pattern = rf"\b{re.escape(dataset_name)}\.(\w+)"
            expression = re.sub(pattern, rf"{alias}.\1", expression)

    if target_dialect:
        try:
            # Transpile to target dialect
            result = sqlglot.transpile(expression, write=target_dialect)
            if result:
                return result[0]
        except Exception as ex:  # pylint: disable=broad-except
            _logger.warning("Failed to translate expression: %s", ex)

    return expression


def parse_source(source: str) -> Dict[str, Optional[str]]:
    """
    Parse an OSI dataset source string.

    Source format: database.schema.table or schema.table or table
    """
    parts = source.split(".")
    if len(parts) == 3:
        return {"catalog": parts[0], "schema": parts[1], "table": parts[2]}
    if len(parts) == 2:
        return {"catalog": None, "schema": parts[0], "table": parts[1]}
    return {"catalog": None, "schema": None, "table": parts[0]}


def _get_column_selections(
    dataset: Dict[str, Any],
    is_fact_table: bool,
) -> List[str]:
    """
    Build column selection expressions for a dataset.

    For the fact table, columns are selected without prefix.
    For dimension tables, columns are prefixed with table name to avoid collisions.
    """
    alias = dataset["name"]
    fields = dataset.get("fields", [])

    if not fields:
        # No fields defined, fall back to SELECT *
        return [f"{alias}.*"]

    selections = []
    for field in fields:
        field_name = field.get("name", "")
        if not field_name:
            continue

        if is_fact_table:
            # Fact table columns: no prefix needed
            selections.append(f"{alias}.{field_name}")
        else:
            # Dimension table columns: prefix with table name to avoid collisions
            selections.append(f"{alias}.{field_name} AS {alias}__{field_name}")

    return selections if selections else [f"{alias}.*"]


def _identify_fact_table(
    datasets: List[Dict[str, Any]],
    relationships: Optional[List[Dict[str, Any]]],
) -> Dict[str, Any]:
    """
    Identify the fact table from datasets based on outgoing relationships.

    The fact table is the one with the most outgoing (foreign key) relationships.
    """
    if not datasets:
        raise CLIError("No datasets defined in OSI model", 1)

    # Count outgoing relationships for each dataset
    outgoing_counts: Dict[str, int] = {dataset["name"]: 0 for dataset in datasets}
    for rel in relationships or []:
        from_dataset = rel.get("from")
        if from_dataset in outgoing_counts:
            outgoing_counts[from_dataset] += 1

    # Sort by outgoing count descending
    sorted_datasets = sorted(
        datasets,
        key=lambda d: outgoing_counts.get(d["name"], 0),
        reverse=True,
    )
    return sorted_datasets[0]


def build_join_sql(  # noqa: C901  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    datasets: List[Dict[str, Any]],
    relationships: List[Dict[str, Any]],
) -> str:
    """
    Build SQL query that JOINs all related tables.

    Identifies the fact table as the one with the most outgoing relationships,
    then LEFT JOINs all dimension tables.

    Column naming:
    - Fact table columns use original names (e.g., order_id, amount)
    - Dimension table columns are prefixed to avoid collisions (e.g., customers__id)
    """
    fact_table = _identify_fact_table(datasets, relationships)

    # Build dataset lookup
    dataset_by_name = {dataset["name"]: dataset for dataset in datasets}

    # Build SELECT clause with columns from all tables
    select_parts = []
    for dataset in datasets:
        is_fact = dataset["name"] == fact_table["name"]
        selections = _get_column_selections(dataset, is_fact_table=is_fact)
        select_parts.extend(selections)

    # Build FROM clause
    fact_source = parse_source(fact_table["source"])
    fact_alias = fact_table["name"]
    fact_table_ref = fact_source["table"]
    if fact_source["schema"]:
        fact_table_ref = f'{fact_source["schema"]}.{fact_table_ref}'
    if fact_source["catalog"]:
        fact_table_ref = f'{fact_source["catalog"]}.{fact_table_ref}'

    from_clause = f"{fact_table_ref} {fact_alias}"

    # Build JOIN clauses
    join_clauses = []
    joined_tables = {fact_table["name"]}

    for rel in relationships or []:
        from_ds_name = rel.get("from")
        to_ds_name = rel.get("to")
        from_columns = rel.get("from_columns", [])
        to_columns = rel.get("to_columns", [])

        # Determine which table to join
        if to_ds_name in joined_tables and from_ds_name not in joined_tables:
            # Join the 'from' table
            join_ds_name = from_ds_name
            join_dataset = dataset_by_name.get(join_ds_name)
            if not join_dataset:
                continue

            on_conditions = [
                f"{join_ds_name}.{from_col} = {to_ds_name}.{to_col}"
                for from_col, to_col in zip(from_columns, to_columns)
            ]
        elif from_ds_name in joined_tables and to_ds_name not in joined_tables:
            # Join the 'to' table
            join_ds_name = to_ds_name
            join_dataset = dataset_by_name.get(join_ds_name)
            if not join_dataset:
                continue

            on_conditions = [
                f"{from_ds_name}.{from_col} = {join_ds_name}.{to_col}"
                for from_col, to_col in zip(from_columns, to_columns)
            ]
        else:
            continue  # pragma: no cover

        join_source = parse_source(join_dataset["source"])
        join_table_ref = join_source["table"]
        if join_source["schema"]:
            join_table_ref = f'{join_source["schema"]}.{join_table_ref}'
        if join_source["catalog"]:
            join_table_ref = f'{join_source["catalog"]}.{join_table_ref}'

        on_clause = " AND ".join(on_conditions)
        join_clauses.append(f"LEFT JOIN {join_table_ref} {join_ds_name} ON {on_clause}")
        joined_tables.add(join_ds_name)

    # Handle tables not yet joined (no relationships)
    for dataset in datasets:
        if dataset["name"] not in joined_tables:
            ds_source = parse_source(dataset["source"])
            ds_table_ref = ds_source["table"]
            if ds_source["schema"]:
                ds_table_ref = f'{ds_source["schema"]}.{ds_table_ref}'
            if ds_source["catalog"]:
                ds_table_ref = f'{ds_source["catalog"]}.{ds_table_ref}'
            # Cross join unrelated tables (rare case)
            join_clauses.append(f"CROSS JOIN {ds_table_ref} {dataset['name']}")
            joined_tables.add(dataset["name"])

    sql = f"SELECT {', '.join(select_parts)}\nFROM {from_clause}"
    if join_clauses:
        sql += "\n" + "\n".join(join_clauses)

    return sql


def get_or_create_physical_dataset(
    client: SupersetClient,
    osi_dataset: Dict[str, Any],
    database: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Get or create a physical dataset for an OSI dataset definition.
    """
    source = parse_source(osi_dataset["source"])
    table_name = source["table"]
    schema = source["schema"]

    # Check if dataset already exists
    filters: Dict[str, Any] = {
        "database": OneToMany(database["id"]),
        "table_name": table_name,
    }
    if schema:
        filters["schema"] = schema

    existing = client.get_datasets(**filters)
    if existing:
        _logger.info("Found existing dataset for %s", osi_dataset["name"])
        return client.get_dataset(existing[0]["id"])

    # Create new dataset
    _logger.info("Creating dataset for %s", osi_dataset["name"])
    kwargs: Dict[str, Any] = {
        "database": database["id"],
        "table_name": table_name,
    }
    if schema:
        kwargs["schema"] = schema
    if source["catalog"]:
        kwargs["catalog"] = source["catalog"]

    dataset = client.create_dataset(**kwargs)
    return client.get_dataset(dataset["id"])


def get_referenced_datasets(expression: str, dataset_names: List[str]) -> List[str]:
    """
    Extract dataset names referenced in a metric expression.

    Looks for patterns like "dataset_name.column_name" in the expression.
    """
    referenced = set()
    for ds_name in dataset_names:
        # Match dataset_name.column pattern
        pattern = rf"\b{re.escape(ds_name)}\.\w+"
        if re.search(pattern, expression):
            referenced.add(ds_name)
    return list(referenced)


def get_metrics_for_dataset(
    osi_metrics: List[Dict[str, Any]],
    dataset_name: str,
    all_dataset_names: List[str],
) -> List[Dict[str, Any]]:
    """
    Get metrics that reference only the specified dataset.

    Returns metrics that can be expressed from a single dataset.
    """
    single_dataset_metrics = []

    for metric in osi_metrics:
        expression = get_osi_expression(metric)
        if not expression:
            continue

        referenced = get_referenced_datasets(expression, all_dataset_names)

        # Only include if this metric references exactly this one dataset
        if referenced == [dataset_name]:
            single_dataset_metrics.append(metric)

    return single_dataset_metrics


def build_metric_for_physical_dataset(
    metric: Dict[str, Any],
    dataset_name: str,
    target_dialect: Optional[str],
) -> SupersetMetricDefinition:
    """
    Build a Superset metric for a physical dataset.

    Strips the dataset prefix from column references since we're on the physical table.
    """
    name = metric.get("name", "")
    expression = get_osi_expression(metric) or ""

    # Remove dataset prefix from expression (e.g., "store_sales.amount" -> "amount")
    pattern = rf"\b{re.escape(dataset_name)}\.(\w+)"
    cleaned_expression = re.sub(pattern, r"\1", expression)

    # Translate to target dialect
    if target_dialect:
        try:
            result = sqlglot.transpile(cleaned_expression, write=target_dialect)
            if result:
                cleaned_expression = result[0]
        except Exception as ex:  # pylint: disable=broad-except
            _logger.warning("Failed to translate expression: %s", ex)

    superset_metric: SupersetMetricDefinition = {
        "metric_name": name,
        "expression": cleaned_expression,
        "metric_type": "sql",
        "verbose_name": name.replace("_", " ").title(),
    }

    description = metric.get("description")
    if description:
        superset_metric["description"] = description

    return superset_metric


def create_physical_datasets(  # pylint: disable=too-many-arguments,too-many-locals
    client: SupersetClient,
    osi_datasets: List[Dict[str, Any]],
    database: Dict[str, Any],
    osi_metrics: Optional[List[Dict[str, Any]]] = None,
    target_dialect: Optional[str] = None,
    reload_columns: bool = True,
    merge_metadata: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """
    Create physical datasets for each OSI dataset.

    Also adds metrics that reference only a single dataset to that dataset.

    Returns a mapping of dataset name to Superset dataset.
    Failures are logged and a summary is printed at the end.
    """
    result = {}
    failures: List[tuple[str, str]] = []  # (name, error message)
    all_dataset_names = [ds["name"] for ds in osi_datasets]
    osi_metrics = osi_metrics or []

    for osi_dataset in osi_datasets:
        name = osi_dataset["name"]
        try:
            dataset = get_or_create_physical_dataset(client, osi_dataset, database)
            result[name] = dataset

            # Find metrics that reference only this dataset
            dataset_metrics = get_metrics_for_dataset(
                osi_metrics,
                name,
                all_dataset_names,
            )

            if dataset_metrics:
                # Build Superset metrics
                superset_metrics = [
                    build_metric_for_physical_dataset(m, name, target_dialect)
                    for m in dataset_metrics
                ]

                # Compute final metrics using merge logic
                existing_metrics = dataset.get("metrics", [])
                final_metrics = compute_metrics(
                    existing_metrics,
                    superset_metrics,
                    reload_columns,
                    merge_metadata,
                )

                # Count new metrics for output
                existing_names = {m["metric_name"] for m in existing_metrics}
                new_count = sum(
                    1 for m in final_metrics if m["metric_name"] not in existing_names
                )

                client.update_dataset(dataset["id"], metrics=final_metrics)
                if new_count > 0:
                    click.echo(
                        f"  Created/found physical dataset: {name} "
                        f"(+{new_count} metric(s))",
                    )
                else:
                    click.echo(f"  Created/found physical dataset: {name}")
            else:
                click.echo(f"  Created/found physical dataset: {name}")

        except Exception as ex:  # pylint: disable=broad-except
            error_msg = str(ex)
            _logger.error("Failed to create dataset %s: %s", name, error_msg)
            click.echo(f"  Failed to create physical dataset: {name} ({error_msg})")
            failures.append((name, error_msg))

    # Print summary if there were failures
    if failures:
        click.echo(
            click.style(
                f"\nWarning: {len(failures)} of {len(osi_datasets)} physical "
                f"dataset(s) failed to sync:",
                fg="yellow",
            ),
        )
        for name, error in failures:
            click.echo(click.style(f"  - {name}: {error}", fg="yellow"))

    return result


def build_metrics(
    osi_metrics: List[Dict[str, Any]],
    target_dialect: Optional[str],
    dataset_aliases: Dict[str, str],
    strip_prefixes: bool = True,
) -> List[SupersetMetricDefinition]:
    """
    Convert OSI metrics to Superset metric format with dialect translation.

    When strip_prefixes is True (default for denormalized datasets), dataset
    prefixes like "store_sales." are removed from expressions since the virtual
    dataset subquery doesn't have table aliases.
    """
    superset_metrics: List[SupersetMetricDefinition] = []

    for metric in osi_metrics:
        name = metric.get("name", "")
        expression = get_osi_expression(metric)
        if not expression:
            _logger.warning("Metric %s has no valid expression, skipping", name)
            continue

        # Translate expression
        translated = translate_expression(expression, target_dialect, dataset_aliases)

        # Strip dataset prefixes for denormalized virtual datasets
        # e.g., "SUM(store_sales.ss_ext_sales_price)" -> "SUM(ss_ext_sales_price)"
        if strip_prefixes:
            for alias in dataset_aliases:
                translated = re.sub(rf"\b{re.escape(alias)}\.", "", translated)

        superset_metric: SupersetMetricDefinition = {
            "metric_name": name,
            "expression": translated,
            "metric_type": "sql",
            "verbose_name": name.replace("_", " ").title(),
        }

        description = metric.get("description")
        if description:
            superset_metric["description"] = description

        superset_metrics.append(superset_metric)

    return superset_metrics


def compute_metrics(
    existing_metrics: List[Dict[str, Any]],
    new_metrics: List[SupersetMetricDefinition],
    reload_columns: bool,
    merge_metadata: bool,
) -> List[Dict[str, Any]]:
    """
    Compute the final list of metrics for updating a dataset.

    reload_columns (default): OSI metrics replace all, Superset-only metrics deleted
    merge_metadata: OSI metrics synced, Superset-only metrics preserved
    if both are false (preserve_metadata): Superset metrics preserved, only add new OSI metrics
    """
    # Index existing metrics by name
    existing_by_name = {m["metric_name"]: m for m in existing_metrics}
    new_by_name = {m["metric_name"]: m for m in new_metrics}

    final_metrics: List[Dict[str, Any]] = []

    # Process new metrics from OSI
    for name, metric in new_by_name.items():
        if reload_columns or merge_metadata or name not in existing_by_name:
            final_metric = dict(metric)
            # If metric exists, include its ID for update instead of create
            if name in existing_by_name:
                final_metric["id"] = existing_by_name[name]["id"]
            final_metrics.append(final_metric)

    # Preserve existing Superset metrics if not reload_columns
    if not reload_columns:
        for existing_name, existing_metric in existing_by_name.items():
            if not merge_metadata or existing_name not in new_by_name:
                # Clean metadata and add to final list
                cleaned = clean_metadata(existing_metric)
                final_metrics.append(cleaned)

    return final_metrics


def build_columns_from_fields(
    osi_datasets: List[Dict[str, Any]],
    target_dialect: Optional[str],
    fact_table_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Build column metadata from OSI field definitions.

    Returns list of column definitions with descriptions and is_dttm flags.

    Args:
        osi_datasets: List of OSI dataset definitions
        target_dialect: Target SQL dialect for expression translation
        fact_table_name: Name of the fact table. If provided, dimension table
            columns will be prefixed with {table_name}__ to match the JOIN SQL.
    """
    columns = []
    seen_columns = set()

    for dataset in osi_datasets:
        alias = dataset["name"]
        is_fact_table = fact_table_name is None or alias == fact_table_name

        for field in dataset.get("fields", []):
            field_name = field.get("name", "")
            expression = get_osi_expression(field)

            # For denormalized views, dimension table columns are prefixed
            if is_fact_table:
                column_name = field_name
            else:
                column_name = f"{alias}__{field_name}"

            # Skip if we've already processed this column
            if column_name in seen_columns:
                continue
            seen_columns.add(column_name)

            column: Dict[str, Any] = {
                "column_name": column_name,
            }

            # Check if it's a time dimension
            dimension = field.get("dimension", {})
            if dimension.get("is_time"):
                column["is_dttm"] = True

            # Add description
            description = field.get("description")
            if description:
                column["description"] = description

            # Add verbose name from description or field name
            column["verbose_name"] = description or field_name.replace("_", " ").title()

            # Check if it's a computed column (expression != column name)
            if expression and expression != field_name:
                translated = translate_expression(
                    expression,
                    target_dialect,
                    {alias: alias},
                )
                column["expression"] = translated

            columns.append(column)

    return columns


def get_or_create_denormalized_dataset(  # pylint: disable=too-many-arguments,too-many-locals
    client: SupersetClient,
    osi_model: Dict[str, Any],
    database: Dict[str, Any],
    target_dialect: Optional[str],
    reload_columns: bool = True,
    merge_metadata: bool = False,
) -> Dict[str, Any]:
    """
    Create or update the denormalized virtual dataset with JOINs and metrics.

    Also sets column metadata (is_dttm, descriptions) from OSI field definitions.
    """
    model_name = osi_model.get("name", "osi_model")
    datasets = osi_model.get("datasets", [])
    relationships = osi_model.get("relationships", [])
    metrics = osi_model.get("metrics", [])

    # Build dataset name for the denormalized view
    denorm_name = f"{model_name}_denormalized"

    # Parse schema from first dataset
    if datasets:
        first_source = parse_source(datasets[0]["source"])
        schema = first_source["schema"]
    else:
        schema = None

    # Check if denormalized dataset already exists
    filters: Dict[str, Any] = {
        "database": OneToMany(database["id"]),
        "table_name": denorm_name,
    }
    if schema:
        filters["schema"] = schema

    existing = client.get_datasets(**filters)

    # Build the JOIN SQL
    join_sql = build_join_sql(datasets, relationships)
    _logger.debug("Generated JOIN SQL:\n%s", join_sql)

    # Build dataset aliases for metric translation
    dataset_aliases = {ds["name"]: ds["name"] for ds in datasets}

    # Build metrics from OSI
    superset_metrics = build_metrics(metrics, target_dialect, dataset_aliases)

    # Build column metadata from OSI fields (with prefixed names for dimensions)
    fact_table = _identify_fact_table(datasets, relationships) if datasets else None
    fact_table_name = fact_table["name"] if fact_table else None
    column_metadata = build_columns_from_fields(
        datasets,
        target_dialect,
        fact_table_name,
    )

    if existing:
        # Update existing dataset
        dataset_id = existing[0]["id"]
        _logger.info("Updating existing denormalized dataset %s", denorm_name)
        dataset = client.get_dataset(dataset_id)

        # Compute final metrics using merge logic
        existing_metrics = dataset.get("metrics", [])
        final_metrics = compute_metrics(
            existing_metrics,
            superset_metrics,
            reload_columns,
            merge_metadata,
        )

        # Update with new SQL, metrics, and column metadata
        update_payload: Dict[str, Any] = {
            "sql": join_sql,
            "metrics": final_metrics,
            "description": osi_model.get("description", ""),
        }
        if column_metadata:
            update_payload["columns"] = column_metadata

        client.update_dataset(dataset_id, **update_payload)
        return client.get_dataset(dataset_id)

    # Create new virtual dataset
    _logger.info("Creating denormalized dataset %s", denorm_name)
    kwargs: Dict[str, Any] = {
        "database": database["id"],
        "table_name": denorm_name,
        "sql": join_sql,
    }
    if schema:
        kwargs["schema"] = schema

    dataset = client.create_dataset(**kwargs)
    dataset_id = dataset["id"]

    # Update with metrics, description, and column metadata
    create_payload: Dict[str, Any] = {
        "description": osi_model.get("description", ""),
    }
    if superset_metrics:
        create_payload["metrics"] = superset_metrics
    if column_metadata:
        create_payload["columns"] = column_metadata

    if create_payload:
        client.update_dataset(dataset_id, **create_payload)

    return client.get_dataset(dataset_id)


def sync_osi(
    client: SupersetClient,
    osi_model: Dict[str, Any],
    database: Dict[str, Any],
    reload_columns: bool = True,
    merge_metadata: bool = False,
) -> Dict[str, Any]:
    """
    Main sync function that creates all datasets and metrics from OSI model.

    Args:
        client: SupersetClient instance
        osi_model: Parsed OSI model dictionary
        database: Database configuration from Superset
        reload_columns: If True (default), OSI metrics replace all existing metrics.
                       If False, existing Superset metrics are preserved.
        merge_metadata: If True, update existing metrics from OSI while preserving
                       Superset-only metrics. Only applies when reload_columns=False.

    Returns the denormalized dataset with all metrics.
    """
    # Detect target dialect
    sqlalchemy_uri = database.get("sqlalchemy_uri", "")
    target_dialect = detect_dialect(sqlalchemy_uri)
    if target_dialect:
        click.echo(f"Detected SQL dialect: {target_dialect}")
    else:
        click.echo("Using default SQL dialect")

    datasets = osi_model.get("datasets", [])
    metrics = osi_model.get("metrics", [])

    # Step 1: Create physical datasets (with single-dataset metrics)
    click.echo(f"Creating {len(datasets)} physical dataset(s)...")
    create_physical_datasets(
        client,
        datasets,
        database,
        osi_metrics=metrics,
        target_dialect=target_dialect,
        reload_columns=reload_columns,
        merge_metadata=merge_metadata,
    )

    # Step 2: Create denormalized virtual dataset with JOINs and all metrics
    click.echo("Creating denormalized virtual dataset...")
    denorm_dataset = get_or_create_denormalized_dataset(
        client,
        osi_model,
        database,
        target_dialect,
        reload_columns=reload_columns,
        merge_metadata=merge_metadata,
    )

    click.echo(
        f"Created denormalized dataset '{denorm_dataset['table_name']}' "
        f"with {len(metrics)} metric(s)",
    )

    return denorm_dataset
