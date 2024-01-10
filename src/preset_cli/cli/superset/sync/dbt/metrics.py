"""
Metric conversion.

This module is used to convert dbt metrics into Superset metrics.
"""

# pylint: disable=consider-using-f-string

import json
import logging
from collections import defaultdict
from typing import Dict, List, Optional, Set

import sqlglot
from sqlglot import Expression, exp, parse_one
from sqlglot.expressions import Alias, Case, Identifier, If, Join, Select, Table, Where
from sqlglot.optimizer import traverse_scope

from preset_cli.api.clients.dbt import (
    FilterSchema,
    MetricSchema,
    MFMetricWithSQLSchema,
    MFSQLEngine,
    ModelSchema,
)
from preset_cli.api.clients.superset import SupersetMetricDefinition
from preset_cli.cli.superset.sync.dbt.exposures import ModelKey

_logger = logging.getLogger(__name__)

# dbt => sqlglot
DIALECT_MAP = {
    MFSQLEngine.BIGQUERY: "bigquery",
    MFSQLEngine.DUCKDB: "duckdb",
    MFSQLEngine.REDSHIFT: "redshift",
    MFSQLEngine.POSTGRES: "postgres",
    MFSQLEngine.SNOWFLAKE: "snowflake",
    MFSQLEngine.DATABRICKS: "databricks",
}


def get_metric_expression(unique_id: str, metrics: Dict[str, MetricSchema]) -> str:
    """
    Return a SQL expression for a given dbt metric using sqlglot.
    """
    if unique_id not in metrics:
        raise Exception(f"Invalid metric {unique_id}")

    metric = metrics[unique_id]
    if "calculation_method" in metric:
        # dbt >= 1.3
        type_ = metric["calculation_method"]
        sql = metric["expression"]
    else:
        # dbt < 1.3
        type_ = metric["type"]
        sql = metric["sql"]

    if metric.get("filters"):
        sql = apply_filters(sql, metric["filters"])

    simple_mappings = {
        "count": "COUNT",
        "sum": "SUM",
        "average": "AVG",
        "min": "MIN",
        "max": "MAX",
    }

    if type_ in simple_mappings:
        function = simple_mappings[type_]
        return f"{function}({sql})"

    if type_ == "count_distinct":
        return f"COUNT(DISTINCT {sql})"

    if type_ in {"expression", "derived"}:
        expression = sqlglot.parse_one(sql)
        tokens = expression.find_all(exp.Column)

        for token in tokens:
            if token.sql() in metrics:
                parent_sql = get_metric_expression(token.sql(), metrics)
                parent_expression = sqlglot.parse_one(parent_sql)
                token.replace(parent_expression)

        return expression.sql()

    sorted_metric = dict(sorted(metric.items()))
    raise Exception(f"Unable to generate metric expression from: {sorted_metric}")


def apply_filters(sql: str, filters: List[FilterSchema]) -> str:
    """
    Apply filters to SQL expression.
    """
    condition = " AND ".join(
        "{field} {operator} {value}".format(**filter_) for filter_ in filters
    )
    return f"CASE WHEN {condition} THEN {sql} END"


def is_derived(metric: MetricSchema) -> bool:
    """
    Return if the metric is derived.
    """
    return (
        metric.get("calculation_method") == "derived"  # dbt >= 1.3
        or metric.get("type") == "expression"  # dbt < 1.3
        or metric.get("type") == "derived"  # WTF dbt Cloud
    )


def get_metrics_for_model(
    model: ModelSchema,
    metrics: List[MetricSchema],
) -> List[MetricSchema]:
    """
    Given a list of metrics, return those that are based on a given model.
    """
    metric_map = {metric["unique_id"]: metric for metric in metrics}
    related_metrics = []

    for metric in metrics:
        parents = set()
        queue = [metric]
        while queue:
            node = queue.pop()
            depends_on = node["depends_on"]
            if is_derived(node):
                queue.extend(metric_map[parent] for parent in depends_on)
            else:
                parents.update(depends_on)

        if len(parents) > 1:
            _logger.warning(
                "Metric %s cannot be calculated because it depends on multiple models: %s",
                metric["name"],
                ", ".join(sorted(parents)),
            )
            continue

        if parents == {model["unique_id"]}:
            related_metrics.append(metric)

    return related_metrics


def get_metric_models(unique_id: str, metrics: List[MetricSchema]) -> Set[str]:
    """
    Given a metric, return the models it depends on.
    """
    metric_map = {metric["unique_id"]: metric for metric in metrics}
    metric = metric_map[unique_id]
    depends_on = metric["depends_on"]

    if is_derived(metric):
        return {
            model
            for parent in depends_on
            for model in get_metric_models(parent, metrics)
        }

    return set(depends_on)


def get_metric_definition(
    unique_id: str,
    metrics: List[MetricSchema],
) -> SupersetMetricDefinition:
    """
    Build a Superset metric definition from an OG (< 1.6) dbt metric.
    """
    metric_map = {metric["unique_id"]: metric for metric in metrics}
    metric = metric_map[unique_id]
    name = metric["name"]
    meta = metric.get("meta", {})
    kwargs = meta.pop("superset", {})

    return {
        "expression": get_metric_expression(unique_id, metric_map),
        "metric_name": name,
        "metric_type": (metric.get("type") or metric.get("calculation_method")),
        "verbose_name": metric.get("label", name),
        "description": metric.get("description", ""),
        "extra": json.dumps(meta),
        **kwargs,  # type: ignore
    }


def get_superset_metrics_per_model(
    og_metrics: List[MetricSchema],
    sl_metrics: Optional[List[MFMetricWithSQLSchema]] = None,
) -> Dict[str, List[SupersetMetricDefinition]]:
    """
    Build a dictionary of Superset metrics for each dbt model.
    """
    superset_metrics = defaultdict(list)
    for metric in og_metrics:
        metric_models = get_metric_models(metric["unique_id"], og_metrics)
        if len(metric_models) != 1:
            _logger.warning(
                "Metric %s cannot be calculated because it depends on multiple models: %s",
                metric["name"],
                ", ".join(sorted(metric_models)),
            )
            continue

        metric_definition = get_metric_definition(
            metric["unique_id"],
            og_metrics,
        )
        model = metric_models.pop()
        superset_metrics[model].append(metric_definition)

    for sl_metric in sl_metrics or []:
        metric_definition = convert_metric_flow_to_superset(
            sl_metric["name"],
            sl_metric["description"],
            sl_metric["type"],
            sl_metric["sql"],
            sl_metric["dialect"],
        )
        model = sl_metric["model"]
        superset_metrics[model].append(metric_definition)

    return superset_metrics


def extract_aliases(parsed_query: Expression) -> Dict[str, str]:
    """
    Extract column aliases from a SQL query.
    """
    aliases = {}
    for expression in parsed_query.find_all(Alias):
        alias_name = expression.alias
        expression_text = expression.this.sql()
        aliases[alias_name] = expression_text

    return aliases


def convert_query_to_projection(sql: str, dialect: MFSQLEngine) -> str:
    """
    Convert a MetricFlow compiled SQL to a projection.
    """
    parsed_query = parse_one(sql, dialect=DIALECT_MAP.get(dialect))

    # extract aliases from inner query
    scopes = traverse_scope(parsed_query)
    has_subquery = len(scopes) > 1
    aliases = extract_aliases(scopes[0].expression) if has_subquery else {}

    # find the metric expression
    select_expression = parsed_query.find(Select)
    if select_expression.find(Join):
        raise ValueError("Unable to convert metrics with JOINs")

    projection = select_expression.args["expressions"]
    if len(projection) > 1:
        raise ValueError("Unable to convert metrics with multiple selected expressions")

    metric_expression = (
        projection[0].this if isinstance(projection[0], Alias) else projection[0]
    )

    # replace aliases with their original expressions
    for node, _, _ in metric_expression.walk():
        if isinstance(node, Identifier) and node.sql() in aliases:
            node.replace(parse_one(aliases[node.sql()]))

    # convert WHERE predicate to a CASE statement
    where_expression = parsed_query.find(Where)
    if where_expression:
        for node, _, _ in where_expression.walk():
            if isinstance(node, Identifier) and node.sql() in aliases:
                node.replace(parse_one(aliases[node.sql()]))

        case_expression = Case(
            ifs=[If(this=where_expression.this, true=metric_expression.this)],
        )
        metric_expression.set("this", case_expression)

    return metric_expression.sql()


def convert_metric_flow_to_superset(
    name: str,
    description: str,
    metric_type: str,
    sql: str,
    dialect: MFSQLEngine,
) -> SupersetMetricDefinition:
    """
    Convert a MetricFlow metric to a Superset metric.

    Before MetricFlow we could build the metrics based on the metadata returned by the
    GraphQL API. With MetricFlow we only have access to the compiled SQL used to
    compute the metric, so we need to parse it and build a single projection for
    Superset.

    For example, this:

        SELECT
            SUM(order_count) AS large_order
        FROM (
            SELECT
                order_total AS order_id__order_total_dim
                , 1 AS order_count
            FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_106
        ) subq_796
        WHERE order_id__order_total_dim >= 20

    Becomes:

        SUM(CASE WHEN order_total > 20 THEN 1 END)

    """
    return {
        "expression": convert_query_to_projection(sql, dialect),
        "metric_name": name,
        "metric_type": metric_type,
        "verbose_name": name,
        "description": description,
    }


class MultipleModelsError(Exception):
    """
    Raised when a metric depends on multiple models.
    """


def get_model_from_sql(
    sql: str,
    dialect: MFSQLEngine,
    model_map: Dict[ModelKey, ModelSchema],
) -> ModelSchema:
    """
    Return the model associated with a SQL query.
    """
    parsed_query = parse_one(sql, dialect=DIALECT_MAP.get(dialect))
    sources = list(parsed_query.find_all(Table))
    if len(sources) > 1:
        raise MultipleModelsError(
            f"Unable to convert metrics with multiple sources: {sql}",
        )

    table = sources[0]
    key = ModelKey(table.db, table.name)

    return model_map[key]
