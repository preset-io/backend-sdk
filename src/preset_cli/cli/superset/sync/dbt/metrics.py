"""
Metric conversion.

This module is used to convert dbt metrics into Superset metrics.
"""

# pylint: disable=consider-using-f-string

import json
import logging
import re
from collections import defaultdict
from typing import Dict, List, Optional, Set

import sqlglot
from sqlglot import Expression, ParseError, exp, parse_one
from sqlglot.expressions import (
    Alias,
    Case,
    Distinct,
    Identifier,
    If,
    Join,
    Select,
    Table,
    Where,
)
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


# pylint: disable=too-many-locals
def get_metric_expression(metric_name: str, metrics: Dict[str, MetricSchema]) -> str:
    """
    Return a SQL expression for a given dbt metric using sqlglot.
    """
    if metric_name not in metrics:
        raise Exception(f"Invalid metric {metric_name}")

    metric = metrics[metric_name]
    if "calculation_method" in metric:
        # dbt >= 1.3
        type_ = metric["calculation_method"]
        sql = metric["expression"]
    elif "sql" in metric:
        # dbt < 1.3
        type_ = metric["type"]
        sql = metric["sql"]
    else:
        raise Exception(f"Unable to generate metric expression from: {metric}")

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
        if metric.get("skip_parsing"):
            return sql.strip()

        try:
            expression = sqlglot.parse_one(sql, dialect=metric["dialect"])
            tokens = expression.find_all(exp.Column)

            for token in tokens:
                if token.sql() in metrics:
                    parent_sql = get_metric_expression(token.sql(), metrics)
                    parent_expression = sqlglot.parse_one(
                        parent_sql,
                        dialect=metric["dialect"],
                    )
                    token.replace(parent_expression)

            return expression.sql(dialect=metric["dialect"])
        except ParseError:
            sql = replace_metric_syntax(sql, metric["depends_on"], metrics)
            return sql

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
    metric_name: str,
    metrics: List[MetricSchema],
) -> SupersetMetricDefinition:
    """
    Build a Superset metric definition from an OG (< 1.6) dbt metric.
    """
    metric_map = {metric["name"]: metric for metric in metrics}
    metric = metric_map[metric_name]
    meta = metric.get("meta", {})
    kwargs = meta.pop("superset", {})

    return {
        "expression": get_metric_expression(metric_name, metric_map),
        "metric_name": metric_name,
        "metric_type": (metric.get("type") or metric.get("calculation_method")),
        "verbose_name": metric.get("label", metric_name),
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
        # dbt supports creating derived metrics with raw syntax. In case the metric doesn't
        # rely on other metrics (or rely on other metrics that aren't associated with any
        # model), it's required to specify the dataset the metric should be associated with
        # under the ``meta.superset.model`` key. If the derived metric is just an expression
        # with no dependency, it's not required to parse the metric SQL.
        if model := metric.get("meta", {}).get("superset", {}).pop("model", None):
            if len(metric["depends_on"]) == 0:
                metric["skip_parsing"] = True
        else:
            metric_models = get_metric_models(metric["unique_id"], og_metrics)
            if len(metric_models) == 0:
                _logger.warning(
                    "Metric %s cannot be calculated because it's not associated with any model."
                    " Please specify the model under metric.meta.superset.model.",
                    metric["name"],
                )
                continue

            if len(metric_models) != 1:
                _logger.warning(
                    "Metric %s cannot be calculated because it depends on multiple models: %s",
                    metric["name"],
                    ", ".join(sorted(metric_models)),
                )
                continue
            model = metric_models.pop()

        metric_definition = get_metric_definition(
            metric["name"],
            og_metrics,
        )
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

        # Remove DISTINCT from metric to avoid conficting with CASE
        distinct = False
        for node, _, _ in metric_expression.this.walk():
            if isinstance(node, Distinct):
                distinct = True
                node.replace(node.expressions[0])

        for node, _, _ in where_expression.walk():
            if isinstance(node, Identifier) and node.sql() in aliases:
                node.replace(parse_one(aliases[node.sql()]))

        case_expression = Case(
            ifs=[If(this=where_expression.this, true=metric_expression.this)],
        )

        if distinct:
            case_expression = Distinct(expressions=[case_expression])

        metric_expression.set("this", case_expression)

    return metric_expression.sql(dialect=DIALECT_MAP.get(dialect))


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


def get_models_from_sql(
    sql: str,
    dialect: MFSQLEngine,
    model_map: Dict[ModelKey, ModelSchema],
) -> Optional[List[ModelSchema]]:
    """
    Return the model associated with a SQL query.
    """
    parsed_query = parse_one(sql, dialect=DIALECT_MAP.get(dialect))
    sources = list(parsed_query.find_all(Table))

    for table in sources:
        if ModelKey(table.db, table.name) not in model_map:
            return None

    return [model_map[ModelKey(table.db, table.name)] for table in sources]


def replace_metric_syntax(
    sql: str,
    dependencies: List[str],
    metrics: Dict[str, MetricSchema],
) -> str:
    """
    Replace metric keys with their SQL syntax.
    This method is a fallback in case ``sqlglot`` raises a ``ParseError``.
    """
    for parent_metric in dependencies:
        parent_metric_name = parent_metric.split(".")[-1]
        pattern = r"\b" + re.escape(parent_metric_name) + r"\b"
        parent_metric_syntax = get_metric_expression(
            parent_metric_name,
            metrics,
        )
        sql = re.sub(pattern, parent_metric_syntax, sql)

    return sql.strip()
