"""
Metric conversion.

This module is used to convert dbt metrics into Superset metrics.
"""

# pylint: disable=consider-using-f-string

import json
import logging
from collections import defaultdict
from typing import Dict, List, Set

import sqlparse
from sqlparse.sql import Identifier, TokenList

from preset_cli.api.clients.dbt import FilterSchema, MetricSchema, ModelSchema
from preset_cli.api.clients.superset import SupersetMetricDefinition

_logger = logging.getLogger(__name__)


def get_metric_expression(unique_id: str, metrics: Dict[str, MetricSchema]) -> str:
    """
    Return a SQL expression for a given dbt metric.
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
        statement = sqlparse.parse(sql)[0]
        tokens = statement.tokens[:]
        while tokens:
            token = tokens.pop(0)

            if isinstance(token, Identifier) and token.value in metrics:
                parent_sql = get_metric_expression(token.value, metrics)
                token.tokens = sqlparse.parse(parent_sql)[0].tokens
            elif isinstance(token, TokenList):
                tokens.extend(token.tokens)

        return str(statement)

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
    metrics: List[MetricSchema],
) -> Dict[str, List[SupersetMetricDefinition]]:
    """
    Build a dictionary of Superset metrics for each dbt model.
    """
    superset_metrics = defaultdict(list)
    for metric in metrics:
        metric_models = get_metric_models(metric["unique_id"], metrics)
        if len(metric_models) != 1:
            _logger.warning(
                "Metric %s cannot be calculated because it depends on multiple models: %s",
                metric["name"],
                ", ".join(sorted(metric_models)),
            )
            continue

        metric_definition = get_metric_definition(
            metric["unique_id"],
            metrics,
        )
        model = metric_models.pop()
        superset_metrics[model].append(metric_definition)

    return superset_metrics
