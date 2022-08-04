"""
Metric conversion.

This module is used to convert dbt metrics into Superset metrics.
"""

# pylint: disable=consider-using-f-string

from functools import partial
from typing import Any, Dict, List, TypedDict

from jinja2 import Template


class FilterType(TypedDict):
    """
    A type for filters.

    See https://docs.getdbt.com/docs/building-a-dbt-project/metrics#filters.
    """

    field: str
    operator: str
    value: str


class MetricType(TypedDict, total=False):
    """
    A type for a metric.

    See https://docs.getdbt.com/docs/building-a-dbt-project/metrics#available-properties.
    """

    # required
    name: str
    model: str
    type: str
    sql: str
    timestamp: str
    time_grains: List[str]

    # optional
    label: str
    description: str
    dimensions: List[str]
    filters: List[FilterType]
    meta: Dict[str, Any]


def get_metric_expression(metric_name: str, metrics: Dict[str, MetricType]) -> str:
    """
    Return a SQL expression for a given dbt metric.
    """
    if metric_name not in metrics:
        raise Exception(f"Invalid metric {metric_name}")

    metric = metrics[metric_name]
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

    if type_ == "expression":
        template = Template(sql)
        return template.render(metric=partial(get_metric_expression, metrics=metrics))

    raise Exception(f"Unable to generate metric expression from: {metric}")


def apply_filters(sql: str, filters: List[FilterType]) -> str:
    """
    Apply filters to SQL expression.
    """
    condition = " AND ".join(
        "{field} {operator} {value}".format(**filter_) for filter_ in filters
    )
    return f"CASE WHEN {condition} THEN {sql} END"
