"""
Metric conversion.

This module is used to convert dbt metrics into Superset metrics.
"""

# pylint: disable=consider-using-f-string

from typing import Any, Dict, List, TypedDict


def get_metric_expression(metric: Dict[str, Any]) -> str:
    """
    Return a SQL expression for a given dbt metric.
    """
    # required properties
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

    # for expressions we need to unroll the sub expressions first
    if type_ == "expression":
        raise NotImplementedError("Not yet available")

    raise Exception(f"Unable to generate metric expression from: {metric}")


class FilterType(TypedDict):
    """
    A type for filters.

    See https://docs.getdbt.com/docs/building-a-dbt-project/metrics#filters.
    """

    field: str
    operator: str
    value: str


def apply_filters(sql: str, filters: List[FilterType]) -> str:
    """
    Apply filters to SQL expression.
    """
    condition = " AND ".join(
        "{field} {operator} {value}".format(**filter_) for filter_ in filters
    )
    return f"CASE WHEN {condition} THEN {sql} END"
