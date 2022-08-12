"""
Tests for metrics.
"""

from typing import Dict

import pytest

from preset_cli.api.clients.dbt import MetricSchema
from preset_cli.cli.superset.sync.dbt.metrics import get_metric_expression


def test_get_metric_expression() -> None:
    """
    Tests for ``get_metric_expression``.
    """
    metric_schema = MetricSchema()
    metrics: Dict[str, MetricSchema] = {
        "one": metric_schema.load(
            {
                "type": "count",
                "sql": "user_id",
                "filters": [
                    {"field": "is_paying", "operator": "is", "value": "true"},
                    {"field": "lifetime_value", "operator": ">=", "value": "100"},
                    {"field": "company_name", "operator": "!=", "value": "'Acme, Inc'"},
                    {"field": "signup_date", "operator": ">=", "value": "'2020-01-01'"},
                ],
            },
        ),
        "two": metric_schema.load(
            {
                "type": "count_distinct",
                "sql": "user_id",
            },
        ),
        "three": metric_schema.load(
            {
                "type": "expression",
                "sql": "{{metric('one')}} - {{metric('two')}}",
            },
        ),
        "four": metric_schema.load(
            {
                "type": "hllsketch",
                "sql": "user_id",
            },
        ),
    }
    assert get_metric_expression("one", metrics) == (
        "COUNT(CASE WHEN is_paying is true AND lifetime_value >= 100 AND "
        "company_name != 'Acme, Inc' AND signup_date >= '2020-01-01' THEN user_id END)"
    )

    assert get_metric_expression("two", metrics) == "COUNT(DISTINCT user_id)"

    assert get_metric_expression("three", metrics) == (
        "COUNT(CASE WHEN is_paying is true AND lifetime_value >= 100 AND "
        "company_name != 'Acme, Inc' AND signup_date >= '2020-01-01' THEN user_id END) "
        "- COUNT(DISTINCT user_id)"
    )

    with pytest.raises(Exception) as excinfo:
        get_metric_expression("four", metrics)
    assert str(excinfo.value) == (
        "Unable to generate metric expression from: "
        "{'sql': 'user_id', 'type': 'hllsketch'}"
    )

    with pytest.raises(Exception) as excinfo:
        get_metric_expression("five", metrics)
    assert str(excinfo.value) == "Invalid metric five"
