"""
Tests for metrics.
"""

import pytest

from preset_cli.cli.superset.sync.dbt.metrics import get_metric_expression


def test_get_metric_expression() -> None:
    """
    Tests for ``get_metric_expression``.
    """
    metric = {
        "type": "count",
        "sql": "user_id",
        "filters": [
            {"field": "is_paying", "operator": "is", "value": "true"},
            {"field": "lifetime_value", "operator": ">=", "value": "100"},
            {"field": "company_name", "operator": "!=", "value": "'Acme, Inc'"},
            {"field": "signup_date", "operator": ">=", "value": "'2020-01-01'"},
        ],
    }
    assert get_metric_expression(metric) == (
        "COUNT(CASE WHEN is_paying is true AND lifetime_value >= 100 AND "
        "company_name != 'Acme, Inc' AND signup_date >= '2020-01-01' THEN user_id END)"
    )

    metric = {
        "type": "count_distinct",
        "sql": "user_id",
    }
    assert get_metric_expression(metric) == "COUNT(DISTINCT user_id)"

    metric = {
        "type": "expression",
        "sql": "{{metric('base_sum_metric')}} - {{metric('base_average_metric')}}",
    }
    with pytest.raises(NotImplementedError) as excinfo:
        get_metric_expression(metric)
    assert str(excinfo.value) == "Not yet available"

    metric = {
        "type": "hllsketch",
        "sql": "user_id",
    }
    with pytest.raises(Exception) as excinfo:
        get_metric_expression(metric)
    assert str(excinfo.value) == (
        "Unable to generate metric expression from: "
        "{'type': 'hllsketch', 'sql': 'user_id'}"
    )
