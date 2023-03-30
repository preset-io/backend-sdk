"""
Tests for metrics.
"""

from typing import Dict

import pytest
from pytest_mock import MockerFixture

from preset_cli.api.clients.dbt import MetricSchema
from preset_cli.cli.superset.sync.dbt.metrics import (
    get_metric_expression,
    get_metrics_for_model,
)


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
                "sql": "one - two",
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


def test_get_metric_expression_new_schema() -> None:
    """
    Test ``get_metric_expression`` with the dbt 1.3 schema.

    See https://docs.getdbt.com/guides/migration/versions/upgrading-to-v1.3#for-users-of-dbt-metrics
    """
    metric_schema = MetricSchema()
    metrics: Dict[str, MetricSchema] = {
        "one": metric_schema.load(
            {
                "calculation_method": "count",
                "expression": "user_id",
                "filters": [
                    {"field": "is_paying", "operator": "is", "value": "true"},
                    {"field": "lifetime_value", "operator": ">=", "value": "100"},
                    {"field": "company_name", "operator": "!=", "value": "'Acme, Inc'"},
                    {"field": "signup_date", "operator": ">=", "value": "'2020-01-01'"},
                ],
            },
        ),
    }
    assert get_metric_expression("one", metrics) == (
        "COUNT(CASE WHEN is_paying is true AND lifetime_value >= 100 AND "
        "company_name != 'Acme, Inc' AND signup_date >= '2020-01-01' THEN user_id END)"
    )


def test_get_metrics_for_model(mocker: MockerFixture) -> None:
    """
    Test ``get_metrics_for_model``.
    """
    _logger = mocker.patch("preset_cli.cli.superset.sync.dbt.metrics._logger")

    metrics = [
        {
            "unique_id": "metric.superset.a",
            "depends_on": ["model.superset.table"],
            "name": "a",
        },
        {
            "unique_id": "metric.superset.b",
            "depends_on": ["model.superset.table"],
            "name": "b",
        },
        {
            "unique_id": "metric.superset.c",
            "depends_on": ["model.superset.other_table"],
            "name": "c",
        },
        {
            "unique_id": "metric.superset.d",
            "depends_on": ["metric.superset.a", "metric.superset.b"],
            "name": "d",
            "calculation_method": "derived",
        },
        {
            "unique_id": "metric.superset.e",
            "depends_on": ["metric.superset.a", "metric.superset.c"],
            "name": "e",
            "calculation_method": "derived",
        },
    ]

    model = {"unique_id": "model.superset.table"}
    assert get_metrics_for_model(model, metrics) == [  # type: ignore
        {
            "unique_id": "metric.superset.a",
            "depends_on": ["model.superset.table"],
            "name": "a",
        },
        {
            "unique_id": "metric.superset.b",
            "depends_on": ["model.superset.table"],
            "name": "b",
        },
        {
            "unique_id": "metric.superset.d",
            "depends_on": ["metric.superset.a", "metric.superset.b"],
            "name": "d",
            "calculation_method": "derived",
        },
    ]
    _logger.warning.assert_called_with(
        "Metric %s cannot be calculated because it depends on multiple models: %s",
        "e",
        "model.superset.other_table, model.superset.table",
    )

    model = {"unique_id": "model.superset.other_table"}
    assert get_metrics_for_model(model, metrics) == [  # type: ignore
        {
            "unique_id": "metric.superset.c",
            "depends_on": ["model.superset.other_table"],
            "name": "c",
        },
    ]
