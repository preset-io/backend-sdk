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
        "load_fill_by_weight": metric_schema.load(
            {
                "depends_on": [
                    "metric.breakthrough_dw.load_weight_lbs",
                    "metric.breakthrough_dw.load_weight_capacity_lbs",
                ],
                "description": "The Load Fill by Weight",
                "filters": [],
                "label": "Load Fill by Weight",
                "meta": {},
                "name": "load_fill_by_weight",
                "sql": "load_weight_lbs / load_weight_capacity_lbs",
                "type": "derived",
                "unique_id": "metric.breakthrough_dw.load_fill_by_weight",
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

    assert (
        get_metric_expression("load_fill_by_weight", metrics)
        == "load_weight_lbs / load_weight_capacity_lbs"
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


def test_get_metrics_derived_dbt_core() -> None:
    """
    Test derived metrics in dbt Core.
    """

    metrics = [
        {
            "name": "paying_customers",
            "resource_type": "metric",
            "package_name": "jaffle_shop",
            "path": "schema.yml",
            "original_file_path": "models/schema.yml",
            "unique_id": "metric.jaffle_shop.paying_customers",
            "fqn": ["jaffle_shop", "paying_customers"],
            "description": "",
            "label": "Customers who bought something",
            "calculation_method": "count",
            "expression": "customer_id",
            "filters": [{"field": "number_of_orders", "operator": ">", "value": "0"}],
            "time_grains": [],
            "dimensions": [],
            "timestamp": None,
            "window": None,
            "model": "ref('customers')",
            "model_unique_id": None,
            "meta": {},
            "tags": [],
            "config": {"enabled": True},
            "unrendered_config": {},
            "sources": [],
            "depends_on": ["model.jaffle_shop.customers"],
            "refs": [["customers"]],
            "metrics": [],
            "created_at": 1680229920.1190348,
        },
        {
            "name": "total_customers",
            "resource_type": "metric",
            "package_name": "jaffle_shop",
            "path": "schema.yml",
            "original_file_path": "models/schema.yml",
            "unique_id": "metric.jaffle_shop.total_customers",
            "fqn": ["jaffle_shop", "total_customers"],
            "description": "",
            "label": "Total customers",
            "calculation_method": "count",
            "expression": "customer_id",
            "filters": [],
            "time_grains": [],
            "dimensions": [],
            "timestamp": None,
            "window": None,
            "model": "ref('customers')",
            "model_unique_id": None,
            "meta": {},
            "tags": [],
            "config": {"enabled": True},
            "unrendered_config": {},
            "sources": [],
            "depends_on": ["model.jaffle_shop.customers"],
            "refs": [["customers"]],
            "metrics": [],
            "created_at": 1680229920.122923,
        },
        {
            "name": "ratio_of_paying_customers",
            "resource_type": "metric",
            "package_name": "jaffle_shop",
            "path": "schema.yml",
            "original_file_path": "models/schema.yml",
            "unique_id": "metric.jaffle_shop.ratio_of_paying_customers",
            "fqn": ["jaffle_shop", "ratio_of_paying_customers"],
            "description": "",
            "label": "Percentage of paying customers",
            "calculation_method": "derived",
            "expression": "paying_customers / total_customers",
            "filters": [],
            "time_grains": [],
            "dimensions": [],
            "timestamp": None,
            "window": None,
            "model": None,
            "model_unique_id": None,
            "meta": {},
            "tags": [],
            "config": {"enabled": True},
            "unrendered_config": {},
            "sources": [],
            "depends_on": [
                "metric.jaffle_shop.paying_customers",
                "metric.jaffle_shop.total_customers",
            ],
            "refs": [],
            "metrics": [["paying_customers"], ["total_customers"]],
            "created_at": 1680230520.212716,
        },
    ]
    model = {"unique_id": "model.jaffle_shop.customers"}
    assert get_metrics_for_model(model, metrics) == metrics  # type: ignore


def test_get_metrics_derived_dbt_cloud() -> None:
    """
    Test derived metrics in dbt Cloud.
    """
    metrics = [
        {
            "depends_on": ["model.jaffle_shop.customers"],
            "description": "The number of paid customers using the product",
            "filters": [{"field": "number_of_orders", "operator": "=", "value": "0"}],
            "label": "New customers",
            "meta": {},
            "name": "new_customers",
            "sql": "customer_id",
            "type": "count",
            "unique_id": "metric.jaffle_shop.new_customers",
        },
        {
            "depends_on": ["model.jaffle_shop.customers"],
            "description": "",
            "filters": [{"field": "number_of_orders", "operator": ">", "value": "0"}],
            "label": "Customers who bought something",
            "meta": {},
            "name": "paying_customers",
            "sql": "customer_id",
            "type": "count",
            "unique_id": "metric.jaffle_shop.paying_customers",
        },
        {
            "depends_on": [
                "metric.jaffle_shop.paying_customers",
                "metric.jaffle_shop.total_customers",
            ],
            "description": "",
            "filters": [],
            "label": "Percentage of paying customers",
            "meta": {},
            "name": "ratio_of_paying_customers",
            "sql": "paying_customers / total_customers",
            "type": "derived",
            "unique_id": "metric.jaffle_shop.ratio_of_paying_customers",
        },
        {
            "depends_on": ["model.jaffle_shop.customers"],
            "description": "",
            "filters": [],
            "label": "Total customers",
            "meta": {},
            "name": "total_customers",
            "sql": "customer_id",
            "type": "count",
            "unique_id": "metric.jaffle_shop.total_customers",
        },
    ]
    model = {"unique_id": "model.jaffle_shop.customers"}
    assert get_metrics_for_model(model, metrics) == metrics  # type: ignore
