"""
Tests for the DJ helpers.
"""

# pylint: disable=line-too-long

import json
from uuid import UUID

import pytest
from pytest_mock import MockerFixture
from yarl import URL

from preset_cli.cli.superset.sync.dj.lib import (
    get_database,
    get_or_create_dataset,
    sync_cube,
)


def test_sync_cube(mocker: MockerFixture) -> None:
    """
    Test the `sync_cube` function.
    """
    dj_client = mocker.MagicMock()
    dj_client._session.post().json.side_effect = [  # pylint: disable=protected-access
        {
            "data": {
                "findNodes": [
                    {
                        "name": "default.repair_orders_cube",
                        "current": {
                            "description": "Repair Orders Cube",
                            "displayName": "Repair Orders Cube",
                            "cubeMetrics": [
                                {
                                    "name": "default.avg_repair_price",
                                    "description": "Average repair price",
                                    "extractedMeasures": {
                                        "derivedExpression": "SUM(price_sum_252381cf) / SUM(price_count_252381cf)",
                                    },
                                },
                                {
                                    "name": "default.total_repair_cost",
                                    "description": "Total repair cost",
                                    "extractedMeasures": {
                                        "derivedExpression": "sum(price_sum_252381cf)",
                                    },
                                },
                                {
                                    "name": "default.total_repair_order_discounts",
                                    "description": "Total repair order discounts",
                                    "extractedMeasures": {
                                        "derivedExpression": "sum(price_discount_sum_94fc7ec3)",
                                    },
                                },
                            ],
                            "cubeDimensions": [
                                {"name": "default.dispatcher.company_name"},
                                {"name": "default.hard_hat.state"},
                            ],
                        },
                    },
                ],
            },
        },
        {
            "data": {
                "measuresSql": [
                    {
                        "sql": """WITH default_DOT_repair_order_details AS (
  SELECT
    default_DOT_repair_order_details.repair_order_id,
    default_DOT_repair_order_details.repair_type_id,
    default_DOT_repair_order_details.price,
    default_DOT_repair_order_details.quantity,
    default_DOT_repair_order_details.discount
  FROM roads.repair_order_details AS default_DOT_repair_order_details
), default_DOT_repair_order AS (
  SELECT
    default_DOT_repair_orders.repair_order_id,
    default_DOT_repair_orders.municipality_id,
    default_DOT_repair_orders.hard_hat_id,
    default_DOT_repair_orders.dispatcher_id
  FROM roads.repair_orders AS default_DOT_repair_orders
), default_DOT_dispatcher AS (
  SELECT
    default_DOT_dispatchers.dispatcher_id,
    default_DOT_dispatchers.company_name,
    default_DOT_dispatchers.phone
  FROM roads.dispatchers AS default_DOT_dispatchers
), default_DOT_hard_hat AS (
  SELECT
    default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.last_name,
    default_DOT_hard_hats.first_name,
    default_DOT_hard_hats.title,
    default_DOT_hard_hats.birth_date,
    default_DOT_hard_hats.hire_date,
    default_DOT_hard_hats.address,
    default_DOT_hard_hats.city,
    default_DOT_hard_hats.state,
    default_DOT_hard_hats.postal_code,
    default_DOT_hard_hats.country,
    default_DOT_hard_hats.manager,
    default_DOT_hard_hats.contractor_id
  FROM roads.hard_hats AS default_DOT_hard_hats
), default_DOT_repair_order_details_built AS (
  SELECT
    default_DOT_repair_order_details.price,
    default_DOT_repair_order_details.discount,
    default_DOT_dispatcher.company_name AS default_DOT_dispatcher_DOT_company_name,
    default_DOT_hard_hat.state AS default_DOT_hard_hat_DOT_state
  FROM default_DOT_repair_order_details
  LEFT JOIN default_DOT_repair_order
    ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
  LEFT JOIN default_DOT_dispatcher
    ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
  LEFT JOIN default_DOT_hard_hat
    ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
)
SELECT
  default_DOT_repair_order_details_built.default_DOT_dispatcher_DOT_company_name,
  default_DOT_repair_order_details_built.default_DOT_hard_hat_DOT_state,
  COUNT(price) AS price_count_252381cf,
  SUM(price) AS price_sum_252381cf,
  SUM(price * discount) AS price_discount_sum_94fc7ec3
FROM default_DOT_repair_order_details_built
GROUP BY
  default_DOT_repair_order_details_built.default_DOT_dispatcher_DOT_company_name,
  default_DOT_repair_order_details_built.default_DOT_hard_hat_DOT_state""",
                    },
                ],
            },
        },
    ]
    superset_client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.dj.lib.get_database",
        return_value={
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
            "allow_file_upload": False,
            "allow_multi_catalog": False,
            "allow_run_async": False,
            "allows_cost_estimate": False,
            "allows_subquery": True,
            "allows_virtual_table_explore": True,
            "backend": "duckdb",
            "changed_by": {"first_name": "Superset", "last_name": "Admin"},
            "changed_on": "2025-06-18T21:20:37.785659",
            "changed_on_delta_humanized": "an hour ago",
            "created_by": {"first_name": "Superset", "last_name": "Admin"},
            "database_name": "DuckDB",
            "disable_data_preview": False,
            "disable_drill_to_detail": False,
            "engine_information": {
                "disable_ssh_tunneling": False,
                "supports_dynamic_catalog": False,
                "supports_file_upload": True,
                "supports_oauth2": False,
            },
            "explore_database_id": 2,
            "expose_in_sqllab": True,
            "extra": '{"allows_virtual_table_explore":true}',
            "force_ctas_schema": None,
            "id": 2,
            "uuid": "a1ad7bd5-b1a3-4d64-afb1-a84c2f4d7715",
        },
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dj.lib.get_or_create_dataset",
        return_value={
            "data": {
                "always_filter_main_dttm": False,
                "cache_timeout": None,
                "catalog": None,
                "column_formats": {},
                "columns": [
                    {
                        "advanced_data_type": None,
                        "certification_details": None,
                        "certified_by": None,
                        "column_name": "default_DOT_dispatcher_DOT_company_name",
                        "description": None,
                        "expression": None,
                        "filterable": True,
                        "groupby": True,
                        "id": 779,
                        "is_certified": False,
                        "is_dttm": False,
                        "python_date_format": None,
                        "type": "STRING",
                        "type_generic": 1,
                        "uuid": "442043ef-96aa-4860-92e4-fd3767d2f237",
                        "verbose_name": None,
                        "warning_markdown": None,
                    },
                    {
                        "advanced_data_type": None,
                        "certification_details": None,
                        "certified_by": None,
                        "column_name": "default_DOT_hard_hat_DOT_state",
                        "description": None,
                        "expression": None,
                        "filterable": True,
                        "groupby": True,
                        "id": 780,
                        "is_certified": False,
                        "is_dttm": False,
                        "python_date_format": None,
                        "type": "STRING",
                        "type_generic": 1,
                        "uuid": "95e28283-f0f0-4047-bc7e-ceba94032d62",
                        "verbose_name": None,
                        "warning_markdown": None,
                    },
                    {
                        "advanced_data_type": None,
                        "certification_details": None,
                        "certified_by": None,
                        "column_name": "price_count_252381cf",
                        "description": None,
                        "expression": None,
                        "filterable": True,
                        "groupby": True,
                        "id": 781,
                        "is_certified": False,
                        "is_dttm": False,
                        "python_date_format": None,
                        "type": "NUMBER",
                        "type_generic": None,
                        "uuid": "312d8a53-60bb-472f-b17e-a4b375fc8835",
                        "verbose_name": None,
                        "warning_markdown": None,
                    },
                    {
                        "advanced_data_type": None,
                        "certification_details": None,
                        "certified_by": None,
                        "column_name": "price_sum_252381cf",
                        "description": None,
                        "expression": None,
                        "filterable": True,
                        "groupby": True,
                        "id": 782,
                        "is_certified": False,
                        "is_dttm": False,
                        "python_date_format": None,
                        "type": "NUMBER",
                        "type_generic": None,
                        "uuid": "3cff4dcc-fc3c-4b86-bcf9-de84d6073681",
                        "verbose_name": None,
                        "warning_markdown": None,
                    },
                    {
                        "advanced_data_type": None,
                        "certification_details": None,
                        "certified_by": None,
                        "column_name": "price_discount_sum_94fc7ec3",
                        "description": None,
                        "expression": None,
                        "filterable": True,
                        "groupby": True,
                        "id": 783,
                        "is_certified": False,
                        "is_dttm": False,
                        "python_date_format": None,
                        "type": "NUMBER",
                        "type_generic": None,
                        "uuid": "f6c7f69b-4005-49cf-af61-2c41f8fea173",
                        "verbose_name": None,
                        "warning_markdown": None,
                    },
                ],
                "database": {
                    "allow_multi_catalog": False,
                    "allows_cost_estimate": False,
                    "allows_subquery": True,
                    "allows_virtual_table_explore": True,
                    "backend": "duckdb",
                    "configuration_method": "sqlalchemy_form",
                    "disable_data_preview": False,
                    "disable_drill_to_detail": False,
                    "engine_information": {
                        "disable_ssh_tunneling": False,
                        "supports_dynamic_catalog": False,
                        "supports_file_upload": True,
                        "supports_oauth2": False,
                    },
                    "explore_database_id": 2,
                    "id": 2,
                    "name": "DuckDB",
                    "parameters": {
                        "access_token": "",
                        "database": "/app/default.duckdb",
                        "query": {},
                    },
                    "parameters_schema": {
                        "properties": {
                            "access_token": {
                                "default": "https://app.motherduck.com/token-request?appName=Superset&close=y",
                                "description": "MotherDuck token",
                                "nullable": True,
                                "type": "string",
                            },
                            "database": {
                                "description": "Database name",
                                "type": "string",
                            },
                            "query": {
                                "additionalProperties": {},
                                "description": "Additional parameters",
                                "type": "object",
                            },
                        },
                        "type": "object",
                    },
                    "schema_options": {},
                },
                "datasource_name": "default.repair_orders_cube",
                "default_endpoint": None,
                "description": None,
                "edit_url": "/tablemodelview/edit/28",
                "extra": None,
                "fetch_values_predicate": None,
                "filter_select": True,
                "filter_select_enabled": True,
                "folders": None,
                "granularity_sqla": [],
                "health_check_message": None,
                "id": 28,
                "is_sqllab_view": False,
                "main_dttm_col": None,
                "metrics": [
                    {
                        "certification_details": None,
                        "certified_by": None,
                        "currency": None,
                        "d3format": None,
                        "description": None,
                        "expression": "COUNT(*)",
                        "id": 76,
                        "is_certified": False,
                        "metric_name": "count",
                        "uuid": "a8f2a5da-1986-41b7-a846-e72d9747e959",
                        "verbose_name": "COUNT(*)",
                        "warning_markdown": None,
                        "warning_text": None,
                    },
                ],
                "name": "roads.default.repair_orders_cube",
                "normalize_columns": False,
                "offset": 0,
                "order_by_choices": [
                    [
                        '["default_DOT_dispatcher_DOT_company_name", true]',
                        "default_DOT_dispatcher_DOT_company_name [asc]",
                    ],
                    [
                        '["default_DOT_dispatcher_DOT_company_name", false]',
                        "default_DOT_dispatcher_DOT_company_name [desc]",
                    ],
                    [
                        '["default_DOT_hard_hat_DOT_state", true]',
                        "default_DOT_hard_hat_DOT_state [asc]",
                    ],
                    [
                        '["default_DOT_hard_hat_DOT_state", false]',
                        "default_DOT_hard_hat_DOT_state [desc]",
                    ],
                    ['["price_count_252381cf", true]', "price_count_252381cf [asc]"],
                    ['["price_count_252381cf", false]', "price_count_252381cf [desc]"],
                    [
                        '["price_discount_sum_94fc7ec3", true]',
                        "price_discount_sum_94fc7ec3 [asc]",
                    ],
                    [
                        '["price_discount_sum_94fc7ec3", false]',
                        "price_discount_sum_94fc7ec3 [desc]",
                    ],
                    ['["price_sum_252381cf", true]', "price_sum_252381cf [asc]"],
                    ['["price_sum_252381cf", false]', "price_sum_252381cf [desc]"],
                ],
                "owners": [
                    {
                        "first_name": "Superset",
                        "id": 1,
                        "last_name": "Admin",
                        "username": "admin",
                    },
                ],
                "params": None,
                "perm": "[DuckDB].[default.repair_orders_cube](id:28)",
                "schema": "roads",
                "select_star": 'SELECT\n  *\nFROM roads."default.repair_orders_cube"\nLIMIT 100',
                "sql": """WITH default_DOT_repair_order_details AS (
  SELECT
    default_DOT_repair_order_details.repair_order_id,
    default_DOT_repair_order_details.repair_type_id,
    default_DOT_repair_order_details.price,
    default_DOT_repair_order_details.quantity,
    default_DOT_repair_order_details.discount
  FROM roads.repair_order_details AS default_DOT_repair_order_details
), default_DOT_repair_order AS (
  SELECT
    default_DOT_repair_orders.repair_order_id,
    default_DOT_repair_orders.municipality_id,
    default_DOT_repair_orders.hard_hat_id,
    default_DOT_repair_orders.dispatcher_id
  FROM roads.repair_orders AS default_DOT_repair_orders
), default_DOT_dispatcher AS (
  SELECT
    default_DOT_dispatchers.dispatcher_id,
    default_DOT_dispatchers.company_name,
    default_DOT_dispatchers.phone
  FROM roads.dispatchers AS default_DOT_dispatchers
), default_DOT_hard_hat AS (
  SELECT
    default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.last_name,
    default_DOT_hard_hats.first_name,
    default_DOT_hard_hats.title,
    default_DOT_hard_hats.birth_date,
    default_DOT_hard_hats.hire_date,
    default_DOT_hard_hats.address,
    default_DOT_hard_hats.city,
    default_DOT_hard_hats.state,
    default_DOT_hard_hats.postal_code,
    default_DOT_hard_hats.country,
    default_DOT_hard_hats.manager,
    default_DOT_hard_hats.contractor_id
  FROM roads.hard_hats AS default_DOT_hard_hats
), default_DOT_repair_order_details_built AS (
  SELECT
    default_DOT_repair_order_details.price,
    default_DOT_repair_order_details.discount,
    default_DOT_dispatcher.company_name AS default_DOT_dispatcher_DOT_company_name,
    default_DOT_hard_hat.state AS default_DOT_hard_hat_DOT_state
  FROM default_DOT_repair_order_details
  LEFT JOIN default_DOT_repair_order
    ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
  LEFT JOIN default_DOT_dispatcher
    ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
  LEFT JOIN default_DOT_hard_hat
    ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
)
SELECT
  default_DOT_repair_order_details_built.default_DOT_dispatcher_DOT_company_name,
  default_DOT_repair_order_details_built.default_DOT_hard_hat_DOT_state,
  COUNT(price) AS price_count_252381cf,
  SUM(price) AS price_sum_252381cf,
  SUM(price * discount) AS price_discount_sum_94fc7ec3
FROM default_DOT_repair_order_details_built
GROUP BY
  default_DOT_repair_order_details_built.default_DOT_dispatcher_DOT_company_name,
  default_DOT_repair_order_details_built.default_DOT_hard_hat_DOT_state""",
                "table_name": "default.repair_orders_cube",
                "template_params": None,
                "time_grain_sqla": [
                    ["PT1S", "Second"],
                    ["PT1M", "Minute"],
                    ["PT1H", "Hour"],
                    ["P1D", "Day"],
                    ["P1W", "Week"],
                    ["P1M", "Month"],
                    ["P3M", "Quarter"],
                    ["P1Y", "Year"],
                ],
                "type": "table",
                "uid": "28__table",
                "verbose_map": {
                    "__timestamp": "Time",
                    "count": "COUNT(*)",
                    "default_DOT_dispatcher_DOT_company_name": "default_DOT_dispatcher_DOT_company_name",
                    "default_DOT_hard_hat_DOT_state": "default_DOT_hard_hat_DOT_state",
                    "price_count_252381cf": "price_count_252381cf",
                    "price_discount_sum_94fc7ec3": "price_discount_sum_94fc7ec3",
                    "price_sum_252381cf": "price_sum_252381cf",
                },
            },
            "id": 28,
            "result": {
                "always_filter_main_dttm": False,
                "catalog": None,
                "database": 2,
                "normalize_columns": False,
                "schema": "roads",
                "sql": """WITH default_DOT_repair_order_details AS (
  SELECT
    default_DOT_repair_order_details.repair_order_id,
    default_DOT_repair_order_details.repair_type_id,
    default_DOT_repair_order_details.price,
    default_DOT_repair_order_details.quantity,
    default_DOT_repair_order_details.discount
  FROM roads.repair_order_details AS default_DOT_repair_order_details
), default_DOT_repair_order AS (
  SELECT
    default_DOT_repair_orders.repair_order_id,
    default_DOT_repair_orders.municipality_id,
    default_DOT_repair_orders.hard_hat_id,
    default_DOT_repair_orders.dispatcher_id
  FROM roads.repair_orders AS default_DOT_repair_orders
), default_DOT_dispatcher AS (
  SELECT
    default_DOT_dispatchers.dispatcher_id,
    default_DOT_dispatchers.company_name,
    default_DOT_dispatchers.phone
  FROM roads.dispatchers AS default_DOT_dispatchers
), default_DOT_hard_hat AS (
  SELECT
    default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.last_name,
    default_DOT_hard_hats.first_name,
    default_DOT_hard_hats.title,
    default_DOT_hard_hats.birth_date,
    default_DOT_hard_hats.hire_date,
    default_DOT_hard_hats.address,
    default_DOT_hard_hats.city,
    default_DOT_hard_hats.state,
    default_DOT_hard_hats.postal_code,
    default_DOT_hard_hats.country,
    default_DOT_hard_hats.manager,
    default_DOT_hard_hats.contractor_id
  FROM roads.hard_hats AS default_DOT_hard_hats
), default_DOT_repair_order_details_built AS (
  SELECT
    default_DOT_repair_order_details.price,
    default_DOT_repair_order_details.discount,
    default_DOT_dispatcher.company_name AS default_DOT_dispatcher_DOT_company_name,
    default_DOT_hard_hat.state AS default_DOT_hard_hat_DOT_state
  FROM default_DOT_repair_order_details
  LEFT JOIN default_DOT_repair_order
    ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
  LEFT JOIN default_DOT_dispatcher
    ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
  LEFT JOIN default_DOT_hard_hat
    ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
)
SELECT
  default_DOT_repair_order_details_built.default_DOT_dispatcher_DOT_company_name,
  default_DOT_repair_order_details_built.default_DOT_hard_hat_DOT_state,
  COUNT(price) AS price_count_252381cf,
  SUM(price) AS price_sum_252381cf,
  SUM(price * discount) AS price_discount_sum_94fc7ec3
FROM default_DOT_repair_order_details_built
GROUP BY
  default_DOT_repair_order_details_built.default_DOT_dispatcher_DOT_company_name,
  default_DOT_repair_order_details_built.default_DOT_hard_hat_DOT_state""",
                "table_name": "default.repair_orders_cube",
            },
        },
    )

    sync_cube(
        UUID("a1ad7bd5-b1a3-4d64-afb1-a84c2f4d7715"),
        "schema",
        dj_client,
        superset_client,
        "test_cube",
        URL("https://dj.example.org/"),
    )

    superset_client.update_dataset.assert_has_calls(
        [
            mocker.call(28, override_columns=True, metrics=[]),
            mocker.call(
                28,
                override_columns=False,
                metrics=[
                    {
                        "metric_name": "default.avg_repair_price",
                        "expression": "SUM(price_sum_252381cf) / SUM(price_count_252381cf)",
                        "description": "Average repair price",
                    },
                    {
                        "metric_name": "default.total_repair_cost",
                        "expression": "sum(price_sum_252381cf)",
                        "description": "Total repair cost",
                    },
                    {
                        "metric_name": "default.total_repair_order_discounts",
                        "expression": "sum(price_discount_sum_94fc7ec3)",
                        "description": "Total repair order discounts",
                    },
                ],
                description="Repair Orders Cube",
                is_managed_externally=True,
                external_url=URL("https://dj.example.org/nodes/test_cube"),
                extra=json.dumps(
                    {
                        "certification": {
                            "certified_by": "DJ",
                            "details": "This table is created by DJ.",
                        },
                    },
                ),
                sql="""WITH default_DOT_repair_order_details AS (
  SELECT
    default_DOT_repair_order_details.repair_order_id,
    default_DOT_repair_order_details.repair_type_id,
    default_DOT_repair_order_details.price,
    default_DOT_repair_order_details.quantity,
    default_DOT_repair_order_details.discount
  FROM roads.repair_order_details AS default_DOT_repair_order_details
), default_DOT_repair_order AS (
  SELECT
    default_DOT_repair_orders.repair_order_id,
    default_DOT_repair_orders.municipality_id,
    default_DOT_repair_orders.hard_hat_id,
    default_DOT_repair_orders.dispatcher_id
  FROM roads.repair_orders AS default_DOT_repair_orders
), default_DOT_dispatcher AS (
  SELECT
    default_DOT_dispatchers.dispatcher_id,
    default_DOT_dispatchers.company_name,
    default_DOT_dispatchers.phone
  FROM roads.dispatchers AS default_DOT_dispatchers
), default_DOT_hard_hat AS (
  SELECT
    default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.last_name,
    default_DOT_hard_hats.first_name,
    default_DOT_hard_hats.title,
    default_DOT_hard_hats.birth_date,
    default_DOT_hard_hats.hire_date,
    default_DOT_hard_hats.address,
    default_DOT_hard_hats.city,
    default_DOT_hard_hats.state,
    default_DOT_hard_hats.postal_code,
    default_DOT_hard_hats.country,
    default_DOT_hard_hats.manager,
    default_DOT_hard_hats.contractor_id
  FROM roads.hard_hats AS default_DOT_hard_hats
), default_DOT_repair_order_details_built AS (
  SELECT
    default_DOT_repair_order_details.price,
    default_DOT_repair_order_details.discount,
    default_DOT_dispatcher.company_name AS default_DOT_dispatcher_DOT_company_name,
    default_DOT_hard_hat.state AS default_DOT_hard_hat_DOT_state
  FROM default_DOT_repair_order_details
  LEFT JOIN default_DOT_repair_order
    ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
  LEFT JOIN default_DOT_dispatcher
    ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
  LEFT JOIN default_DOT_hard_hat
    ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
)
SELECT
  default_DOT_repair_order_details_built.default_DOT_dispatcher_DOT_company_name,
  default_DOT_repair_order_details_built.default_DOT_hard_hat_DOT_state,
  COUNT(price) AS price_count_252381cf,
  SUM(price) AS price_sum_252381cf,
  SUM(price * discount) AS price_discount_sum_94fc7ec3
FROM default_DOT_repair_order_details_built
GROUP BY
  default_DOT_repair_order_details_built.default_DOT_dispatcher_DOT_company_name,
  default_DOT_repair_order_details_built.default_DOT_hard_hat_DOT_state""",
            ),
        ],
    )


def test_get_database(mocker: MockerFixture) -> None:
    """
    Test the `get_database` function.
    """
    superset_client = mocker.MagicMock()
    superset_client.get_databases.return_value = [{"id": 1, "name": "TestDB"}]

    assert get_database(
        superset_client,
        UUID("a1ad7bd5-b1a3-4d64-afb1-a84c2f4d7715"),
    ) == {"id": 1, "name": "TestDB"}


def test_get_database_not_found(mocker: MockerFixture) -> None:
    """
    Test the `get_database` function.
    """
    superset_client = mocker.MagicMock()
    superset_client.get_databases.return_value = []

    with pytest.raises(ValueError) as excinfo:
        get_database(superset_client, UUID("a1ad7bd5-b1a3-4d64-afb1-a84c2f4d7715"))
    assert (
        str(excinfo.value)
        == "Database with UUID a1ad7bd5-b1a3-4d64-afb1-a84c2f4d7715 not found in Superset."
    )


def test_get_or_create_dataset_existing(mocker: MockerFixture) -> None:
    """
    Test the `get_or_create_dataset` function when the dataset exists.
    """
    superset_client = mocker.MagicMock()
    superset_client.get_datasets.return_value = [{"id": 42}]

    get_or_create_dataset(
        superset_client,
        {"id": 1, "name": "TestDB"},
        "schema",
        "test_cube",
        "SELECT * FROM t",
    )

    superset_client.get_dataset.assert_called_once_with(42)


def test_get_or_create_dataset_new(mocker: MockerFixture) -> None:
    """
    Test the `get_or_create_dataset` function for creating a new dataset.
    """
    superset_client = mocker.MagicMock()
    superset_client.get_datasets.return_value = []

    get_or_create_dataset(
        superset_client,
        {"id": 1, "name": "TestDB"},
        "schema",
        "test_cube",
        "SELECT * FROM t",
    )

    superset_client.create_dataset.assert_called_once_with(
        database=1,
        catalog=None,
        schema="schema",
        table_name="test_cube",
        sql="SELECT * FROM t",
    )
