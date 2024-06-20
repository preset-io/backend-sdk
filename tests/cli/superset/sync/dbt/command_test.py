"""
Tests for the dbt import command.
"""
# pylint: disable=invalid-name, too-many-lines, line-too-long

import os
from pathlib import Path
from subprocess import CalledProcessError

import pytest
import yaml
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.api.clients.dbt import MFSQLEngine
from preset_cli.cli.superset.main import superset_cli
from preset_cli.cli.superset.sync.dbt.command import (
    get_account_id,
    get_job,
    get_project_id,
)
from preset_cli.exceptions import CLIError, DatabaseNotFoundError

dirname, _ = os.path.split(os.path.abspath(__file__))
with open(os.path.join(dirname, "manifest.json"), encoding="utf-8") as fp:
    manifest_contents = fp.read()
with open(os.path.join(dirname, "manifest-metricflow.json"), encoding="utf-8") as fp:
    manifest_metricflow_contents = fp.read()


profiles_contents = yaml.dump(
    {
        "default": {
            "outputs": {
                "dev": {
                    "type": "bigquery",
                },
            },
        },
        "my_project": {
            "outputs": {
                "dev": {
                    "type": "bigquery",
                },
            },
        },
        "athena_project": {
            "outputs": {
                "dev": {
                    "type": "athena",
                },
            },
        },
    },
)

dbt_core_models = [
    {
        "database": "examples_dev",
        "columns": [],
        "meta": {},
        "description": "",
        "name": "messages_channels",
        "tags": [],
        "schema": "public",
        "unique_id": "model.superset_examples.messages_channels",
        "created_at": 1642628933.004452,
        "children": ["metric.superset_examples.cnt"],
        "depends_on": {
            "macros": [],
            "nodes": [
                "source.superset_examples.public.channels",
                "source.superset_examples.public.messages",
            ],
        },
        "unrendered_config": {"materialized": "view"},
        "resource_type": "model",
        "path": "slack/messages_channels.sql",
        "extra_ctes": [],
        "package_name": "superset_examples",
        "alias": "messages_channels",
        "relation_name": '"examples_dev"."public"."messages_channels"',
        "config": {
            "enabled": True,
            "alias": None,
            "schema": None,
            "database": None,
            "tags": [],
            "meta": {},
            "materialized": "view",
            "persist_docs": {},
            "quoting": {},
            "column_types": {},
            "full_refresh": None,
            "on_schema_change": "ignore",
            "post-hook": [],
            "pre-hook": [],
        },
        "patch_path": None,
        "compiled_sql": (
            "SELECT messages.ts, channels.name, messages.text "
            'FROM "examples_dev"."public"."messages" messages '
            'JOIN "examples_dev"."public"."channels" channels '
            "ON messages.channel_id = channels.id"
        ),
        "extra_ctes_injected": True,
        "deferred": False,
        "root_path": "/Users/beto/Projects/dbt-examples/superset_examples",
        "original_file_path": "models/slack/messages_channels.sql",
        "refs": [],
        "fqn": ["superset_examples", "slack", "messages_channels"],
        "raw_sql": (
            "SELECT messages.ts, channels.name, messages.text "
            "FROM {{ source ('public', 'messages') }} messages "
            "JOIN {{ source ('public', 'channels') }} channels "
            "ON messages.channel_id = channels.id"
        ),
        "build_path": None,
        "sources": [["public", "channels"], ["public", "messages"]],
        "checksum": {
            "name": "sha256",
            "checksum": "b4ce232b28280daa522b37e12c36b67911e2a98456b8a3b99440075ec5564609",
        },
        "docs": {"show": True},
        "compiled_path": "target/compiled/superset_examples/models/slack/messages_channels.sql",
        "compiled": True,
    },
]

dbt_core_metrics = [
    {
        "label": "",
        "sql": "*",
        "depends_on": ["model.superset_examples.messages_channels"],
        "meta": {},
        "description": "",
        "name": "cnt",
        "type": "count",
        "filters": [],
        "unique_id": "metric.superset_examples.cnt",
        "created_at": 1642630986.1942852,
        "package_name": "superset_examples",
        "sources": [],
        "root_path": "/Users/beto/Projects/dbt-examples/superset_examples",
        "path": "slack/schema.yml",
        "resource_type": "metric",
        "original_file_path": "models/slack/schema.yml",
        "model": "ref('messages_channels')",
        "timestamp": None,
        "fqn": ["superset_examples", "slack", "cnt"],
        "time_grains": [],
        "tags": [],
        "refs": [["messages_channels"]],
        "dimensions": [],
    },
]

superset_metrics = {
    "model.superset_examples.messages_channels": [
        {
            "description": "",
            "expression": "COUNT(*)",
            "extra": "{}",
            "metric_name": "cnt",
            "metric_type": "count",
            "verbose_name": "",
        },
    ],
}

dbt_cloud_models = [
    {
        "database": "examples_dev",
        "description": "",
        "meta": {},
        "name": "messages_channels",
        "schema": "public",
        "unique_id": "model.superset_examples.messages_channels",
    },
    {
        "database": "some_other_table",
        "description": "",
        "meta": {},
        "name": "some_other_table",
        "schema": "public",
        "unique_id": "model.superset_examples.some_other_table",
    },
]

dbt_cloud_metrics = [
    {
        "depends_on": ["model.superset_examples.messages_channels"],
        "description": "",
        "filters": [],
        "label": "",
        "meta": {},
        "name": "cnt",
        "sql": "*",
        "type": "count",
        "unique_id": "metric.superset_examples.cnt",
    },
    {
        "depends_on": ["a", "b"],
        "description": "",
        "filters": [],
        "label": "",
        "meta": {},
        "name": "multiple parents",
        "sql": "*",
        "type": "count",
        "unique_id": "c",
    },
]

dbt_metricflow_metrics = [
    {"name": "a", "type": "Simple", "description": "The simplest metric"},
    {"name": "b", "type": "derived", "description": "Too complex for Superset"},
    {"name": "c", "type": "derived", "description": "Multiple models"},
]


dbt_metricflow_models = [
    {
        "unique_id": "model.jaffle_shop.stg_products",
        "description": "Product (food and drink items that can be ordered) data with basic cleaning and transformation applied, one row per product.",
        "tags": [],
        "columns": [
            {
                "name": "product_id",
                "description": "The unique key for each product.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
        ],
        "schema": "dbt_beto",
        "meta": {},
        "database": "dbt-tutorial-347100",
        "config": {
            "enabled": True,
            "alias": None,
            "schema": None,
            "database": None,
            "tags": [],
            "meta": {},
            "group": None,
            "materialized": "view",
            "incremental_strategy": None,
            "persist_docs": {},
            "quoting": {},
            "column_types": {},
            "full_refresh": None,
            "unique_key": None,
            "on_schema_change": "ignore",
            "on_configuration_change": "apply",
            "grants": {},
            "packages": [],
            "docs": {"show": True, "node_color": None},
            "contract": {"enforced": False},
            "post-hook": [],
            "pre-hook": [],
        },
        "name": "stg_products",
        "deferred": False,
        "contract": {"enforced": False, "checksum": None},
        "metrics": [],
        "docs": {"show": True, "node_color": None},
        "package_name": "jaffle_shop",
        "raw_code": "with\n\nsource as (\n\n    select * from {{ source('ecom', 'raw_products') }}\n\n),\n\nrenamed as (\n\n    select\n\n        ----------  ids\n        sku as product_id,\n\n        ---------- text\n        name as product_name,\n        type as product_type,\n        description as product_description,\n\n\n        ---------- numerics\n        (price / 100.0) as product_price,\n\n        ---------- booleans\n        case\n            when type = 'jaffle' then 1\n            else 0\n        end as is_food_item,\n\n        case\n            when type = 'beverage' then 1\n            else 0\n        end as is_drink_item\n\n    from source\n\n)\n\nselect * from renamed",
        "access": "protected",
        "original_file_path": "models/staging/stg_products.sql",
        "group": None,
        "patch_path": "jaffle_shop://models/staging/stg_products.yml",
        "build_path": None,
        "extra_ctes": [],
        "fqn": ["jaffle_shop", "staging", "stg_products"],
        "language": "sql",
        "extra_ctes_injected": True,
        "checksum": {
            "name": "sha256",
            "checksum": "76a701136875b4f04bcd365446a2dc3e3c433136a2d30b2d9ede55298c84404e",
        },
        "path": "staging/stg_products.sql",
        "unrendered_config": {"materialized": "view"},
        "deprecation_date": None,
        "resource_type": "model",
        "relation_name": "`dbt-tutorial-347100`.`dbt_beto`.`stg_products`",
        "compiled_path": "target/compiled/jaffle_shop/models/staging/stg_products.sql",
        "children": [
            "model.jaffle_shop.order_items",
            "semantic_model.jaffle_shop.stg_products",
            "test.jaffle_shop.not_null_stg_products_product_id.6373b0acf3",
            "test.jaffle_shop.unique_stg_products_product_id.7d950a1467",
        ],
        "refs": [],
        "latest_version": None,
        "sources": [["ecom", "raw_products"]],
        "alias": "stg_products",
        "version": None,
        "compiled": True,
        "created_at": 1702514525.401679,
        "compiled_code": "with\n\nsource as (\n\n    select * from `dbt-tutorial-347100`.`dbt_beto`.`raw_products`\n\n),\n\nrenamed as (\n\n    select\n\n        ----------  ids\n        sku as product_id,\n\n        ---------- text\n        name as product_name,\n        type as product_type,\n        description as product_description,\n\n\n        ---------- numerics\n        (price / 100.0) as product_price,\n\n        ---------- booleans\n        case\n            when type = 'jaffle' then 1\n            else 0\n        end as is_food_item,\n\n        case\n            when type = 'beverage' then 1\n            else 0\n        end as is_drink_item\n\n    from source\n\n)\n\nselect * from renamed",
        "constraints": [],
        "depends_on": {"macros": [], "nodes": ["source.jaffle_shop.ecom.raw_products"]},
    },
    {
        "unique_id": "model.jaffle_shop.stg_customers",
        "description": "Customer data with basic cleaning and transformation applied, one row per customer.",
        "tags": [],
        "columns": [
            {
                "name": "customer_id",
                "description": "The unique key for each customer.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
        ],
        "schema": "dbt_beto",
        "meta": {},
        "database": "dbt-tutorial-347100",
        "config": {
            "enabled": True,
            "alias": None,
            "schema": None,
            "database": None,
            "tags": [],
            "meta": {},
            "group": None,
            "materialized": "view",
            "incremental_strategy": None,
            "persist_docs": {},
            "quoting": {},
            "column_types": {},
            "full_refresh": None,
            "unique_key": None,
            "on_schema_change": "ignore",
            "on_configuration_change": "apply",
            "grants": {},
            "packages": [],
            "docs": {"show": True, "node_color": None},
            "contract": {"enforced": False},
            "post-hook": [],
            "pre-hook": [],
        },
        "name": "stg_customers",
        "deferred": False,
        "contract": {"enforced": False, "checksum": None},
        "metrics": [],
        "docs": {"show": True, "node_color": None},
        "package_name": "jaffle_shop",
        "raw_code": "with\n\nsource as (\n\n    select * from {{ source('ecom', 'raw_customers') }}\n\n),\n\nrenamed as (\n\n    select\n\n        ----------  ids\n        id as customer_id,\n\n        ---------- text\n        name as customer_name\n\n    from source\n\n)\n\nselect * from renamed",
        "access": "protected",
        "original_file_path": "models/staging/stg_customers.sql",
        "group": None,
        "patch_path": "jaffle_shop://models/staging/stg_customers.yml",
        "build_path": None,
        "extra_ctes": [],
        "fqn": ["jaffle_shop", "staging", "stg_customers"],
        "language": "sql",
        "extra_ctes_injected": True,
        "checksum": {
            "name": "sha256",
            "checksum": "37b269b48f94b4526ee48b7123397b9a2f457266e97bf5b876b988cbce9eeef6",
        },
        "path": "staging/stg_customers.sql",
        "unrendered_config": {"materialized": "view"},
        "deprecation_date": None,
        "resource_type": "model",
        "relation_name": "`dbt-tutorial-347100`.`dbt_beto`.`stg_customers`",
        "compiled_path": "target/compiled/jaffle_shop/models/staging/stg_customers.sql",
        "children": [
            "model.jaffle_shop.customers",
            "test.jaffle_shop.not_null_stg_customers_customer_id.e2cfb1f9aa",
            "test.jaffle_shop.relationships_orders_customer_id__customer_id__ref_stg_customers_.918495ce16",
            "test.jaffle_shop.unique_stg_customers_customer_id.c7614daada",
        ],
        "refs": [],
        "latest_version": None,
        "sources": [["ecom", "raw_customers"]],
        "alias": "stg_customers",
        "version": None,
        "compiled": True,
        "created_at": 1702514525.416814,
        "compiled_code": "with\n\nsource as (\n\n    select * from `dbt-tutorial-347100`.`dbt_beto`.`raw_customers`\n\n),\n\nrenamed as (\n\n    select\n\n        ----------  ids\n        id as customer_id,\n\n        ---------- text\n        name as customer_name\n\n    from source\n\n)\n\nselect * from renamed",
        "constraints": [],
        "depends_on": {
            "macros": [],
            "nodes": ["source.jaffle_shop.ecom.raw_customers"],
        },
    },
    {
        "unique_id": "model.jaffle_shop.stg_supplies",
        "description": "List of our supply expenses data with basic cleaning and transformation applied.\nOne row per supply cost, not per supply. As supply costs fluctuate they receive a new row with a new UUID. Thus there can be multiple rows per supply_id.\n",
        "tags": [],
        "columns": [
            {
                "name": "supply_uuid",
                "description": "The unique key of our supplies per cost.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
        ],
        "schema": "dbt_beto",
        "meta": {},
        "database": "dbt-tutorial-347100",
        "config": {
            "enabled": True,
            "alias": None,
            "schema": None,
            "database": None,
            "tags": [],
            "meta": {},
            "group": None,
            "materialized": "view",
            "incremental_strategy": None,
            "persist_docs": {},
            "quoting": {},
            "column_types": {},
            "full_refresh": None,
            "unique_key": None,
            "on_schema_change": "ignore",
            "on_configuration_change": "apply",
            "grants": {},
            "packages": [],
            "docs": {"show": True, "node_color": None},
            "contract": {"enforced": False},
            "post-hook": [],
            "pre-hook": [],
        },
        "name": "stg_supplies",
        "deferred": False,
        "contract": {"enforced": False, "checksum": None},
        "metrics": [],
        "docs": {"show": True, "node_color": None},
        "package_name": "jaffle_shop",
        "raw_code": "with\n\nsource as (\n\n    select * from {{ source('ecom', 'raw_supplies') }}\n\n),\n\nrenamed as (\n\n    select\n\n        ----------  ids\n        {{ dbt_utils.generate_surrogate_key(['id', 'sku']) }} as supply_uuid,\n        id as supply_id,\n        sku as product_id,\n\n        ---------- text\n        name as supply_name,\n\n---------- numerics\n        (cost / 100.0) as supply_cost,\n\n        ---------- booleans\n        perishable as is_perishable_supply\n\n    from source\n\n)\n\nselect * from renamed",
        "access": "protected",
        "original_file_path": "models/staging/stg_supplies.sql",
        "group": None,
        "patch_path": "jaffle_shop://models/staging/stg_supplies.yml",
        "build_path": None,
        "extra_ctes": [],
        "fqn": ["jaffle_shop", "staging", "stg_supplies"],
        "language": "sql",
        "extra_ctes_injected": True,
        "checksum": {
            "name": "sha256",
            "checksum": "f3e61efc6f6c2d522c3c9385d9914eae23ff7bf129c6fb6f11b2dbdecef6f50f",
        },
        "path": "staging/stg_supplies.sql",
        "unrendered_config": {"materialized": "view"},
        "deprecation_date": None,
        "resource_type": "model",
        "relation_name": "`dbt-tutorial-347100`.`dbt_beto`.`stg_supplies`",
        "compiled_path": "target/compiled/jaffle_shop/models/staging/stg_supplies.sql",
        "children": [
            "model.jaffle_shop.order_items",
            "test.jaffle_shop.not_null_stg_supplies_supply_uuid.515c6eda6d",
            "test.jaffle_shop.unique_stg_supplies_supply_uuid.c9e3edcfed",
        ],
        "refs": [],
        "latest_version": None,
        "sources": [["ecom", "raw_supplies"]],
        "alias": "stg_supplies",
        "version": None,
        "compiled": True,
        "created_at": 1702514525.3776808,
        "compiled_code": "with\n\nsource as (\n\n    select * from `dbt-tutorial-347100`.`dbt_beto`.`raw_supplies`\n\n),\n\nrenamed as (\n\n    select\n\n        ----------  ids\n        \n    \nto_hex(md5(cast(coalesce(cast(id as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(sku as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) as supply_uuid,\n        id as supply_id,\n        sku as product_id,\n\n        ---------- text\n        name as supply_name,\n\n        ---------- numerics\n        (cost / 100.0) as supply_cost,\n\n        ---------- booleans\n        perishable as is_perishable_supply\n\n    from source\n\n)\n\nselect * from renamed",
        "constraints": [],
        "depends_on": {
            "macros": ["macro.dbt_utils.generate_surrogate_key"],
            "nodes": ["source.jaffle_shop.ecom.raw_supplies"],
        },
    },
    {
        "unique_id": "model.jaffle_shop.stg_orders",
        "description": "Order data with basic cleaning and transformation applied, one row per order.",
        "tags": [],
        "columns": [
            {
                "name": "order_id",
                "description": "The unique key for each order.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
        ],
        "schema": "dbt_beto",
        "meta": {},
        "database": "dbt-tutorial-347100",
        "config": {
            "enabled": True,
            "alias": None,
            "schema": None,
            "database": None,
            "tags": [],
            "meta": {},
            "group": None,
            "materialized": "view",
            "incremental_strategy": None,
            "persist_docs": {},
            "quoting": {},
            "column_types": {},
            "full_refresh": None,
            "unique_key": None,
            "on_schema_change": "ignore",
            "on_configuration_change": "apply",
            "grants": {},
            "packages": [],
            "docs": {"show": True, "node_color": None},
            "contract": {"enforced": False},
            "post-hook": [],
            "pre-hook": [],
        },
        "name": "stg_orders",
        "deferred": False,
        "contract": {"enforced": False, "checksum": None},
        "metrics": [],
        "docs": {"show": True, "node_color": None},
        "package_name": "jaffle_shop",
        "raw_code": "with\n\nsource as (\n\n    select * from {{ source('ecom', 'raw_orders') }}\n    -- if you generate a larger dataset, you can limit the timespan to the current time with the following line\n    -- where ordered_at <= {{ var('truncate_timespan_to') }}\n\n),\n\nrenamed as (\n\n    select\n\n        ----------  ids\n        id as order_id,\n        store_id as location_id,\n        customer as customer_id,\n\n        ---------- numerics\n        (order_total / 100.0) as order_total,\n(tax_paid / 100.0) as tax_paid,\n\n        ---------- timestamps\n        {{dbt.date_trunc('day','ordered_at')}} as ordered_at\n\n    from source\n\n)\n\nselect * from renamed",
        "access": "protected",
        "original_file_path": "models/staging/stg_orders.sql",
        "group": None,
        "patch_path": "jaffle_shop://models/staging/stg_orders.yml",
        "build_path": None,
        "extra_ctes": [],
        "fqn": ["jaffle_shop", "staging", "stg_orders"],
        "language": "sql",
        "extra_ctes_injected": True,
        "checksum": {
            "name": "sha256",
            "checksum": "64e9b156a536566cf07b1f90d7650a6d3af1c25eabfadefad04ba5533b82a83d",
        },
        "path": "staging/stg_orders.sql",
        "unrendered_config": {"materialized": "view"},
        "deprecation_date": None,
        "resource_type": "model",
        "relation_name": "`dbt-tutorial-347100`.`dbt_beto`.`stg_orders`",
        "compiled_path": "target/compiled/jaffle_shop/models/staging/stg_orders.sql",
        "children": [
            "model.jaffle_shop.order_items",
            "model.jaffle_shop.orders",
            "test.jaffle_shop.not_null_stg_orders_order_id.81cfe2fe64",
            "test.jaffle_shop.unique_stg_orders_order_id.e3b841c71a",
        ],
        "refs": [],
        "latest_version": None,
        "sources": [["ecom", "raw_orders"]],
        "alias": "stg_orders",
        "version": None,
        "compiled": True,
        "created_at": 1702514525.396977,
        "compiled_code": "with\n\nsource as (\n\n    select * from `dbt-tutorial-347100`.`dbt_beto`.`raw_orders`\n    -- if you generate a larger dataset, you can limit the timespan to the current time with the following line\n    -- where ordered_at <= current_timestamp()\n\n),\n\nrenamed as (\n\n    select\n\n        ---------ids\n        id as order_id,\n        store_id as location_id,\n        customer as customer_id,\n\n        ---------- numerics\n        (order_total / 100.0) as order_total,\n        (tax_paid / 100.0) as tax_paid,\n\n        ---------- timestamps\n        timestamp_trunc(\n        cast(ordered_at as timestamp),\n        day\n    ) as ordered_at\n\n    from source\n\n)\n\nselect * from renamed",
        "constraints": [],
        "depends_on": {
            "macros": ["macro.dbt.current_timestamp", "macro.dbt.date_trunc"],
            "nodes": ["source.jaffle_shop.ecom.raw_orders"],
        },
    },
    {
        "unique_id": "model.jaffle_shop.stg_order_items",
        "description": "Individual food and drink items that make up our orders, one row per item.",
        "tags": [],
        "columns": [
            {
                "name": "order_item_id",
                "description": "The unique key for each order item.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
        ],
        "schema": "dbt_beto",
        "meta": {},
        "database": "dbt-tutorial-347100",
        "config": {
            "enabled": True,
            "alias": None,
            "schema": None,
            "database": None,
            "tags": [],
            "meta": {},
            "group": None,
            "materialized": "view",
            "incremental_strategy": None,
            "persist_docs": {},
            "quoting": {},
            "column_types": {},
            "full_refresh": None,
            "unique_key": None,
            "on_schema_change": "ignore",
            "on_configuration_change": "apply",
            "grants": {},
            "packages": [],
            "docs": {"show": True, "node_color": None},
            "contract": {"enforced": False},
            "post-hook": [],
            "pre-hook": [],
        },
        "name": "stg_order_items",
        "deferred": False,
        "contract": {"enforced": False, "checksum": None},
        "metrics": [],
        "docs": {"show": True, "node_color": None},
        "package_name": "jaffle_shop",
        "raw_code": "with\n\nsource as (\n\n    select * from {{ source('ecom', 'raw_items') }}\n\n),\n\nrenamed as (\n\n    select\n\n        ----------  ids\n        id as order_item_id,\n        order_id,\n        sku as product_id\n\n    from source\n\n)\n\nselect * from renamed",
        "access": "protected",
        "original_file_path": "models/staging/stg_order_items.sql",
        "group": None,
        "patch_path": "jaffle_shop://models/staging/stg_order_items.yml",
        "build_path": None,
        "extra_ctes": [],
        "fqn": ["jaffle_shop", "staging", "stg_order_items"],
        "language": "sql",
        "extra_ctes_injected": True,
        "checksum": {
            "name": "sha256",
            "checksum": "c4551967544f92a36cf257eccdb2c2806343ce82e1c6d96c59adbd9263539ce2",
        },
        "path": "staging/stg_order_items.sql",
        "unrendered_config": {"materialized": "view"},
        "deprecation_date": None,
        "resource_type": "model",
        "relation_name": "`dbt-tutorial-347100`.`dbt_beto`.`stg_order_items`",
        "compiled_path": "target/compiled/jaffle_shop/models/staging/stg_order_items.sql",
        "children": [
            "model.jaffle_shop.order_items",
            "test.jaffle_shop.not_null_stg_order_items_order_item_id.26a7e2bc35",
            "test.jaffle_shop.unique_stg_order_items_order_item_id.90e333a108",
        ],
        "refs": [],
        "latest_version": None,
        "sources": [["ecom", "raw_items"]],
        "alias": "stg_order_items",
        "version": None,
        "compiled": True,
        "created_at": 1702514525.436424,
        "compiled_code": "with\n\nsource as (\n\n    select * from `dbt-tutorial-347100`.`dbt_beto`.`raw_items`\n\n),\n\nrenamed as (\n\n    select\n\n        ----------  ids\n        id as order_item_id,\n        order_id,\n        sku as product_id\n\n    from source\n\n)\n\nselect * from renamed",
        "constraints": [],
        "depends_on": {"macros": [], "nodes": ["source.jaffle_shop.ecom.raw_items"]},
    },
    {
        "unique_id": "model.jaffle_shop.stg_locations",
        "description": "List of open locations with basic cleaning and transformation applied, one row per location.",
        "tags": [],
        "columns": [
            {
                "name": "location_id",
                "description": "The unique key for each location.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
        ],
        "schema": "dbt_beto",
        "meta": {},
        "database": "dbt-tutorial-347100",
        "config": {
            "enabled": True,
            "alias": None,
            "schema": None,
            "database": None,
            "tags": [],
            "meta": {},
            "group": None,
            "materialized": "view",
            "incremental_strategy": None,
            "persist_docs": {},
            "quoting": {},
            "column_types": {},
            "full_refresh": None,
            "unique_key": None,
            "on_schema_change": "ignore",
            "on_configuration_change": "apply",
            "grants": {},
            "packages": [],
            "docs": {"show": True, "node_color": None},
            "contract": {"enforced": False},
            "post-hook": [],
            "pre-hook": [],
        },
        "name": "stg_locations",
        "deferred": False,
        "contract": {"enforced": False, "checksum": None},
        "metrics": [],
        "docs": {"show": True, "node_color": None},
        "package_name": "jaffle_shop",
        "raw_code": "with\n\nsource as (\n\n    select * from {{ source('ecom', 'raw_stores') }}\n\n    -- if you generate a larger dataset, you can limit the timespan to the current time with the following line\n    -- where ordered_at <= {{ var('truncate_timespan_to') }}\n),\n\nrenamed as (\n\n    select\n\n        ----------  ids\n        id as location_id,\n\n        ---------- text\n        name as location_name,\n\n        ---------- numerics\n        tax_rate,\n\n        ---------- timestamps\n        {{dbt.date_trunc('day', 'opened_at')}} as opened_at\n\n    from source\n\n)\n\nselect * from renamed",
        "access": "protected",
        "original_file_path": "models/staging/stg_locations.sql",
        "group": None,
        "patch_path": "jaffle_shop://models/staging/stg_locations.yml",
        "build_path": None,
        "extra_ctes": [],
        "fqn": ["jaffle_shop", "staging", "stg_locations"],
        "language": "sql",
        "extra_ctes_injected": True,
        "checksum": {
            "name": "sha256",
            "checksum": "9e3a52c057340df711ad32e83c567143175fd1fc54f2d5c75ff4fbc198a597e3",
        },
        "path": "staging/stg_locations.sql",
        "unrendered_config": {"materialized": "view"},
        "deprecation_date": None,
        "resource_type": "model",
        "relation_name": "`dbt-tutorial-347100`.`dbt_beto`.`stg_locations`",
        "compiled_path": "target/compiled/jaffle_shop/models/staging/stg_locations.sql",
        "children": [
            "semantic_model.jaffle_shop.locations",
            "test.jaffle_shop.not_null_stg_locations_location_id.3d237927d2",
            "test.jaffle_shop.unique_stg_locations_location_id.2e2fc58ecc",
        ],
        "refs": [],
        "latest_version": None,
        "sources": [["ecom", "raw_stores"]],
        "alias": "stg_locations",
        "version": None,
        "compiled": True,
        "created_at": 1702514525.44064,
        "compiled_code": "with\n\nsource as (\n\n    select * from `dbt-tutorial-347100`.`dbt_beto`.`raw_stores`\n\n    -- if you generate a larger dataset, you can limit the timespan to the current time with the following line\n    -- where ordered_at <= current_timestamp()\n),\n\nrenamed as (\n\n    select\n\n        ----------  ids\n        id as location_id,\n\n        ---------- text\n        name as location_name,\n\n        ---------- numerics\n        tax_rate,\n\n        ---------- timestamps\n        timestamp_trunc(\n        cast(opened_at as timestamp),\n        day\n    ) as opened_at\n\n    from source\n\n)\n\nselect * from renamed",
        "constraints": [],
        "depends_on": {
            "macros": ["macro.dbt.current_timestamp", "macro.dbt.date_trunc"],
            "nodes": ["source.jaffle_shop.ecom.raw_stores"],
        },
    },
    {
        "unique_id": "model.jaffle_shop.customers",
        "description": "Customer overview data mart, offering key details for each unique customer. One row per customer.",
        "tags": [],
        "columns": [
            {
                "name": "customer_id",
                "description": "The unique key of the orders mart.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "customer_name",
                "description": "Customers' full name.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "count_lifetime_orders",
                "description": "Total number of orders a customer has ever placed.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "first_ordered_at",
                "description": "The timestamp when a customer placed their first order.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "last_ordered_at",
                "description": "The timestamp of a customer's most recent order.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "lifetime_spend_pretax",
                "description": "The sum of all the pre-tax subtotals of every order a customer has placed.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "lifetime_spend",
                "description": "The sum of all the order totals (including tax) that a customer has ever placed.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "customer_type",
                "description": "Options are 'new' or 'returning', indicating if a customer has ordered more than once or has only placed their first order to date.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
        ],
        "schema": "dbt_beto",
        "meta": {},
        "database": "dbt-tutorial-347100",
        "config": {
            "enabled": True,
            "alias": None,
            "schema": None,
            "database": None,
            "tags": [],
            "meta": {},
            "group": None,
            "materialized": "table",
            "incremental_strategy": None,
            "persist_docs": {},
            "quoting": {},
            "column_types": {},
            "full_refresh": None,
            "unique_key": None,
            "on_schema_change": "ignore",
            "on_configuration_change": "apply",
            "grants": {},
            "packages": [],
            "docs": {"show": True, "node_color": None},
            "contract": {"enforced": False},
            "post-hook": [],
            "pre-hook": [],
        },
        "name": "customers",
        "deferred": False,
        "contract": {"enforced": False, "checksum": None},
        "metrics": [],
        "docs": {"show": True, "node_color": None},
        "package_name": "jaffle_shop",
        "raw_code": "with\n\ncustomers as (\n\n    select * from {{ ref('stg_customers') }}\n\n),\n\norders_table as (\n\n    select * from {{ ref('orders') }}\n\n),\n\norder_items_table as (\n\n    select * from {{ ref('order_items') }}\n),\n\norder_summary as (\n\n    select\n        customer_id,\n\n        count(distinct orders.order_id) as count_lifetime_orders,\n        count(distinct orders.order_id) > 1 as is_repeat_buyer,\n        min(orders.ordered_at) as first_ordered_at,\n        max(orders.ordered_at) as last_ordered_at,\n        sum(order_items.product_price) as lifetime_spend_pretax,\n        sum(orders.order_total) as lifetime_spend\n\n    from orders_table as orders\n    \n    left join order_items_table as order_items on orders.order_id = order_items.order_id\n    \n    group by 1\n\n),\n\njoined as (\n\n    select\n        customers.*,\n        order_summary.count_lifetime_orders,\n        order_summary.first_ordered_at,\n        order_summary.last_ordered_at,\n        order_summary.lifetime_spend_pretax,\n        order_summary.lifetime_spend,\n\n        case\n            when order_summary.is_repeat_buyer then 'returning'\n            else 'new'\n        end as customer_type\n\n    from customers\n\n    left join order_summary\n        on customers.customer_id = order_summary.customer_id\n\n)\n\nselect * from joined",
        "access": "protected",
        "original_file_path": "models/marts/customers.sql",
        "group": None,
        "patch_path": "jaffle_shop://models/marts/customers.yml",
        "build_path": None,
        "extra_ctes": [],
        "fqn": ["jaffle_shop", "marts", "customers"],
        "language": "sql",
        "extra_ctes_injected": True,
        "checksum": {
            "name": "sha256",
            "checksum": "aab6679f91377f285bfb321bce22611ae372aabbdeb6081457bcb2fa9ac40b10",
        },
        "path": "marts/customers.sql",
        "unrendered_config": {"materialized": "table"},
        "deprecation_date": None,
        "resource_type": "model",
        "relation_name": "`dbt-tutorial-347100`.`dbt_beto`.`customers`",
        "compiled_path": "target/compiled/jaffle_shop/models/marts/customers.sql",
        "children": [
            "semantic_model.jaffle_shop.customers",
            "test.jaffle_shop.accepted_values_customers_customer_type__new__returning.d12f0947c8",
            "test.jaffle_shop.not_null_customers_customer_id.5c9bf9911d",
            "test.jaffle_shop.unique_customers_customer_id.c5af1ff4b1",
        ],
        "refs": [
            {"name": "stg_customers", "package": None, "version": None},
            {"name": "orders", "package": None, "version": None},
            {"name": "order_items", "package": None, "version": None},
        ],
        "latest_version": None,
        "sources": [],
        "alias": "customers",
        "version": None,
        "compiled": True,
        "created_at": 1702514525.450856,
        "compiled_code": "with\n\ncustomers as (\n\n    select * from `dbt-tutorial-347100`.`dbt_beto`.`stg_customers`\n\n),\n\norders_table as (\n\n    select * from `dbt-tutorial-347100`.`dbt_beto`.`orders`\n\n),\n\norder_items_table as (\n\n    select * from `dbt-tutorial-347100`.`dbt_beto`.`order_items`\n),\n\norder_summary as (\n\n    select\n        customer_id,\n\n        count(distinct orders.order_id) as count_lifetime_orders,\n        count(distinct orders.order_id) > 1 as is_repeat_buyer,\n        min(orders.ordered_at) as first_ordered_at,\n        max(orders.ordered_at) as last_ordered_at,\n        sum(order_items.product_price) as lifetime_spend_pretax,\n        sum(orders.order_total) as lifetime_spend\n\n    from orders_table as orders\n    \n    left join order_items_table as order_items on orders.order_id = order_items.order_id\n    \n    group by 1\n\n),\n\njoined as (\n\n    select\n        customers.*,\norder_summary.count_lifetime_orders,\n        order_summary.first_ordered_at,\n        order_summary.last_ordered_at,\n        order_summary.lifetime_spend_pretax,\n        order_summary.lifetime_spend,\n\n        case\n            when order_summary.is_repeat_buyer then 'returning'\n            else 'new'\n        end as customer_type\n\n    from customers\n\n    left join order_summary\n        on customers.customer_id = order_summary.customer_id\n\n)\n\nselect * from joined",
        "constraints": [],
        "depends_on": {
            "macros": [],
            "nodes": [
                "model.jaffle_shop.stg_customers",
                "model.jaffle_shop.orders",
                "model.jaffle_shop.order_items",
            ],
        },
    },
    {
        "unique_id": "model.jaffle_shop.orders",
        "description": "Order overview data mart, offering key details for each order inlcluding if it's a customer's first order and a food vs. drink item breakdown. One row per order.",
        "tags": [],
        "columns": [
            {
                "name": "order_id",
                "description": "The unique key of the orders mart.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "customer_id",
                "description": "The foreign key relating to the customer who placed the order.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "location_id",
                "description": "The foreign key relating to the location the order was placed at.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "order_total",
                "description": "The total amount of the order in USD including tax.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "ordered_at",
                "description": "The timestamp the order was placed at.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "count_food_items",
                "description": "The number of individual food items ordered.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "count_drink_items",
                "description": "The number of individual drink items ordered.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "count_items",
                "description": "The total number of both food and drink items ordered.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "subtotal_food_items",
                "description": "The sum of all the food item prices without tax.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "subtotal_drink_items",
                "description": "The sum of all the drink item prices without tax.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "subtotal",
                "description": "The sum total of both food and drink item prices without tax.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "order_cost",
                "description": "The sum of supply expenses to fulfill the order.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "location_name",
                "description": "The full location name of where this order was placed. Denormalized from `stg_locations`.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "is_first_order",
                "description": "A boolean indicating if this order is from a new customer placing their first order.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "is_food_order",
                "description": "A boolean indicating if this order included any food items.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
            {
                "name": "is_drink_order",
                "description": "A boolean indicating if this order included any drink items.",
                "meta": {},
                "data_type": None,
                "constraints": [],
                "quote": None,
                "tags": [],
            },
        ],
        "schema": "dbt_beto",
        "meta": {},
        "database": "dbt-tutorial-347100",
        "config": {
            "enabled": True,
            "alias": None,
            "schema": None,
            "database": None,
            "tags": [],
            "meta": {},
            "group": None,
            "materialized": "table",
            "incremental_strategy": None,
            "persist_docs": {},
            "quoting": {},
            "column_types": {},
            "full_refresh": None,
            "unique_key": None,
            "on_schema_change": "ignore",
            "on_configuration_change": "apply",
            "grants": {},
            "packages": [],
            "docs": {"show": True, "node_color": None},
            "contract": {"enforced": False},
            "post-hook": [],
            "pre-hook": [],
        },
        "name": "orders",
        "deferred": False,
        "contract": {"enforced": False, "checksum": None},
        "metrics": [],
        "docs": {"show": True, "node_color": None},
        "package_name": "jaffle_shop",
        "raw_code": "with \n\norders as (\n    \n    select * from {{ ref('stg_orders')}}\n\n),\n\norder_items_table as (\n    \n    select * from {{ ref('order_items')}}\n\n),\n\norder_items_summary as (\n\n    select\n\n        order_items.order_id,\n\n        sum(supply_cost) as order_cost,\n        sum(is_food_item) as count_food_items,\n        sum(is_drink_item) as count_drink_items\n\n\n    from order_items_table as order_items\n\n    group by 1\n\n),\n\n\ncompute_booleans as (\n    select\n\n        orders.*,\n        count_food_items > 0 as is_food_order,\n        count_drink_items > 0 as is_drink_order,\n        order_cost\n\n    from orders\n    \n    left join order_items_summary on orders.order_id = order_items_summary.order_id\n)\n\nselect * from compute_booleans",
        "access": "protected",
        "original_file_path": "models/marts/orders.sql",
        "group": None,
        "patch_path": "jaffle_shop://models/marts/orders.yml",
        "build_path": None,
        "extra_ctes": [],
        "fqn": ["jaffle_shop", "marts", "orders"],
        "language": "sql",
        "extra_ctes_injected": True,
        "checksum": {
            "name": "sha256",
            "checksum": "82065db130ac94225e37c84233aba8621ccab7b2317b8d6cf01e86471cbb19a3",
        },
        "path": "marts/orders.sql",
        "unrendered_config": {"materialized": "table"},
        "deprecation_date": None,
        "resource_type": "model",
        "relation_name": "`dbt-tutorial-347100`.`dbt_beto`.`orders`",
        "compiled_path": "target/compiled/jaffle_shop/models/marts/orders.sql",
        "children": [
            "model.jaffle_shop.customers",
            "semantic_model.jaffle_shop.orders",
            "test.jaffle_shop.not_null_orders_order_id.cf6c17daed",
            "test.jaffle_shop.relationships_orders_customer_id__customer_id__ref_stg_customers_.918495ce16",
            "test.jaffle_shop.unique_orders_order_id.fed79b3a6e",
        ],
        "refs": [
            {"name": "stg_orders", "package": None, "version": None},
            {"name": "order_items", "package": None, "version": None},
        ],
        "latest_version": None,
        "sources": [],
        "alias": "orders",
        "version": None,
        "compiled": True,
        "created_at": 1702514525.4792378,
        "compiled_code": "with \n\norders as (\n    \n    select * from `dbt-tutorial-347100`.`dbt_beto`.`stg_orders`\n\n),\n\norder_items_table as (\n    \n    select * from `dbt-tutorial-347100`.`dbt_beto`.`order_items`\n\n),\n\norder_items_summary as (\n\n    select\n\n        order_items.order_id,\n\n        sum(supply_cost) as order_cost,\n        sum(is_food_item) as count_food_items,\n        sum(is_drink_item) as count_drink_items\n\n\n    from order_items_table as order_items\n\n    group by 1\n\n),\n\n\ncompute_booleans as (\n    select\n\n        orders.*,\n        count_food_items > 0 as is_food_order,\n        count_drink_items > 0 as is_drink_order,\n        order_cost\n\n    from orders\n    \n    left join order_items_summary on orders.order_id = order_items_summary.order_id\n)\n\nselect * from compute_booleans",
        "constraints": [],
        "depends_on": {
            "macros": [],
            "nodes": ["model.jaffle_shop.stg_orders", "model.jaffle_shop.order_items"],
        },
    },
    {
        "unique_id": "model.jaffle_shop.metricflow_time_spine",
        "description": "",
        "tags": [],
        "columns": [],
        "schema": "dbt_beto",
        "meta": {},
        "database": "dbt-tutorial-347100",
        "config": {
            "enabled": True,
            "alias": None,
            "schema": None,
            "database": None,
            "tags": [],
            "meta": {},
            "group": None,
            "materialized": "table",
            "incremental_strategy": None,
            "persist_docs": {},
            "quoting": {},
            "column_types": {},
            "full_refresh": None,
            "unique_key": None,
            "on_schema_change": "ignore",
            "on_configuration_change": "apply",
            "grants": {},
            "packages": [],
            "docs": {"show": True, "node_color": None},
            "contract": {"enforced": False},
            "post-hook": [],
            "pre-hook": [],
        },
        "name": "metricflow_time_spine",
        "deferred": False,
        "contract": {"enforced": False, "checksum": None},
        "metrics": [],
        "docs": {"show": True, "node_color": None},
        "package_name": "jaffle_shop",
        "raw_code": "-- metricflow_time_spine.sql\nwith \n\ndays as (\n    \n    --for BQ adapters use \"DATE('01/01/2000','mm/dd/yyyy')\"\n    {{ dbt_date.get_base_dates(n_dateparts=365*10, datepart=\"day\") }}\n\n),\n\ncast_to_date as (\n\n    select \n        cast(date_day as date) as date_day\n    \n    from days\n\n)\n\nselect * from cast_to_date",
        "access": "protected",
        "original_file_path": "models/marts/metricflow_time_spine.sql",
        "group": None,
        "patch_path": None,
        "build_path": None,
        "extra_ctes": [],
        "fqn": ["jaffle_shop", "marts", "metricflow_time_spine"],
        "language": "sql",
        "extra_ctes_injected": True,
        "checksum": {
            "name": "sha256",
            "checksum": "2c46f934140af8bb4e9f5b66b9764dab4e6830cd5c6358e66ab51d70238f60e6",
        },
        "path": "marts/metricflow_time_spine.sql",
        "unrendered_config": {"materialized": "table"},
        "deprecation_date": None,
        "resource_type": "model",
        "relation_name": "`dbt-tutorial-347100`.`dbt_beto`.`metricflow_time_spine`",
        "compiled_path": "target/compiled/jaffle_shop/models/marts/metricflow_time_spine.sql",
        "children": [],
        "refs": [],
        "latest_version": None,
        "sources": [],
        "alias": "metricflow_time_spine",
        "version": None,
        "compiled": True,
        "created_at": 1702514525.26042,
        "compiled_code": "-- metricflow_time_spine.sql\nwith \n\ndays as (\n    \n    --for BQ adapters use \"DATE('01/01/2000','mm/dd/yyyy')\"\n    \n    with date_spine as\n(\n\n    \n\n\n\n\n\nwith rawdata as (\n\n    \n\n    \n\n    with p as (\n        select 0 as generated_number union all select 1\n    ), unioned as (\n\n    select\n\n    \n    p0.generated_number * power(2, 0)\n     + \n    \n    p1.generated_number * power(2, 1)\n     + \n    \n    p2.generated_number * power(2, 2)\n     + \n    \n    p3.generated_number * power(2, 3)\n     + \n    \n    p4.generated_number * power(2, 4)\n     + \n    \n    p5.generated_number * power(2, 5)\n     + \n    \n    p6.generated_number * power(2, 6)\n     + \n    \n    p7.generated_number * power(2, 7)\n     + \n    \n    p8.generated_number * power(2, 8)\n     + \n    \n    p9.generated_number * power(2, 9)\n     + \n    \n    p10.generated_number * power(2, 10)\n     + \n    \n    p11.generated_number * power(2, 11)\n    \n    \n    + 1\n    as generated_number\n\n    from\n\n    \n    p as p0\n     cross join \n    \n    p as p1\n     cross join \n    \n    p as p2\n     cross join \n    \n    p as p3\n     cross join \n    \n    p as p4\n     cross join \n    \n    p as p5\n     cross join \n    \n    p as p6\n     cross join \n    \n    p as p7\n     cross join \n    \n    p as p8\n     cross join \n    \n    p as p9\n     cross join \n    \n    p as p10\n     cross join \n    \n    p as p11\n    \n    \n\n    )\n\n    select *\n    from unioned\n    where generated_number <= 3651\n    order by generated_number\n\n\n\n),\n\nall_periods as (\n\n    select (\n        \n\n        datetime_add(\n            cast( \n\n        datetime_add(\n            cast( cast(timestamp(datetime(current_timestamp(), 'America/Los_Angeles')) as date) as datetime),\n        interval -3650 day\n        )\n\n as datetime),\n        interval row_number() over (order by 1) - 1 day\n        )\n\n\n    ) as date_day\n    from rawdata\n\n),\n\nfiltered as (\n\n    select *\n    from all_periods\n    where date_day <= cast(\n\n        datetime_add(\n            cast( cast(timestamp(datetime(current_timestamp(), 'America/Los_Angeles')) as date) as datetime),\n        interval 1 day\n        )\n\n as date)\n\n)\n\nselect * from filtered\n\n\n\n)\nselect\n    cast(d.date_day as TIMESTAMP) as date_day\nfrom\n    date_spine d\n\n\n\n),\n\ncast_to_date as (\n\n    select \n        cast(date_day as date) as date_day\n    \n    from days\n\n)\n\nselect * from cast_to_date",
        "constraints": [],
        "depends_on": {"macros": ["macro.dbt_date.get_base_dates"], "nodes": []},
    },
    {
        "unique_id": "model.jaffle_shop.order_items",
        "description": "",
        "tags": [],
        "columns": [],
        "schema": "dbt_beto",
        "meta": {},
        "database": "dbt-tutorial-347100",
        "config": {
            "enabled": True,
            "alias": None,
            "schema": None,
            "database": None,
            "tags": [],
            "meta": {},
            "group": None,
            "materialized": "table",
            "incremental_strategy": None,
            "persist_docs": {},
            "quoting": {},
            "column_types": {},
            "full_refresh": None,
            "unique_key": None,
            "on_schema_change": "ignore",
            "on_configuration_change": "apply",
            "grants": {},
            "packages": [],
            "docs": {"show": True, "node_color": None},
            "contract": {"enforced": False},
            "post-hook": [],
            "pre-hook": [],
        },
        "name": "order_items",
        "deferred": False,
        "contract": {"enforced": False, "checksum": None},
        "metrics": [],
        "docs": {"show": True, "node_color": None},
        "package_name": "jaffle_shop",
        "raw_code": "with \n\norder_items as (\n\n    select * from {{ ref('stg_order_items') }}\n\n),\n\n\norders as (\n    \n    select * from {{ ref('stg_orders')}}\n),\n\nproducts as (\n\n    select * from {{ ref('stg_products') }}\n\n),\n\nsupplies as (\n\n  select * from {{ ref('stg_supplies') }}\n\n),\n\norder_supplies_summary as (\n\n  select\n    product_id,\n    sum(supply_cost) as supply_cost\n\n  from supplies\n\ngroup by 1\n),\n\njoined as (\n    select\n        order_items.*,\n        products.product_price,\n        order_supplies_summary.supply_cost,\n        products.is_food_item,\n        products.is_drink_item,\n        orders.ordered_at\n\n    from order_items\n\n    left join orders on order_items.order_id  = orders.order_id\n    \n    left join products on order_items.product_id = products.product_id\n    \n    left join order_supplies_summary on order_items.product_id = order_supplies_summary.product_id\n    \n)\n\nselect * from joined",
        "access": "protected",
        "original_file_path": "models/marts/order_items.sql",
        "group": None,
        "patch_path": None,
        "build_path": None,
        "extra_ctes": [],
        "fqn": ["jaffle_shop", "marts", "order_items"],
        "language": "sql",
        "extra_ctes_injected": True,
        "checksum": {
            "name": "sha256",
            "checksum": "6391e10e8c9765f055e2e3736e0bc3bb7794d4fa95f506ee275229968aab231a",
        },
        "path": "marts/order_items.sql",
        "unrendered_config": {"materialized": "table"},
        "deprecation_date": None,
        "resource_type": "model",
        "relation_name": "`dbt-tutorial-347100`.`dbt_beto`.`order_items`",
        "compiled_path": "target/compiled/jaffle_shop/models/marts/order_items.sql",
        "children": [
            "model.jaffle_shop.customers",
            "model.jaffle_shop.orders",
            "semantic_model.jaffle_shop.order_item",
        ],
        "refs": [
            {"name": "stg_order_items", "package": None, "version": None},
            {"name": "stg_orders", "package": None, "version": None},
            {"name": "stg_products", "package": None, "version": None},
            {"name": "stg_supplies", "package": None, "version": None},
        ],
        "latest_version": None,
        "sources": [],
        "alias": "order_items",
        "version": None,
        "compiled": True,
        "created_at": 1702514525.303211,
        "compiled_code": "with \n\norder_items as (\n\n    select * from `dbt-tutorial-347100`.`dbt_beto`.`stg_order_items`\n\n),\n\n\norders as (\n    \n    select * from `dbt-tutorial-347100`.`dbt_beto`.`stg_orders`\n),\n\nproducts as (\n\n    select * from `dbt-tutorial-347100`.`dbt_beto`.`stg_products`\n\n),\n\nsupplies as (\n\n  select * from `dbt-tutorial-347100`.`dbt_beto`.`stg_supplies`\n\n),\n\norder_supplies_summary as (\n\n  select\n    product_id,\n    sum(supply_cost) as supply_cost\n\n  from supplies\n\n  group by 1\n),\n\njoined as (\n    select\n        order_items.*,\n        products.product_price,\n        order_supplies_summary.supply_cost,\n        products.is_food_item,\n        products.is_drink_item,\n        orders.ordered_at\n\n    from order_items\n\n    left join orders on order_items.order_id  = orders.order_id\n    \n    left join products on order_items.product_id = products.product_id\n    \n    left join order_supplies_summary on order_items.product_id = order_supplies_summary.product_id\n    \n)\n\nselect * from joined",
        "constraints": [],
        "depends_on": {
            "macros": [],
            "nodes": [
                "model.jaffle_shop.stg_order_items",
                "model.jaffle_shop.stg_orders",
                "model.jaffle_shop.stg_products",
                "model.jaffle_shop.stg_supplies",
            ],
        },
    },
]


superset_metricflow_metrics = {
    "model.jaffle_shop.orders": [
        {
            "expression": "COUNT(DISTINCT customer_id)",
            "metric_name": "customers_with_orders",
            "metric_type": "simple",
            "verbose_name": "customers_with_orders",
            "description": "Distict count of customers placing orders",
        },
        {
            "expression": "SUM(order_total)",
            "metric_name": "order_total",
            "metric_type": "simple",
            "verbose_name": "order_total",
            "description": "Sum of total order amonunt. Includes tax + revenue.",
        },
        {
            "expression": "SUM(CASE WHEN order_total >= 20 THEN 1 END)",
            "metric_name": "large_order",
            "metric_type": "simple",
            "verbose_name": "large_order",
            "description": "Count of orders with order total over 20.",
        },
        {
            "expression": "SUM(1)",
            "metric_name": "orders",
            "metric_type": "simple",
            "verbose_name": "orders",
            "description": "Count of orders.",
        },
        {
            "expression": "SUM(CASE WHEN is_food_order = TRUE THEN 1 END)",
            "metric_name": "food_orders",
            "metric_type": "simple",
            "verbose_name": "food_orders",
            "description": "Count of orders that contain food order items",
        },
        {
            "expression": "SUM(order_cost)",
            "metric_name": "order_cost",
            "metric_type": "simple",
            "verbose_name": "order_cost",
            "description": "Sum of cost for each order item.",
        },
    ],
    "model.jaffle_shop.order_items": [
        {
            "expression": "SUM(product_price)",
            "metric_name": "revenue",
            "metric_type": "simple",
            "verbose_name": "revenue",
            "description": "Sum of the product revenue for each order item. Excludes tax.",
        },
        {
            "expression": "SUM(CASE WHEN is_food_item = 1 THEN product_price ELSE 0 END)",
            "metric_name": "food_revenue",
            "metric_type": "simple",
            "verbose_name": "food_revenue",
            "description": "The revenue from food in each order",
        },
        {
            "expression": "CAST(SUM(CASE WHEN is_food_item = 1 THEN product_price ELSE 0 END) AS DOUBLE) / CAST(NULLIF(SUM(product_price), 0) AS DOUBLE)",
            "metric_name": "food_revenue_pct",
            "metric_type": "ratio",
            "verbose_name": "food_revenue_pct",
            "description": "The % of order revenue from food.",
        },
        {
            "expression": "SUM(product_price)",
            "metric_name": "cumulative_revenue",
            "metric_type": "cumulative",
            "verbose_name": "cumulative_revenue",
            "description": "The cumulative revenue for all orders.",
        },
    ],
}


def test_dbt_core(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=([], []),
    )
    sync_exposures = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_exposures",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--exposures",
            str(exposures),
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "default",
        "default",
        "dev",
        False,
        False,
        "",
    )

    sync_datasets.assert_called_with(
        client,
        dbt_core_models,
        superset_metrics,
        sync_database(),
        False,
        "",
        reload_columns=True,
        merge_metadata=False,
    )
    sync_exposures.assert_called_with(
        client,
        exposures,
        sync_datasets()[0],
        {("public", "messages_channels"): dbt_core_models[0]},
    )


def test_dbt_core_metricflow(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command with Metricflow metrics.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_metricflow_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.subprocess.run",
        side_effect=[
            mocker.MagicMock(
                stdout="""✔ Success 🦄 - query completed after 0.28 seconds
🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):
SELECT
  COUNT(DISTINCT customer_id) AS customers_with_orders
FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_3""",
            ),
            mocker.MagicMock(
                stdout="""✔ Success 🦄 - query completed after 0.37 seconds
🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):
SELECT
  COUNT(DISTINCT customers_with_orders) AS new_customer
FROM (
  SELECT
    customers_src_0.customer_type AS customer__customer_type
    , orders_src_3.customer_id AS customers_with_orders
  FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_3
  LEFT OUTER JOIN
    `dbt-tutorial-347100`.`dbt_beto`.`customers` customers_src_0
  ON
    orders_src_3.customer_id = customers_src_0.customer_id
) subq_7
WHERE customer__customer_type  = 'new'""",
            ),
            mocker.MagicMock(
                stdout="""✔ Success 🦄 - query completed after 0.29 seconds
🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):
SELECT
  SUM(order_total) AS order_total
FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_3""",
            ),
            mocker.MagicMock(
                stdout="""✔ Success 🦄 - query completed after 0.35 seconds
🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):
SELECT
  SUM(order_count) AS large_order
FROM (
  SELECT
    order_total AS order_id__order_total_dim
    , 1 AS order_count
  FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_3
) subq_2
WHERE order_id__order_total_dim >= 20""",
            ),
            mocker.MagicMock(
                stdout="""✔ Success 🦄 - query completed after 0.28 seconds
🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):
SELECT
  SUM(1) AS orders
FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_3""",
            ),
            mocker.MagicMock(
                stdout="""✔ Success 🦄 - query completed after 0.31 seconds
🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):
SELECT
  SUM(order_count) AS food_orders
FROM (
  SELECT
    is_food_order AS order_id__is_food_order
    , 1 AS order_count
  FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_3
) subq_2
WHERE order_id__is_food_order = true""",
            ),
            mocker.MagicMock(
                stdout="""✔ Success 🦄 - query completed after 0.24 seconds
🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):
SELECT
  SUM(product_price) AS revenue
FROM `dbt-tutorial-347100`.`dbt_beto`.`order_items` order_item_src_2""",
            ),
            mocker.MagicMock(
                stdout="""✔ Success 🦄 - query completed after 0.28 seconds
🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):
SELECT
  SUM(order_cost) AS order_cost
FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_3""",
            ),
            CalledProcessError(1, cmd="mf", output="Error occurred"),
            mocker.MagicMock(
                stdout="""✔ Success 🦄 - query completed after 0.26 seconds
🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):
SELECT
  SUM(case when is_food_item = 1 then product_price else 0 end) AS food_revenue
FROM `dbt-tutorial-347100`.`dbt_beto`.`order_items` order_item_src_2""",
            ),
            mocker.MagicMock(
                stdout="""✔ Success 🦄 - query completed after 0.31 seconds
🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):
SELECT
  CAST(SUM(case when is_food_item = 1 then product_price else 0 end) AS FLOAT64) / CAST(NULLIF(SUM(product_price), 0) AS FLOAT64) AS food_revenue_pct
FROM `dbt-tutorial-347100`.`dbt_beto`.`order_items` order_item_src_2""",
            ),
            CalledProcessError(1, cmd="mf", output="Error occurred"),
            mocker.MagicMock(
                stdout="""✔ Success 🦄 - query completed after 0.43 seconds
🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):
SELECT
  revenue - cost AS order_gross_profit
FROM (
  SELECT
    subq_4.revenue AS revenue
    , subq_9.cost AS cost
  FROM (
    SELECT
      SUM(product_price) AS revenue
    FROM `dbt-tutorial-347100`.`dbt_beto`.`order_items` order_item_src_2
  ) subq_4
  CROSS JOIN (
    SELECT
      SUM(order_cost) AS cost
    FROM `dbt-tutorial-347100`.`dbt_beto`.`orders` orders_src_3
  ) subq_9
) subq_10""",
            ),
            mocker.MagicMock(
                stdout="""✔ Success 🦄 - query completed after 0.30 seconds
🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):
SELECT
  SUM(product_price) AS cumulative_revenue
FROM `dbt-tutorial-347100`.`dbt_beto`.`order_items` order_item_src_2""",
            ),
        ],
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--exposures",
            str(exposures),
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "default",
        "default",
        "dev",
        False,
        False,
        "",
    )


def test_dbt_core_metricflow_not_found(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``dbt-core`` command when ``mf`` is not found.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_metricflow_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.subprocess.run",
        side_effect=FileNotFoundError(2, "No such file or directory: 'mf'"),
    )
    _logger = mocker.patch("preset_cli.cli.superset.sync.dbt.command._logger")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--exposures",
            str(exposures),
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    _logger.warning.assert_called_with(
        "`mf` command not found, if you're using Metricflow make sure you have it "
        "installed in order to sync metrics",
    )


def test_dbt_core_metricflow_dialect_not_found(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``dbt-core`` command when  the project dialect is not
    compatible with MetricFlow.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_metricflow_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)

    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    get_sl_metric_mock = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_sl_metric",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--project",
            "athena_project",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    get_sl_metric_mock.assert_not_called()


def test_dbt_core_preserve_metadata(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``dbt-core`` command with ``--preserve-metadata`` flag.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=([], []),
    )
    sync_exposures = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_exposures",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--exposures",
            str(exposures),
            "--preserve-metadata",
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "default",
        "default",
        "dev",
        False,
        False,
        "",
    )

    sync_datasets.assert_called_with(
        client,
        dbt_core_models,
        superset_metrics,
        sync_database(),
        False,
        "",
        reload_columns=False,
        merge_metadata=False,
    )
    sync_exposures.assert_called_with(
        client,
        exposures,
        sync_datasets()[0],
        {("public", "messages_channels"): dbt_core_models[0]},
    )


def test_dbt_core_preserve_columns(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``dbt-core`` command with ``--preserve-columns`` flag.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=([], []),
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--preserve-columns",
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "default",
        "default",
        "dev",
        False,
        False,
        "",
    )

    sync_datasets.assert_called_with(
        client,
        dbt_core_models,
        superset_metrics,
        sync_database(),
        False,
        "",
        reload_columns=False,
        merge_metadata=False,
    )


def test_dbt_core_merge_metadata(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``dbt-core`` command with ``--merge-metadata`` flag.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=([], []),
    )
    sync_exposures = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_exposures",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--exposures",
            str(exposures),
            "--merge-metadata",
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "default",
        "default",
        "dev",
        False,
        False,
        "",
    )

    sync_datasets.assert_called_with(
        client,
        dbt_core_models,
        superset_metrics,
        sync_database(),
        False,
        "",
        reload_columns=False,
        merge_metadata=True,
    )
    sync_exposures.assert_called_with(
        client,
        exposures,
        sync_datasets()[0],
        {("public", "messages_channels"): dbt_core_models[0]},
    )


def test_dbt_core_load_profile_jinja_variables(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``dbt-core`` command loading a profiles.yml containing Jinja
    variables.
    """
    jinja_profile_content = yaml.dump(
        {
            "default": {
                "outputs": {
                    "dev": {
                        "type": "{{ env_var('DBT_TYPE') }}",
                        "dbname": "{{ env_var('DBT_DBNAME') }}",
                        "host": "{{ env_var('DBT_HOST') }}",
                        "user": "{{ env_var('DBT_USER') }}",
                        "pass": "{{ env_var('DBT_PASS') }}",
                        "port": "{{ env_var('DBT_PORT') | as_number }}",
                        "threads": "{{ env_var('DBT_THREADS') | as_number }}",
                        "schema": "{{ env_var('DBT_SCHEMA') }}",
                    },
                },
            },
        },
    )

    mocker.patch.dict(
        os.environ,
        {
            "DBT_DBNAME": "database",
            "DBT_HOST": "hostname",
            "DBT_USER": "username",
            "DBT_PASS": "password",
            "DBT_PORT": "5432",
            "DBT_SCHEMA": "schema",
            "DBT_THREADS": "1",
            "DBT_TYPE": "postgres",
        },
    )

    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=jinja_profile_content)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--exposures",
            str(exposures),
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "default",
        "default",
        "dev",
        False,
        False,
        "",
    )


def test_dbt_core_raise_failures_flag_no_failures(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``dbt-core`` command with the ``--raise-failures`` flag.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=(["working_dataset"], []),
    )
    list_failed_models = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.list_failed_models",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--target",
            "dev",
            "--raise-failures",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "default",
        "default",
        "dev",
        False,
        False,
        "",
    )
    sync_datasets.assert_called_with(
        client,
        dbt_core_models,
        superset_metrics,
        sync_database(),
        False,
        "",
        reload_columns=True,
        merge_metadata=False,
    )
    list_failed_models.assert_not_called()


def test_dbt_core_raise_failures_flag_deprecation_warning(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``dbt-core`` command with the ``--raise-failures`` flag
    with a deprecation warning.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    dbt_project = root / "default/dbt_project.yml"
    fs.create_file(
        dbt_project,
        contents=yaml.dump(
            {
                "name": "my_project",
                "profile": "default",
                "target-path": "target",
            },
        ),
    )
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=(["working_dataset"], []),
    )
    list_failed_models = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.list_failed_models",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(dbt_project),
            "--profiles",
            str(profiles),
            "--target",
            "dev",
            "--raise-failures",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    sync_database.assert_called_with(
        client,
        profiles,
        "my_project",
        "default",
        "dev",
        False,
        False,
        "",
    )
    sync_datasets.assert_called_with(
        client,
        dbt_core_models,
        superset_metrics,
        sync_database(),
        False,
        "",
        reload_columns=True,
        merge_metadata=False,
    )
    list_failed_models.assert_not_called()


def test_dbt_core_raise_failures_flag_with_failures(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``dbt-core`` command with the ``--raise-failures`` flag
    when datasets fail to sync.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    dbt_project = root / "default/dbt_project.yml"
    fs.create_file(
        dbt_project,
        contents=yaml.dump(
            {
                "name": "my_project",
                "profile": "default",
                "target-path": "target",
            },
        ),
    )
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=(["working_dataset"], ["failed_dataset", "another_failure"]),
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(dbt_project),
            "--profiles",
            str(profiles),
            "--target",
            "dev",
            "--raise-failures",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert (
        "Below model(s) failed to sync:\n - failed_dataset\n - another_failure\n"
        in result.output
    )
    sync_database.assert_called_with(
        client,
        profiles,
        "my_project",
        "default",
        "dev",
        False,
        False,
        "",
    )
    sync_datasets.assert_called_with(
        client,
        dbt_core_models,
        superset_metrics,
        sync_database(),
        False,
        "",
        reload_columns=True,
        merge_metadata=False,
    )


def test_dbt_core_raise_failures_flag_with_failures_and_deprecation(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``dbt-core`` command with the ``--raise-failures`` flag
    when datasets fail to sync.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=(["working_dataset"], ["failed_dataset", "another_failure"]),
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--target",
            "dev",
            "--raise-failures",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert (
        "Below model(s) failed to sync:\n - failed_dataset\n - another_failure\n"
        in result.output
    )
    sync_database.assert_called_with(
        client,
        profiles,
        "default",
        "default",
        "dev",
        False,
        False,
        "",
    )
    sync_datasets.assert_called_with(
        client,
        dbt_core_models,
        superset_metrics,
        sync_database(),
        False,
        "",
        reload_columns=True,
        merge_metadata=False,
    )


def test_dbt_core_preserve_and_merge(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``dbt-core`` command with both
    the ``--preserve-metadata`` and ``--merge-metadata`` flags.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)

    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--preserve-metadata",
            "--merge-metadata",
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert (
        "``--preserve-columns`` / ``--preserve-metadata`` and ``--merge-metadata``"
        in result.output
    )
    assert "can't be combined. Please include only one to the command." in result.output


def test_dbt_core_dbt_project(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command with a ``dbt_project.yml`` file.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    dbt_project = root / "default/dbt_project.yml"
    fs.create_file(
        dbt_project,
        contents=yaml.dump(
            {
                "name": "my_project",
                "profile": "default",
                "target-path": "target",
            },
        ),
    )
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(dbt_project),
            "--profiles",
            str(profiles),
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "my_project",
        "default",
        "dev",
        False,
        False,
        "",
    )


def test_dbt_core_invalid_argument(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command with an invalid argument.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)
    wrong = root / "wrong"
    fs.create_file(wrong)

    mocker.patch("preset_cli.cli.superset.sync.dbt.command.SupersetClient")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(wrong),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert (
        result.output
        == "FILE should be either ``manifest.json`` or ``dbt_project.yml``\n"
    )


def test_dbt(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt`` command.

    Initially ``dbt-core`` was just ``dbt``. This aliases was added for backwards
    compatibility.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=([], []),
    )
    sync_exposures = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_exposures",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt",
            str(manifest),
            "--profiles",
            str(profiles),
            "--exposures",
            str(exposures),
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "default",
        "default",
        "dev",
        False,
        False,
        "",
    )
    models = [
        {
            "meta": {},
            "tags": [],
            "columns": [],
            "schema": "public",
            "name": "messages_channels",
            "database": "examples_dev",
            "description": "",
            "unique_id": "model.superset_examples.messages_channels",
            "extra_ctes": [],
            "compiled_path": "target/compiled/superset_examples/models/slack/messages_channels.sql",
            "build_path": None,
            "path": "slack/messages_channels.sql",
            "docs": {"show": True},
            "relation_name": '"examples_dev"."public"."messages_channels"',
            "depends_on": {
                "macros": [],
                "nodes": [
                    "source.superset_examples.public.channels",
                    "source.superset_examples.public.messages",
                ],
            },
            "children": ["metric.superset_examples.cnt"],
            "original_file_path": "models/slack/messages_channels.sql",
            "sources": [["public", "channels"], ["public", "messages"]],
            "resource_type": "model",
            "compiled_sql": (
                "SELECT messages.ts, channels.name, messages.text "
                'FROM "examples_dev"."public"."messages" messages '
                'JOIN "examples_dev"."public"."channels" channels '
                "ON messages.channel_id = channels.id"
            ),
            "config": {
                "enabled": True,
                "alias": None,
                "schema": None,
                "database": None,
                "tags": [],
                "meta": {},
                "materialized": "view",
                "persist_docs": {},
                "quoting": {},
                "column_types": {},
                "full_refresh": None,
                "on_schema_change": "ignore",
                "post-hook": [],
                "pre-hook": [],
            },
            "compiled": True,
            "fqn": ["superset_examples", "slack", "messages_channels"],
            "deferred": False,
            "alias": "messages_channels",
            "checksum": {
                "name": "sha256",
                "checksum": "b4ce232b28280daa522b37e12c36b67911e2a98456b8a3b99440075ec5564609",
            },
            "created_at": 1642628933.004452,
            "raw_sql": (
                "SELECT messages.ts, channels.name, messages.text "
                "FROM {{ source ('public', 'messages') }} messages "
                "JOIN {{ source ('public', 'channels') }} channels "
                "ON messages.channel_id = channels.id"
            ),
            "patch_path": None,
            "root_path": "/Users/beto/Projects/dbt-examples/superset_examples",
            "extra_ctes_injected": True,
            "package_name": "superset_examples",
            "unrendered_config": {"materialized": "view"},
            "refs": [],
        },
    ]
    sync_datasets.assert_called_with(
        client,
        models,
        superset_metrics,
        sync_database(),
        False,
        "",
        reload_columns=True,
        merge_metadata=False,
    )
    sync_exposures.assert_called_with(
        client,
        exposures,
        sync_datasets()[0],
        {("public", "messages_channels"): dbt_core_models[0]},
    )


def test_dbt_core_no_exposures(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command when no exposures file is passed.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)

    mocker.patch("preset_cli.cli.superset.sync.dbt.command.SupersetClient")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    mocker.patch("preset_cli.cli.superset.sync.dbt.command.sync_database")
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=([], []),
    )
    sync_exposures = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_exposures",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_exposures.assert_not_called()


def test_dbt_core_default_profile(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command when the profile is not passed
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=([], []),
    )
    mocker.patch("preset_cli.cli.superset.sync.dbt.command.sync_exposures")
    # pylint: disable=redefined-outer-name
    os = mocker.patch("preset_cli.cli.superset.sync.dbt.command.os")
    os.path.expanduser.return_value = str(profiles)

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--exposures",
            str(exposures),
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "default",
        "default",
        "dev",
        False,
        False,
        "",
    )


def test_dbt_core_no_database(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``dbt-core`` command when no database is found and ``--import-db`` not passed.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    mocker.patch("preset_cli.cli.superset.sync.dbt.command.SupersetClient")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
        side_effect=DatabaseNotFoundError(),
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--exposures",
            str(exposures),
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "No database was found, pass ``--import-db`` to create" in result.output


def test_dbt_core_disallow_edits_superset(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``dbt-core`` command with ``--disallow-edits`` for Superset legacy installation.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    dbt_project = root / "default/dbt_project.yml"
    fs.create_file(
        dbt_project,
        contents=yaml.dump(
            {
                "name": "my_project",
                "profile": "default",
                "target-path": "target",
            },
        ),
    )
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(dbt_project),
            "--profiles",
            str(profiles),
            "--target",
            "dev",
            "--disallow-edits",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    sync_database.assert_called_with(
        client,
        profiles,
        "my_project",
        "default",
        "dev",
        False,
        True,
        "",
    )


def test_dbt_cloud(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=([], []),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_job",
        return_value={"id": 123, "name": "My job", "environment_id": 456},
    )

    dbt_client.get_models.return_value = dbt_cloud_models
    dbt_client.get_og_metrics.return_value = dbt_cloud_metrics
    dbt_client.get_sl_dialect.return_value = MFSQLEngine.BIGQUERY
    dbt_client.get_sl_metrics.return_value = dbt_metricflow_metrics
    dbt_client.get_sl_metric_sql.side_effect = [
        "SELECT COUNT(*) FROM public.messages_channels",
        "SELECT COUNT(*) FROM public.messages_channels JOIN public.some_other_table",
        None,
    ]
    database = mocker.MagicMock()
    superset_client.get_databases.return_value = [database]
    superset_client.get_database.return_value = database

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "1",
            "2",
            "123",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_datasets.assert_called_with(
        superset_client,
        dbt_cloud_models,
        {
            "model.superset_examples.messages_channels": [
                {
                    "description": "",
                    "expression": "COUNT(*)",
                    "extra": "{}",
                    "metric_name": "cnt",
                    "metric_type": "count",
                    "verbose_name": "",
                },
                {
                    "description": "The simplest metric",
                    "expression": "COUNT(*)",
                    "metric_name": "a",
                    "metric_type": "Simple",
                    "verbose_name": "a",
                },
            ],
        },
        database,
        False,
        "",
        reload_columns=True,
        merge_metadata=False,
    )


def test_dbt_cloud_preserve_metadata(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command with the ``--preserve-metadata`` flag.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=([], []),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_job",
        return_value={"id": 123, "name": "My job", "environment_id": 456},
    )

    dbt_client.get_models.return_value = dbt_cloud_models
    dbt_client.get_og_metrics.return_value = dbt_cloud_metrics
    database = mocker.MagicMock()
    superset_client.get_databases.return_value = [database]
    superset_client.get_database.return_value = database

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "1",
            "2",
            "123",
            "--preserve-metadata",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_datasets.assert_called_with(
        superset_client,
        dbt_cloud_models,
        superset_metrics,
        database,
        False,
        "",
        reload_columns=False,
        merge_metadata=False,
    )


def test_dbt_cloud_preserve_columns(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command with the ``--preserve-columns`` flag.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=([], []),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_job",
        return_value={"id": 123, "name": "My job", "environment_id": 456},
    )

    dbt_client.get_models.return_value = dbt_cloud_models
    dbt_client.get_og_metrics.return_value = dbt_cloud_metrics
    database = mocker.MagicMock()
    superset_client.get_databases.return_value = [database]
    superset_client.get_database.return_value = database

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "1",
            "2",
            "123",
            "--preserve-columns",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_datasets.assert_called_with(
        superset_client,
        dbt_cloud_models,
        superset_metrics,
        database,
        False,
        "",
        reload_columns=False,
        merge_metadata=False,
    )


def test_dbt_cloud_merge_metadata(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command with the ``--merge-metadata`` flag.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=([], []),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_job",
        return_value={"id": 123, "name": "My job", "environment_id": 456},
    )

    dbt_client.get_models.return_value = dbt_cloud_models
    dbt_client.get_og_metrics.return_value = dbt_cloud_metrics
    database = mocker.MagicMock()
    superset_client.get_databases.return_value = [database]
    superset_client.get_database.return_value = database

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "1",
            "2",
            "123",
            "--merge-metadata",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_datasets.assert_called_with(
        superset_client,
        dbt_cloud_models,
        superset_metrics,
        database,
        False,
        "",
        reload_columns=False,
        merge_metadata=True,
    )


def test_dbt_cloud_raise_failures_flag_no_failures(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command with the ``--raise-failures`` flag.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=(["working_dataset"], []),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_job",
        return_value={"id": 123, "name": "My job", "environment_id": 456},
    )
    list_failed_models = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.list_failed_models",
    )

    dbt_client.get_models.return_value = dbt_cloud_models
    dbt_client.get_og_metrics.return_value = dbt_cloud_metrics
    database = mocker.MagicMock()
    superset_client.get_databases.return_value = [database]
    superset_client.get_database.return_value = database

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "1",
            "2",
            "123",
            "--raise-failures",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_datasets.assert_called_with(
        superset_client,
        dbt_cloud_models,
        superset_metrics,
        database,
        False,
        "",
        reload_columns=True,
        merge_metadata=False,
    )
    list_failed_models.assert_not_called()


def test_dbt_cloud_raise_failures_flag_with_failures(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command with the ``--raise-failures`` flag
    when datasets fail to sync.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=(["working_dataset"], ["failed_dataset", "another_failure"]),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_job",
        return_value={"id": 123, "name": "My job", "environment_id": 456},
    )

    dbt_client.get_models.return_value = dbt_cloud_models
    dbt_client.get_og_metrics.return_value = dbt_cloud_metrics
    database = mocker.MagicMock()
    superset_client.get_databases.return_value = [database]
    superset_client.get_database.return_value = database

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "1",
            "2",
            "123",
            "--raise-failures",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert (
        "Below model(s) failed to sync:\n - failed_dataset\n - another_failure\n"
        in result.output
    )
    sync_datasets.assert_called_with(
        superset_client,
        dbt_cloud_models,
        superset_metrics,
        database,
        False,
        "",
        reload_columns=True,
        merge_metadata=False,
    )


def test_dbt_cloud_preserve_and_merge(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command with both
    the ``--preserve-metadata`` and ``--merge-metadata`` flags.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=(["working_dataset"], ["failed_dataset", "another_failure"]),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_job",
        return_value={"id": 123, "name": "My job", "environment_id": 456},
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.list_failed_models",
    )

    dbt_client.get_models.return_value = dbt_cloud_models
    dbt_client.get_og_metrics.return_value = dbt_cloud_metrics
    database = mocker.MagicMock()
    superset_client.get_databases.return_value = [database]
    superset_client.get_database.return_value = database

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "1",
            "2",
            "123",
            "--preserve-metadata",
            "--merge-metadata",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert (
        "``--preserve-columns`` / ``--preserve-metadata`` and ``--merge-metadata``"
        in result.output
    )
    assert "can't be combined. Please include only one to the command." in result.output


def test_dbt_cloud_no_job_id(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command when no job ID is specified.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
        return_value=([], []),
    )

    dbt_client.get_models.return_value = dbt_cloud_models
    dbt_client.get_og_metrics.return_value = dbt_cloud_metrics
    dbt_client.get_accounts.return_value = [{"id": 1, "name": "My account"}]
    dbt_client.get_projects.return_value = [{"id": 1000, "name": "My project"}]
    dbt_client.get_jobs.return_value = [
        {"id": 123, "name": "My job", "environment_id": 456},
    ]
    database = mocker.MagicMock()
    superset_client.get_databases.return_value = [database]
    superset_client.get_database.return_value = database

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    dbt_client.get_database_name.assert_called_with(123)
    dbt_client.get_models.assert_called_with(123)
    dbt_client.get_og_metrics.assert_called_with(123)
    sync_datasets.assert_called_with(
        superset_client,
        dbt_cloud_models,
        superset_metrics,
        database,
        False,
        "",
        reload_columns=True,
        merge_metadata=False,
    )


def test_get_account_id(mocker: MockerFixture) -> None:
    """
    Test the ``get_account_id`` helper.
    """
    client = mocker.MagicMock()

    client.get_accounts.return_value = []
    with pytest.raises(CLIError) as excinfo:
        get_account_id(client)
    assert excinfo.type == CLIError
    assert excinfo.value.exit_code == 1
    assert "No accounts available" in str(excinfo.value)

    client.get_accounts.return_value = [
        {"id": 1, "name": "My account"},
    ]
    assert get_account_id(client) == 1

    client.get_accounts.return_value = [
        {"id": 1, "name": "My account"},
        {"id": 3, "name": "My other account"},
    ]
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.input",
        side_effect=["invalid", "2"],
    )
    assert get_account_id(client) == 3


def test_get_project_id(mocker: MockerFixture) -> None:
    """
    Test the ``get_project_id`` helper.
    """
    client = mocker.MagicMock()

    client.get_projects.return_value = []
    with pytest.raises(CLIError) as excinfo:
        get_project_id(client, account_id=42)
    assert excinfo.type == CLIError
    assert excinfo.value.exit_code == 1
    assert "No project available" in str(excinfo.value)

    client.get_projects.return_value = [
        {"id": 1, "name": "My project"},
    ]
    assert get_project_id(client, account_id=42) == 1

    client.get_projects.return_value = [
        {"id": 1, "name": "My project"},
        {"id": 3, "name": "My other project"},
    ]
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.input",
        side_effect=["invalid", "2"],
    )
    assert get_project_id(client, account_id=42) == 3

    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_account_id",
        return_value=42,
    )
    client.get_projects.return_value = [
        {"id": 1, "name": "My project"},
    ]
    assert get_project_id(client) == 1
    client.get_projects.assert_called_with(42)


def test_get_job(mocker: MockerFixture) -> None:
    """
    Test the ``get_job`` helper.
    """
    client = mocker.MagicMock()

    client.get_jobs.return_value = []
    with pytest.raises(CLIError) as excinfo:
        get_job(client, account_id=42, project_id=43)
    assert excinfo.type == CLIError
    assert excinfo.value.exit_code == 1
    assert "No jobs available" in str(excinfo.value)

    client.get_jobs.return_value = [
        {"id": 1, "name": "My job", "environment_id": 456},
    ]
    assert get_job(client, account_id=42, project_id=43) == {
        "id": 1,
        "name": "My job",
        "environment_id": 456,
    }

    client.get_jobs.return_value = [
        {"id": 1, "name": "My job", "environment_id": 456},
        {"id": 3, "name": "My other job", "environment_id": 456},
    ]
    assert get_job(client, account_id=42, project_id=43, job_id=3) == {
        "id": 3,
        "name": "My other job",
        "environment_id": 456,
    }
    with pytest.raises(ValueError) as excinfo:
        get_job(client, account_id=42, project_id=43, job_id=2)
    assert str(excinfo.value) == "Job 2 not available"

    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.input",
        side_effect=["invalid", "2"],
    )
    assert get_job(client, account_id=42, project_id=43) == {
        "id": 3,
        "name": "My other job",
        "environment_id": 456,
    }

    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_account_id",
        return_value=42,
    )
    client.get_jobs.return_value = [
        {"id": 1, "name": "My job", "environment_id": 456},
    ]
    assert get_job(client, project_id=43) == {
        "id": 1,
        "name": "My job",
        "environment_id": 456,
    }
    client.get_jobs.assert_called_with(42, 43)


def test_dbt_cloud_no_database(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command when no database is found.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    dbt_client.get_database_name.return_value = "my_db"
    superset_client.get_databases.return_value = []
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_job",
        return_value={"id": 123, "name": "My job", "environment_id": 456},
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "1",
            "2",
            "123",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.output == 'No database named "my_db" was found\n'


def test_dbt_cloud_invalid_job_id(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command when an invalid job ID is passed.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    dbt_client.get_database_name.return_value = "my_db"
    superset_client.get_databases.return_value = []
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_job",
        side_effect=ValueError("Job 123 not available"),
    )

    runner = CliRunner()
    runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "1",
            "2",
            "123",
        ],
        catch_exceptions=False,
    )


def test_dbt_cloud_multiple_databases(mocker: MockerFixture) -> None:
    """
    Test the ``dbt-cloud`` command when multiple databases are found.

    This should never happen, since Supersret has a uniqueness contraint on the table
    name. Nevertheless, test this for completeness.
    """
    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    dbt_client.get_database_name.return_value = "my_db"
    superset_client.get_databases.return_value = [
        mocker.MagicMock(),
        mocker.MagicMock(),
    ]
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_job",
        return_value={"id": 123, "name": "My job", "environment_id": 456},
    )

    runner = CliRunner()
    with pytest.raises(Exception) as excinfo:
        runner.invoke(
            superset_cli,
            [
                "https://superset.example.org/",
                "sync",
                "dbt-cloud",
                "XXX",
                "1",
                "2",
                "123",
            ],
            catch_exceptions=False,
        )
    assert str(excinfo.value) == "More than one database with the same name found"


def test_dbt_core_exposures_only(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``--exposures-only`` option with dbt core.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    manifest = root / "default/target/manifest.json"
    fs.create_file(manifest, contents=manifest_contents)
    profiles = root / ".dbt/profiles.yml"
    fs.create_file(profiles, contents=profiles_contents)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_datasets.return_value = [
        {"schema": "public", "table_name": "messages_channels"},
        {"schema": "public", "table_name": "some_other_table"},
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    sync_database = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_database",
    )
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
    )
    sync_exposures = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_exposures",
    )

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-core",
            str(manifest),
            "--profiles",
            str(profiles),
            "--exposures",
            str(exposures),
            "--exposures-only",
            "--project",
            "default",
            "--target",
            "dev",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_database.assert_not_called()
    sync_datasets.assert_not_called()
    sync_exposures.assert_called_with(
        client,
        exposures,
        [
            {"schema": "public", "table_name": "messages_channels"},
        ],
        {("public", "messages_channels"): dbt_core_models[0]},
    )


def test_dbt_cloud_exposures_only(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``--exposures-only`` option with dbt cloud.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    exposures = root / "models/exposures.yml"
    fs.create_file(exposures)

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.SupersetClient",
    )
    superset_client = SupersetClient()
    superset_client.get_datasets.return_value = [
        {"schema": "public", "table_name": "messages_channels"},
        {"schema": "public", "table_name": "some_other_table"},
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    DBTClient = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.DBTClient",
    )
    dbt_client = DBTClient()
    sync_datasets = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_datasets",
    )
    sync_exposures = mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.sync_exposures",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.command.get_job",
        return_value={"id": 123, "name": "My job", "environment_id": 456},
    )

    dbt_client.get_models.return_value = dbt_cloud_models
    dbt_client.get_og_metrics.return_value = dbt_cloud_metrics
    database = mocker.MagicMock()
    superset_client.get_databases.return_value = [database]
    superset_client.get_database.return_value = database

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "dbt-cloud",
            "XXX",
            "1",
            "2",
            "123",
            "--exposures",
            str(exposures),
            "--exposures-only",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    sync_datasets.assert_not_called()
    sync_exposures.assert_called_with(
        superset_client,
        exposures,
        [
            {"schema": "public", "table_name": "messages_channels"},
            {"schema": "public", "table_name": "some_other_table"},
        ],
        {
            ("public", "messages_channels"): dbt_cloud_models[0],
            ("public", "some_other_table"): dbt_cloud_models[1],
        },
    )
