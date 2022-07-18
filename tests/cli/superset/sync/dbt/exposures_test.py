"""
Tests for ``preset_cli.cli.superset.sync.dbt.exposures``.
"""
# pylint: disable=invalid-name

import copy
import json
from pathlib import Path
from typing import Any, Dict

import yaml
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from yarl import URL

from preset_cli.cli.superset.sync.dbt.exposures import (
    get_chart_depends_on,
    get_dashboard_depends_on,
    sync_exposures,
)

dashboard_response: Dict[str, Any] = {
    "result": {
        "certification_details": None,
        "certified_by": None,
        "changed_by": {
            "first_name": "admin",
            "id": 1,
            "last_name": "admin",
            "username": "admin",
        },
        "changed_by_name": "admin admin",
        "changed_by_url": "/superset/profile/admin",
        "changed_on": "2022-03-27T13:23:25.741970",
        "changed_on_delta_humanized": "29 seconds ago",
        "charts": ["Example chart"],
        "css": None,
        "dashboard_title": "Example dashboard",
        "id": 12,
        "json_metadata": None,
        "owners": [
            {"first_name": "admin", "id": 1, "last_name": "admin", "username": "admin"},
        ],
        "position_json": None,
        "published": False,
        "roles": [],
        "slug": None,
        "thumbnail_url": "/api/v1/dashboard/12/thumbnail/1f7a46435e3ff1fefc4adc9e73aa0ae7/",
        "url": "/superset/dashboard/12/",
    },
}


datasets_response: Dict[str, Any] = {
    "result": [
        {
            "cache_timeout": None,
            "column_formats": {},
            "column_types": [1, 2],
            "columns": [
                {
                    "certification_details": None,
                    "certified_by": None,
                    "column_name": "name",
                    "description": None,
                    "expression": None,
                    "filterable": True,
                    "groupby": True,
                    "id": 842,
                    "is_certified": False,
                    "is_dttm": False,
                    "python_date_format": None,
                    "type": "VARCHAR(255)",
                    "type_generic": 1,
                    "verbose_name": None,
                    "warning_markdown": None,
                },
            ],
            "database": {
                "allow_multi_schema_metadata_fetch": False,
                "allows_cost_estimate": None,
                "allows_subquery": True,
                "allows_virtual_table_explore": True,
                "backend": "postgresql",
                "disable_data_preview": False,
                "explore_database_id": 3,
                "id": 3,
                "name": "superset_examples_dev",
            },
            "datasource_name": "messages_channels",
            "default_endpoint": None,
            "edit_url": "/tablemodelview/edit/27",
            "fetch_values_predicate": None,
            "filter_select": False,
            "filter_select_enabled": False,
            "granularity_sqla": [["ts", "ts"]],
            "health_check_message": None,
            "id": 27,
            "is_sqllab_view": False,
            "main_dttm_col": "ts",
            "metrics": [
                {
                    "certification_details": None,
                    "certified_by": None,
                    "d3format": None,
                    "description": "",
                    "expression": "count(*)",
                    "id": 35,
                    "is_certified": False,
                    "metric_name": "cnt",
                    "verbose_name": "count(*)",
                    "warning_markdown": None,
                    "warning_text": None,
                },
            ],
            "name": "public.messages_channels",
            "offset": 0,
            "order_by_choices": [
                ['["name", True]', "name [asc]"],
                ['["name", False]', "name [desc]"],
                ['["text", True]', "text [asc]"],
                ['["text", False]', "text [desc]"],
                ['["ts", True]', "ts [asc]"],
                ['["ts", False]', "ts [desc]"],
            ],
            "owners": [],
            "params": None,
            "perm": "[superset_examples_dev].[messages_channels](id:27)",
            "schema": "public",
            "select_star": "SELECT *\nFROM public.messages_channels\nLIMIT 100",
            "sql": None,
            "table_name": "messages_channels",
            "template_params": None,
            "time_grain_sqla": [
                [None, "Original value"],
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
            "uid": "27__table",
            "verbose_map": {"__timestamp": "Time", "cnt": "count(*)", "name": "name"},
        },
    ],
}

chart_response = {
    "description_columns": {},
    "id": 1,
    "label_columns": {
        "cache_timeout": "Cache Timeout",
        "certification_details": "Certification Details",
        "certified_by": "Certified By",
        "dashboards.dashboard_title": "Dashboards Dashboard Title",
        "dashboards.id": "Dashboards Id",
        "dashboards.json_metadata": "Dashboards Json Metadata",
        "description": "Description",
        "is_managed_externally": "Is Managed Externally",
        "owners.first_name": "Owners First Name",
        "owners.id": "Owners Id",
        "owners.last_name": "Owners Last Name",
        "owners.username": "Owners Username",
        "params": "Params",
        "query_context": "Query Context",
        "slice_name": "Slice Name",
        "viz_type": "Viz Type",
    },
    "result": {
        "cache_timeout": None,
        "certification_details": None,
        "certified_by": None,
        "dashboards": [{"dashboard_title": "Test", "id": 1, "json_metadata": None}],
        "description": None,
        "is_managed_externally": False,
        "owners": [
            {"first_name": "admin", "id": 1, "last_name": "admin", "username": "admin"},
        ],
        "params": json.dumps(
            {
                "adhoc_filters": [],
                "all_columns": ["number_of_orders"],
                "color_pn": True,
                "datasource": "1__table",
                "extra_form_data": {},
                "granularity_sqla": "first_order_date",
                "groupby": [],
                "order_by_cols": [],
                "order_desc": True,
                "percent_metrics": [],
                "query_mode": "raw",
                "row_limit": 10000,
                "server_page_length": 10,
                "show_cell_bars": True,
                "table_timestamp_format": "smart_date",
                "time_grain_sqla": "P1D",
                "time_range": "No filter",
                "viz_type": "table",
            },
        ),
        "query_context": json.dumps(
            {
                "datasource": {"id": 1, "type": "table"},
                "force": False,
                "queries": [
                    {
                        "time_range": "No filter",
                        "granularity": "first_order_date",
                        "filters": [],
                        "extras": {
                            "time_grain_sqla": "P1D",
                            "having": "",
                            "having_druid": [],
                            "where": "",
                        },
                        "applied_time_extras": {},
                        "columns": ["number_of_orders"],
                        "orderby": [],
                        "annotation_layers": [],
                        "row_limit": 10000,
                        "timeseries_limit": 0,
                        "order_desc": True,
                        "url_params": {},
                        "custom_params": {},
                        "custom_form_data": {},
                        "post_processing": [],
                    },
                ],
                "form_data": {
                    "viz_type": "table",
                    "datasource": "1__table",
                    "granularity_sqla": "first_order_date",
                    "time_grain_sqla": "P1D",
                    "time_range": "No filter",
                    "query_mode": "raw",
                    "groupby": [],
                    "all_columns": ["number_of_orders"],
                    "percent_metrics": [],
                    "adhoc_filters": [],
                    "order_by_cols": [],
                    "row_limit": 10000,
                    "server_page_length": 10,
                    "include_time": False,
                    "order_desc": True,
                    "table_timestamp_format": "smart_date",
                    "show_cell_bars": True,
                    "color_pn": True,
                    "extra_form_data": {},
                    "force": False,
                    "result_format": "json",
                    "result_type": "full",
                },
                "result_format": "json",
                "result_type": "full",
            },
        ),
        "slice_name": "Example chart",
        "viz_type": "pie",
    },
    "show_columns": [
        "cache_timeout",
        "certified_by",
        "certification_details",
        "dashboards.dashboard_title",
        "dashboards.id",
        "dashboards.json_metadata",
        "description",
        "owners.first_name",
        "owners.id",
        "owners.last_name",
        "owners.username",
        "params",
        "slice_name",
        "viz_type",
        "query_context",
        "is_managed_externally",
    ],
    "show_title": "Show Slice",
}


dataset_response = {
    "description_columns": {},
    "id": 27,
    "label_columns": {
        "cache_timeout": "Cache Timeout",
        "columns.changed_on": "Columns Changed On",
        "columns.column_name": "Columns Column Name",
        "columns.created_on": "Columns Created On",
        "columns.description": "Columns Description",
        "columns.expression": "Columns Expression",
        "columns.extra": "Columns Extra",
        "columns.filterable": "Columns Filterable",
        "columns.groupby": "Columns Groupby",
        "columns.id": "Columns Id",
        "columns.is_active": "Columns Is Active",
        "columns.is_dttm": "Columns Is Dttm",
        "columns.python_date_format": "Columns Python Date Format",
        "columns.type": "Columns Type",
        "columns.type_generic": "Columns Type Generic",
        "columns.uuid": "Columns Uuid",
        "columns.verbose_name": "Columns Verbose Name",
        "database.backend": "Database Backend",
        "database.database_name": "Database Database Name",
        "database.id": "Database Id",
        "datasource_type": "Datasource Type",
        "default_endpoint": "Default Endpoint",
        "description": "Description",
        "extra": "Extra",
        "fetch_values_predicate": "Fetch Values Predicate",
        "filter_select_enabled": "Filter Select Enabled",
        "id": "Id",
        "is_sqllab_view": "Is Sqllab View",
        "main_dttm_col": "Main Dttm Col",
        "metrics": "Metrics",
        "offset": "Offset",
        "owners.first_name": "Owners First Name",
        "owners.id": "Owners Id",
        "owners.last_name": "Owners Last Name",
        "owners.username": "Owners Username",
        "schema": "Schema",
        "sql": "Sql",
        "table_name": "Table Name",
        "template_params": "Template Params",
        "url": "Url",
    },
    "result": {
        "cache_timeout": None,
        "columns": [
            {
                "changed_on": "2022-03-27T13:21:33.957609",
                "column_name": "ts",
                "created_on": "2022-03-27T13:21:33.957602",
                "description": None,
                "expression": None,
                "extra": None,
                "filterable": True,
                "groupby": True,
                "id": 841,
                "is_active": True,
                "is_dttm": True,
                "python_date_format": None,
                "type": "TIMESTAMP WITHOUT TIME ZONE",
                "type_generic": 2,
                "uuid": "e607d7fd-90bf-4420-a35e-9dc7af555e0d",
                "verbose_name": None,
            },
            {
                "changed_on": "2022-03-27T13:21:33.958499",
                "column_name": "name",
                "created_on": "2022-03-27T13:21:33.958493",
                "description": None,
                "expression": None,
                "extra": None,
                "filterable": True,
                "groupby": True,
                "id": 842,
                "is_active": True,
                "is_dttm": False,
                "python_date_format": None,
                "type": "VARCHAR(255)",
                "type_generic": 1,
                "uuid": "76a523f0-1aad-4608-87a4-daf22172e1da",
                "verbose_name": None,
            },
            {
                "changed_on": "2022-03-27T13:21:33.975750",
                "column_name": "text",
                "created_on": "2022-03-27T13:21:33.975743",
                "description": None,
                "expression": None,
                "extra": None,
                "filterable": True,
                "groupby": True,
                "id": 843,
                "is_active": True,
                "is_dttm": False,
                "python_date_format": None,
                "type": "TEXT",
                "type_generic": 1,
                "uuid": "610b91b0-e8de-4703-bbe9-5b27fb6c6a4e",
                "verbose_name": None,
            },
        ],
        "database": {
            "backend": "postgresql",
            "database_name": "superset_examples_dev",
            "id": 3,
        },
        "datasource_type": "table",
        "default_endpoint": None,
        "description": "",
        "extra": json.dumps(
            {
                "resource_type": "model",
                "unique_id": "model.superset_examples.messages_channels",
                "depends_on": "ref('messages_channels')",
            },
        ),
        "fetch_values_predicate": None,
        "filter_select_enabled": False,
        "id": 27,
        "is_sqllab_view": False,
        "main_dttm_col": "ts",
        "metrics": [
            {
                "changed_on": "2022-03-27T13:21:34.298657",
                "created_on": "2022-03-27T13:21:34.248023",
                "d3format": None,
                "description": "",
                "expression": "count(*)",
                "extra": None,
                "id": 35,
                "metric_name": "cnt",
                "metric_type": "count",
                "uuid": "c4b74ceb-a19c-494a-9b90-9c68ed5bc8cb",
                "verbose_name": "count(*)",
                "warning_text": None,
            },
        ],
        "offset": 0,
        "owners": [],
        "schema": "public",
        "sql": None,
        "table_name": "messages_channels",
        "template_params": None,
        "url": "/tablemodelview/edit/27",
    },
    "show_columns": [
        "id",
        "database.database_name",
        "database.id",
        "table_name",
        "sql",
        "filter_select_enabled",
        "fetch_values_predicate",
        "schema",
        "description",
        "main_dttm_col",
        "offset",
        "default_endpoint",
        "cache_timeout",
        "is_sqllab_view",
        "template_params",
        "owners.id",
        "owners.username",
        "owners.first_name",
        "owners.last_name",
        "columns.changed_on",
        "columns.column_name",
        "columns.created_on",
        "columns.description",
        "columns.expression",
        "columns.filterable",
        "columns.groupby",
        "columns.id",
        "columns.is_active",
        "columns.extra",
        "columns.is_dttm",
        "columns.python_date_format",
        "columns.type",
        "columns.uuid",
        "columns.verbose_name",
        "metrics",
        "datasource_type",
        "url",
        "extra",
        "columns.type_generic",
        "database.backend",
    ],
    "show_title": "Show Sqla Table",
}

related_objects_response = {
    "charts": {
        "count": 1,
        "result": [{"id": 1, "slice_name": "Example chart", "viz_type": "pie"}],
    },
    "dashboards": {
        "count": 1,
        "result": [
            {
                "id": 12,
                "json_metadata": None,
                "slug": None,
                "title": "Example dashboard",
            },
        ],
    },
}


def test_get_dashboard_depends_on(mocker: MockerFixture) -> None:
    """
    Test ``get_dashboard_depends_on``.
    """
    client = mocker.MagicMock()
    client.get_dataset.return_value = dataset_response
    session = client.auth.get_session()
    session.get().json.return_value = datasets_response

    depends_on = get_dashboard_depends_on(client, dashboard_response["result"])
    assert depends_on == ["ref('messages_channels')"]


def test_get_dashboard_depends_on_no_extra(mocker: MockerFixture) -> None:
    """
    Test ``get_dashboard_depends_on`` without an extra payload.
    """
    client = mocker.MagicMock()
    modified_dataset_response = copy.deepcopy(dataset_response)
    modified_dataset_response["result"]["extra"] = None  # type: ignore
    client.get_dataset.return_value = modified_dataset_response
    session = client.auth.get_session()
    session.get().json.return_value = datasets_response

    depends_on = get_dashboard_depends_on(client, dashboard_response["result"])
    assert not depends_on


def test_get_dashboard_depends_on_invalid_extra(mocker: MockerFixture) -> None:
    """
    Test ``get_dashboard_depends_on`` when the extra payload is not JSON.
    """
    client = mocker.MagicMock()
    modified_dataset_response = copy.deepcopy(dataset_response)
    modified_dataset_response["result"]["extra"] = "{[("  # type: ignore
    client.get_dataset.return_value = modified_dataset_response
    session = client.auth.get_session()
    session.get().json.return_value = datasets_response

    depends_on = get_dashboard_depends_on(client, dashboard_response["result"])
    assert not depends_on


def test_get_chart_depends_on(mocker: MockerFixture) -> None:
    """
    Test ``get_chart_depends_on``.
    """
    client = mocker.MagicMock()
    client.get_dataset.return_value = dataset_response

    depends_on = get_chart_depends_on(client, chart_response["result"])
    assert depends_on == ["ref('messages_channels')"]


def test_get_chart_depends_on_no_extra(mocker: MockerFixture) -> None:
    """
    Test ``get_chart_depends_on`` without an extra payload.
    """
    client = mocker.MagicMock()
    modified_dataset_response = copy.deepcopy(dataset_response)
    modified_dataset_response["result"]["extra"] = None  # type: ignore
    client.get_dataset.return_value = modified_dataset_response

    depends_on = get_chart_depends_on(client, chart_response["result"])
    assert not depends_on


def test_sync_exposures(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``sync_exposures``.
    """
    root = Path("/path/to/root")
    fs.create_dir(root / "models")
    exposures = root / "models/exposures.yml"

    client = mocker.MagicMock()
    client.baseurl = URL("https://superset.example.org/")
    client.get_chart.return_value = chart_response
    client.get_dashboard.return_value = dashboard_response
    session = client.auth.get_session()
    session.get().json.return_value = related_objects_response
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.exposures.get_dashboard_depends_on",
        return_value=["ref('messages_channels')"],
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.exposures.get_chart_depends_on",
        return_value=["ref('messages_channels')"],
    )

    datasets = [dataset_response["result"]]
    sync_exposures(client, exposures, datasets)

    with open(exposures, encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == {
        "version": 2,
        "exposures": [
            {
                "name": "Example chart [chart]",
                "type": "analysis",
                "maturity": "low",
                "url": (
                    "https://superset.example.org/superset/explore/"
                    "?form_data=%7B%22slice_id%22:+1%7D"
                ),
                "description": "",
                "depends_on": ["ref('messages_channels')"],
                "owner": {"name": "admin admin", "email": "unknown"},
            },
            {
                "name": "Example dashboard [dashboard]",
                "type": "dashboard",
                "maturity": "low",
                "url": "https://superset.example.org/superset/dashboard/12/",
                "description": "",
                "depends_on": ["ref('messages_channels')"],
                "owner": {"name": "admin admin", "email": "unknown"},
            },
        ],
    }


def test_sync_exposures_no_charts_no_dashboards(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``sync_exposures`` when no dashboads use the datasets.
    """
    root = Path("/path/to/root")
    fs.create_dir(root / "models")
    exposures = root / "models/exposures.yml"

    client = mocker.MagicMock()
    client.baseurl = URL("https://superset.example.org/")
    session = client.auth.get_session()
    no_related_objects_response = copy.deepcopy(related_objects_response)
    no_related_objects_response["charts"]["result"] = []
    no_related_objects_response["dashboards"]["result"] = []
    session.get().json.return_value = no_related_objects_response

    datasets = [dataset_response["result"]]
    sync_exposures(client, exposures, datasets)

    with open(exposures, encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == {
        "version": 2,
        "exposures": [],
    }
