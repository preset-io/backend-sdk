"""
Tests for ``preset_cli.api.clients.superset``.
"""
# pylint: disable=too-many-lines, trailing-whitespace

import json
from io import BytesIO
from unittest import mock
from uuid import UUID
from zipfile import ZipFile, is_zipfile

import pytest
import yaml
from pytest_mock import MockerFixture
from requests_mock.mocker import Mocker
from yarl import URL

from preset_cli import __version__
from preset_cli.api.clients.superset import (
    RuleType,
    SupersetClient,
    convert_to_adhoc_column,
    convert_to_adhoc_metric,
    parse_html_array,
)
from preset_cli.api.operators import OneToMany
from preset_cli.auth.main import Auth
from preset_cli.exceptions import ErrorLevel, SupersetError


def test_run_query(requests_mock: Mocker) -> None:
    """
    Test the ``run_query`` method.
    """
    requests_mock.post(
        "https://superset.example.org/superset/sql_json/",
        json={
            "query_id": 2,
            "status": "success",
            "data": [{"value": 1}],
            "columns": [{"name": "value", "type": "INT", "is_date": False}],
            "selected_columns": [{"name": "value", "type": "INT", "is_date": False}],
            "expanded_columns": [],
            "query": {
                "changedOn": "2022-03-25T15:37:00.393660",
                "changed_on": "2022-03-25T15:37:00.393660",
                "dbId": 1,
                "db": "examples",
                "endDttm": 1648222620417.808,
                "errorMessage": None,
                "executedSql": "SELECT 1 AS value\nLIMIT 1001",
                "id": "IrwwY8Ky14",
                "queryId": 2,
                "limit": 1000,
                "limitingFactor": "NOT_LIMITED",
                "progress": 100,
                "rows": 1,
                "schema": "public",
                "ctas": False,
                "serverId": 2,
                "sql": "SELECT 1 AS value",
                "sqlEditorId": "1",
                "startDttm": 1648222620279.198000,
                "state": "success",
                "tab": "Untitled Query 1",
                "tempSchema": None,
                "tempTable": None,
                "userId": 1,
                "user": "admin admin",
                "resultsKey": None,
                "trackingUrl": None,
                "extra": {"cancel_query": 35121, "progress": None},
            },
        },
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    results = client.run_query(database_id=1, sql="SELECT 1 AS value", limit=10)
    assert results.to_dict() == {"value": {0: 1}}


def test_run_query_error(requests_mock: Mocker) -> None:
    """
    Test the ``run_query`` method when an error occurs.
    """
    errors = [
        {
            "message": "Only SELECT statements are allowed against this database.",
            "error_type": "DML_NOT_ALLOWED_ERROR",
            "level": "error",
            "extra": {
                "issue_codes": [
                    {
                        "code": 1022,
                        "message": (
                            "Issue 1022 - Database does not allow data manipulation."
                        ),
                    },
                ],
            },
        },
    ]
    requests_mock.post(
        "https://superset.example.org/superset/sql_json/",
        json={"errors": errors},
        headers={"Content-Type": "application/json"},
        status_code=400,
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    with pytest.raises(SupersetError) as excinfo:
        client.run_query(database_id=1, sql="SSELECT 1 AS value", limit=10)
    assert excinfo.value.errors == [
        {
            "message": "Only SELECT statements are allowed against this database.",
            "error_type": "DML_NOT_ALLOWED_ERROR",
            "level": ErrorLevel.ERROR,
            "extra": {
                "issue_codes": [
                    {
                        "code": 1022,
                        "message": "Issue 1022 - Database does not allow data manipulation.",
                    },
                ],
            },
        },
    ]


def test_convert_to_adhoc_metric(mocker: MockerFixture) -> None:
    """
    Test ``convert_to_adhoc_metric``.
    """
    mocker.patch("preset_cli.api.clients.superset.uuid4", return_value=1234)
    assert convert_to_adhoc_metric("COUNT(*)") == {
        "aggregate": None,
        "column": None,
        "expressionType": "SQL",
        "hasCustomLabel": False,
        "isNew": False,
        "label": "COUNT(*)",
        "optionName": "metric_1234",
        "sqlExpression": "COUNT(*)",
    }


def test_convert_to_adhoc_column() -> None:
    """
    Test ``convert_to_adhoc_column`..
    """
    assert convert_to_adhoc_column("UPPER(name)") == {
        "label": "UPPER(name)",
        "sqlExpression": "UPPER(name)",
    }


def test_get_data(requests_mock: Mocker) -> None:
    """
    Test the ``run_query`` method.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/27",
        json={
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
        },
    )
    requests_mock.post(
        "https://superset.example.org/api/v1/chart/data",
        json={
            "result": [
                {
                    "cache_key": "ddf314a4bb44dd7f4a2f5b880e3c2503",
                    "cached_dttm": None,
                    "cache_timeout": 300,
                    "applied_template_filters": [],
                    "annotation_data": {},
                    "error": None,
                    "is_cached": None,
                    "query": """SELECT name AS name,
       count(*) AS cnt
FROM public.messages_channels
GROUP BY name
LIMIT 10000;

""",
                    "status": "success",
                    "stacktrace": None,
                    "rowcount": 29,
                    "from_dttm": None,
                    "to_dttm": None,
                    "colnames": ["name", "cnt"],
                    "indexnames": [
                        0,
                        1,
                        2,
                        3,
                        4,
                        5,
                        6,
                        7,
                        8,
                        9,
                        10,
                        11,
                        12,
                        13,
                        14,
                        15,
                        16,
                        17,
                        18,
                        19,
                        20,
                        21,
                        22,
                        23,
                        24,
                        25,
                        26,
                        27,
                        28,
                    ],
                    "coltypes": [1, 0],
                    "data": [
                        {"name": "newsletter", "cnt": 8},
                        {"name": "support", "cnt": 114},
                        {"name": "dashboard-level-access", "cnt": 57},
                        {"name": "beginners", "cnt": 368},
                        {"name": "community-feedback", "cnt": 27},
                        {"name": "graduation", "cnt": 38},
                        {"name": "superset_stage_alerts", "cnt": 3},
                        {"name": "contributing", "cnt": 29},
                        {"name": "github-notifications", "cnt": 2975},
                        {"name": "helm-k8-deployment", "cnt": 72},
                        {"name": "globalnav_search", "cnt": 1},
                        {"name": "design", "cnt": 4},
                        {"name": "localization", "cnt": 7},
                        {"name": "commits", "cnt": 1},
                        {"name": "apache-releases", "cnt": 53},
                        {"name": "jobs", "cnt": 22},
                        {"name": "general", "cnt": 383},
                        {"name": "announcements", "cnt": 19},
                        {"name": "visualization_plugins", "cnt": 50},
                        {"name": "product_feedback", "cnt": 41},
                        {"name": "dashboard-filters", "cnt": 75},
                        {"name": "superset-champions", "cnt": 40},
                        {"name": "introductions", "cnt": 141},
                        {"name": "embedd-dashboards", "cnt": 10},
                        {"name": "cypress-tests", "cnt": 7},
                        {"name": "superset_prod_reports", "cnt": 3},
                        {"name": "feature-requests", "cnt": 22},
                        {"name": "dashboards", "cnt": 87},
                        {"name": "developers", "cnt": 27},
                    ],
                    "result_format": "json",
                    "applied_filters": [],
                    "rejected_filters": [],
                },
            ],
        },
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    results = client.get_data(27, ["cnt"], ["name"])
    assert results.to_dict() == {
        "name": {
            0: "newsletter",
            1: "support",
            2: "dashboard-level-access",
            3: "beginners",
            4: "community-feedback",
            5: "graduation",
            6: "superset_stage_alerts",
            7: "contributing",
            8: "github-notifications",
            9: "helm-k8-deployment",
            10: "globalnav_search",
            11: "design",
            12: "localization",
            13: "commits",
            14: "apache-releases",
            15: "jobs",
            16: "general",
            17: "announcements",
            18: "visualization_plugins",
            19: "product_feedback",
            20: "dashboard-filters",
            21: "superset-champions",
            22: "introductions",
            23: "embedd-dashboards",
            24: "cypress-tests",
            25: "superset_prod_reports",
            26: "feature-requests",
            27: "dashboards",
            28: "developers",
        },
        "cnt": {
            0: 8,
            1: 114,
            2: 57,
            3: 368,
            4: 27,
            5: 38,
            6: 3,
            7: 29,
            8: 2975,
            9: 72,
            10: 1,
            11: 4,
            12: 7,
            13: 1,
            14: 53,
            15: 22,
            16: 383,
            17: 19,
            18: 50,
            19: 41,
            20: 75,
            21: 40,
            22: 141,
            23: 10,
            24: 7,
            25: 3,
            26: 22,
            27: 87,
            28: 27,
        },
    }


def test_get_data_parameters(mocker: MockerFixture) -> None:
    """
    Test different parameters passed to ``get_data``.
    """
    auth = mocker.MagicMock()
    session = auth.get_session()
    session.get().json.return_value = {
        "result": {
            "columns": [],
            "metrics": [],
        },
    }
    session.post().json.return_value = {
        "result": [
            {
                "data": [{"a": 1}],
            },
        ],
    }
    mocker.patch("preset_cli.api.clients.superset.uuid4", return_value=1234)

    client = SupersetClient("https://superset.example.org/", auth)
    client.get_data(
        27,
        ["cnt"],
        ["name"],
        time_column="ts",
        is_timeseries=True,
        granularity="P1M",
    )

    session.post.assert_has_calls(
        [
            mock.call(),
            mock.call(
                URL("https://superset.example.org/api/v1/chart/data"),
                json={
                    "datasource": {"id": 27, "type": "table"},
                    "force": False,
                    "queries": [
                        {
                            "annotation_layers": [],
                            "applied_time_extras": {},
                            "columns": [{"label": "name", "sqlExpression": "name"}],
                            "custom_form_data": {},
                            "custom_params": {},
                            "extras": {
                                "having": "",
                                "having_druid": [],
                                "time_grain_sqla": "P1M",
                                "where": "",
                            },
                            "filters": [],
                            "granularity": "ts",
                            "is_timeseries": True,
                            "metrics": [
                                {
                                    "aggregate": None,
                                    "column": None,
                                    "expressionType": "SQL",
                                    "hasCustomLabel": False,
                                    "isNew": False,
                                    "label": "cnt",
                                    "optionName": "metric_1234",
                                    "sqlExpression": "cnt",
                                },
                            ],
                            "order_desc": True,
                            "orderby": [],
                            "row_limit": 10000,
                            "time_range": "No filter",
                            "timeseries_limit": 0,
                            "url_params": {},
                        },
                    ],
                    "result_format": "json",
                    "result_type": "full",
                },
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": f"Apache Superset Client ({__version__})",
                    "Referer": "https://superset.example.org/",
                },
            ),
            mock.call().ok.__bool__(),
            mock.call().json(),
        ],
    )


def test_get_data_time_column_error(requests_mock: Mocker) -> None:
    """
    Test when the time column is ambiguous in ``get_data``.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/27",
        json={
            "result": {
                "columns": [
                    {"column_name": "event_time", "is_dttm": True},
                    {"column_name": "server_time", "is_dttm": True},
                ],
                "metrics": [],
            },
        },
    )
    requests_mock.post(
        "https://superset.example.org/api/v1/chart/data",
        json={
            "result": [
                {
                    "data": [{"a": 1}],
                },
            ],
        },
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    with pytest.raises(Exception) as excinfo:
        client.get_data(27, ["cnt"], ["name"])
    assert str(excinfo.value) == (
        "Unable to determine time column, please pass `time_series` "
        "as one of: event_time, server_time"
    )


def test_get_data_error(requests_mock: Mocker) -> None:
    """
    Test ``get_data`` with a generic error.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/27",
        json={
            "result": {
                "columns": [],
                "metrics": [],
            },
        },
    )
    requests_mock.post(
        "https://superset.example.org/api/v1/chart/data",
        json={"errors": [{"message": "An error occurred"}]},
        headers={"Content-Type": "application/json"},
        status_code=500,
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    with pytest.raises(SupersetError) as excinfo:
        client.get_data(27, ["cnt"], ["name"], time_column="ts")
    assert excinfo.value.errors == [{"message": "An error occurred"}]


def test_get_resource(requests_mock: Mocker) -> None:
    """
    Test the generic ``get_resource`` method.
    """
    # the payload schema is irrelevant, since it's passed through unmodified
    requests_mock.get(
        "https://superset.example.org/api/v1/database/1",
        json={"Hello": "world"},
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    response = client.get_resource("database", 1)
    assert response == {"Hello": "world"}
    assert (
        requests_mock.last_request.headers["Referer"] == "https://superset.example.org/"
    )


def test_get_resources(requests_mock: Mocker) -> None:
    """
    Test the generic ``get_resources`` method.
    """
    # the payload schema is irrelevant, since it's passed through unmodified
    requests_mock.get(
        "https://superset.example.org/api/v1/database/?q="
        "(filters:!(),order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:0,page_size:100)",
        json={"result": [1]},
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/database/?q="
        "(filters:!(),order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:1,page_size:100)",
        json={"result": [2]},
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/database/?q="
        "(filters:!(),order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:2,page_size:100)",
        json={"result": []},
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    response = client.get_resources("database")
    assert response == [1, 2]
    assert (
        requests_mock.last_request.headers["Referer"] == "https://superset.example.org/"
    )


def test_get_resources_filtered_equal(requests_mock: Mocker) -> None:
    """
    Test the generic ``get_resources`` method with an equal filter.
    """
    # the payload schema is irrelevant, since it's passed through unmodified
    requests_mock.get(
        "https://superset.example.org/api/v1/database/?q="
        "(filters:!((col:database_name,opr:eq,value:my_db)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:0,page_size:100)",
        json={"result": [{"Hello": "world"}]},
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/database/?q="
        "(filters:!((col:database_name,opr:eq,value:my_db)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:1,page_size:100)",
        json={"result": []},
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    response = client.get_resources("database", database_name="my_db")
    assert response == [{"Hello": "world"}]
    assert (
        requests_mock.last_request.headers["Referer"] == "https://superset.example.org/"
    )


def test_get_resources_filtered_one_to_many(requests_mock: Mocker) -> None:
    """
    Test the generic ``get_resources`` method with a one-to-many filter.
    """
    # the payload schema is irrelevant, since it's passed through unmodified
    requests_mock.get(
        "https://superset.example.org/api/v1/database/?q="
        "(filters:!((col:database,opr:rel_o_m,value:1)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:0,page_size:100)",
        json={"result": [{"Hello": "world"}]},
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/database/?q="
        "(filters:!((col:database,opr:rel_o_m,value:1)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:1,page_size:100)",
        json={"result": []},
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    response = client.get_resources("database", database=OneToMany(1))
    assert response == [{"Hello": "world"}]
    assert (
        requests_mock.last_request.headers["Referer"] == "https://superset.example.org/"
    )


def test_create_resource(requests_mock: Mocker) -> None:
    """
    Test the generic ``create_resource`` method.
    """
    requests_mock.post(
        "https://superset.example.org/api/v1/database/",
        json={"Hello": "world"},
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    response = client.create_resource(
        "database",
        database_name="my_db",
        sqlalchemy_uri="gsheets://",
    )
    assert response == {"Hello": "world"}
    assert (
        requests_mock.last_request.headers["Referer"] == "https://superset.example.org/"
    )
    assert requests_mock.last_request.json() == {
        "database_name": "my_db",
        "sqlalchemy_uri": "gsheets://",
    }


def test_update_resource(requests_mock: Mocker) -> None:
    """
    Test the generic ``update_resource`` method.
    """
    requests_mock.put(
        "https://superset.example.org/api/v1/database/1",
        json={"Hello": "world"},
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    response = client.update_resource(
        resource_name="database",
        resource_id=1,
        database_name="my_other_db",
    )
    assert response == {"Hello": "world"}
    assert (
        requests_mock.last_request.headers["Referer"] == "https://superset.example.org/"
    )
    assert requests_mock.last_request.json() == {
        "database_name": "my_other_db",
    }


def test_update_resource_with_query_args(requests_mock: Mocker) -> None:
    """
    Test the generic ``update_resource`` method.
    """
    requests_mock.put(
        "https://superset.example.org/api/v1/database/1?override_columns=true",
        json={"Hello": "world"},
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    response = client.update_resource(
        resource_name="database",
        resource_id=1,
        query_args={"override_columns": "true"},
        database_name="my_other_db",
    )
    assert response == {"Hello": "world"}
    assert (
        requests_mock.last_request.headers["Referer"] == "https://superset.example.org/"
    )
    assert requests_mock.last_request.json() == {
        "database_name": "my_other_db",
    }


def test_get_database(mocker: MockerFixture) -> None:
    """
    Test the ``get_database`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    get_resource = mocker.patch.object(client, "get_resource")

    client.get_database(1)
    get_resource.assert_called_with("database", 1)


def test_get_databases(mocker: MockerFixture) -> None:
    """
    Test the ``get_databases`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    get_resources = mocker.patch.object(client, "get_resources")

    client.get_databases()
    get_resources.assert_called_with("database")
    client.get_databases(database_name="my_db")
    get_resources.assert_called_with("database", database_name="my_db")


def test_create_database(mocker: MockerFixture) -> None:
    """
    Test the ``create_database`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    create_resource = mocker.patch.object(client, "create_resource")

    client.create_database(database_name="my_db", sqlalchemy_uri="gsheets://")
    create_resource.assert_called_with(
        "database",
        database_name="my_db",
        sqlalchemy_uri="gsheets://",
    )


def test_update_database(mocker: MockerFixture) -> None:
    """
    Test the ``update_database`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    update_resource = mocker.patch.object(client, "update_resource")

    client.update_database(1, database_name="my_other_db")
    update_resource.assert_called_with("database", 1, database_name="my_other_db")


def test_get_dataset(mocker: MockerFixture) -> None:
    """
    Test the ``get_dataset`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    get_resource = mocker.patch.object(client, "get_resource")

    client.get_dataset(1)
    get_resource.assert_called_with("dataset", 1)


def test_get_datasets(mocker: MockerFixture) -> None:
    """
    Test the ``get_datasets`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    get_resources = mocker.patch.object(client, "get_resources")

    client.get_datasets()
    get_resources.assert_called_with("dataset")
    client.get_datasets(dataset_name="my_db")
    get_resources.assert_called_with("dataset", dataset_name="my_db")


def test_create_dataset(mocker: MockerFixture) -> None:
    """
    Test the ``create_dataset`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    create_resource = mocker.patch.object(client, "create_resource")

    client.create_dataset(dataset_name="my_db", sqlalchemy_uri="gsheets://")
    create_resource.assert_called_with(
        "dataset",
        dataset_name="my_db",
        sqlalchemy_uri="gsheets://",
    )


def test_update_dataset(mocker: MockerFixture) -> None:
    """
    Test the ``update_dataset`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    update_resource = mocker.patch.object(client, "update_resource")

    client.update_dataset(1, dataset_name="my_other_db")
    update_resource.assert_called_with(
        "dataset", 1, {"override_columns": "true"}, dataset_name="my_other_db",
    )


def test_get_chart(mocker: MockerFixture) -> None:
    """
    Test the ``get_chart`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    get_resource = mocker.patch.object(client, "get_resource")

    client.get_chart(1)
    get_resource.assert_called_with("chart", 1)


def test_get_charts(mocker: MockerFixture) -> None:
    """
    Test the ``get_charts`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    get_resources = mocker.patch.object(client, "get_resources")

    client.get_charts()
    get_resources.assert_called_with("chart")
    client.get_charts(chart_name="my_db")
    get_resources.assert_called_with("chart", chart_name="my_db")


def test_get_dashboard(mocker: MockerFixture) -> None:
    """
    Test the ``get_dashboard`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    get_resource = mocker.patch.object(client, "get_resource")

    client.get_dashboard(1)
    get_resource.assert_called_with("dashboard", 1)


def test_get_dashboards(mocker: MockerFixture) -> None:
    """
    Test the ``get_dashboards`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    get_resources = mocker.patch.object(client, "get_resources")

    client.get_dashboards()
    get_resources.assert_called_with("dashboard")
    client.get_dashboards(dashboard_name="my_db")
    get_resources.assert_called_with("dashboard", dashboard_name="my_db")


def test_create_dashboard(mocker: MockerFixture) -> None:
    """
    Test the ``create_dashboard`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    create_resource = mocker.patch.object(client, "create_resource")

    client.create_dashboard(dashboard_name="my_db", sqlalchemy_uri="gsheets://")
    create_resource.assert_called_with(
        "dashboard",
        dashboard_name="my_db",
        sqlalchemy_uri="gsheets://",
    )


def test_update_dashboard(mocker: MockerFixture) -> None:
    """
    Test the ``update_dashboard`` method.
    """
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    update_resource = mocker.patch.object(client, "update_resource")

    client.update_dashboard(1, dashboard_name="my_other_db")
    update_resource.assert_called_with("dashboard", 1, dashboard_name="my_other_db")


def test_export_zip(requests_mock: Mocker) -> None:
    """
    Test the ``export_zip`` method.
    """
    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        with bundle.open("test.txt", "w") as output:
            output.write(b"Hello!")
    buf.seek(0)

    requests_mock.get(
        "https://superset.example.org/api/v1/database/export/?q=%21%281%2C2%2C3%29",
        content=buf.getvalue(),
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    response = client.export_zip("database", [1, 2, 3])
    assert is_zipfile(response)
    with ZipFile(response) as bundle:
        assert bundle.namelist() == ["test.txt"]
        assert bundle.read("test.txt") == b"Hello!"

    assert (
        requests_mock.last_request.headers["Referer"] == "https://superset.example.org/"
    )


def test_export_zip_pagination(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test the ``export_zip`` method with pagination.
    """
    page1 = BytesIO()
    with ZipFile(page1, "w") as bundle:
        with bundle.open("test1.txt", "w") as output:
            output.write(b"Hello!")
    page1.seek(0)

    page2 = BytesIO()
    with ZipFile(page2, "w") as bundle:
        with bundle.open("test2.txt", "w") as output:
            output.write(b"Bye!")
    page2.seek(0)

    requests_mock.get(
        "https://superset.example.org/api/v1/database/export/?q=%21%281%2C2%29",
        content=page1.getvalue(),
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/database/export/?q=%21%283%29",
        content=page2.getvalue(),
    )

    mocker.patch("preset_cli.api.clients.superset.MAX_IDS_IN_EXPORT", new=2)

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    response = client.export_zip("database", [1, 2, 3])
    assert is_zipfile(response)
    with ZipFile(response) as bundle:
        assert bundle.namelist() == ["test1.txt", "test2.txt"]

    assert (
        requests_mock.last_request.headers["Referer"] == "https://superset.example.org/"
    )


def test_export_zip_error(requests_mock: Mocker) -> None:
    """
    Test the ``export_zip`` method when an error occurs.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/database/export/?q=%21%281%2C2%2C3%29",
        json={"errors": [{"message": "An error occurred"}]},
        headers={"Content-Type": "application/json"},
        status_code=500,
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    with pytest.raises(SupersetError) as excinfo:
        client.export_zip("database", [1, 2, 3])
    assert excinfo.value.errors == [{"message": "An error occurred"}]
    assert (
        requests_mock.last_request.headers["Referer"] == "https://superset.example.org/"
    )


def test_import_zip(requests_mock: Mocker) -> None:
    """
    Test the ``import_zip`` method.
    """
    requests_mock.post(
        "https://superset.example.org/api/v1/database/import/",
        json={"message": "OK"},
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    data = BytesIO("I'm a ZIP".encode("utf-8"))
    response = client.import_zip("database", data, overwrite=True)
    assert response is True
    assert (
        requests_mock.last_request.headers["Referer"] == "https://superset.example.org/"
    )
    assert requests_mock.last_request.headers["Accept"] == "application/json"

    boundary = (
        requests_mock.last_request.headers["Content-type"]
        .split(";")[1]
        .split("=")[1]
        .strip()
    )
    assert requests_mock.last_request.text == (
        f'--{boundary}\r\nContent-Disposition: form-data; name="overwrite"\r\n\r\n'
        f'true\r\n--{boundary}\r\nContent-Disposition: form-data; name="formData"; '
        f'filename="formData"\r\n\r\nI\'m a ZIP\r\n--{boundary}--\r\n'
    )


def test_import_zip_error(requests_mock: Mocker) -> None:
    """
    Test the ``import_zip`` method when an error occurs.
    """
    requests_mock.post(
        "https://superset.example.org/api/v1/database/import/",
        json={"errors": [{"message": "An error occurred"}]},
        headers={"Content-Type": "application/json"},
        status_code=500,
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    data = BytesIO("I'm a ZIP".encode("utf-8"))
    with pytest.raises(SupersetError) as excinfo:
        client.import_zip("database", data, overwrite=True)
    assert excinfo.value.errors == [{"message": "An error occurred"}]
    assert (
        requests_mock.last_request.headers["Referer"] == "https://superset.example.org/"
    )


def test_export_users(requests_mock: Mocker) -> None:
    """
    Test ``export_users``.
    """
    requests_mock.get("https://superset.example.org/users/list/")
    requests_mock.get(
        "https://superset.example.org/users/list/?psize_UserDBModelView=100&page_UserDBModelView=0",
        text="""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
  </head>
  <body>
    <table></table>
    <table>
      <tr>
        <th></th>
        <th>First Name</th>
        <th>Last Name</th>
        <th>User Name</th>
        <th>Email</th>
        <th>Is Active?</th>
        <th>Role</th>
      </tr>
      <tr>
        <td><a href="/users/edit/1">Edit</a></td>
        <td>Alice</td>
        <td>Doe</td>
        <td>adoe</td>
        <td>adoe@example.com</td>
        <td>True</td>
        <td>[Admin]</td>
      </tr>
      <tr>
        <td><a href="/users/edit/2">Edit</a></td>
        <td>Bob</td>
        <td>Doe</td>
        <td>bdoe</td>
        <td>bdoe@example.com</td>
        <td>True</td>
        <td>[Alpha]</td>
      </tr>
    </table>
  </body>
</html>
        """,
    )
    requests_mock.get(
        "https://superset.example.org/users/list/?psize_UserDBModelView=100&page_UserDBModelView=1",
        text="""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
  </head>
  <body>
    <table></table>
    <table>
      <tr>
        <th></th>
        <th>First Name</th>
        <th>Last Name</th>
        <th>User Name</th>
        <th>Email</th>
        <th>Is Active?</th>
        <th>Role</th>
      </tr>
    </table>
  </body>
</html>
        """,
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    assert list(client.export_users()) == [
        {
            "id": 1,
            "first_name": "Alice",
            "last_name": "Doe",
            "username": "adoe",
            "email": "adoe@example.com",
            "role": ["Admin"],
        },
        {
            "id": 2,
            "first_name": "Bob",
            "last_name": "Doe",
            "username": "bdoe",
            "email": "bdoe@example.com",
            "role": ["Alpha"],
        },
    ]


def test_export_users_preset(requests_mock: Mocker) -> None:
    """
    Test ``export_users``.
    """
    requests_mock.get("https://superset.example.org/users/list/", status_code=404)
    requests_mock.get(
        "https://manage.app.preset.io/api/v1/teams/",
        json={
            "payload": [{"name": "team1"}],
        },
    )
    requests_mock.get(
        "https://manage.app.preset.io/api/v1/teams/team1/workspaces/",
        json={
            "payload": [{"id": 1, "hostname": "superset.example.org"}],
        },
    )
    requests_mock.get(
        "https://manage.app.preset.io/api/v1/teams/team1/workspaces/1/memberships",
        json={
            "payload": [
                {
                    "user": {
                        "username": "adoe",
                        "first_name": "Alice",
                        "last_name": "Doe",
                        "email": "adoe@example.com",
                    },
                },
                {
                    "user": {
                        "username": "bdoe",
                        "first_name": "Bob",
                        "last_name": "Doe",
                        "email": "bdoe@example.com",
                    },
                },
            ],
        },
    )
    requests_mock.get(
        "https://superset.example.org/roles/add",
        text="""
<select id="user">
    <option value="1">Alice Doe</option>
    <option value="2">Bob Doe</option>
</select>
    """,
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    assert list(client.export_users()) == [
        {
            "id": 1,
            "first_name": "Alice",
            "last_name": "Doe",
            "username": "adoe",
            "email": "adoe@example.com",
            "role": [],
        },
        {
            "id": 2,
            "first_name": "Bob",
            "last_name": "Doe",
            "username": "bdoe",
            "email": "bdoe@example.com",
            "role": [],
        },
    ]


def test_export_rls(requests_mock: Mocker) -> None:
    """
    Test ``export_rls``.
    """
    requests_mock.get(
        (
            "https://superset.example.org/rowlevelsecurityfiltersmodelview/list/?"
            "psize_RowLevelSecurityFiltersModelView=100&"
            "page_RowLevelSecurityFiltersModelView=0"
        ),
        text="""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
  </head>
  <body>
    <table></table>
    <table>
      <tr>
        <th></th>
        <th>Name</th>
        <th>Filter Type</th>
        <th>Tables</th>
        <th>Roles</th>
        <th>Clause</th>
        <th>Creator</th>
        <th>Modified</th>
      </tr>
      <tr>
        <td><input id="1" /></td>
        <td>My rule</td>
        <td>Regular</td>
        <td>[main.test_table]</td>
        <td>client_id = 9</td>
        <td>admin admin</td>
        <td>35 minutes ago</td>
      </tr>
    </table>
  </body>
</html>
        """,
    )
    requests_mock.get(
        (
            "https://superset.example.org/rowlevelsecurityfiltersmodelview/list/?"
            "psize_RowLevelSecurityFiltersModelView=100&"
            "page_RowLevelSecurityFiltersModelView=1"
        ),
        text="""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
  </head>
  <body>
    <table></table>
    <table>
      <tr>
        <th></th>
        <th>Name</th>
        <th>Filter Type</th>
        <th>Tables</th>
        <th>Roles</th>
        <th>Clause</th>
        <th>Creator</th>
        <th>Modified</th>
      </tr>
    </table>
  </body>
</html>
        """,
    )
    requests_mock.get(
        "https://superset.example.org/rowlevelsecurityfiltersmodelview/show/1",
        text="""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
  </head>
  <body>
    <table>
      <tr><th>Name</th><td>My Rule</td></tr>
      <tr><th>Description</th><td>This is a rule. There are many others like it, but this one is mine.</td></tr>
      <tr><th>Filter Type</th><td>Regular</td></tr>
      <tr><th>Tables</th><td>[main.test_table]</td></tr>
      <tr><th>Roles</th><td>[Gamma]</td></tr>
      <tr><th>Group Key</th><td>department</td></tr>
      <tr><th>Clause</th><td>client_id = 9</td></tr>
    </table>
  </body>
</html>
        """,
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    assert list(client.export_rls()) == [
        {
            "name": "My Rule",
            "description": "This is a rule. There are many others like it, but this one is mine.",
            "filter_type": "Regular",
            "tables": ["main.test_table"],
            "roles": ["Gamma"],
            "group_key": "department",
            "clause": "client_id = 9",
        },
    ]


def test_export_ownership(mocker: MockerFixture) -> None:
    """
    Test ``export_ownership``.
    """
    mocker.patch.object(
        SupersetClient,
        "export_users",
        return_value=[
            {"id": 1, "email": "admin@example.com"},
            {"id": 2, "email": "adoe@example.com"},
        ],
    )
    mocker.patch.object(
        SupersetClient,
        "get_uuids",
        return_value={
            1: UUID("e0d20af0-cef9-4bdb-80b4-745827f441bf"),
        },
    )
    mocker.patch.object(
        SupersetClient,
        "get_resources",
        return_value=[
            {
                "slice_name": "My chart",
                "id": 1,
                "owners": [{"id": 1}, {"id": 2}],
            },
        ],
    )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    assert list(client.export_ownership("chart")) == [
        {
            "name": "My chart",
            "owners": ["admin@example.com", "adoe@example.com"],
            "uuid": UUID("e0d20af0-cef9-4bdb-80b4-745827f441bf"),
        },
    ]


def test_get_uuids(requests_mock: Mocker) -> None:
    """
    Test the ``get_uuids`` method.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/chart/?q="
        "(filters:!(),order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:0,page_size:100)",
        json={"result": [{"id": 1}, {"id": 2}, {"id": 3}]},
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/chart/?q="
        "(filters:!(),order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:1,page_size:100)",
        json={"result": []},
    )

    uuids = [
        "c9d100b8-4fa5-4b7a-8a71-9803b5343674",
        "2826c33b-7d13-4830-865e-d62630b20dee",
        "0ac7464e-14e7-4c54-ab22-7cbd4536fccc",
    ]
    for i in range(3):
        name = f"chart_export/chart/chart{i+1}.yaml"
        uuid = uuids[i]
        buf = BytesIO()
        with ZipFile(buf, "w") as bundle:
            with bundle.open("metadata.yaml", "w") as output:
                output.write(b"Hello!")
            with bundle.open(name, "w") as output:
                output.write(yaml.dump({"uuid": uuid}).encode())
        buf.seek(0)

        requests_mock.get(
            f"https://superset.example.org/api/v1/chart/export/?q=%21%28{i+1}%29",
            content=buf.getvalue(),
        )

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    assert client.get_uuids("chart") == {
        1: UUID("c9d100b8-4fa5-4b7a-8a71-9803b5343674"),
        2: UUID("2826c33b-7d13-4830-865e-d62630b20dee"),
        3: UUID("0ac7464e-14e7-4c54-ab22-7cbd4536fccc"),
    }


def test_parse_html_array() -> None:
    """
    Test ``parse_html_array``.
    """
    assert (
        parse_html_array(
            """
                    
                      [main.test_table]
                    
                    """,
        )
        == ["main.test_table"]
    )
    assert (
        parse_html_array(
            """
                
                    
                        
                            main.sales
                        
                            public.FCC 2018 Survey
""",
        )
        == ["main.sales", "public.FCC 2018 Survey"]
    )


def test_import_rls(requests_mock: Mocker) -> None:
    """
    Test the ``import_rls`` method.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:schema,opr:eq,value:main),"
        "(col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:0,page_size:100)",
        json={"result": [{"id": 1}]},
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:schema,opr:eq,value:main),"
        "(col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:1,page_size:100)",
        json={"result": []},
    )
    requests_mock.get(
        "https://superset.example.org/roles/list/?_flt_0_name=Gamma",
        text="""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
  </head>
  <body>
    <table></table>
    <table>
      <tr>
        <td></td>
        <td>Name</td>
      </tr>
      <tr>
        <td><input id="1" /></td>
        <td>Gamma</td>
      </tr>
    </table>
  </body>
</html>
        """,
    )
    requests_mock.post(
        "https://superset.example.org/rowlevelsecurityfiltersmodelview/add",
    )

    rls: RuleType = {
        "clause": "client_id = 9",
        "description": "Rule description",
        "filter_type": "Regular",
        "group_key": "department",
        "name": "Rule name",
        "roles": ["Gamma"],
        "tables": ["main.test_table"],
    }

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    client.import_rls(rls)

    assert requests_mock.last_request.text == (
        "name=Rule+name&"
        "description=Rule+description&"
        "filter_type=Regular&"
        "tables=1&"
        "roles=1&"
        "group_key=department&"
        "clause=client_id+%3D+9"
    )


def test_import_rls_no_schema(requests_mock: Mocker) -> None:
    """
    Test the ``import_rls`` method when the table has no schema.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:0,page_size:100)",
        json={"result": [{"id": 1}]},
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:1,page_size:100)",
        json={"result": []},
    )
    requests_mock.get(
        "https://superset.example.org/roles/list/?_flt_0_name=Gamma",
        text="""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
  </head>
  <body>
    <table></table>
    <table>
      <tr>
        <td></td>
        <td>Name</td>
      </tr>
      <tr>
        <td><input id="1" /></td>
        <td>Gamma</td>
      </tr>
    </table>
  </body>
</html>
        """,
    )
    requests_mock.post(
        "https://superset.example.org/rowlevelsecurityfiltersmodelview/add",
    )

    rls: RuleType = {
        "clause": "client_id = 9",
        "description": "Rule description",
        "filter_type": "Regular",
        "group_key": "department",
        "name": "Rule name",
        "roles": ["Gamma"],
        "tables": ["test_table"],
    }

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    client.import_rls(rls)

    assert requests_mock.last_request.text == (
        "name=Rule+name&"
        "description=Rule+description&"
        "filter_type=Regular&"
        "tables=1&"
        "roles=1&"
        "group_key=department&"
        "clause=client_id+%3D+9"
    )


def test_import_rls_no_table(requests_mock: Mocker) -> None:
    """
    Test the ``import_rls`` method when no tables are found.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:schema,opr:eq,value:main),"
        "(col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:0,page_size:100)",
        json={"result": []},
    )

    rls: RuleType = {
        "clause": "client_id = 9",
        "description": "Rule description",
        "filter_type": "Regular",
        "group_key": "department",
        "name": "Rule name",
        "roles": ["Gamma"],
        "tables": ["main.test_table"],
    }

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    with pytest.raises(Exception) as excinfo:
        client.import_rls(rls)
    assert str(excinfo.value) == "Cannot find table: main.test_table"


def test_import_rls_multiple_tables(requests_mock: Mocker) -> None:
    """
    Test the ``import_rls`` method when multiple tables are found.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:schema,opr:eq,value:main),"
        "(col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:0,page_size:100)",
        json={"result": [{"id": 1}, {"id": 2}]},
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:schema,opr:eq,value:main),"
        "(col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:1,page_size:100)",
        json={"result": []},
    )

    rls: RuleType = {
        "clause": "client_id = 9",
        "description": "Rule description",
        "filter_type": "Regular",
        "group_key": "department",
        "name": "Rule name",
        "roles": ["Gamma"],
        "tables": ["main.test_table"],
    }

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    with pytest.raises(Exception) as excinfo:
        client.import_rls(rls)
    assert str(excinfo.value) == "More than one table found: main.test_table"


def test_import_rls_no_role(requests_mock: Mocker) -> None:
    """
    Test the ``import_rls`` method when no role is found.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:schema,opr:eq,value:main),"
        "(col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:0,page_size:100)",
        json={"result": [{"id": 1}]},
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:schema,opr:eq,value:main),"
        "(col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:1,page_size:100)",
        json={"result": []},
    )
    requests_mock.get(
        "https://superset.example.org/roles/list/?_flt_0_name=Gamma",
        text="""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
  </head>
  <body>
    <table></table>
    <table>
      <tr>
        <td></td>
        <td>Name</td>
      </tr>
    </table>
  </body>
</html>
        """,
    )

    rls: RuleType = {
        "clause": "client_id = 9",
        "description": "Rule description",
        "filter_type": "Regular",
        "group_key": "department",
        "name": "Rule name",
        "roles": ["Gamma"],
        "tables": ["main.test_table"],
    }

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    with pytest.raises(Exception) as excinfo:
        client.import_rls(rls)
    assert str(excinfo.value) == "Cannot find role: Gamma"


def test_import_rls_multiple_roles(requests_mock: Mocker) -> None:
    """
    Test the ``import_rls`` method when multiple roles are found.

    Should not happen.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:schema,opr:eq,value:main),"
        "(col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:0,page_size:100)",
        json={"result": [{"id": 1}]},
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:schema,opr:eq,value:main),"
        "(col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:1,page_size:100)",
        json={"result": []},
    )
    requests_mock.get(
        "https://superset.example.org/roles/list/?_flt_0_name=Gamma",
        text="""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
  </head>
  <body>
    <table></table>
    <table>
      <tr>
        <td></td>
        <td>Name</td>
      </tr>
      <tr>
        <td><input id="1" /></td>
        <td>Gamma</td>
      </tr>
      <tr>
        <td><input id="2" /></td>
        <td>Gamma</td>
      </tr>
    </table>
  </body>
</html>
        """,
    )

    rls: RuleType = {
        "clause": "client_id = 9",
        "description": "Rule description",
        "filter_type": "Regular",
        "group_key": "department",
        "name": "Rule name",
        "roles": ["Gamma"],
        "tables": ["main.test_table"],
    }

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)

    with pytest.raises(Exception) as excinfo:
        client.import_rls(rls)
    assert str(excinfo.value) == "More than one role found: Gamma"


def test_import_rls_anchor_role_id(requests_mock: Mocker) -> None:
    """
    Test the ``import_rls`` method when the role ID is in an anchor tag.
    """
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:schema,opr:eq,value:main),"
        "(col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:0,page_size:100)",
        json={"result": [{"id": 1}]},
    )
    requests_mock.get(
        "https://superset.example.org/api/v1/dataset/?q="
        "(filters:!((col:schema,opr:eq,value:main),"
        "(col:table_name,opr:eq,value:test_table)),"
        "order_column:changed_on_delta_humanized,"
        "order_direction:desc,page:1,page_size:100)",
        json={"result": []},
    )
    requests_mock.get(
        "https://superset.example.org/roles/list/?_flt_0_name=Gamma",
        text="""
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8">
  </head>
  <body>
    <table></table>
    <table>
      <tr>
        <td></td>
        <td>Name</td>
      </tr>
      <tr>
        <td><a href="/roles/edit/1">Edit</a></td>
        <td>Gamma</td>
      </tr>
    </table>
  </body>
</html>
        """,
    )
    requests_mock.post(
        "https://superset.example.org/rowlevelsecurityfiltersmodelview/add",
    )

    rls: RuleType = {
        "clause": "client_id = 9",
        "description": "Rule description",
        "filter_type": "Regular",
        "group_key": "department",
        "name": "Rule name",
        "roles": ["Gamma"],
        "tables": ["main.test_table"],
    }

    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    client.import_rls(rls)

    assert requests_mock.last_request.text == (
        "name=Rule+name&"
        "description=Rule+description&"
        "filter_type=Regular&"
        "tables=1&"
        "roles=1&"
        "group_key=department&"
        "clause=client_id+%3D+9"
    )


def test_import_ownership(mocker: MockerFixture, requests_mock: Mocker) -> None:
    """
    Test the ``import_ownership`` method.
    """
    requests_mock.put("https://superset.example.org/api/v1/dataset/1", json={})
    mocker.patch.object(
        SupersetClient,
        "export_users",
        return_value=[
            {"id": 1, "email": "admin@example.com"},
            {"id": 2, "email": "adoe@example.com"},
        ],
    )
    mocker.patch.object(
        SupersetClient,
        "get_uuids",
        return_value={
            1: UUID("e0d20af0-cef9-4bdb-80b4-745827f441bf"),
        },
    )
    auth = Auth()
    client = SupersetClient("https://superset.example.org/", auth)
    client.import_ownership(
        "dataset",
        [
            {
                "name": "test_table",
                "owners": ["admin@example.com", "adoe@example.com"],
                "uuid": "e0d20af0-cef9-4bdb-80b4-745827f441bf",
            },
        ],
    )

    assert requests_mock.last_request.json() == {"owners": [1, 2]}
