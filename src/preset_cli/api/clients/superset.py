# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
A simple client for running SQL queries against Superset:

    >>> from yarl import URL
    >>> from preset_cli.api.clients.superset import SupersetClient
    >>> from preset_cli.auth.main import UsernamePasswordAuth
    >>> url = URL("http://localhost:8088/")
    >>> auth = UsernamePasswordAuth(url, "admin", "admin")  # doctest: +SKIP
    >>> client = SupersetClient(url, auth)  # doctest: +SKIP
    >>> sql = "SELECT platform, rank FROM video_game_sales LIMIT 2"
    >>> print(client.run_query(database_id=1, sql=sql))  # doctest: +SKIP
      platform  rank
    0      Wii     1
    1      NES     2

Data is returned in a Pandas Dataframe.

"""

import json
import uuid
from datetime import datetime
from enum import IntEnum
from io import BytesIO
from typing import Any, Dict, List, Literal, Optional, TypedDict, Union
from uuid import uuid4

import pandas as pd
import prison
from yarl import URL

from preset_cli import __version__
from preset_cli.api.operators import Equal, Operator
from preset_cli.auth.main import Auth
from preset_cli.lib import validate_response


class GenericDataType(IntEnum):
    """
    Generic database column type that fits both frontend and backend.
    """

    NUMERIC = 0
    STRING = 1
    TEMPORAL = 2
    BOOLEAN = 3


class AdhocMetricColumn(TypedDict, total=False):
    """
    Schema for an adhoc metric column.
    """

    column_name: Optional[str]
    description: Optional[str]
    expression: Optional[str]
    filterable: bool
    groupby: bool
    id: int
    is_dttm: bool
    python_date_format: Optional[str]
    type: str
    type_generic: GenericDataType
    verbose_name: Optional[str]


class MetricType(TypedDict):
    """
    Schema for an adhoc metric in the Chart API.
    """

    aggregate: Optional[str]
    column: Optional[AdhocMetricColumn]
    expressionType: Literal["SIMPLE", "SQL"]
    hasCustomLabel: Optional[bool]
    label: Optional[str]
    sqlExpression: Optional[str]
    isNew: bool
    optionName: str


def convert_to_adhoc_metric(expression: str) -> MetricType:
    """
    Convert an adhoc metric to an object.
    """
    return {
        "aggregate": None,
        "column": None,
        "expressionType": "SQL",
        "hasCustomLabel": False,
        "isNew": False,
        "label": expression,
        "optionName": f"metric_{uuid4()}",
        "sqlExpression": expression,
    }


class ColumnType(TypedDict):
    """
    Schema for an adhoc column in the Chart API.
    """

    label: str
    sqlExpression: str


def convert_to_adhoc_column(expression: str) -> ColumnType:
    """
    Convert an adhoc column to an object.
    """
    return {
        "label": expression,
        "sqlExpression": expression,
    }


def shortid() -> str:
    """
    Generate a short ID suited for a SQL Lab client ID.
    """
    return str(uuid.uuid4())[-12:]


class SupersetClient:  # pylint: disable=too-many-public-methods

    """
    A client for running queries against Superset.
    """

    def __init__(self, baseurl: Union[str, URL], auth: Auth):
        # convert to URL if necessary
        self.baseurl = URL(baseurl)
        self.auth = auth

    def run_query(self, database_id: int, sql: str, limit: int = 1000) -> pd.DataFrame:
        """
        Run a SQL query, returning a Pandas dataframe.
        """
        url = self.baseurl / "superset/sql_json/"
        data = {
            "client_id": shortid()[:10],
            "database_id": database_id,
            "json": True,
            "runAsync": False,
            "schema": None,
            "sql": sql,
            "sql_editor_id": "1",
            "tab": "Untitled Query 2",
            "tmp_table_name": "",
            "select_as_cta": False,
            "ctas_method": "TABLE",
            "queryLimit": limit,
            "expand_data": True,
        }
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": f"Apache Superset Client ({__version__})",
            "Referer": str(self.baseurl),
        }
        headers.update(self.auth.get_headers())

        session = self.auth.get_session()
        response = session.post(url, json=data, headers=headers)
        validate_response(response)

        payload = response.json()

        return pd.DataFrame(payload["data"])

    def get_data(  # pylint: disable=too-many-locals, too-many-arguments
        self,
        dataset_id: int,
        metrics: List[str],
        columns: List[str],
        is_timeseries: bool = False,
        time_column: Optional[str] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        granularity: Optional[str] = None,
        where: str = "",
        having: str = "",
        row_limit: int = 10000,
        force: bool = False,
    ) -> pd.DataFrame:
        """
        Run a dimensional query.
        """
        dataset = self.get_dataset(dataset_id)["result"]

        if time_column is None:
            time_columns = [
                column["column_name"]
                for column in dataset["columns"]
                if column["is_dttm"]
            ]
            if len(time_columns) > 1:
                options = ", ".join(time_columns)
                raise Exception(
                    f"Unable to determine time column, please pass `time_series` "
                    f"as one of: {options}",
                )
            time_column = time_columns[0]

        time_range = (
            "No filter"
            if start is None and end is None
            else f"{start or ''} : {end or ''}"
        )

        # convert adhoc metrics to a proper object, if needed
        metric_names = [metric["metric_name"] for metric in dataset["metrics"]]
        processed_metrics = [
            metric if metric in metric_names else convert_to_adhoc_metric(metric)
            for metric in metrics
        ]

        # same for columns
        column_names = [column["column_name"] for column in dataset["columns"]]
        processed_columns = [
            column if column in column_names else convert_to_adhoc_column(column)
            for column in columns
        ]

        url = self.baseurl / "api/v1/chart/data"
        data: Dict[str, Any] = {
            "datasource": {"id": dataset_id, "type": "table"},
            "force": force,
            "queries": [
                {
                    "annotation_layers": [],
                    "applied_time_extras": {},
                    "columns": processed_columns,
                    "custom_form_data": {},
                    "custom_params": {},
                    "extras": {"having": having, "having_druid": [], "where": where},
                    "filters": [],
                    "is_timeseries": is_timeseries,
                    "metrics": processed_metrics,
                    "order_desc": True,
                    "orderby": [],
                    "row_limit": row_limit,
                    "time_range": time_range,
                    "timeseries_limit": 0,
                    "url_params": {},
                },
            ],
            "result_format": "json",
            "result_type": "full",
        }
        if is_timeseries:
            data["queries"][0]["granularity"] = time_column
            data["queries"][0]["extras"]["time_grain_sqla"] = granularity

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": f"Apache Superset Client ({__version__})",
            "Referer": str(self.baseurl),
        }
        headers.update(self.auth.get_headers())

        session = self.auth.get_session()
        response = session.post(url, json=data, headers=headers)
        validate_response(response)

        payload = response.json()

        return pd.DataFrame(payload["result"][0]["data"])

    def get_resource(self, resource_name: str, resource_id: int) -> Any:
        """
        Return a single resource.
        """
        url = self.baseurl / "api/v1" / resource_name / str(resource_id)

        session = self.auth.get_session()
        headers = self.auth.get_headers()
        headers["Referer"] = str(self.baseurl)
        response = session.get(url, headers=headers)
        validate_response(response)

        resource = response.json()

        return resource

    def get_resources(self, resource_name: str, **kwargs: Any) -> List[Any]:
        """
        Return one or more of a resource, possibly filtered.
        """
        operations = {
            k: v if isinstance(v, Operator) else Equal(v) for k, v in kwargs.items()
        }
        query = prison.dumps(
            {
                "filters": [
                    dict(col=col, opr=value.operator, value=value.value)
                    for col, value in operations.items()
                ],
            },
        )
        url = self.baseurl / "api/v1" / resource_name / "" % {"q": query}

        session = self.auth.get_session()
        headers = self.auth.get_headers()
        headers["Referer"] = str(self.baseurl)
        response = session.get(url, headers=headers)
        validate_response(response)

        payload = response.json()
        resources = payload["result"]

        return resources

    def create_resource(self, resource_name: str, **kwargs: Any) -> Any:
        """
        Create a resource.
        """
        url = self.baseurl / "api/v1" / resource_name / ""

        session = self.auth.get_session()
        headers = self.auth.get_headers()
        headers["Referer"] = str(self.baseurl)
        response = session.post(url, json=kwargs, headers=headers)
        validate_response(response)

        resource = response.json()

        return resource

    def update_resource(
        self,
        resource_name: str,
        resource_id: int,
        query_args: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Update a resource.
        """
        url = self.baseurl / "api/v1" / resource_name / str(resource_id)
        if query_args:
            url %= query_args

        session = self.auth.get_session()
        headers = self.auth.get_headers()
        headers["Referer"] = str(self.baseurl)
        response = session.put(url, json=kwargs, headers=headers)
        validate_response(response)

        resource = response.json()

        return resource

    def get_database(self, database_id: int) -> Any:
        """
        Return a single database.
        """
        return self.get_resource("database", database_id)

    def get_databases(self, **kwargs: str) -> List[Any]:
        """
        Return databases, possibly filtered.
        """
        return self.get_resources("database", **kwargs)

    def create_database(self, **kwargs: Any) -> Any:
        """
        Create a database.
        """
        return self.create_resource("database", **kwargs)

    def update_database(self, database_id: int, **kwargs: Any) -> Any:
        """
        Update a database.
        """
        query_args = {"override_columns": "true"}
        return self.update_resource("database", database_id, query_args, **kwargs)

    def get_dataset(self, dataset_id: int) -> Any:
        """
        Return a single dataset.
        """
        return self.get_resource("dataset", dataset_id)

    def get_datasets(self, **kwargs: str) -> List[Any]:
        """
        Return datasets, possibly filtered.
        """
        return self.get_resources("dataset", **kwargs)

    def create_dataset(self, **kwargs: Any) -> Any:
        """
        Create a dataset.
        """
        return self.create_resource("dataset", **kwargs)

    def update_dataset(self, dataset_id: int, **kwargs: Any) -> Any:
        """
        Update a dataset.
        """
        return self.update_resource("dataset", dataset_id, **kwargs)

    def get_chart(self, chart_id: int) -> Any:
        """
        Return a single chart.
        """
        return self.get_resource("chart", chart_id)

    def get_charts(self, **kwargs: str) -> List[Any]:
        """
        Return charts, possibly filtered.
        """
        return self.get_resources("chart", **kwargs)

    def get_dashboard(self, dashboard_id: int) -> Any:
        """
        Return a single dashboard.
        """
        return self.get_resource("dashboard", dashboard_id)

    def get_dashboards(self, **kwargs: str) -> List[Any]:
        """
        Return dashboards, possibly filtered.
        """
        return self.get_resources("dashboard", **kwargs)

    def create_dashboard(self, **kwargs: Any) -> Any:
        """
        Create a dashboard.
        """
        return self.create_resource("dashboard", **kwargs)

    def update_dashboard(self, dashboard_id: int, **kwargs: Any) -> Any:
        """
        Update a dashboard.
        """
        return self.update_resource("dashboard", dashboard_id, **kwargs)

    def export_zip(self, resource_name: str, ids: List[int]) -> BytesIO:
        """
        Export one or more of a resource.
        """
        url = self.baseurl / "api/v1" / resource_name / "export/"
        params = {"q": prison.dumps(ids)}

        session = self.auth.get_session()
        headers = self.auth.get_headers()
        headers["Referer"] = str(self.baseurl)
        response = session.get(url, params=params, headers=headers)
        validate_response(response)

        return BytesIO(response.content)

    def import_zip(
        self,
        resource_name: str,
        data: BytesIO,
        overwrite: bool = False,
    ) -> bool:
        """
        Import a ZIP bundle.
        """
        url = self.baseurl / "api/v1" / resource_name / "import/"

        session = self.auth.get_session()
        headers = self.auth.get_headers()
        headers["Referer"] = str(self.baseurl)
        headers["Accept"] = "application/json"
        response = session.post(
            url,
            files=dict(formData=data),
            data=dict(overwrite=json.dumps(overwrite)),
            headers=headers,
        )
        validate_response(response)

        payload = response.json()

        return payload["message"] == "OK"
