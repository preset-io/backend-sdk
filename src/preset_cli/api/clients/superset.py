"""
A simple client for running SQL queries (and more) against Superset:

    >>> from yarl import URL
    >>> from preset_cli.api.clients.superset import SupersetClient
    >>> from preset_cli.auth.superset import UsernamePasswordAuth
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

# pylint: disable=consider-using-f-string, too-many-lines

import json
import logging
import re
import uuid
from datetime import datetime
from enum import IntEnum
from io import BytesIO
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    TypedDict,
    Union,
    cast,
)
from uuid import UUID, uuid4
from zipfile import ZipFile

import pandas as pd
import prison
import yaml
from bs4 import BeautifulSoup
from yarl import URL

from preset_cli import __version__
from preset_cli.api.clients.preset import PresetClient
from preset_cli.api.operators import Equal, Operator
from preset_cli.auth.main import Auth
from preset_cli.lib import remove_root, validate_response
from preset_cli.typing import UserType

_logger = logging.getLogger(__name__)

MAX_PAGE_SIZE = 100
MAX_IDS_IN_EXPORT = 50


PERMISSION_MAP = {
    "all datasource access on all_datasource_access": "All dataset access",
    "all database access on all_database_access": "All database access",
    "all query access on all_query_access": "All query access",
}
DATABASE_PERMISSION = re.compile(
    r"database access on \[(?P<database_name>.*?)\]\.\(id:\d+\)",
)
SCHEMA_PERMISSION = re.compile(
    r"schema access on \[(?P<database_name>.*?)\].\[(?P<schema_name>.*?)\]",
)
DATASET_PERMISSION = re.compile(
    r"datasource access on \[(?P<database_name>.*?)\].\[(?P<dataset_name>.*?)\]\(id:\d+\)",
)


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


class SupersetMetricDefinition(TypedDict, total=False):
    """
    Definition of a Superset metric.

    Used in the PUT API for datasets.
    """

    id: int
    expression: str
    metric_name: str
    metric_type: str
    verbose_name: str
    description: str
    extra: str
    warning_text: str
    d3format: str
    currency: str
    uuid: str


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


def parse_html_array(value: str) -> List[str]:
    """
    Parse an array scraped from the HTML CRUD view.
    """
    value = value.strip()

    if value.startswith("[") and value.endswith("]"):
        parts = [part.strip() for part in value[1:-1].split(",")]
    else:
        parts = [part.strip() for part in value.split("\n")]

    return [part for part in parts if part.strip()]


class RoleType(TypedDict):
    """
    Schema for a role.
    """

    name: str
    permissions: List[str]
    users: List[str]


class RuleType(TypedDict):
    """
    Schema for an RLS rule.
    """

    name: Optional[str]
    description: Optional[str]
    filter_type: str
    tables: List[str]
    roles: List[str]
    group_key: str
    clause: str


class OwnershipType(TypedDict):
    """
    Schema for resource ownership.
    """

    name: str
    uuid: UUID
    owners: List[str]


class SupersetClient:  # pylint: disable=too-many-public-methods

    """
    A client for running queries against Superset.
    """

    def __init__(self, baseurl: Union[str, URL], auth: Auth):
        # convert to URL if necessary
        self.baseurl = URL(baseurl)
        self.auth = auth

        self.session = auth.session
        self.session.headers.update(auth.get_headers())
        self.session.headers["Referer"] = str(self.baseurl)
        self.session.headers["User-Agent"] = f"Apache Superset Client ({__version__})"

    def run_query(
        self,
        database_id: int,
        sql: str,
        schema: Optional[str] = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """
        Run a SQL query, returning a Pandas dataframe.
        """
        payload = self._run_query(database_id, sql, schema, limit)

        return pd.DataFrame(payload["data"])

    def _run_query(
        self,
        database_id: int,
        sql: str,
        schema: Optional[str] = None,
        limit: int = 1000,
    ) -> Dict[str, Any]:
        url = self.baseurl / "api/v1/sqllab/execute/"
        data = {
            "client_id": shortid()[:10],
            "database_id": database_id,
            "json": True,
            "runAsync": False,
            "schema": schema,
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
        }
        self.session.headers.update(headers)

        _logger.debug("POST %s\n%s", url, json.dumps(data, indent=4))
        response = self.session.post(url, json=data)

        # Legacy superset installations don't have the SQL API endpoint yet
        if response.status_code == 404:
            url = self.baseurl / "superset/sql_json/"
            _logger.debug("POST %s\n%s", url, json.dumps(data, indent=4))
            response = self.session.post(url, json=data)

        validate_response(response)
        payload = response.json()

        return payload

    def get_data(  # pylint: disable=too-many-locals, too-many-arguments
        self,
        dataset_id: int,
        metrics: List[str],
        columns: List[str],
        order_by: Optional[List[str]] = None,
        order_desc: bool = True,
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
        dataset = self.get_dataset(dataset_id)

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

        # and order bys
        processed_orderbys = [
            (orderby, not order_desc)
            if orderby in metric_names
            else (convert_to_adhoc_metric(orderby), not order_desc)
            for orderby in (order_by or [])
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
                    "order_desc": order_desc,
                    "orderby": processed_orderbys,
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
        }
        self.session.headers.update(headers)

        _logger.debug("POST %s\n%s", url, json.dumps(data, indent=4))
        response = self.session.post(url, json=data)
        validate_response(response)

        payload = response.json()

        return pd.DataFrame(payload["result"][0]["data"])

    def get_resource(self, resource_name: str, resource_id: int) -> Any:
        """
        Return a single resource.
        """
        url = self.baseurl / "api/v1" / resource_name / str(resource_id)

        _logger.debug("GET %s", url)
        response = self.session.get(url)
        validate_response(response)

        resource = response.json()["result"]

        return resource

    def get_resources(self, resource_name: str, **kwargs: Any) -> List[Any]:
        """
        Return one or more of a resource, possibly filtered.
        """
        resources = []
        operations = {
            k: v if isinstance(v, Operator) else Equal(v) for k, v in kwargs.items()
        }

        # paginate endpoint until no results are returned
        page = 0
        while True:
            query = prison.dumps(
                {
                    "filters": [
                        dict(col=col, opr=value.operator, value=value.value)
                        for col, value in operations.items()
                    ],
                    "order_column": "changed_on_delta_humanized",
                    "order_direction": "desc",
                    "page": page,
                    "page_size": MAX_PAGE_SIZE,
                },
            )
            url = self.baseurl / "api/v1" / resource_name / "" % {"q": query}

            _logger.debug("GET %s", url)
            response = self.session.get(url)
            validate_response(response)

            payload = response.json()

            if not payload["result"]:
                break

            resources.extend(payload["result"])
            page += 1

        return resources

    def create_resource(self, resource_name: str, **kwargs: Any) -> Any:
        """
        Create a resource.
        """
        url = self.baseurl / "api/v1" / resource_name / ""

        _logger.debug("POST %s\n%s", url, json.dumps(kwargs, indent=4))
        response = self.session.post(url, json=kwargs)
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

        response = self.session.put(url, json=kwargs)
        validate_response(response)

        resource = response.json()

        return resource

    def get_resource_endpoint_info(self, resource_name: str, **kwargs: Any) -> Any:
        """
        Get resource endpoint info (such as available columns) possibly filtered.
        """
        query = prison.dumps({"keys": list(kwargs["keys"])} if "keys" in kwargs else {})

        url = self.baseurl / "api/v1" / resource_name / "_info" % {"q": query}
        _logger.debug("GET %s", url)
        response = self.session.get(url)
        validate_response(response)

        endpoint_info = response.json()

        return endpoint_info

    def validate_key_in_resource_schema(
        self, resource_name: str, key_name: str, **kwargs: Any
    ) -> Any:
        """
        Validate if a key is present in a resource schema.
        """
        schema_validation = {}

        endpoint_info = self.get_resource_endpoint_info(resource_name, **kwargs)

        for key in kwargs.get("keys", ["add_columns", "edit_columns"]):
            schema_columns = [column["name"] for column in endpoint_info.get(key, [])]
            schema_validation[key] = key_name in schema_columns

        return schema_validation

    def get_database(self, database_id: int) -> Any:
        """
        Return a single database.
        """
        database = self.get_resource("database", database_id)
        if "sqlalchemy_uri" in database:
            return database

        url = self.baseurl / "api/v1/database" / str(database_id) / "connection"
        response = self.session.get(url)
        validate_response(response)

        resource = response.json()["result"]

        return resource

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
        return self.update_resource("database", database_id, **kwargs)

    def get_dataset(self, dataset_id: int) -> Any:
        """
        Return a single dataset.
        """
        return self.get_resource("dataset", dataset_id)

    def get_refreshed_dataset_columns(self, dataset_id: int) -> List[Any]:
        """
        Return dataset columns.
        """
        url = self.baseurl / "datasource/external_metadata/table" / str(dataset_id)
        _logger.debug("GET %s", url)
        response = self.session.get(url)
        validate_response(response)

        resource = response.json()
        return resource

    def get_datasets(self, **kwargs: str) -> List[Any]:
        """
        Return datasets, possibly filtered.
        """
        return self.get_resources("dataset", **kwargs)

    def create_dataset(self, **kwargs: Any) -> Any:
        """
        Create a dataset.
        """
        if "sql" not in kwargs:
            return self.create_resource("dataset", **kwargs)

        # Check if the dataset creation supports sql directly
        not_legacy = self.validate_key_in_resource_schema(
            "dataset",
            "sql",
            keys=["add_columns"],
        )
        not_legacy = not_legacy["add_columns"]
        if not_legacy:
            return self.create_resource("dataset", **kwargs)

        # run query to determine columns types
        payload = self._run_query(
            database_id=kwargs["database"],
            sql=kwargs["sql"],
            schema=kwargs["schema"],
            limit=1,
        )

        # now add the virtual dataset
        columns = payload["columns"]
        for column in columns:
            column["column_name"] = column["name"]
            column["groupby"] = True
            # Superset <= 1.4 returns ``is_date`` instead of ``is_dttm``
            if column.get("is_dttm") or column.get("is_date"):
                column["type_generic"] = 2
            elif column["type"] is None:
                column["type"] = "UNKNOWN"
                column["type_generic"] = 1
            elif column["type"].lower() == "string":
                column["type_generic"] = 1
            else:
                column["type_generic"] = 0
        payload = {
            "sql": kwargs["sql"],
            "dbId": kwargs["database"],
            "schema": kwargs["schema"],
            "datasourceName": kwargs["table_name"],
            "columns": columns,
        }
        data = {"data": json.dumps(payload)}

        url = self.baseurl / "superset/sqllab_viz/"
        _logger.debug("POST %s\n%s", url, json.dumps(data, indent=4))
        response = self.session.post(url, data=data)
        validate_response(response)

        payload = response.json()

        # Superset <= 1.4 returns ``{"table_id": dataset_id}`` rather than the dataset payload
        return payload["data"] if "data" in payload else {"id": payload["table_id"]}

    def update_dataset(
        self,
        dataset_id: int,
        override_columns: bool = False,
        **kwargs: Any,
    ) -> Any:
        """
        Update a dataset.
        """
        query_args = {"override_columns": "true" if override_columns else "false"}
        return self.update_resource("dataset", dataset_id, query_args, **kwargs)

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

        buf = BytesIO()
        with ZipFile(buf, "w") as bundle:
            while ids:
                page, ids = ids[:MAX_IDS_IN_EXPORT], ids[MAX_IDS_IN_EXPORT:]
                params = {"q": prison.dumps(page)}
                _logger.debug("GET %s", url % params)
                response = self.session.get(url, params=params)
                validate_response(response)

                # write files from response to main ZIP bundle
                with ZipFile(BytesIO(response.content)) as subset:
                    for name in subset.namelist():
                        bundle.writestr(name, subset.read(name))

        buf.seek(0)

        return buf

    def get_uuids(self, resource_name: str) -> Dict[int, UUID]:
        """
        Get UUID of a list of resources.

        Still method is very inneficient, but it's the only way to get the mapping
        between IDs and UUIDs in older versions of Superset.
        """
        url = self.baseurl / "api/v1" / resource_name / "export/"

        uuids: Dict[int, UUID] = {}
        for resource in self.get_resources(resource_name):
            id_ = resource["id"]
            params = {"q": prison.dumps([id_])}
            _logger.debug("GET %s", url % params)
            response = self.session.get(url, params=params)

            with ZipFile(BytesIO(response.content)) as export:
                for name in export.namelist():
                    config = yaml.load(export.read(name), Loader=yaml.SafeLoader)
                    name = remove_root(name)
                    if name.startswith(resource_name):
                        uuids[id_] = UUID(config["uuid"])

        return uuids

    def import_zip(
        self,
        resource_name: str,
        form_data: BytesIO,
        overwrite: bool = False,
    ) -> bool:
        """
        Import a ZIP bundle.
        """
        key = "bundle" if resource_name == "assets" else "formData"
        files = {key: form_data}
        url = self.baseurl / "api/v1" / resource_name / "import/"

        self.session.headers.update({"Accept": "application/json"})
        data = {"overwrite": json.dumps(overwrite)}
        _logger.debug("POST %s\n%s", url, json.dumps(data, indent=4))
        response = self.session.post(
            url,
            files=files,
            data=data,
        )
        validate_response(response)

        payload = response.json()

        return payload["message"] == "OK"

    def get_rls(self, **kwargs: str) -> List[Any]:
        """
        Return RLS rules, possibly filtered.
        """
        return self.get_resources("rowlevelsecurity", **kwargs)

    def export_users(self) -> Iterator[UserType]:
        """
        Return all users.
        """
        # For on-premise OSS Superset we can fetch the list of users by crawling the
        # ``/users/list/`` page. For a Preset workspace we need custom logic to talk
        # to Manager.
        url = self.baseurl / "users/list/"
        _logger.debug("GET %s", url)
        response = self.session.get(url)
        if response.ok:
            return self._export_users_superset()
        return self._export_users_preset()

    def _export_users_preset(self) -> Iterator[UserType]:
        """
        Return all users from a Preset workspace.
        """
        # TODO (betodealmeida): remove hardcoded Manager URL
        client = PresetClient("https://api.app.preset.io/", self.auth)
        return client.export_users(self.baseurl)

    def _export_users_superset(self) -> Iterator[UserType]:
        """
        Return all users from a standalone Superset instance.

        Since this is not exposed via an API we need to crawl the CRUD page.
        """
        page = 0
        while True:
            params = {
                "psize_UserDBModelView": MAX_PAGE_SIZE,
                "page_UserDBModelView": page,
            }
            url = self.baseurl / "users/list/"
            page += 1

            _logger.debug("GET %s", url % params)
            response = self.session.get(url, params=params)
            soup = BeautifulSoup(response.text, features="html.parser")
            table = soup.find_all("table")[1]
            trs = table.find_all("tr")
            if len(trs) == 1:
                break

            for tr in trs[1:]:  # pylint: disable=invalid-name
                tds = tr.find_all("td")
                yield {
                    "id": int(tds[0].find("a").attrs["href"].split("/")[-1]),
                    "first_name": tds[1].text,
                    "last_name": tds[2].text,
                    "username": tds[3].text,
                    "email": tds[4].text,
                    "role": parse_html_array(tds[6].text.strip()),
                }

    def export_roles(self) -> Iterator[RoleType]:  # pylint: disable=too-many-locals
        """
        Return all roles.
        """
        user_email_map = {user["id"]: user["email"] for user in self.export_users()}

        page = 0
        while True:
            params = {
                # Superset
                "psize_RoleModelView": MAX_PAGE_SIZE,
                "page_RoleModelView": page,
                # Preset
                "psize_DataRoleModelView": MAX_PAGE_SIZE,
                "page_DataRoleModelView": page,
            }
            url = self.baseurl / "roles/list/"
            page += 1

            _logger.debug("GET %s", url % params)
            response = self.session.get(url, params=params)
            soup = BeautifulSoup(response.text, features="html.parser")
            table = soup.find_all("table")[1]
            trs = table.find_all("tr")
            if len(trs) == 1:
                break

            for tr in trs[1:]:  # pylint: disable=invalid-name
                tds = tr.find_all("td")

                td = tds[0]  # pylint: disable=invalid-name
                if td.find("a"):
                    role_id = int(td.find("a").attrs["href"].split("/")[-1])
                else:
                    role_id = int(td.find("input").attrs["id"])
                role_url = self.baseurl / "roles/edit" / str(role_id)

                _logger.debug("GET %s", role_url)
                response = self.session.get(role_url)
                soup = BeautifulSoup(response.text, features="html.parser")

                name = soup.find("input", {"name": "name"}).attrs["value"]
                permissions = [
                    option.text.strip()
                    for option in soup.find("select", id="permissions").find_all(
                        "option",
                    )
                    if "selected" in option.attrs
                ]
                users = [
                    user_email_map[int(option.attrs["value"])]
                    for option in soup.find("select", id="user").find_all("option")
                    if "selected" in option.attrs
                    and int(option.attrs["value"]) in user_email_map
                ]

                yield {
                    "name": name,
                    "permissions": permissions,
                    "users": users,
                }

    def export_rls_legacy(self) -> Iterator[RuleType]:
        """
        Return all RLS rules from legacy endpoint.
        """
        page = 0
        while True:
            params = {
                "psize_RowLevelSecurityFiltersModelView": MAX_PAGE_SIZE,
                "page_RowLevelSecurityFiltersModelView": page,
            }
            url = self.baseurl / "rowlevelsecurityfiltersmodelview/list/"
            page += 1

            _logger.debug("GET %s", url % params)
            response = self.session.get(url, params=params)
            soup = BeautifulSoup(response.text, features="html.parser")
            try:
                table = soup.find_all("table")[1]
            except IndexError:
                return
            trs = table.find_all("tr")
            if len(trs) == 1:
                break

            for tr in trs[1:]:  # pylint: disable=invalid-name
                tds = tr.find_all("td")

                # extract the ID to fetch each RLS in a separate request, since the list
                # view doesn't have all the columns we need
                rule_id = int(tds[0].find("input").attrs["id"])
                rule_url = (
                    self.baseurl
                    / "rowlevelsecurityfiltersmodelview/show"
                    / str(rule_id)
                )

                _logger.debug("GET %s", rule_url)
                response = self.session.get(rule_url)
                soup = BeautifulSoup(response.text, features="html.parser")
                table = soup.find("table")
                keys: List[Tuple[str, Callable[[Any], Any]]] = [
                    ("name", str),
                    ("description", str),
                    ("filter_type", str),
                    ("tables", parse_html_array),
                    ("roles", parse_html_array),
                    ("group_key", str),
                    ("clause", str),
                ]

                # Before Superset 2.1.0, RLS dont have name and description
                if table.find("th").text.strip() == "Filter Type":
                    keys.remove(("name", str))
                    keys.remove(("description", str))

                yield cast(
                    RuleType,
                    {
                        key: parse(tr.find("td").text.strip())
                        for (key, parse), tr in zip(keys, table.find_all("tr"))
                    },
                )

    def export_rls(self) -> Iterator[RuleType]:
        """
        Return all RLS rules.
        """
        url = self.baseurl / "api/v1/rowlevelsecurity/"
        response = self.session.get(url)
        if response.status_code == 200:
            for rule in self.get_rls():
                keys = [
                    "name",
                    "description",
                    "filter_type",
                    "tables",
                    "roles",
                    "group_key",
                    "clause",
                ]
                data = {}
                for key in keys:
                    if key == "tables":
                        data[key] = [
                            f"{inner_item['schema']}.{inner_item['table_name']}"
                            for inner_item in rule.get(key, [])
                        ]
                    elif key == "roles":
                        data[key] = [
                            inner_item["name"] for inner_item in rule.get(key, [])
                        ]
                    else:
                        data[key] = rule.get(key)
                yield cast(RuleType, data)

        else:
            yield from self.export_rls_legacy()

    def import_role(self, role: RoleType) -> None:  # pylint: disable=too-many-locals
        """
        Import a given role.

        Note: this only works with Preset workspaces for now, since it translates the
        Superset permissions to the Preset permissions.
        """
        user_id_map = {user["email"]: user["id"] for user in self.export_users()}
        user_ids = [
            user_id_map[email] for email in role["users"] if email in user_id_map
        ]

        url = self.baseurl / "roles/add"
        _logger.debug("GET %s", url)
        response = self.session.get(url)
        soup = BeautifulSoup(response.text, features="html.parser")
        select = soup.find("select", id="permissions")
        permission_id_map = {
            option.text: int(option.attrs["value"])
            for option in select.find_all("option")
        }

        permission_ids: List[int] = []
        for permission in role["permissions"]:
            # map to custom Preset permissions
            if permission in PERMISSION_MAP:
                permission = PERMISSION_MAP[permission]
            elif match_ := DATABASE_PERMISSION.match(permission):
                permission = "Database access on {database_name}".format(
                    **match_.groupdict()
                )
            elif match_ := SCHEMA_PERMISSION.match(permission):
                permission = "Schema access on {database_name}.{schema_name}".format(
                    **match_.groupdict()
                )
            elif match_ := DATASET_PERMISSION.match(permission):
                permission = "Dataset access on {database_name}.{dataset_name}".format(
                    **match_.groupdict()
                )

            if permission in permission_id_map:
                permission_ids.append(permission_id_map[permission])
            else:
                _logger.warning("Permission %s not found in target", permission)

        data = {
            "name": role["name"],
            "user": user_ids,
            "permissions": permission_ids,
        }

        # update if existing
        search_url = self.baseurl / "roles/list/" % {"_flt_3_name": role["name"]}
        _logger.debug("GET %s", search_url)
        response = self.session.get(search_url)
        soup = BeautifulSoup(response.text, features="html.parser")
        tables = soup.find_all("table")
        if len(tables) == 2:
            table = tables[1]
            trs = table.find_all("tr")
            if len(trs) == 2:
                tr = trs[1]  # pylint: disable=invalid-name
                tds = tr.find_all("td")

                td = tds[0]  # pylint: disable=invalid-name
                if td.find("a"):
                    role_id = int(td.find("a").attrs["href"].split("/")[-1])
                else:
                    role_id = int(td.find("input").attrs["id"])

                update_url = self.baseurl / "roles/edit" / str(role_id)
                _logger.debug("POST %s\n%s", update_url, json.dumps(data, indent=4))
                response = self.session.post(update_url, data=data)
                validate_response(response)
                return

        _logger.debug("POST %s\n%s", url, json.dumps(data, indent=4))
        response = self.session.post(url, data=data)
        validate_response(response)

    def import_rls(self, rls: RuleType) -> None:  # pylint: disable=too-many-locals
        """
        Import a given RLS rule.
        """
        table_ids: List[int] = []
        for table in rls["tables"]:
            if "." in table:
                schema, table_name = table.split(".", 1)
                datasets = self.get_datasets(schema=schema, table_name=table_name)
            else:
                datasets = self.get_datasets(table_name=table)

            if not datasets:
                raise Exception(f"Cannot find table: {table}")
            if len(datasets) > 1:
                raise Exception(f"More than one table found: {table}")
            table_ids.append(datasets[0]["id"])

        role_ids: List[int] = []
        for role_name in rls["roles"]:
            role_id = self.get_role_id(role_name)
            if self.get_role_permissions(role_id):
                raise Exception(
                    f"Role {role_name} currently has permissions associated with it. To "
                    "use it with RLS it should have no permissions.",
                )
            role_ids.append(role_id)

        url = self.baseurl / "rowlevelsecurityfiltersmodelview/add"
        data = {
            "name": rls["name"],
            "description": rls["description"],
            "filter_type": rls["filter_type"],
            "tables": table_ids,
            "roles": role_ids,
            "group_key": rls["group_key"],
            "clause": rls["clause"],
        }
        _logger.debug("POST %s\n%s", url, json.dumps(data, indent=4))
        response = self.session.post(url, data=data)
        validate_response(response)

    def get_role_permissions(self, role_id: int) -> List[int]:
        """
        Return the IDs of permissions associated with a role.
        """
        url = self.baseurl / "roles/edit" / str(role_id)
        _logger.debug("GET %s", url)
        response = self.session.get(url)

        soup = BeautifulSoup(response.text, features="html.parser")
        return [
            int(option.attrs["value"])
            for option in soup.find("select", id="permissions").find_all("option")
            if "selected" in option.attrs
        ]

    def get_role_id(self, role_name: str) -> int:
        """
        Return the ID of a given role.
        """
        params = {"_flt_3_name": role_name}
        url = self.baseurl / "roles/list/"
        _logger.debug("GET %s", url % params)
        response = self.session.get(url, params=params)

        soup = BeautifulSoup(response.text, features="html.parser")
        tables = soup.find_all("table")
        if len(tables) < 2:
            raise Exception(f"Cannot find role: {role_name}")
        trs = tables[1].find_all("tr")
        if len(trs) == 1:
            raise Exception(f"Cannot find role: {role_name}")
        if len(trs) > 2:
            raise Exception(f"More than one role found: {role_name}")

        tds = trs[1].find_all("td")
        td = tds[0]  # pylint: disable=invalid-name
        if td.find("a"):
            id_ = int(td.find("a").attrs["href"].split("/")[-1])
        else:
            id_ = int(td.find("input").attrs["id"])

        return id_

    def export_ownership(self, resource_name: str) -> Iterator[OwnershipType]:
        """
        Return information about resource ownership.
        """
        emails = {user["id"]: user["email"] for user in self.export_users()}
        uuids = self.get_uuids(resource_name)
        name_key = {
            "dataset": "table_name",
            "chart": "slice_name",
            "dashboard": "dashboard_title",
        }[resource_name]

        for resource in self.get_resources(resource_name):
            yield {
                "name": resource[name_key],
                "uuid": uuids[resource["id"]],
                "owners": [emails[owner["id"]] for owner in resource.get("owners", [])],
            }

    def import_ownership(
        self,
        resource_name: str,
        ownership: List[Dict[str, Any]],
    ) -> None:
        """
        Import ownership on resources.
        """
        user_ids = {user["email"]: user["id"] for user in self.export_users()}
        resource_ids = {str(v): k for k, v in self.get_uuids(resource_name).items()}

        for item in ownership:
            if item["uuid"] not in resource_ids:
                continue
            resource_id = resource_ids[item["uuid"]]
            owner_ids = [user_ids[email] for email in item["owners"]]
            self.update_resource(resource_name, resource_id, owners=owner_ids)

    def update_role(self, role_id: int, **kwargs: Any) -> None:
        """
        Update a role.
        """
        # fetch current role definition
        url = self.baseurl / "roles/edit" / str(role_id)
        _logger.debug("GET %s", url)
        response = self.session.get(url)

        soup = BeautifulSoup(response.text, features="html.parser")
        name = soup.find("input", {"name": "name"}).attrs["value"]
        user_ids = [
            int(option.attrs["value"])
            for option in soup.find("select", id="user").find_all("option")
            if "selected" in option.attrs
        ]
        permission_ids = [
            int(option.attrs["value"])
            for option in soup.find("select", id="permissions").find_all("option")
            if "selected" in option.attrs
        ]
        data = {
            "name": name,
            "user": user_ids,
            "permissions": permission_ids,
        }
        data.update(kwargs)

        _logger.debug("POST %s\n%s", url, json.dumps(data, indent=4))
        self.session.post(url, data=data)
