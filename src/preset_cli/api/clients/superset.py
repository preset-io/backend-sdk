"""
A simple client for running SQL queries (and more) against Superset:

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
import requests
import yaml
from bs4 import BeautifulSoup
from yarl import URL

from preset_cli import __version__
from preset_cli.api.clients.preset import PresetClient
from preset_cli.api.operators import Equal, Operator
from preset_cli.auth.main import Auth
from preset_cli.lib import remove_root, validate_response
from preset_cli.typing import UserType

MAX_PAGE_SIZE = 100
MAX_IDS_IN_EXPORT = 100


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


class RuleType(TypedDict):
    """
    Schema for an RLS rule.
    """

    name: str
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
        url = self.baseurl / "superset/sql_json/"
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

            session = self.auth.get_session()
            headers = self.auth.get_headers()
            headers["Referer"] = str(self.baseurl)
            response = session.get(url, headers=headers)
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
        return self.update_resource("database", database_id, **kwargs)

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
        query_args = {"override_columns": "true"}
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
        session = self.auth.get_session()
        headers = self.auth.get_headers()
        headers["Referer"] = str(self.baseurl)
        url = self.baseurl / "api/v1" / resource_name / "export/"

        buf = BytesIO()
        with ZipFile(buf, "w") as bundle:
            while ids:
                page, ids = ids[:MAX_IDS_IN_EXPORT], ids[MAX_IDS_IN_EXPORT:]
                params = {"q": prison.dumps(page)}
                response = session.get(url, params=params, headers=headers)
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
        session = self.auth.get_session()
        headers = self.auth.get_headers()
        headers["Referer"] = str(self.baseurl)
        url = self.baseurl / "api/v1" / resource_name / "export/"

        uuids: Dict[int, UUID] = {}
        for resource in self.get_resources(resource_name):
            id_ = resource["id"]
            params = {"q": prison.dumps([id_])}
            response = session.get(url, params=params, headers=headers)

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

    def export_users(self) -> Iterator[UserType]:
        """
        Return all users.
        """
        session = self.auth.get_session()
        headers = self.auth.get_headers()
        headers["Referer"] = str(self.baseurl)

        # For on-premise OSS Superset we can fetch the list of users by crawling the
        # ``/users/list/`` page. For a Preset workspace we need custom logic to talk
        # to Manager.
        response = session.get(self.baseurl / "users/list/", headers=headers)
        if response.ok:
            return self._export_users_superset(session, headers)
        return self._export_users_preset()

    def _export_users_preset(self) -> Iterator[UserType]:
        """
        Return all users from a Preset workspace.
        """
        # TODO (beto): remove hardcoded Manager URL
        client = PresetClient("https://manage.app.preset.io/", self.auth)
        return client.export_users(self.baseurl)

    def _export_users_superset(
        self,
        session: requests.Session,
        headers: Dict[str, str],
    ) -> Iterator[UserType]:
        """
        Return all users from a standalone Superset instance.

        Since this is not exposed via an API we need to crawl the CRUD page.
        """
        session = self.auth.get_session()
        headers = self.auth.get_headers()
        headers["Referer"] = str(self.baseurl)

        page = 0
        while True:
            params = {
                "psize_UserDBModelView": MAX_PAGE_SIZE,
                "page_UserDBModelView": page,
            }
            url = self.baseurl / "users/list/"
            page += 1

            response = session.get(url, params=params, headers=headers)
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

    def export_rls(self) -> Iterator[RuleType]:
        """
        Return all RLS rules.
        """
        session = self.auth.get_session()
        headers = self.auth.get_headers()
        headers["Referer"] = str(self.baseurl)

        page = 0
        while True:
            params = {
                "psize_RowLevelSecurityFiltersModelView": MAX_PAGE_SIZE,
                "page_RowLevelSecurityFiltersModelView": page,
            }
            url = self.baseurl / "rowlevelsecurityfiltersmodelview/list/"
            page += 1

            response = session.get(url, params=params, headers=headers)
            soup = BeautifulSoup(response.text, features="html.parser")
            table = soup.find_all("table")[1]
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

                response = session.get(rule_url, headers=headers)
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
                yield cast(
                    RuleType,
                    {
                        key: parse(tr.find("td").text.strip())
                        for (key, parse), tr in zip(keys, table.find_all("tr"))
                    },
                )

    def import_rls(self, rls: RuleType) -> None:  # pylint: disable=too-many-locals
        """
        Import a given RLS rule.
        """
        session = self.auth.get_session()
        headers = self.auth.get_headers()
        headers["Referer"] = str(self.baseurl)

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
        for role in rls["roles"]:
            params = {"_flt_0_name": role}
            url = self.baseurl / "roles/list/"
            response = session.get(url, params=params, headers=headers)
            soup = BeautifulSoup(response.text, features="html.parser")
            trs = soup.find_all("table")[1].find_all("tr")
            if len(trs) == 1:
                raise Exception(f"Cannot find role: {role}")
            if len(trs) > 2:
                raise Exception(f"More than one role found: {role}")
            td = trs[1].find("td")  # pylint: disable=invalid-name
            if td.find("a"):
                id_ = int(td.find("a").attrs["href"].split("/")[-1])
            else:
                id_ = int(td.find("input").attrs["id"])
            role_ids.append(id_)

        url = self.baseurl / "rowlevelsecurityfiltersmodelview/add"
        response = session.post(
            url,
            data={
                # "csrf_token": "needed?",
                "name": rls["name"],
                "description": rls["description"],
                "filter_type": rls["filter_type"],
                "tables": table_ids,
                "roles": role_ids,
                "group_key": rls["group_key"],
                "clause": rls["clause"],
            },
            headers=headers,
        )
        validate_response(response)

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
            resource_id = resource_ids[item["uuid"]]
            owner_ids = [user_ids[email] for email in item["owners"]]
            self.update_resource(resource_name, resource_id, owners=owner_ids)
