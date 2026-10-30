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
    Set,
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
from preset_cli.api.operators import Equal, In, Operator
from preset_cli.auth.main import Auth
from preset_cli.exceptions import SupersetError
from preset_cli.lib import remove_root, validate_response
from preset_cli.typing import UserType

_logger = logging.getLogger(__name__)

MAX_PAGE_SIZE = 100
MAX_IDS_IN_EXPORT = 50
PRESET_WORKSPACE_DOMAIN = "preset.io"
DATA_ACCESS_PERMISSION_NAMES = frozenset(
    {
        "all datasource access",
        "all database access",
        "all query access",
        "database access",
        "datasource access",
        "schema access",
        "catalog access",
    },
)
PERMISSION_ID_SUFFIX = re.compile(r"\(id:[0-9a-fA-F-]+\)$")


def format_permission(permission: str, view_menu: str) -> str:
    """
    Convert the permission name from API responses into human-readable
    values for role exports.
    """
    return f"{permission.replace('_', ' ')} on {view_menu}"


def normalize_role_permission(permission: str) -> str:
    """
    Remove instance-specific IDs from database and datasource grants.

    Keep the permission type and complete resource name, including catalogs.
    Action permissions are matched exactly, even when their names contain IDs.
    """
    if permission.startswith(("database access on [", "datasource access on [")):
        return PERMISSION_ID_SUFFIX.sub("", permission)
    return permission


def resolve_role_permissions(
    permissions: List[str],
    available: List[Tuple[str, int]],
) -> List[int]:
    """
    Resolve exported permission strings against the target's raw permissions.

    Names shared by multiple target resources cannot be resolved safely from
    the export format. Reject those before changing any role relationships.
    """
    permission_ids_by_name: Dict[str, Set[int]] = {}
    for name, permission_id in available:
        key = normalize_role_permission(name)
        permission_ids_by_name.setdefault(key, set()).add(permission_id)

    permission_ids: List[int] = []
    for permission in permissions:
        matches = permission_ids_by_name.get(
            normalize_role_permission(permission),
            set(),
        )
        if len(matches) > 1:
            raise ValueError(f"Permission {permission!r} is ambiguous in target")
        if matches:
            permission_ids.append(next(iter(matches)))
        else:
            _logger.warning("Permission %s not found in target", permission)
    return permission_ids


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
    groups: List[str]


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

    def __init__(
        self,
        baseurl: Union[str, URL],
        auth: Auth,
        preset_baseurl: Union[str, URL] = "https://api.app.preset.io/",
    ):
        # convert to URL if necessary
        self.baseurl = URL(baseurl)
        self.auth = auth
        self.preset_baseurl = URL(preset_baseurl)

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
            (
                (orderby, not order_desc)
                if orderby in metric_names
                else (convert_to_adhoc_metric(orderby), not order_desc)
            )
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

    def get_resources(
        self,
        resource_name: str,
        order_column: str = "changed_on_delta_humanized",
        **kwargs: Any,
    ) -> List[Any]:
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
                    "order_column": order_column,
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

        _logger.debug("PUT %s\n%s", url, json.dumps(kwargs, indent=4))
        response = self.session.put(url, json=kwargs)
        validate_response(response)

        resource = response.json()

        return resource

    def delete_resource(self, resource_name: str, resource_id: int):
        """
        Delete a resource.
        """
        url = self.baseurl / "api/v1" / resource_name / str(resource_id)

        response = self.session.delete(url)
        validate_response(response)

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
        self,
        resource_name: str,
        key_name: str,
        **kwargs: Any,
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

    def update_chart(self, chart_id: int, **kwargs: Any) -> Any:
        """
        Update a chart.
        """
        return self.update_resource("chart", chart_id, **kwargs)

    def delete_chart(self, chart_id):
        """
        delete a chart.
        """
        self.delete_resource("chart", chart_id)

    def delete_dataset(self, dataset_id: int) -> None:
        """
        Delete a dataset.
        """
        self.delete_resource("dataset", dataset_id)

    def delete_database(self, database_id: int) -> None:
        """
        Delete a database.
        """
        self.delete_resource("database", database_id)

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

    def delete_dashboard(self, dashboard_id: int) -> Any:
        """
        Delete a dashboard.
        """
        self.delete_resource("dashboard", dashboard_id)

    def update_role_permissions(self, role_id: int, permission_ids: List[int]) -> None:
        """
        Replace a role's permissions via the Superset REST API.
        """
        url = self.baseurl / "api/v1/security/roles" / str(role_id) / "permissions"
        payload = {"permission_view_menu_ids": permission_ids}
        _logger.debug("POST %s\n%s", url, json.dumps(payload, indent=4))
        validate_response(self.session.post(url, json=payload))

    def update_role_users(self, role_id: int, user_ids: List[int]) -> None:
        """
        Replace a role's directly assigned users via the Superset REST API.
        """
        url = self.baseurl / "api/v1/security/roles" / str(role_id) / "users"
        payload = {"user_ids": user_ids}
        _logger.debug("PUT %s\n%s", url, json.dumps(payload, indent=4))
        validate_response(self.session.put(url, json=payload))

    def update_role_groups(self, role_id: int, group_ids: List[int]) -> None:
        """
        Replace a role's assigned groups via the Superset REST API.
        """
        url = self.baseurl / "api/v1/security/roles" / str(role_id) / "groups"
        payload = {"group_ids": group_ids}
        _logger.debug("PUT %s\n%s", url, json.dumps(payload, indent=4))
        validate_response(self.session.put(url, json=payload))

    def get_users(self, **kwargs: str) -> List[UserType]:
        """
        Return users.

        Tries the REST API first (``/api/v1/security/users/``), which works for both
        Preset Cloud and recent Superset versions. Older Superset instances don't expose
        this endpoint, so we fall back to crawling the CRUD HTML page. Filtering is only
        supported for the API path.
        """
        try:
            return self._get_users_api(**kwargs)
        except SupersetError:
            return self._get_users_legacy()

    def _get_users_api(self, **kwargs: str) -> List[UserType]:
        """
        Return users, possibly filtered.
        """
        return [
            {
                "id": user["id"],
                "first_name": user["first_name"],
                "last_name": user["last_name"],
                "username": user["username"],
                "email": user["email"],
                "role": [role["name"] for role in user.get("roles", [])],
                # TODO: Include groups as well
            }
            for user in self.get_resources("security/users", "id", **kwargs)
        ]

    def get_report(self, report_id: int) -> Any:
        """
        Return a single report.
        """
        return self.get_resource("report", report_id)

    def get_reports(self, **kwargs: str) -> List[Any]:
        """
        Return reports, possibly filtered.
        """
        return self.get_resources("report", **kwargs)

    def create_report(self, **kwargs: Any) -> Any:
        """
        Create a report.
        """
        return self.create_resource("report", **kwargs)

    def update_report(self, report_id: int, **kwargs: Any) -> Any:
        """
        Update a report.
        """
        return self.update_resource("report", report_id, **kwargs)

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

    def get_uuids(
        self,
        resource_name: str,
        ids: Set[int] | None = None,
    ) -> Dict[int, UUID]:
        """
        Get UUID of a list of resources, possibly filtering by IDs.

        This method is very inneficient, but it's the only way to get the mapping
        between IDs and UUIDs in older versions of Superset.

        #TODO: Rely on UUIDs from API responses
        """
        url = self.baseurl / "api/v1" / resource_name / "export/"

        uuids: Dict[int, UUID] = {}
        resources = (
            self.get_resources(resource_name, id=In(list(ids)))
            if ids
            else self.get_resources(resource_name)
        )
        for resource in resources:
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

    def _is_preset_workspace(self) -> bool:
        """
        Return whether the target is a Preset workspace or a Superset instance.
        """
        return str(self.baseurl.host).endswith(f".{PRESET_WORKSPACE_DOMAIN}")

    def _get_users_legacy(self) -> List[UserType]:
        """
        Return all users from an older Superset instance.

        Since this is not exposed via an API we need to crawl the CRUD page.
        """
        users: List[UserType] = []
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
                users.append(
                    {
                        "id": int(tds[0].find("a").attrs["href"].split("/")[-1]),
                        "first_name": tds[1].text,
                        "last_name": tds[2].text,
                        "username": tds[3].text,
                        "email": tds[4].text,
                        "role": parse_html_array(tds[6].text.strip()),
                    },
                )
        return users

    def export_roles(self) -> List[RoleType]:
        """
        Return all roles.
        """
        if self._is_preset_workspace():
            return self._export_dars_preset()

        try:
            return self._export_roles_superset()
        except SupersetError:
            return self._export_roles_legacy()

    def _export_roles_superset(self) -> List[RoleType]:
        """
        Return all roles from a standalone Superset instance via the REST API.
        """
        user_email_map = {user["id"]: user["email"] for user in self.get_users()}
        perm_map = {
            perm["id"]: format_permission(
                perm["permission"]["name"],
                perm["view_menu"]["name"],
            )
            for perm in self.get_resources("security/permissions-resources", "id")
        }
        group_map = {
            group["id"]: group["name"]
            for group in self.get_resources("security/groups", "id")
        }

        roles: List[RoleType] = []
        for role in self.get_resources("security/roles/search", "id"):
            roles.append(
                {
                    "name": role["name"],
                    "permissions": [
                        perm_map[perm_id]
                        for perm_id in role["permission_ids"]
                        if perm_id in perm_map
                    ],
                    "users": [
                        user_email_map[user_id]
                        for user_id in role["user_ids"]
                        if user_id in user_email_map
                    ],
                    "groups": [
                        group_map[group_id]
                        for group_id in role["group_ids"]
                        if group_id in group_map
                    ],
                },
            )
        return roles

    def _export_dars_preset(self) -> List[RoleType]:
        """
        Return all DARs from a Preset workspace via the DAR API.
        """
        user_email_map = {user["id"]: user["email"] for user in self.get_users()}
        roles: List[RoleType] = []
        for role in self.get_resources("security/dar", "name"):
            roles.append(
                {
                    "name": role["name"],
                    "permissions": [
                        format_permission(perm["permission"], perm["view_menu"])
                        for perm in role["permissions"]
                    ],
                    "users": [
                        user_email_map[user["id"]]
                        for user in role["users"]
                        if user["id"] in user_email_map
                    ],
                    "groups": [group["name"] for group in role.get("groups", [])],
                },
            )
        return roles

    def _export_roles_legacy(  # pylint: disable=too-many-locals
        self,
    ) -> List[RoleType]:
        """
        Return all roles by crawling the CRUD HTML pages (older Superset versions).
        """
        user_email_map = {user["id"]: user["email"] for user in self.get_users()}
        roles: List[RoleType] = []
        page = 0
        while True:
            params = {
                "psize_RoleModelView": MAX_PAGE_SIZE,
                "page_RoleModelView": page,
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

                roles.append(
                    {
                        "name": name,
                        "permissions": permissions,
                        "users": users,
                        # Legacy Superset instances didn't support groups
                        "groups": [],
                    },
                )
        return roles

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

    def import_role(self, role: RoleType) -> None:
        """
        Import a given role.

        When migrating Superset roles into Preset, only data-access perms
        are migrated. Action perms are skipped with a warning (since in
        Preset Cloud these are controlled via Workspace Roles).
        """
        if self._is_preset_workspace():
            return self._import_dar_preset(role)

        try:
            existing_role_id = self._find_role_id_superset(role["name"])
        except SupersetError:
            return self._import_role_legacy(role)

        return self._import_role_superset(role, existing_role_id)

    def _find_role_id_superset(self, name: str) -> Optional[int]:
        """
        Return the id of an existing role with the given exact name via the
        standard Superset Role REST API, or ``None`` if none exists.

        Name filters on ``security/roles/search`` only support partial
        matching, so we fetch the full list and match exactly, like
        ``_export_roles_superset`` already does.

        Raises ``SupersetError`` if the REST security API isn't available
        (eg, an older Superset version), so callers can fall back to the
        legacy CRUD forms.
        """
        for existing in self.get_resources("security/roles/search", "id"):
            if existing["name"] == name:
                return existing["id"]
        return None

    def _find_dar_preset(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Return an existing DAR with the given exact name, or ``None`` if
        none exists, including its current permissions. DAR name filters only
        support partial matching, so we fetch the full list and match
        exactly.
        """
        for existing in self.get_resources("security/dar", "name"):
            if existing["name"] == name:
                return existing
        return None

    def _import_role_superset(
        self,
        role: RoleType,
        existing_role_id: Optional[int],
    ) -> None:
        """
        Import a role into a standalone Superset instance via the REST API.

        Match raw permission names from ``security/permissions-resources``,
        ignoring source-instance database and datasource IDs.
        """
        permission_ids = resolve_role_permissions(
            role["permissions"],
            [
                (
                    format_permission(
                        perm["permission"]["name"],
                        perm["view_menu"]["name"],
                    ),
                    perm["id"],
                )
                for perm in self.get_resources("security/permissions-resources", "id")
            ],
        )

        user_email_id_map = {user["email"]: user["id"] for user in self.get_users()}
        user_ids: List[int] = []
        for email in role["users"]:
            if email not in user_email_id_map:
                raise Exception(f"User {email} not found in target")
            user_ids.append(user_email_id_map[email])

        group_name_id_map = {
            group["name"]: group["id"]
            for group in self.get_resources("security/groups", "id")
        }
        group_ids: List[int] = []
        for group_name in role.get("groups", []):
            if group_name not in group_name_id_map:
                raise Exception(f"Group {group_name} not found in target")
            group_ids.append(group_name_id_map[group_name])

        if existing_role_id is not None:
            self.update_resource("security/roles", existing_role_id, name=role["name"])
            role_id = existing_role_id
        else:
            role_id = self.create_resource("security/roles", name=role["name"])["id"]

        self.update_role_permissions(role_id, permission_ids)
        self.update_role_users(role_id, user_ids)
        self.update_role_groups(role_id, group_ids)

    def _import_dar_preset(
        self,
        role: RoleType,
    ) -> None:
        """
        Import a role into a Preset workspace as a Data Access Role (DAR).

        Only data-access perms (database/catalog/schema/dataset access) can
        be represented by a DAR. Action perms can't,and are skipped with a
        warning. This allows migrating a role exported from Superset OSS
        into a Preset workspace.

        A source name that conflicts with an existing workspace role must be
        renamed before importing; workspace roles are never updated here.
        """
        data_permissions: List[str] = []
        skipped_actions = 0
        for permission in role["permissions"]:
            if permission.partition(" on ")[0] not in DATA_ACCESS_PERMISSION_NAMES:
                _logger.warning(
                    "Permission %r cannot be represented in a Data Access Role "
                    "(action permissions are not supported) and will be skipped",
                    permission,
                )
                skipped_actions += 1
                continue

            data_permissions.append(permission)

        # Preset labels raw datasource_access as "Dataset access" in "name".
        # Match the raw fields used by exports instead of translating labels.
        permission_ids = resolve_role_permissions(
            data_permissions,
            [
                (format_permission(perm["permission"], perm["view_menu"]), perm["id"])
                for perm in self.get_resources("security/dar_permissions", "id")
            ],
        )

        user_email_id_map = {user["email"]: user["id"] for user in self.get_users()}
        user_ids: List[int] = []
        for email in role["users"]:
            if email not in user_email_id_map:
                raise Exception(f"User {email} not found in target")
            user_ids.append(user_email_id_map[email])

        group_name_id_map = {
            group["name"]: group["id"]
            for group in self.get_resources("security/groups", "id")
        }
        group_ids: List[int] = []
        for group_name in role.get("groups", []):
            if group_name not in group_name_id_map:
                raise Exception(f"Group {group_name} not found in target")
            group_ids.append(group_name_id_map[group_name])

        payload: Dict[str, Any] = {
            "name": role["name"],
            "permissions": permission_ids,
            "users": user_ids,
            "groups": group_ids,
        }

        existing_dar = self._find_dar_preset(role["name"])
        if existing_dar is not None:
            self.update_resource("security/dar", existing_dar["id"], **payload)
        else:
            self.create_resource("security/dar", **payload)

        _logger.info(
            "Role %s: imported %d data-access permission(s), skipped %d action "
            "permission(s) unsupported by Data Access Roles",
            role["name"],
            len(permission_ids),
            skipped_actions,
        )

        if role["permissions"] and not permission_ids:
            if existing_dar and existing_dar["permissions"]:
                _logger.warning(
                    "Role %s overwritten as an empty Data Access Role: all %d "
                    "existing data-access permission(s) were removed because none "
                    "of its %d source permissions could be imported. This role "
                    "grants no data access.",
                    role["name"],
                    len(existing_dar["permissions"]),
                    len(role["permissions"]),
                )
            else:
                _logger.warning(
                    "Role %s imported as an empty Data Access Role: none of its %d "
                    "source permissions could be imported. This role grants no data "
                    "access.",
                    role["name"],
                    len(role["permissions"]),
                )

    def _import_role_legacy(  # pylint: disable=too-many-locals
        self,
        role: RoleType,
    ) -> None:
        """
        Import a role by crawling the CRUD HTML pages (for Superset
        instances without the REST security API).

        Note: like ``_export_roles_legacy``, this doesn't support groups --
        legacy Superset instances predate the Groups feature.
        """
        user_id_map = {user["email"]: user["id"] for user in self.get_users()}
        user_ids = [
            user_id_map[email] for email in role["users"] if email in user_id_map
        ]

        url = self.baseurl / "roles/add"
        _logger.debug("GET %s", url)
        response = self.session.get(url)
        soup = BeautifulSoup(response.text, features="html.parser")
        select = soup.find("select", id="permissions")
        permission_ids = resolve_role_permissions(
            role["permissions"],
            [
                (option.text.strip(), int(option.attrs["value"]))
                for option in select.find_all("option")
            ],
        )

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

    def export_ownership(
        self,
        resource_name: str,
        requested_ids: Set[int],
        users: Dict[int, str],
        exclude_old_users: bool,
    ) -> Iterator[OwnershipType]:
        """
        Return information about resource ownership.
        """
        uuids = self.get_uuids(resource_name, requested_ids)
        name_key = {
            "dataset": "table_name",
            "chart": "slice_name",
            "dashboard": "dashboard_title",
        }[resource_name]

        resources = (
            self.get_resources(resource_name, id=In(list(requested_ids)))
            if requested_ids
            else self.get_resources(resource_name)
        )
        for resource in resources:
            info: OwnershipType = {
                "name": resource[name_key],
                "uuid": uuids[resource["id"]],
                "owners": [],
            }
            for owner in resource.get("owners", []):
                if user := users.get(owner["id"]):
                    info["owners"].append(user)
                else:
                    if not exclude_old_users:
                        raise Exception(
                            f"User {owner['first_name']} {owner['last_name']} owns the "
                            f"{resource_name} {resource[name_key]} but is not a member in the team."
                            " You can trigger this command with ``--exclude-old-users`` to "
                            "automatically remove old members.",
                        )
                    _logger.warning(
                        "User %s %s not found in team. Removing from ownership config for %s %s.",
                        owner["first_name"],
                        owner["last_name"],
                        resource_name,
                        resource[name_key],
                    )
            yield info

    def import_ownership(
        self,
        resource_name: str,
        ownership: Dict[str, Any],
        user_ids: Dict[str, int],
        resource_ids: Dict[str, int],
    ) -> None:
        """
        Import ownership on resources.
        """
        if ownership["uuid"] in resource_ids:
            resource_id = resource_ids[ownership["uuid"]]
            owner_ids = [user_ids[email.lower()] for email in ownership["owners"]]
            self.update_resource(resource_name, resource_id, owners=owner_ids)
        else:
            raise Exception(
                f"Resource {ownership['name']} not found in the target instance.",
            )

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
