"""
A simple client for interacting with the dbt API.

References:

    - https://docs.getdbt.com/dbt-cloud/api-v2
    - https://docs.getdbt.com/docs/dbt-cloud/dbt-cloud-api/metadata/schema/metadata-schema-seeds
    - https://github.com/dbt-labs/dbt-cloud-openapi-spec/blob/master/openapi-v3.yaml
"""

# pylint: disable=invalid-name, too-few-public-methods

import logging
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Type

from marshmallow import INCLUDE, Schema, fields, pre_load
from python_graphql_client import GraphqlClient
from yarl import URL

from preset_cli import __version__
from preset_cli.auth.main import Auth

_logger = logging.getLogger(__name__)

REST_ENDPOINT = URL("https://cloud.getdbt.com/")
METADATA_GRAPHQL_ENDPOINT = URL("https://metadata.cloud.getdbt.com/graphql")
SEMANTIC_LAYER_GRAPHQL_ENDPOINT = URL(
    "https://semantic-layer.cloud.getdbt.com/api/graphql",
)


class PostelSchema(Schema):
    """
    Be liberal in what you accept, and conservative in what you send.

    A schema that allows unknown fields. This way if the API returns new fields that
    the client is not expecting no errors will be thrown when validating the payload.
    """

    class Meta:
        """
        Ignore unknown and unnecessary fields.
        """

        unknown = INCLUDE


def PostelEnumField(enum: Type[Enum], *args: Any, **kwargs: Any) -> fields.Field:
    """
    Lenient replacement for ``EnumField``.

    This allows us to keep track of the enums expected in a field, while still
    accepting any unexpected new values that are introduced.
    """
    if issubclass(enum, str):
        return fields.String(*args, **kwargs)

    if issubclass(enum, int):
        return fields.Integer(*args, **kwargs)

    return fields.Raw(*args, **kwargs)


class AccountSchema(PostelSchema):
    """
    Schema for a dbt account.
    """

    id = fields.Integer()
    name = fields.String()


class ProjectSchema(PostelSchema):
    """
    Schema for a dbt project.
    """

    id = fields.Integer(allow_none=True)
    name = fields.String()


class JobSchema(PostelSchema):
    """
    Schema for a dbt job.
    """

    id = fields.Integer(allow_none=True)
    name = fields.String()


class ModelSchema(PostelSchema):
    """
    Schema for a model.
    """

    depends_on = fields.List(fields.String())
    children = fields.List(fields.String())
    database = fields.String()
    schema = fields.String()
    description = fields.String()
    meta = fields.Raw()
    name = fields.String()
    alias = fields.String(allow_none=True)
    unique_id = fields.String()
    tags = fields.List(fields.String())
    columns = fields.Raw(allow_none=True)
    config = fields.Dict(fields.String(), fields.Raw(allow_none=True))

    @pre_load
    def rename_fields(  # pylint: disable=unused-argument
        self,
        data: Dict[str, Any],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Handle keys that can have camelCase or snake_case and Core/Cloud differences.
        """
        if "uniqueId" in data:
            data["unique_id"] = data.pop("uniqueId")

        if "childrenL1" in data:
            data["children"] = data.pop("childrenL1")

        if isinstance(columns := data.get("columns"), dict):
            data["columns"] = list(columns.values())

        if "dependsOn" in data:
            data["depends_on"] = data.pop("dependsOn")
        depends_on = data.get("depends_on", [])

        if isinstance(depends_on, dict):
            data["depends_on"] = depends_on["nodes"]

        return data


class FilterSchema(PostelSchema):
    """
    Schema for a metric filter.
    """

    field = fields.String()
    operator = fields.String()
    value = fields.String()


class MetricSchema(PostelSchema):
    """
    Base schema for a dbt metric.
    """

    name = fields.String()
    label = fields.String()
    description = fields.String()
    meta = fields.Raw()


class OGMetricSchema(MetricSchema):
    """
    Schema for an OG metric.
    """

    depends_on = fields.List(fields.String())
    filters = fields.List(fields.Nested(FilterSchema))
    sql = fields.String()
    type = fields.String()
    unique_id = fields.String()
    # dbt >= 1.3
    calculation_method = fields.String()
    expression = fields.String()
    dialect = fields.String()
    skip_parsing = fields.Boolean(allow_none=True)

    @pre_load
    def rename_fields(  # pylint: disable=unused-argument
        self,
        data: Dict[str, Any],
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Handle keys that can have camelCase or snake_case and Core/Cloud differences.
        """
        if "uniqueId" in data:
            data["unique_id"] = data.pop("uniqueId")

        if "dependsOn" in data:
            data["depends_on"] = data.pop("dependsOn")
        depends_on = data.get("depends_on", [])

        if isinstance(depends_on, dict):
            data["depends_on"] = depends_on["nodes"]

        return data


class MFMetricType(str, Enum):
    """
    Type of the MetricFlow metric.
    """

    SIMPLE = "SIMPLE"
    RATIO = "RATIO"
    CUMULATIVE = "CUMULATIVE"
    DERIVED = "DERIVED"


class MFMetricSchema(MetricSchema):
    """
    Schema for a MetricFlow metric.
    """

    type = PostelEnumField(MFMetricType)


class MFSQLEngine(str, Enum):
    """
    Databases supported by MetricFlow.
    """

    BIGQUERY = "BIGQUERY"
    DUCKDB = "DUCKDB"
    REDSHIFT = "REDSHIFT"
    POSTGRES = "POSTGRES"
    SNOWFLAKE = "SNOWFLAKE"
    DATABRICKS = "DATABRICKS"


class MFMetricWithSQLSchema(MFMetricSchema):
    """
    MetricFlow metric with dialect and SQL, as well as model.
    """

    sql = fields.String()
    dialect = PostelEnumField(MFSQLEngine)
    model = fields.String()


def get_custom_urls(access_url: Optional[str] = None) -> Dict[str, URL]:
    """
    Return new custom URLs for dbt Cloud access.
    """
    if access_url is None:
        return {
            "admin": REST_ENDPOINT,
            "discovery": METADATA_GRAPHQL_ENDPOINT,
            "semantic-layer": SEMANTIC_LAYER_GRAPHQL_ENDPOINT,
        }

    regex_pattern = r"""
        (?P<code>
            [a-zA-Z0-9]
            (?:
                [a-zA-Z0-9-]{0,61}
                [a-zA-Z0-9]
            )?
        )
        \.
        (?P<region>
            [a-zA-Z0-9]
            (?:
                [a-zA-Z0-9-]{0,61}
                [a-zA-Z0-9]
            )?
        )
        \.dbt.com
        $
    """

    parsed = URL(access_url)
    if match := re.match(regex_pattern, parsed.host, re.VERBOSE):
        return {
            "admin": parsed.with_host(
                f"{match['code']}.{match['region']}.dbt.com",
            ),
            "discovery": parsed.with_host(
                f"{match['code']}.metadata.{match['region']}.dbt.com",
            )
            / "graphql",
            "semantic-layer": parsed.with_host(
                f"{match['code']}.semantic-layer.{match['region']}.dbt.com",
            )
            / "api/graphql",
        }

    raise Exception("Invalid host in custom URL")


class DBTClient:  # pylint: disable=too-few-public-methods
    """
    A client for the dbt API.
    """

    def __init__(self, auth: Auth, access_url: Optional[str] = None):
        urls = get_custom_urls(access_url)
        self.metadata_graphql_client = GraphqlClient(endpoint=urls["discovery"])
        self.semantic_layer_graphql_client = GraphqlClient(
            endpoint=urls["semantic-layer"],
        )
        self.baseurl = urls["admin"]

        self.session = auth.session
        self.session.headers.update(auth.get_headers())
        self.session.headers["User-Agent"] = "Preset CLI"
        self.session.headers["X-Client-Version"] = __version__
        self.session.headers["X-dbt-partner-source"] = "preset"

    def get_accounts(self) -> List[AccountSchema]:
        """
        List all accounts.
        """
        url = self.baseurl / "api/v3/accounts/"
        _logger.debug("GET %s", url)
        response = self.session.get(url)

        payload = response.json()

        if not response.ok:
            raise Exception(payload["status"]["user_message"])

        account_schema = AccountSchema()
        return [account_schema.load(row) for row in payload["data"]]

    def get_projects(self, account_id: int) -> List[ProjectSchema]:
        """
        List all projects.
        """
        url = self.baseurl / "api/v3/accounts" / str(account_id) / "projects/"
        _logger.debug("GET %s", url)
        response = self.session.get(url)

        payload = response.json()

        if not response.ok:
            raise Exception(payload["status"]["user_message"])

        project_schema = ProjectSchema()
        projects = [project_schema.load(project) for project in payload["data"]]

        return projects

    def get_jobs(
        self,
        account_id: int,
        project_id: Optional[int] = None,
    ) -> List[JobSchema]:
        """
        List all jobs, optionally for a project.
        """
        url = self.baseurl / "api/v2/accounts" / str(account_id) / "jobs/"
        params = {"project_id": project_id} if project_id is not None else {}
        _logger.debug("GET %s", url % params)
        response = self.session.get(url, params=params)

        payload = response.json()

        if not response.ok:
            raise Exception(payload["status"]["user_message"])

        job_schema = JobSchema()
        jobs = [job_schema.load(job) for job in payload["data"]]

        return jobs

    def get_models(self, job_id: int) -> List[ModelSchema]:
        """
        Fetch all available models.
        """
        query = """
            query Models($jobId: BigInt!) {
                job(id: $jobId) {
                    models {
                        uniqueId
                        dependsOn
                        childrenL1
                        name
                        database
                        schema
                        description
                        meta
                        tags
                        columns {
                            name
                            description
                            type
                            meta
                        }
                    }
                }
            }
        """
        payload = self.metadata_graphql_client.execute(
            query=query,
            variables={"jobId": job_id},
            headers=self.session.headers,
        )

        model_schema = ModelSchema()
        models = [
            model_schema.load(model) for model in payload["data"]["job"]["models"]
        ]

        return models

    def get_og_metrics(self, job_id: int) -> List[OGMetricSchema]:
        """
        Fetch all available metrics.
        """
        query = """
            query GetMetrics($jobId: BigInt!) {
                job(id: $jobId) {
                    metrics {
                        uniqueId
                        name
                        label
                        type
                        sql
                        filters {
                            field
                            operator
                            value
                        }
                        dependsOn
                        description
                        meta
                    }
                }
            }
        """
        payload = self.metadata_graphql_client.execute(
            query=query,
            variables={"jobId": job_id},
            headers=self.session.headers,
        )

        metric_schema = OGMetricSchema()
        metrics = [
            metric_schema.load(metric)
            for metric in payload["data"]["job"]["metrics"]
            if metric.get("sql")
        ]

        return metrics

    def get_sl_metrics(self, environment_id: int) -> List[MFMetricSchema]:
        """
        Fetch all available metrics.
        """
        query = """
            query GetMetrics($environmentId: BigInt!) {
                metrics(environmentId: $environmentId) {
                    name
                    description
                    type
                    label
                }
            }
        """
        payload = self.semantic_layer_graphql_client.execute(
            query=query,
            variables={"environmentId": environment_id},
            headers=self.session.headers,
        )
        # In case the project doesn't have a semantic layer (old versions)
        if payload["data"] is None:
            return []
        metric_schema = MFMetricSchema()
        metrics = [metric_schema.load(metric) for metric in payload["data"]["metrics"]]

        return metrics

    def get_sl_metric_sql(self, metric: str, environment_id: int) -> Optional[str]:
        """
        Fetch metric SQL.

        We fetch one metric at a time because if one metric fails to compile, the entire
        query fails.
        """
        query = """
            mutation CompileSql($environmentId: BigInt!, $metricsInput: [MetricInput!]) {
                compileSql(
                    environmentId: $environmentId
                    metrics: $metricsInput
                    groupBy: []
                ) {
                    sql
                }
            }
        """
        payload = self.semantic_layer_graphql_client.execute(
            query=query,
            variables={
                "environmentId": environment_id,
                "metricsInput": [{"name": metric}],
            },
            headers=self.session.headers,
        )

        if payload["data"] is None:
            errors = "\n\n".join(
                error["message"] for error in payload.get("errors", [])
            )
            _logger.warning("Unable to convert metric %s: %s", metric, errors)
            return None

        return payload["data"]["compileSql"]["sql"]

    def get_sl_dialect(self, environment_id: int) -> MFSQLEngine:
        """
        Get the dialect used in the MetricFlow project.
        """
        query = """
            query GetEnvironmentInfo($environmentId: BigInt!) {
                environmentInfo(environmentId: $environmentId) {
                    dialect
                }
            }
        """
        payload = self.semantic_layer_graphql_client.execute(
            query=query,
            variables={"environmentId": environment_id},
            headers=self.session.headers,
        )

        return MFSQLEngine(payload["data"]["environmentInfo"]["dialect"])

    # def get_sl_metric_sql(self,

    def get_database_name(self, job_id: int) -> str:
        """
        Return the database name.

        This is done by querying all models in a job. As far as I know there should be
        only one database associated with them.
        """
        models = self.get_models(job_id)
        if not models:
            raise Exception("No models found, can't determine database name")

        databases = {model["database"] for model in models}
        if len(databases) > 1:
            raise Exception("Multiple databases found")

        return databases.pop()
