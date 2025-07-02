"""
A simple client for interacting with the dbt API.

References:

    - https://docs.getdbt.com/dbt-cloud/api-v2
    - https://docs.getdbt.com/docs/dbt-cloud/dbt-cloud-api/metadata/schema/metadata-schema-seeds
    - https://github.com/dbt-labs/dbt-cloud-openapi-spec/blob/master/openapi-v3.yaml
"""

import logging
import re
from typing import Dict, List, Optional

from python_graphql_client import GraphqlClient
from yarl import URL

from preset_cli import __version__
from preset_cli.auth.main import Auth
from preset_cli.cli.superset.sync.dbt.schemas import (
    AccountSchema,
    JobSchema,
    MFMetricSchema,
    MFSQLEngine,
    ModelSchema,
    OGMetricSchema,
    ProjectSchema,
)

_logger = logging.getLogger(__name__)

REST_ENDPOINT = URL("https://cloud.getdbt.com/")
METADATA_GRAPHQL_ENDPOINT = URL("https://metadata.cloud.getdbt.com/graphql")
SEMANTIC_LAYER_GRAPHQL_ENDPOINT = URL(
    "https://semantic-layer.cloud.getdbt.com/api/graphql",
)


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
                    config {
                        meta
                    }
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
