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
from typing import Any, Dict, List, Optional, Type, TypedDict

from marshmallow import INCLUDE, Schema, fields
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
        Allow unknown fields.
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


class DBTAccountPlan(str, Enum):
    """
    Plan for the account.
    """

    developer = "developer"
    team = "team"
    enterprise = "enterprise"


class DBTUserPlan(str, Enum):
    """
    Plan for the user.
    """

    free = "free"
    trial = "trial"
    enterprise = "enterprise"
    developer = "developer"
    team = "team"
    cancelled = "cancelled"


class EnterpriseAuthentication(str, Enum):
    """
    Authentication methods for enterprise.
    """

    none = "none"
    okta = "okta"
    azure_ad = "azure_ad"
    gsuite = "gsuite"


class GitAuthLevel(str, Enum):
    """
    Indicates the git provider authentication level for this user
    """

    personal = "personal"
    team = "team"


class AccountSchema(PostelSchema):
    """
    Schema for a dbt account.
    """

    id = fields.Integer()
    name = fields.String()
    plan = PostelEnumField(DBTAccountPlan)
    pending_cancel = fields.Boolean()
    state = fields.Integer()
    developer_seats = fields.Integer()
    read_only_seats = fields.Integer()
    run_slots = fields.Integer()
    created_at = fields.DateTime()
    updated_at = fields.DateTime()


class LicenseType(str, Enum):
    """
    Types of licenses.
    """

    developer = "developer"
    read_only = "read_only"


class GroupPermission(str, Enum):
    """
    Group permissions.
    """

    owner = "owner"
    member = "member"
    account_admin = "account_admin"
    admin = "admin"
    database_admin = "database_admin"
    git_admin = "git_admin"
    team_admin = "team_admin"
    job_admin = "job_admin"
    job_viewer = "job_viewer"
    analyst = "analyst"
    developer = "developer"
    stakeholder = "stakeholder"
    readonly = "readonly"
    project_creator = "project_creator"
    account_viewer = "account_viewer"
    metadata_only = "metadata_only"
    webhooks_only = "webhooks_only"


class GroupPermissionSchema(PostelSchema):
    """
    Schema for group permissions.
    """

    id = fields.String()
    account_id = fields.String()
    project_id = fields.String(allow_none=True)
    all_projects = fields.Boolean()
    permission_set = PostelEnumField(GroupPermission)
    permission_level = fields.Integer(allow_none=True)
    state = fields.Integer()


class GroupSchema(PostelSchema):
    """
    A dbt group.
    """

    id = fields.Integer()
    account_id = fields.Integer()
    name = fields.String()
    state = fields.Integer()
    assign_by_default = fields.Boolean()
    group_permissions = fields.List(fields.Nested(GroupPermissionSchema))


class PermissionStatement(str, Enum):
    """
    Permission types.
    """

    billing_read = "billing_read"
    billing_write = "billing_write"
    invitations_send = "invitations_send"
    invitations_modify = "invitations_modify"
    invitations_read = "invitations_read"
    members_read = "members_read"
    members_write = "members_write"
    groups_read = "groups_read"
    groups_write = "groups_write"
    license_read = "license_read"
    license_allocate = "license_allocate"
    projects_read = "projects_read"
    projects_develop = "projects_develop"
    projects_write = "projects_write"
    projects_create = "projects_create"
    projects_delete = "projects_delete"
    environments_read = "environments_read"
    environments_write = "environments_write"
    develop_access = "develop_access"
    dbt_adapters_read = "dbt_adapters_read"
    dbt_adapters_write = "dbt_adapters_write"
    credentials_read = "credentials_read"
    credentials_write = "credentials_write"
    connections_read = "connections_read"
    connections_write = "connections_write"
    jobs_read = "jobs_read"
    jobs_write = "jobs_write"
    repositories_read = "repositories_read"
    repositories_write = "repositories_write"
    runs_trigger = "runs_trigger"
    runs_write = "runs_write"
    runs_read = "runs_read"
    permissions_write = "permissions_write"
    permissions_read = "permissions_read"
    account_settings_write = "account_settings_write"
    account_settings_read = "account_settings_read"
    auth_provider_write = "auth_provider_write"
    auth_provider_read = "auth_provider_read"
    service_tokens_write = "service_tokens_write"
    service_tokens_read = "service_tokens_read"
    metadata_read = "metadata_read"
    webhooks_write = "webhooks_write"
    custom_environment_variables_read = "custom_environment_variables_read"
    custom_environment_variables_write = "custom_environment_variables_write"
    audit_log_read = "audit_log_read"


class PermissionStatementSchema(PostelSchema):
    """
    Permission statements.
    """

    permission = PostelEnumField(PermissionStatement)
    target_resource = fields.Integer()
    all_resources = fields.Boolean()


class UserLicenseSchema(PostelSchema):
    """
    Schema for user permissions.
    """

    id = fields.Integer()
    license_type = PostelEnumField(LicenseType)
    user_id = fields.Integer()
    account_id = fields.Integer()
    state = fields.Integer()
    groups = fields.List(fields.Nested(GroupSchema))
    permission_statements = fields.List(fields.Nested(PermissionStatementSchema))


class UserSchema(PostelSchema):
    """
    Schema for a dbt user.
    """

    id = fields.Integer()
    state = fields.Integer()
    name = fields.String()
    lock_reason = fields.String(allow_none=True)
    unlock_if_subscription_renewed = fields.Boolean()
    plan = PostelEnumField(DBTUserPlan)
    pending_cancel = fields.Boolean()
    run_slots = fields.Integer()
    developer_seats = fields.Integer()
    read_only_seats = fields.Integer()
    queue_limit = fields.Integer()
    pod_memory_request_mebibytes = fields.Integer()
    run_duration_limit_seconds = fields.Integer()
    enterprise_authentication_method = PostelEnumField(
        EnterpriseAuthentication,
        allow_none=True,
    )
    enterprise_login_slug = fields.String(allow_none=True)
    enterprise_unique_identifier = fields.String(allow_none=True)
    billing_email_address = fields.String(allow_none=True)
    locked = fields.Boolean()
    unlocked_at = fields.DateTime()
    created_at = fields.DateTime()
    updated_at = fields.DateTime()
    starter_repo_url = fields.String(allow_none=True)
    sso_reauth = fields.Boolean()
    git_auth_level = PostelEnumField(GitAuthLevel, allow_none=True)
    identifier = fields.String()
    docs_job_id = fields.Integer(allow_none=True)
    freshness_job_id = fields.Integer(allow_none=True)
    docs_job = fields.String(allow_none=True)
    freshness_job = fields.String(allow_none=True)
    enterprise_login_url = fields.String(allow_none=True)
    permissions = fields.Nested(UserLicenseSchema)

    # not present in the spec
    develop_file_system = fields.Boolean()
    force_sso = fields.Boolean()


class ConnectionType(str, Enum):
    """
    Connection types.
    """

    postgres = "postgres"
    redshift = "redshift"
    snowflake = "snowflake"
    bigquery = "bigquery"
    adapter = "adapter"


class ConnectionSchema(PostelSchema):
    """
    Schema for connection information.
    """

    id = fields.Integer(allow_none=True)
    account_id = fields.Integer()
    project_id = fields.Integer()
    name = fields.String()
    type = PostelEnumField(ConnectionType)
    state = fields.Integer()
    created_by_id = fields.Integer(allow_none=True)
    created_by_service_token_id = fields.Integer(allow_none=True)
    created_at = fields.DateTime()
    updated_at = fields.DateTime()
    details = fields.Raw()


class GitCloneStrategy(str, Enum):
    """
    Types of strategies for cloning git.
    """

    azure_active_directory_app = "azure_active_directory_app"
    deploy_key = "deploy_key"
    deploy_token = "deploy_token"
    github_app = "github_app"
    git_token = "git_token"


class DeployKeySchema(PostelSchema):
    """
    Schema for a deployment key.
    """

    id = fields.Integer()
    account_id = fields.Integer()
    state = fields.Integer()
    public_key = fields.String()


class RepositorySchema(PostelSchema):
    """
    Schema for a repository.
    """

    id = fields.Integer(allow_none=True)
    account_id = fields.Integer()
    remote_url = fields.String(allow_none=True)
    remote_backend = fields.String(allow_none=True)
    git_clone_strategy = PostelEnumField(GitCloneStrategy)
    deploy_key_id = fields.Integer(allow_none=True)
    github_installation_id = fields.Integer(allow_none=True)
    state = fields.Integer()
    created_at = fields.DateTime()
    updated_at = fields.DateTime()

    # not present in the spec
    full_name = fields.String(allow_none=True)
    repository_credentials_id = fields.Integer(allow_none=True)
    gitlab = fields.String(allow_none=True)
    name = fields.String()
    pull_request_url_template = fields.String(allow_none=True)
    git_provider_id = fields.Integer(allow_none=True)
    git_provider = fields.String(allow_none=True)
    project_id = fields.Integer(allow_none=True)
    deploy_key = fields.Nested(DeployKeySchema)
    github_repo = fields.String(allow_none=True)


class ProjectSchema(PostelSchema):
    """
    Schema for a dbt project.
    """

    id = fields.Integer(allow_none=True)
    account_id = fields.Integer()
    connection = fields.Nested(ConnectionSchema, allow_none=True)
    connection_id = fields.Integer(allow_none=True)
    dbt_project_subdirectory = fields.String(allow_none=True)
    name = fields.String()
    repository = fields.Nested(RepositorySchema, allow_none=True)
    repository_id = fields.Integer(allow_none=True)
    state = fields.Integer()
    created_at = fields.DateTime(allow_none=True)
    updated_at = fields.DateTime(allow_none=True)

    # not present in the spec
    group_permissions = fields.List(
        fields.Nested(GroupPermissionSchema),
        allow_none=True,
    )
    docs_job = fields.Nested("JobSchema", allow_none=True)
    docs_job_id = fields.Integer(allow_none=True)
    freshness_job_id = fields.Integer(allow_none=True)
    freshness_job = fields.Nested("JobSchema", allow_none=True)
    skipped_setup = fields.Boolean(allow_none=True)


class TriggerSchema(PostelSchema):
    """
    Schema for a job trigger.
    """

    github_webhook = fields.Boolean(required=True)
    git_provider_webhook = fields.Boolean()
    schedule = fields.Boolean(required=True)
    custom_branch_only = fields.Boolean()


class SettingsSchema(PostelSchema):
    """
    Schema for job settings.
    """

    threads = fields.Integer(required=True)
    target_name = fields.String(required=True)


class DateType(str, Enum):
    """
    Types of date.
    """

    every_day = "every_day"
    days_of_week = "days_of_week"
    custom_cron = "custom_cron"


class DateSchema(PostelSchema):
    """
    Schema for a date.
    """

    type = PostelEnumField(DateType, required=True)
    days = fields.List(fields.Integer(), allow_none=True)
    cron = fields.String(allow_none=True)


class TimeType(str, Enum):
    """
    Types of time.
    """

    every_hour = "every_hour"
    at_exact_hours = "at_exact_hours"


class TimeSchema(PostelSchema):
    """
    Schema for a time.
    """

    type = PostelEnumField(TimeType, required=True)
    interval = fields.Integer(allow_none=True)
    hours = fields.List(fields.Integer(), allow_none=True)


class StringOrSchema(fields.Field):
    """
    Dynamic schema constructor for fields that could have a string or another schema.
    """

    def __init__(self, nested_schema, *args, **kwargs):
        self.nested_schema = nested_schema
        super().__init__(*args, **kwargs)

    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, str):
            return value

        return self.nested_schema().load(value)


class ScheduleSchema(PostelSchema):
    """
    Schema for a job schedule.
    """

    cron = fields.String()
    date = StringOrSchema(DateSchema, required=True)
    time = StringOrSchema(TimeSchema, required=True)


class ExecutionSchema(PostelSchema):
    """
    Schema for a job execution.
    """

    timeout_seconds = fields.Integer()


class JobSchema(PostelSchema):
    """
    Schema for a dbt job.
    """

    id = fields.Integer(allow_none=True)
    account_id = fields.Integer()
    project_id = fields.Integer()
    environment_id = fields.Integer()
    name = fields.String()
    dbt_version = fields.String(allow_none=True)
    raw_dbt_version = fields.String(allow_none=True)
    triggers = fields.Nested(TriggerSchema)
    execute_steps = fields.List(fields.String())
    settings = fields.Nested(SettingsSchema)
    state = fields.Integer()
    generate_docs = fields.Boolean()
    schedule = fields.Nested(ScheduleSchema)

    # not present in the spec
    lifecycle_webhooks_url = fields.String(allow_none=True)
    cron_humanized = fields.String()
    created_at = fields.DateTime()
    next_run = fields.DateTime(allow_none=True)
    lifecycle_webhooks = fields.Boolean(allow_none=True)
    next_run_humanized = fields.String(allow_none=True)
    deferring_job_definition_id = fields.Integer(allow_none=True)
    deactivated = fields.Boolean()
    is_deferrable = fields.Boolean()
    updated_at = fields.DateTime()
    execution = fields.Nested(ExecutionSchema)
    run_failure_count = fields.Integer()
    run_generate_sources = fields.Boolean()
    generate_sources = fields.Boolean()


class ModelSchema(PostelSchema):
    """
    Schema for a model.
    """

    depends_on = fields.List(fields.String(), data_key="dependsOn")
    children = fields.List(fields.String(), data_key="childrenL1")
    database = fields.String()
    schema = fields.String()
    description = fields.String()
    meta = fields.Raw()
    name = fields.String()
    unique_id = fields.String(data_key="uniqueId")
    tags = fields.List(fields.String())
    columns = fields.Raw()
    config = fields.Dict(fields.String(), fields.Raw(allow_none=True))


class FilterSchema(PostelSchema):
    """
    Schema for a metric filter.
    """

    field = fields.String()
    operator = fields.String()
    value = fields.String()


class MetricSchema(PostelSchema):
    """
    Schema for a metric.
    """

    depends_on = fields.List(fields.String(), data_key="dependsOn")
    description = fields.String()
    filters = fields.List(fields.Nested(FilterSchema))
    meta = fields.Raw()
    name = fields.String()
    label = fields.String()
    sql = fields.String()
    type = fields.String()
    unique_id = fields.String(data_key="uniqueId")
    # dbt >= 1.3
    calculation_method = fields.String()
    expression = fields.String()
    dialect = fields.String()
    skip_parsing = fields.Boolean(allow_none=True)


class MFMetricType(str, Enum):
    """
    Type of the MetricFlow metric.
    """

    SIMPLE = "SIMPLE"
    RATIO = "RATIO"
    CUMULATIVE = "CUMULATIVE"
    DERIVED = "DERIVED"


class MFMetricSchema(PostelSchema):
    """
    Schema for a MetricFlow metric.
    """

    name = fields.String()
    description = fields.String()
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


class DataResponse(TypedDict):
    """
    Type for the GraphQL response.
    """

    data: Dict[str, Any]


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
            ),
            "semantic-layer": parsed.with_host(
                f"{match['code']}.semantic-layer.{match['region']}.dbt.com",
            ),
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
        url = self.baseurl / "api/v2/accounts/"
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
        url = self.baseurl / "api/v2/accounts" / str(account_id) / "projects/"
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

    def get_og_metrics(self, job_id: int) -> List[MetricSchema]:
        """
        Fetch all available metrics.
        """
        query = """
            query GetMetrics($jobId: Int!) {
                metrics(jobId: $jobId) {
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
        """
        payload = self.metadata_graphql_client.execute(
            query=query,
            variables={"jobId": job_id},
            headers=self.session.headers,
        )

        metric_schema = MetricSchema()
        metrics = [metric_schema.load(metric) for metric in payload["data"]["metrics"]]

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
                }
            }
        """
        payload = self.semantic_layer_graphql_client.execute(
            query=query,
            variables={"environmentId": environment_id},
            headers=self.session.headers,
        )

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
