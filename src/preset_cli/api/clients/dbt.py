"""
A simple client for interacting with the dbt API.

References:

    - https://docs.getdbt.com/dbt-cloud/api-v2
    - https://docs.getdbt.com/docs/dbt-cloud/dbt-cloud-api/metadata/schema/metadata-schema-seeds
    - https://github.com/dbt-labs/dbt-cloud-openapi-spec/blob/master/openapi-v3.yaml
"""

# pylint: disable=invalid-name, too-few-public-methods

from enum import Enum
from typing import Any, Dict, List, Type, TypedDict

from marshmallow import INCLUDE, Schema, fields
from python_graphql_client import GraphqlClient
from yarl import URL

from preset_cli import __version__
from preset_cli.auth.main import Auth

REST_ENDPOINT = URL("https://cloud.getdbt.com/")
GRAPHQL_ENDPOINT = URL("https://metadata.cloud.getdbt.com/graphql")


class PostelSchema(Schema):
    """
    Be liberal in what you accept, and conservative in what you send.

    A schema that allows unknown fields. This way if they API returns new fields that
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

    id = fields.Integer()
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

    id = fields.Integer()
    account_id = fields.Integer()
    remote_url = fields.String()
    remote_backend = fields.String()
    git_clone_strategy = PostelEnumField(GitCloneStrategy)
    deploy_key_id = fields.Integer()
    github_installation_id = fields.Integer()
    state = fields.Integer()
    created_at = fields.DateTime()
    updated_at = fields.DateTime()

    # not present in the spec
    full_name = fields.String()
    repository_credentials_id = fields.Integer(allow_none=True)
    gitlab = fields.String(allow_none=True)
    name = fields.String()
    pull_request_url_template = fields.String()
    git_provider_id = fields.Integer()
    git_provider = fields.String(allow_none=True)
    project_id = fields.Integer()
    deploy_key = fields.Nested(DeployKeySchema)
    github_repo = fields.String()


class ProjectSchema(PostelSchema):
    """
    Schema for a dbt project.
    """

    id = fields.Integer()
    account_id = fields.Integer()
    connection = fields.Nested(ConnectionSchema)
    connection_id = fields.Integer()
    dbt_project_subdirectory = fields.String(allow_none=True)
    name = fields.String()
    repository = fields.Nested(RepositorySchema)
    repository_id = fields.Integer()
    state = fields.Integer()
    created_at = fields.DateTime()
    updated_at = fields.DateTime()

    # not present in the spec
    group_permissions = fields.List(fields.Nested(GroupPermissionSchema))
    docs_job = fields.String(allow_none=True)
    docs_job_id = fields.Integer(allow_none=True)
    freshness_job_id = fields.Integer(allow_none=True)
    freshness_job = fields.String(allow_none=True)
    skipped_setup = fields.Boolean()


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


class ScheduleSchema(PostelSchema):
    """
    Schema for a job schedule.
    """

    cron = fields.String()
    date = fields.Nested(DateSchema, required=True)
    time = fields.Nested(TimeSchema, required=True)


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
    next_run = fields.DateTime()
    lifecycle_webhooks = fields.Boolean()
    next_run_humanized = fields.String()
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


class DataResponse(TypedDict):
    """
    Type for the GraphQL response.
    """

    data: Dict[str, Any]


class DBTClient:  # pylint: disable=too-few-public-methods

    """
    A client for the dbt API.
    """

    def __init__(self, auth: Auth):
        self.auth = auth
        self.auth.headers.update(
            {
                "User-Agent": "Preset CLI",
                "X-Client-Version": __version__,
            },
        )
        self.graphql_client = GraphqlClient(endpoint=GRAPHQL_ENDPOINT)
        self.baseurl = REST_ENDPOINT

    def execute(self, query: str, **variables: Any) -> DataResponse:
        """
        Run a GraphQL query.
        """
        return self.graphql_client.execute(
            query=query,
            variables=variables,
            headers=self.auth.get_headers(),
        )

    def get_accounts(self) -> List[AccountSchema]:
        """
        List all accounts.
        """
        session = self.auth.get_session()
        headers = self.auth.get_headers()
        response = session.get(self.baseurl / "api/v2/accounts/", headers=headers)

        payload = response.json()

        account_schema = AccountSchema()
        return [account_schema.load(row) for row in payload["data"]]

    def get_projects(self, account_id: int) -> List[ProjectSchema]:
        """
        List all projects.
        """
        session = self.auth.get_session()
        headers = self.auth.get_headers()
        response = session.get(
            self.baseurl / "api/v2/accounts" / str(account_id) / "projects/",
            headers=headers,
        )

        payload = response.json()

        project_schema = ProjectSchema()
        projects = [project_schema.load(project) for project in payload["data"]]

        return projects

    def get_jobs(self, account_id: int) -> List[JobSchema]:
        """
        List all jobs.
        """
        session = self.auth.get_session()
        headers = self.auth.get_headers()
        response = session.get(
            self.baseurl / "api/v2/accounts" / str(account_id) / "jobs/",
            headers=headers,
        )

        payload = response.json()

        job_schema = JobSchema()
        jobs = [job_schema.load(job) for job in payload["data"]]

        return jobs

    def get_models(self, job_id: int) -> List[ModelSchema]:
        """
        Fetch all available models.
        """
        query = """
            query ($jobId: Int!) {
                models(jobId: $jobId) {
                    uniqueId
                    dependsOn
                    childrenL1
                    name
                    database
                    schema
                    description
                    meta
                    tags
                }
            }
        """
        payload = self.execute(query, jobId=job_id)

        model_schema = ModelSchema()
        models = [model_schema.load(model) for model in payload["data"]["models"]]

        return models

    def get_metrics(self, job_id: int) -> List[Any]:
        """
        Fetch all available metrics.
        """
        query = """
            query ($jobId: Int!) {
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
        payload = self.execute(query, jobId=job_id)

        metric_schema = MetricSchema()
        metrics = [metric_schema.load(metric) for metric in payload["data"]["metrics"]]

        return metrics

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
