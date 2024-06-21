"""
A command to sync dbt models/metrics to Superset and charts/dashboards back as exposures.
"""

import logging
import os.path
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import click
import yaml
from yarl import URL

from preset_cli.api.clients.dbt import (
    DBTClient,
    JobSchema,
    MetricSchema,
    MFMetricWithSQLSchema,
    MFSQLEngine,
    ModelSchema,
)
from preset_cli.api.clients.superset import SupersetClient
from preset_cli.auth.token import TokenAuth
from preset_cli.cli.superset.sync.dbt.databases import sync_database
from preset_cli.cli.superset.sync.dbt.datasets import sync_datasets
from preset_cli.cli.superset.sync.dbt.exposures import ModelKey, sync_exposures
from preset_cli.cli.superset.sync.dbt.lib import (
    apply_select,
    list_failed_models,
    load_profiles,
)
from preset_cli.cli.superset.sync.dbt.metrics import (
    get_models_from_sql,
    get_superset_metrics_per_model,
)
from preset_cli.exceptions import CLIError, DatabaseNotFoundError
from preset_cli.lib import raise_cli_errors

_logger = logging.getLogger(__name__)


@click.command()
@click.argument("file", type=click.Path(exists=True, resolve_path=True))
@click.option("--project", help="Name of the dbt project", default=None)
@click.option("--target", help="Target name", default=None)
@click.option(
    "--profiles",
    help="Location of profiles.yml file",
    type=click.Path(exists=True, resolve_path=True),
)
@click.option(
    "--exposures",
    help="Path to file where exposures will be written",
    type=click.Path(exists=False),
)
@click.option(
    "--import-db",
    is_flag=True,
    default=False,
    help="Import (or update) the database connection to Superset",
)
@click.option(
    "--disallow-edits",
    is_flag=True,
    default=False,
    help="Mark resources as managed externally to prevent edits",
)
@click.option("--external-url-prefix", default="", help="Base URL for resources")
@click.option(
    "--select",
    "-s",
    help="Model selection",
    multiple=True,
)
@click.option(
    "--exclude",
    "-x",
    help="Models to exclude",
    multiple=True,
)
@click.option(
    "--exposures-only",
    is_flag=True,
    default=False,
    help="Do not sync models to datasets and only fetch exposures instead",
)
@click.option(
    "--preserve-columns",
    is_flag=True,
    default=False,
    help="Preserve column and metric configurations defined in Preset",
)
@click.option(
    "--preserve-metadata",
    is_flag=True,
    default=False,
    help="Preserve column and metric configurations defined in Preset",
)
@click.option(
    "--merge-metadata",
    is_flag=True,
    default=False,
    help="Update Preset configurations based on dbt metadata. Preset-only metrics are preserved",
)
@click.option(
    "--raise-failures",
    is_flag=True,
    default=False,
    help="End the execution with an error if a model fails to sync or a deprecated feature is used",
)
@raise_cli_errors
@click.pass_context
def dbt_core(  # pylint: disable=too-many-arguments, too-many-branches, too-many-locals ,too-many-statements # noqa: C901
    ctx: click.core.Context,
    file: str,
    project: Optional[str],
    target: Optional[str],
    select: Tuple[str, ...],
    exclude: Tuple[str, ...],
    profiles: Optional[str] = None,
    exposures: Optional[str] = None,
    import_db: bool = False,
    disallow_edits: bool = False,
    external_url_prefix: str = "",
    exposures_only: bool = False,
    preserve_columns: bool = False,
    preserve_metadata: bool = False,
    merge_metadata: bool = False,
    raise_failures: bool = False,
) -> None:
    """
    Sync models/metrics from dbt Core to Superset and charts/dashboards to dbt exposures.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)
    deprecation_notice: bool = False

    if (preserve_columns or preserve_metadata) and merge_metadata:
        error_message = (
            "``--preserve-columns`` / ``--preserve-metadata`` and ``--merge-metadata``\n"
            "can't be combined. Please include only one to the command."
        )
        raise CLIError(error_message, 1)

    reload_columns = not (preserve_columns or preserve_metadata or merge_metadata)
    preserve_metadata = preserve_columns if preserve_columns else preserve_metadata

    if profiles is None:
        profiles = os.path.expanduser("~/.dbt/profiles.yml")

    file_path = Path(file)

    if "MANAGER_URL" not in ctx.obj and disallow_edits:
        warn_message = (
            "The managed externally feature was only introduced in Superset v1.5."
            "Make sure you are running a compatible version."
        )
        _logger.debug(warn_message)
    if file_path.name == "manifest.json":
        manifest = file_path
        profile = project = project or "default"
    elif file_path.name == "dbt_project.yml":
        deprecation_notice = True
        warn_message = (
            "Passing the dbt_project.yml file is deprecated and "
            "will be removed in a future version. "
            "Please pass the manifest.json file instead."
        )
        _logger.warning(warn_message)
        with open(file_path, encoding="utf-8") as input_:
            dbt_project = yaml.load(input_, Loader=yaml.SafeLoader)

        manifest = file_path.parent / dbt_project["target-path"] / "manifest.json"
        profile = dbt_project["profile"]
        project = project or dbt_project["name"]
    else:
        raise CLIError(
            "FILE should be either ``manifest.json`` or ``dbt_project.yml``",
            1,
        )

    with open(manifest, encoding="utf-8") as input_:
        configs = yaml.load(input_, Loader=yaml.SafeLoader)

    config = load_profiles(Path(profiles), project, profile, target)
    dialect = config[project]["outputs"][target]["type"]
    try:
        mf_dialect = MFSQLEngine(dialect.upper())
    except ValueError:
        mf_dialect = None

    model_schema = ModelSchema()
    models = []
    for config in configs["nodes"].values():
        if config["resource_type"] == "model":
            # conform to the same schema that dbt Cloud uses for models
            unique_id = config["uniqueId"] = config["unique_id"]
            config["children"] = configs["child_map"][unique_id]
            config["columns"] = list(config["columns"].values())
            models.append(model_schema.load(config))
    models = apply_select(models, select, exclude)
    model_map = {ModelKey(model["schema"], model["name"]): model for model in models}

    failures: List[str] = []

    if exposures_only:
        datasets = [
            dataset
            for dataset in client.get_datasets()
            if ModelKey(dataset["schema"], dataset["table_name"]) in model_map
        ]
    else:
        og_metrics = []
        sl_metrics = []
        metric_schema = MetricSchema()
        for config in configs["metrics"].values():
            if "calculation_method" in config or "sql" in config:
                # conform to the same schema that dbt Cloud uses for metrics
                config["dependsOn"] = config.pop("depends_on")["nodes"]
                config["uniqueId"] = config.pop("unique_id")
                config["dialect"] = dialect
                og_metrics.append(metric_schema.load(config))
            # Only validate semantic layer metrics if MF dialect is specified
            elif mf_dialect is not None and (
                sl_metric := get_sl_metric(config, model_map, mf_dialect)
            ):
                sl_metrics.append(sl_metric)

        superset_metrics = get_superset_metrics_per_model(og_metrics, sl_metrics)

        try:
            database = sync_database(
                client,
                Path(profiles),
                project,
                profile,
                target,
                import_db,
                disallow_edits,
                external_url_prefix,
            )
        except DatabaseNotFoundError:
            click.echo("No database was found, pass ``--import-db`` to create")
            return

        datasets, failures = sync_datasets(
            client,
            models,
            superset_metrics,
            database,
            disallow_edits,
            external_url_prefix,
            reload_columns=reload_columns,
            merge_metadata=merge_metadata,
        )

    if exposures:
        exposures = os.path.expanduser(exposures)
        sync_exposures(client, Path(exposures), datasets, model_map)

    if failures and raise_failures:
        failed_models = list_failed_models(failures)
        raise CLIError(failed_models, 1)

    if deprecation_notice and raise_failures:
        raise CLIError("Review deprecation warnings", 1)


def get_account_id(client: DBTClient) -> int:
    """
    Prompt used for an account ID.
    """
    accounts = client.get_accounts()
    if not accounts:
        raise CLIError("No accounts available", 1)
    if len(accounts) == 1:
        account = accounts[0]
        click.echo(
            f'Using account {account["name"]} [id={account["id"]}] since it\'s the only one',
        )
        return account["id"]
    click.echo("Choose an account:")
    for i, account in enumerate(accounts):
        click.echo(f'({i+1}) {account["name"]} [id={account["id"]}]')

    while True:
        try:
            choice = int(input("> "))
        except Exception:  # pylint: disable=broad-except
            choice = -1
        if 0 < choice <= len(accounts):
            return accounts[choice - 1]["id"]
        click.echo("Invalid choice")


def get_project_id(client: DBTClient, account_id: Optional[int] = None) -> int:
    """
    Prompt user for a project id.
    """
    if account_id is None:
        account_id = get_account_id(client)

    projects = client.get_projects(account_id)
    if not projects:
        raise CLIError("No project available", 1)
    if len(projects) == 1:
        return projects[0]["id"]
    click.echo("Choose a project:")
    for i, project in enumerate(projects):
        click.echo(f'({i+1}) {project["name"]} [id={project["id"]}]')

    while True:
        try:
            choice = int(input("> "))
        except Exception:  # pylint: disable=broad-except
            choice = -1
        if 0 < choice <= len(projects):
            return projects[choice - 1]["id"]
        click.echo("Invalid choice")


def get_job(
    client: DBTClient,
    account_id: Optional[int] = None,
    project_id: Optional[int] = None,
    job_id: Optional[int] = None,
) -> JobSchema:
    """
    Prompt users for a job ID.
    """
    if account_id is None:
        account_id = get_account_id(client)
    if project_id is None:
        project_id = get_project_id(client, account_id)

    jobs = client.get_jobs(account_id, project_id)
    if not jobs:
        raise CLIError("No jobs available", 1)

    if job_id is None:
        if len(jobs) == 1:
            return jobs[0]

        click.echo("Choose a job:")
        for i, job in enumerate(jobs):
            click.echo(f'({i+1}) {job["name"]} [id={job["id"]}]')

        while True:
            try:
                choice = int(input("> "))
            except Exception:  # pylint: disable=broad-except
                choice = -1
            if 0 < choice <= len(jobs):
                return jobs[choice - 1]
            click.echo("Invalid choice")

    for job in jobs:
        if job["id"] == job_id:
            return job

    raise ValueError(f"Job {job_id} not available")


def get_sl_metric(
    metric: Dict[str, Any],
    model_map: Dict[ModelKey, ModelSchema],
    dialect: MFSQLEngine,
) -> Optional[MFMetricWithSQLSchema]:
    """
    Compute a SL metric using the ``mf`` CLI.
    """
    mf_metric_schema = MFMetricWithSQLSchema()

    command = ["mf", "query", "--explain", "--metrics", metric["name"]]
    try:
        _logger.info(
            "Using `mf` command to retrieve SQL syntax for metric %s",
            metric["name"],
        )
        result = subprocess.run(command, capture_output=True, text=True, check=True)
    except FileNotFoundError:
        _logger.warning(
            "`mf` command not found, if you're using Metricflow make sure you have it "
            "installed in order to sync metrics",
        )
        return None
    except subprocess.CalledProcessError:
        _logger.warning(
            "Could not generate SQL for metric %s (this happens for some metrics)",
            metric["name"],
        )
        return None

    output = result.stdout.strip()
    start = output.find("SELECT")
    sql = output[start:]

    models = get_models_from_sql(sql, dialect, model_map)
    if not models or len(models) > 1:
        return None
    model = models[0]

    return mf_metric_schema.load(
        {
            "name": metric["name"],
            "type": metric["type"],
            "description": metric["description"],
            "sql": sql,
            "dialect": dialect.value,
            "model": model["unique_id"],
        },
    )


def fetch_sl_metrics(
    dbt_client: DBTClient,
    environment_id: int,
    model_map: Dict[ModelKey, ModelSchema],
) -> Optional[List[MFMetricWithSQLSchema]]:
    """
    Fetch metrics from the semantic layer and return the ones we can map to models.
    """
    dialect = dbt_client.get_sl_dialect(environment_id)
    mf_metric_schema = MFMetricWithSQLSchema()
    sl_metrics: List[MFMetricWithSQLSchema] = []
    for metric in dbt_client.get_sl_metrics(environment_id):
        sql = dbt_client.get_sl_metric_sql(metric["name"], environment_id)
        if sql is None:
            continue

        models = get_models_from_sql(sql, dialect, model_map)
        if not models or len(models) > 1:
            continue
        model = models[0]

        sl_metrics.append(
            mf_metric_schema.load(
                {
                    "name": metric["name"],
                    "type": metric["type"],
                    "description": metric["description"],
                    "sql": sql,
                    "dialect": dialect.value,
                    "model": model["unique_id"],
                },
            ),
        )

    return sl_metrics


@click.command()
@click.argument("token")
@click.argument("account_id", type=click.INT, required=False, default=None)
@click.argument("project_id", type=click.INT, required=False, default=None)
@click.argument("job_id", type=click.INT, required=False, default=None)
@click.option(
    "--disallow-edits",
    is_flag=True,
    default=False,
    help="Mark resources as managed externally to prevent edits",
)
@click.option(
    "--exposures",
    help="Path to file where exposures will be written",
    type=click.Path(exists=False),
)
@click.option("--external-url-prefix", default="", help="Base URL for resources")
@click.option(
    "--select",
    "-s",
    help="Node selection (same syntax as dbt)",
    multiple=True,
)
@click.option(
    "--exclude",
    "-x",
    help="Models to exclude",
    multiple=True,
)
@click.option(
    "--exposures-only",
    is_flag=True,
    default=False,
    help="Do not sync models to datasets and only fetch exposures instead",
)
@click.option(
    "--preserve-columns",
    is_flag=True,
    default=False,
    help="Preserve column and metric configurations defined in Preset",
)
@click.option(
    "--preserve-metadata",
    is_flag=True,
    default=False,
    help="Preserve column and metric configurations defined in Preset",
)
@click.option(
    "--merge-metadata",
    is_flag=True,
    default=False,
    help="Update Preset configurations based on dbt metadata. Preset-only metrics are preserved",
)
@click.option(
    "--access-url",
    help="Custom API URL for dbt Cloud (eg, https://ab123.us1.dbt.com)",
)
@click.option(
    "--raise-failures",
    is_flag=True,
    default=False,
    help="End the execution with an error if a model fails to sync or a deprecated feature is used",
)
@click.pass_context
@raise_cli_errors
def dbt_cloud(  # pylint: disable=too-many-arguments, too-many-locals
    ctx: click.core.Context,
    token: str,
    select: Tuple[str, ...],
    exclude: Tuple[str, ...],
    exposures: Optional[str] = None,
    account_id: Optional[int] = None,
    project_id: Optional[int] = None,
    job_id: Optional[int] = None,
    disallow_edits: bool = False,
    external_url_prefix: str = "",
    exposures_only: bool = False,
    preserve_columns: bool = False,
    preserve_metadata: bool = False,
    merge_metadata: bool = False,
    access_url: Optional[str] = None,
    raise_failures: bool = False,
) -> None:
    """
    Sync models/metrics from dbt Cloud to Superset.
    """
    superset_auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    superset_client = SupersetClient(url, superset_auth)

    dbt_auth = TokenAuth(token)
    dbt_client = DBTClient(dbt_auth, access_url)

    if (preserve_columns or preserve_metadata) and merge_metadata:
        error_message = (
            "``--preserve-columns`` / ``--preserve-metadata`` and ``--merge-metadata``\n"
            "can't be combined. Please include only one to the command."
        )
        raise CLIError(error_message, 1)

    reload_columns = not (preserve_columns or preserve_metadata or merge_metadata)
    preserve_metadata = preserve_columns if preserve_columns else preserve_metadata

    try:
        job = get_job(dbt_client, account_id, project_id, job_id)
    except ValueError as excinfo:
        error_message = f"Job {job_id} not available"
        raise CLIError(error_message, 2) from excinfo

    # with dbt cloud the database must already exist
    database_name = dbt_client.get_database_name(job["id"])
    databases = superset_client.get_databases(database_name=database_name)
    if not databases:
        click.echo(f'No database named "{database_name}" was found')
        return
    if len(databases) > 1:
        raise Exception("More than one database with the same name found")

    # need to get the database by itself so the response has the SQLAlchemy URI
    database = superset_client.get_database(databases[0]["id"])

    models = dbt_client.get_models(job["id"])
    models = apply_select(models, select, exclude)
    model_map = {ModelKey(model["schema"], model["name"]): model for model in models}

    og_metrics = dbt_client.get_og_metrics(job["id"])
    sl_metrics = fetch_sl_metrics(dbt_client, job["environment_id"], model_map)
    superset_metrics = get_superset_metrics_per_model(og_metrics, sl_metrics)

    failures: List[str] = []

    if exposures_only:
        datasets = [
            dataset
            for dataset in superset_client.get_datasets()
            if ModelKey(dataset["schema"], dataset["table_name"]) in model_map
        ]
    else:
        datasets, failures = sync_datasets(
            superset_client,
            models,
            superset_metrics,
            database,
            disallow_edits,
            external_url_prefix,
            reload_columns=reload_columns,
            merge_metadata=merge_metadata,
        )

    if exposures:
        exposures = os.path.expanduser(exposures)
        sync_exposures(superset_client, Path(exposures), datasets, model_map)

    if failures and raise_failures:
        failed_models = list_failed_models(failures)
        raise CLIError(failed_models, 1)
