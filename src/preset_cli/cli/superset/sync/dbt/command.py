"""
A command to sync dbt models/metrics to Superset and charts/dashboards back as exposures.
"""

import os.path
import sys
import warnings
from pathlib import Path
from typing import Optional, Tuple

import click
import yaml
from yarl import URL

from preset_cli.api.clients.dbt import DBTClient, MetricSchema, ModelSchema
from preset_cli.api.clients.superset import SupersetClient
from preset_cli.auth.token import TokenAuth
from preset_cli.cli.superset.sync.dbt.databases import sync_database
from preset_cli.cli.superset.sync.dbt.datasets import sync_datasets
from preset_cli.cli.superset.sync.dbt.exposures import ModelKey, sync_exposures
from preset_cli.cli.superset.sync.dbt.lib import apply_select
from preset_cli.exceptions import DatabaseNotFoundError


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
@click.pass_context
def dbt_core(  # pylint: disable=too-many-arguments, too-many-locals
    ctx: click.core.Context,
    file: str,
    project: Optional[str],
    target: Optional[str],
    select: Tuple[str, ...],
    exclude: Tuple[str, ...],
    profiles: Optional[str] = None,
    exposures: Optional[str] = None,
    import_db: bool = False,
    disallow_edits: bool = True,
    external_url_prefix: str = "",
    exposures_only: bool = False,
) -> None:
    """
    Sync models/metrics from dbt Core to Superset and charts/dashboards to dbt exposures.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    if profiles is None:
        profiles = os.path.expanduser("~/.dbt/profiles.yml")

    file_path = Path(file)
    if file_path.name == "manifest.json":
        warnings.warn(
            (
                "Passing the manifest.json file is deprecated. "
                "Please pass the dbt_project.yml file instead."
            ),
            category=DeprecationWarning,
            stacklevel=2,
        )
        manifest = file_path
        profile = project = project or "default"
    elif file_path.name == "dbt_project.yml":
        with open(file_path, encoding="utf-8") as input_:
            dbt_project = yaml.load(input_, Loader=yaml.SafeLoader)

        manifest = file_path.parent / dbt_project["target-path"] / "manifest.json"
        profile = dbt_project["profile"]
        project = project or dbt_project["name"]
    else:
        click.echo(
            click.style(
                "FILE should be either manifest.json or dbt_project.yml",
                fg="bright_red",
            ),
        )
        sys.exit(1)

    with open(manifest, encoding="utf-8") as input_:
        configs = yaml.load(input_, Loader=yaml.SafeLoader)

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
    model_map = {
        ModelKey(model["schema"], model["name"]): f'ref({model["name"]})'
        for model in models
    }

    if exposures_only:
        datasets = [
            dataset
            for dataset in client.get_datasets()
            if ModelKey(dataset["schema"], dataset["table_name"]) in model_map
        ]
    else:
        metrics = []
        metric_schema = MetricSchema()
        for config in configs["metrics"].values():
            # conform to the same schema that dbt Cloud uses for metrics
            config["dependsOn"] = config.pop("depends_on")["nodes"]
            config["uniqueId"] = config.pop("unique_id")
            metrics.append(metric_schema.load(config))

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
            click.echo("No database was found, pass --import-db to create")
            return

        datasets = sync_datasets(
            client,
            models,
            metrics,
            database,
            disallow_edits,
            external_url_prefix,
        )

    if exposures:
        exposures = os.path.expanduser(exposures)
        sync_exposures(client, Path(exposures), datasets, model_map)


def get_account_id(client: DBTClient) -> int:
    """
    Prompt used for an account ID.
    """
    accounts = client.get_accounts()
    if not accounts:
        click.echo(click.style("No accounts available", fg="bright_red"))
        sys.exit(1)
    if len(accounts) == 1:
        return accounts[0]["id"]
    click.echo("Choose an account:")
    for i, account in enumerate(accounts):
        click.echo(f'({i+1}) {account["name"]}')

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
        click.echo(click.style("No project available", fg="bright_red"))
        sys.exit(1)
    if len(projects) == 1:
        return projects[0]["id"]
    click.echo("Choose a project:")
    for i, project in enumerate(projects):
        click.echo(f'({i+1}) {project["name"]}')

    while True:
        try:
            choice = int(input("> "))
        except Exception:  # pylint: disable=broad-except
            choice = -1
        if 0 < choice <= len(projects):
            return projects[choice - 1]["id"]
        click.echo("Invalid choice")


def get_job_id(
    client: DBTClient,
    account_id: Optional[int] = None,
    project_id: Optional[int] = None,
) -> int:
    """
    Prompt users for a job ID.
    """
    if account_id is None:
        account_id = get_account_id(client)
    if project_id is None:
        project_id = get_project_id(client, account_id)

    jobs = client.get_jobs(account_id, project_id)
    if not jobs:
        click.echo(click.style("No jobs available", fg="bright_red"))
        sys.exit(1)
    if len(jobs) == 1:
        return jobs[0]["id"]

    click.echo("Choose a job:")
    for i, job in enumerate(jobs):
        click.echo(f'({i+1}) {job["name"]}')

    while True:
        try:
            choice = int(input("> "))
        except Exception:  # pylint: disable=broad-except
            choice = -1
        if 0 < choice <= len(jobs):
            return jobs[choice - 1]["id"]
        click.echo("Invalid choice")


@click.command()
@click.argument("token")
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
@click.pass_context
def dbt_cloud(  # pylint: disable=too-many-arguments, too-many-locals
    ctx: click.core.Context,
    token: str,
    select: Tuple[str, ...],
    exclude: Tuple[str, ...],
    exposures: Optional[str] = None,
    job_id: Optional[int] = None,
    disallow_edits: bool = True,
    external_url_prefix: str = "",
    exposures_only: bool = False,
) -> None:
    """
    Sync models/metrics from dbt Cloud to Superset.
    """
    superset_auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    superset_client = SupersetClient(url, superset_auth)

    dbt_auth = TokenAuth(token)
    dbt_client = DBTClient(dbt_auth)

    if job_id is None:
        job_id = get_job_id(dbt_client)

    # with dbt cloud the database must already exist
    database_name = dbt_client.get_database_name(job_id)
    databases = superset_client.get_databases(database_name=database_name)
    if not databases:
        click.echo(f'No database named "{database_name}" was found')
        return
    if len(databases) > 1:
        raise Exception("More than one database with the same name found")

    # need to get the database by itself so the response has the SQLAlchemy URI
    database = superset_client.get_database(databases[0]["id"])

    models = dbt_client.get_models(job_id)
    models = apply_select(models, select, exclude)
    model_map = {
        ModelKey(model["schema"], model["name"]): f'ref({model["name"]})'
        for model in models
    }
    metrics = dbt_client.get_metrics(job_id)

    if exposures_only:
        datasets = [
            dataset
            for dataset in superset_client.get_datasets()
            if ModelKey(dataset["schema"], dataset["table_name"]) in model_map
        ]
    else:
        datasets = sync_datasets(
            superset_client,
            models,
            metrics,
            database,
            disallow_edits,
            external_url_prefix,
        )

    if exposures:
        exposures = os.path.expanduser(exposures)
        sync_exposures(superset_client, Path(exposures), datasets, model_map)
