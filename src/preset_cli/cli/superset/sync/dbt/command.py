"""
A command to sync dbt models/metrics to Superset and charts/dashboards back as exposures.
"""

import os.path
from pathlib import Path
from typing import Optional, Tuple

import click
import yaml
from marshmallow import EXCLUDE
from yarl import URL

from preset_cli.api.clients.dbt import DBTClient, MetricSchema, ModelSchema
from preset_cli.api.clients.superset import SupersetClient
from preset_cli.auth.token import TokenAuth
from preset_cli.cli.superset.sync.dbt.databases import sync_database
from preset_cli.cli.superset.sync.dbt.datasets import sync_datasets
from preset_cli.cli.superset.sync.dbt.exposures import sync_exposures
from preset_cli.cli.superset.sync.dbt.lib import apply_select
from preset_cli.exceptions import DatabaseNotFoundError


@click.command()
@click.argument("manifest", type=click.Path(exists=True, resolve_path=True))
@click.option("--project", help="Name of the dbt project", default="default")
@click.option("--target", help="Target name", default="dev")
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
    help="Import database to Superset",
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
@click.pass_context
def dbt_core(  # pylint: disable=too-many-arguments, too-many-locals
    ctx: click.core.Context,
    manifest: str,
    project: str,
    target: str,
    select: Tuple[str, ...],
    exclude: Tuple[str, ...],
    profiles: Optional[str] = None,
    exposures: Optional[str] = None,
    import_db: bool = False,
    disallow_edits: bool = True,
    external_url_prefix: str = "",
) -> None:
    """
    Sync models/metrics from dbt Core to Superset and charts/dashboards to dbt exposures.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    if profiles is None:
        profiles = os.path.expanduser("~/.dbt/profiles.yml")

    try:
        database = sync_database(
            client,
            Path(profiles),
            project,
            target,
            import_db,
            disallow_edits,
            external_url_prefix,
        )
    except DatabaseNotFoundError:
        click.echo("No database was found, pass --import-db to create")
        return

    with open(manifest, encoding="utf-8") as input_:
        configs = yaml.load(input_, Loader=yaml.SafeLoader)

    model_schema = ModelSchema()
    models = []
    for config in configs["nodes"].values():
        if config["resource_type"] == "model":
            # conform to the same schema that dbt Cloud uses for models
            config["uniqueId"] = config["unique_id"]
            models.append(model_schema.load(config, unknown=EXCLUDE))
    models = apply_select(models, select, exclude)

    metrics = []
    metric_schema = MetricSchema()
    for config in configs["metrics"].values():
        # conform to the same schema that dbt Cloud uses for metrics
        config["dependsOn"] = config["depends_on"]["nodes"]
        config["uniqueID"] = config["unique_id"]
        metrics.append(metric_schema.load(config, unknown=EXCLUDE))

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
        sync_exposures(client, Path(exposures), datasets)


@click.command()
@click.argument("token")
@click.argument("job_id", type=click.INT)
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
    help="Node selection (same syntax as dbt)",
    multiple=True,
)
@click.option(
    "--exclude",
    "-x",
    help="Models to exclude",
    multiple=True,
)
@click.pass_context
def dbt_cloud(  # pylint: disable=too-many-arguments, too-many-locals
    ctx: click.core.Context,
    token: str,
    job_id: int,
    select: Tuple[str, ...],
    exclude: Tuple[str, ...],
    disallow_edits: bool = True,
    external_url_prefix: str = "",
) -> None:
    """
    Sync models/metrics from dbt Cloud to Superset.
    """
    superset_auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    superset_client = SupersetClient(url, superset_auth)

    dbt_auth = TokenAuth(token)
    dbt_client = DBTClient(dbt_auth)

    # with dbt cloud the database must already exist
    database_name = dbt_client.get_database_name(job_id)
    databases = superset_client.get_databases(database_name=database_name)
    if not databases:
        click.echo(f'No database named "{database_name}" was found')
        return
    if len(databases) > 1:
        raise Exception("More than one database with the same name found")

    database = databases[0]
    models = dbt_client.get_models(job_id)
    models = apply_select(models, select, exclude)
    metrics = dbt_client.get_metrics(job_id)

    sync_datasets(
        superset_client,
        models,
        metrics,
        database,
        disallow_edits,
        external_url_prefix,
    )
