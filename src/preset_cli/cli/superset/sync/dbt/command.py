"""
A command to sync DBT models/metrics to Superset and dashboards back as exposures.
"""

import os.path
from pathlib import Path
from typing import Optional

import click
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.sync.dbt.databases import sync_database
from preset_cli.cli.superset.sync.dbt.datasets import sync_datasets
from preset_cli.cli.superset.sync.dbt.exposures import sync_exposures
from preset_cli.exceptions import DatabaseNotFoundError


@click.command()
@click.argument("manifest", type=click.Path(exists=True, resolve_path=True))
@click.option("--project", help="Name of the DBT project", default="default")
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
    help="Mark resources as manged externally to prevent edits",
)
@click.option("--external-url-prefix", default="", help="Base URL for resources")
@click.pass_context
def dbt(  # pylint: disable=too-many-arguments
    ctx: click.core.Context,
    manifest: str,
    project: str,
    target: str,
    profiles: Optional[str] = None,
    exposures: Optional[str] = None,
    import_db: bool = False,
    disallow_edits: bool = True,
    external_url_prefix: str = "",
) -> None:
    """
    Sync DBT models/metrics to Superset and dashboards to DBT exposures.
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

    datasets = sync_datasets(
        client,
        Path(manifest),
        database,
        disallow_edits,
        external_url_prefix,
    )
    if exposures:
        exposures = os.path.expanduser(exposures)
        sync_exposures(client, Path(exposures), datasets)
