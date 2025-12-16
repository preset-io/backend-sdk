"""
CLI command for syncing OSI (Open Semantic Interchange) spec files to Superset.
"""

from __future__ import annotations

import click
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.sync.osi.lib import (
    get_database_id,
    parse_osi_file,
    sync_osi,
)


@click.command()
@click.argument("file", type=click.Path(exists=True))
@click.option(
    "--database-id",
    type=int,
    default=None,
    help="Database ID to use. If not provided, will prompt for selection.",
)
@click.pass_context
def osi(ctx: click.Context, file: str, database_id: int | None) -> None:
    """
    Import an OSI (Open Semantic Interchange) spec file into Superset.

    This command reads an OSI v1 spec file and creates datasets in Superset:

    \b
    - Physical datasets for each dataset defined in the spec
    - A denormalized virtual dataset with JOINs for all relationships
    - Metrics defined in the spec, translated to the database's SQL dialect

    Example:

    \b
        preset-cli superset https://my-workspace.preset.io \\
            sync osi --database-id 5 semantic_model.yaml
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    superset_client = SupersetClient(url, auth)

    # Parse OSI file
    click.echo(f"Parsing OSI file: {file}")
    try:
        osi_model = parse_osi_file(file)
    except Exception as ex:
        raise click.ClickException(f"Failed to parse OSI file: {ex}") from ex

    model_name = osi_model.get("name", "unnamed")
    click.echo(f"Found semantic model: {model_name}")

    # Get or prompt for database ID
    if database_id is None:
        database_id = get_database_id(superset_client)

    # Get database details
    try:
        database = superset_client.get_database(database_id)
    except Exception as ex:
        raise click.ClickException(f"Failed to get database {database_id}: {ex}") from ex

    click.echo(f'Using database: {database["database_name"]} [id={database_id}]')

    # Sync the OSI model
    try:
        denorm_dataset = sync_osi(superset_client, osi_model, database)
        click.echo(
            f"\nSync complete! Denormalized dataset ID: {denorm_dataset['id']}"
        )
    except Exception as ex:
        raise click.ClickException(f"Failed to sync OSI model: {ex}") from ex
