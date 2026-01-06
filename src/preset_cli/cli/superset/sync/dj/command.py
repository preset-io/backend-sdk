"""
A command to sync DJ cubes into a Superset instance.
"""

from __future__ import annotations

import logging
from uuid import UUID

import click
from datajunction import DJClient  # pylint: disable=no-name-in-module
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.sync.dj.lib import sync_cube
from preset_cli.lib import split_comma

_logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--database-uuid",
    required=True,
    help="Database UUID",
)
@click.option(
    "--schema",
    required=True,
    help="Schema where virtual dataset will be created",
)
@click.option(
    "--cubes",
    callback=split_comma,
    help="Comma-separated list of cubes to sync",
)
@click.option(
    "dj_url",
    "--dj-url",
    required=True,
    help="DJ URL",
    default="http://localhost:8000",
)
@click.option(
    "dj_username",
    "--dj-username",
    required=True,
    help="DJ username",
    default="dj",
)
@click.option(
    "dj_password",
    "--dj-password",
    required=True,
    help="DJ password",
    default="dj",
)
@click.option("--external-url-prefix", default="", help="Base URL for resources")
@click.pass_context
def dj(  # pylint: disable=invalid-name,too-many-arguments
    ctx: click.core.Context,
    database_uuid: str,
    schema: str,
    cubes: list[str],
    dj_url: str,
    dj_username: str,
    dj_password: str,
    external_url_prefix: str = "",
) -> None:
    """
    Sync DJ cubes to Superset.
    """
    superset_auth = ctx.obj["AUTH"]
    superset_url = URL(ctx.obj["INSTANCE"])
    superset_client = SupersetClient(superset_url, superset_auth)

    dj_client = DJClient(dj_url)
    dj_client.basic_login(dj_username, dj_password)

    base_url = URL(external_url_prefix) if external_url_prefix else None

    for cube in cubes:
        sync_cube(
            UUID(database_uuid),
            schema,
            dj_client,
            superset_client,
            cube,
            base_url,
        )
