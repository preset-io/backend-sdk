"""
Commands for syncing metastores to and from Superset.
"""

import click

from preset_cli.cli.superset.sync.dbt.command import dbt
from preset_cli.cli.superset.sync.native.command import native


@click.group()
def sync() -> None:
    """
    Sync metadata between Superset and an external repository.
    """


sync.add_command(native)
sync.add_command(dbt)
