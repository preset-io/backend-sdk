"""
Main entry point for the CLI.
"""

import click

from preset_cli.cli.superset.main import superset


@click.group()
def preset_cli():
    """
    A CLI for Preset.
    """


preset_cli.add_command(superset)
