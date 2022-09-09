"""
Tests for the Superset dispatcher.
"""

import click
from click.testing import CliRunner

from preset_cli.cli.superset.main import mutate_commands, superset


def test_mutate_commands() -> None:
    """
    Test ``mutate_commands``.
    """

    @click.group()
    def source_group() -> None:
        """
        A simple group of commands.
        """

    @click.command()
    @click.argument("name")
    def source_command(name: str) -> None:
        """
        Say hello.
        """
        click.echo(f"Hello, {name}!")

    source_group.add_command(source_command)

    @click.group()
    def source_subgroup() -> None:
        """
        A simple subgroup.
        """

    @click.command()
    @click.argument("name")
    def source_subcommand(name: str) -> None:
        """
        Say goodbye
        """
        click.echo(f"Goodbye, {name}!")

    source_subgroup.add_command(source_subcommand)
    source_group.add_command(source_subgroup)

    @click.group()
    @click.pass_context
    def target_group(ctx: click.core.Context) -> None:
        """
        The target group to which commands will be added to.
        """
        ctx.ensure_object(dict)
        ctx.obj["WORKSPACES"] = ["instance1", "instance2"]

    mutate_commands(source_group, target_group)

    runner = CliRunner()

    result = runner.invoke(
        target_group,
        ["source-command", "Alice"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert (
        result.output
        == """
instance1
Hello, Alice!

instance2
Hello, Alice!
"""
    )

    result = runner.invoke(
        target_group,
        ["source-subgroup", "source-subcommand", "Alice"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert (
        result.output
        == """
instance1
Goodbye, Alice!

instance2
Goodbye, Alice!
"""
    )


def test_superset() -> None:
    """
    Test the ``superset`` command.
    """
    runner = CliRunner()

    result = runner.invoke(superset, ["--help"], catch_exceptions=False)
    assert result.exit_code == 0
    assert (
        result.output
        == """Usage: superset [OPTIONS] COMMAND [ARGS]...

  Send commands to one or more Superset instances.

Options:
  --help  Show this message and exit.

Commands:
  export
  export-assets
  export-ownership
  export-rls
  export-users
  import-assets
  import-ownership
  import-rls
  sql
  sync
"""
    )

    result = runner.invoke(superset, ["export", "--help"], catch_exceptions=False)
    assert result.exit_code == 0
    assert (
        result.output
        == """Usage: superset export [OPTIONS] DIRECTORY

Options:
  --overwrite  Overwrite existing resources
  --help       Show this message and exit.
"""
    )
