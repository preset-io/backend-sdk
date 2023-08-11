"""
Tests for the Superset dispatcher.
"""

import click
from click.testing import CliRunner
from pytest_mock import MockerFixture
from yarl import URL

from preset_cli.cli.superset.main import mutate_commands, superset, superset_cli


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
  export-roles
  export-users
  import-assets
  import-ownership
  import-rls
  import-roles
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
  --overwrite               Overwrite existing resources
  --disable-jinja-escaping  Disable Jinja template escaping
  --asset-type TEXT         Asset type
  --database-ids TEXT       Comma separated list of database IDs to export
  --dataset-ids TEXT        Comma separated list of dataset IDs to export
  --chart-ids TEXT          Comma separated list of chart IDs to export
  --dashboard-ids TEXT      Comma separated list of dashboard IDs to export
  --help                    Show this message and exit.
"""
    )


def test_superset_jwt_auth(mocker: MockerFixture) -> None:
    """
    Test passing a JWT to authenticate with Superset.
    """
    # pylint: disable=invalid-name
    SupersetJWTAuth = mocker.patch("preset_cli.cli.superset.main.SupersetJWTAuth")

    runner = CliRunner()
    runner.invoke(
        superset_cli,
        ["--jwt-token=SECRET", "http://localhost:8088/", "export"],
        catch_exceptions=False,
    )

    SupersetJWTAuth.assert_called_with("SECRET", URL("http://localhost:8088/"))
