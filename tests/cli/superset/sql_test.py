"""
Test the ``sql`` command.
"""
# pylint: disable=invalid-name, unused-argument, redefined-outer-name

from io import StringIO
from pathlib import Path

import pandas as pd
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from yarl import URL

from preset_cli.cli.superset.main import superset_cli
from preset_cli.cli.superset.sql import run_query, run_session
from preset_cli.exceptions import ErrorLevel, SupersetError


def test_run_query(mocker: MockerFixture) -> None:
    """
    Test ``run_query``.
    """
    client = mocker.MagicMock()
    client.run_query.return_value = pd.DataFrame([{"answer": 42}])
    click = mocker.patch("preset_cli.cli.superset.sql.click")

    run_query(client=client, database_id=1, schema=None, query="SELECT 42 AS answer")
    client.run_query.assert_called_with(1, "SELECT 42 AS answer", None)
    click.echo.assert_called_with("  answer\n--------\n      42")


def test_run_query_superset_error(mocker: MockerFixture) -> None:
    """
    Test ``run_query`` when a ``SupersetError`` happens.
    """
    client = mocker.MagicMock()
    client.run_query.side_effect = SupersetError(
        [
            {
                "message": "Only SELECT statements are allowed against this database.",
                "error_type": "DML_NOT_ALLOWED_ERROR",
                "level": ErrorLevel.ERROR,
                "extra": {
                    "issue_codes": [
                        {
                            "code": 1022,
                            "message": "Issue 1022 - Database does not allow data manipulation.",
                        },
                    ],
                },
            },
        ],
    )
    click = mocker.patch("preset_cli.cli.superset.sql.click")

    run_query(client=client, database_id=1, schema=None, query="SSELECT 1")
    click.style.assert_called_with(
        "Only SELECT statements are allowed against this database.",
        fg="bright_red",
    )


def test_run_query_exception(mocker: MockerFixture) -> None:
    """
    Test ``run_query`` when a different exception happens.
    """
    client = mocker.MagicMock()
    client.run_query.side_effect = Exception("Unexpected error")
    traceback = mocker.patch("preset_cli.cli.superset.sql.traceback")

    run_query(client=client, database_id=1, schema=None, query="SSELECT 1")
    traceback.print_exc.assert_called_with()


def test_run_session(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``run_session``.
    """
    history = Path("/path/to/.config/preset-cli/")
    os = mocker.patch("preset_cli.cli.superset.sql.os")
    os.path.expanduser.return_value = str(history)

    client = mocker.MagicMock()
    client.run_query.return_value = pd.DataFrame([{"answer": 42}])

    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("preset_cli.cli.superset.sql.PromptSession")
    session = PromptSession()
    session.prompt.side_effect = ["SELECT 42 AS answer;", "", EOFError()]

    run_session(
        client=client,
        database_id=1,
        database_name="GSheets",
        schema=None,
        url=URL("https://superset.example.org/"),
    )
    result = stdout.getvalue()
    assert (
        result
        == """  answer
--------
      42
Goodbye!
"""
    )


def test_run_session_multiple_commands(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``run_session``.
    """
    history = Path("/path/to/.config/preset-cli/")
    os = mocker.patch("preset_cli.cli.superset.sql.os")
    os.path.expanduser.return_value = str(history)

    client = mocker.MagicMock()
    client.run_query.side_effect = [
        pd.DataFrame([{"answer": 42}]),
        pd.DataFrame([{"question": "Life, universe, everything"}]),
    ]

    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("preset_cli.cli.superset.sql.PromptSession")
    session = PromptSession()
    session.prompt.side_effect = [
        "SELECT 42 AS answer;",
        "SELECT 'Life, universe, everything' AS question;",
        "",
        EOFError(),
    ]

    run_session(
        client=client,
        database_id=1,
        database_name="GSheets",
        schema=None,
        url=URL("https://superset.example.org/"),
    )
    result = stdout.getvalue()
    assert (
        result
        == """  answer
--------
      42
question
--------------------------
Life, universe, everything
Goodbye!
"""
    )


def test_run_session_multiline(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``run_session`` with multilines.
    """
    history = Path("/path/to/.config/preset-cli/")
    os = mocker.patch("preset_cli.cli.superset.sql.os")
    os.path.expanduser.return_value = str(history)

    client = mocker.MagicMock()
    client.run_query.return_value = pd.DataFrame([{"the\nanswer": "foo\nbar"}])

    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("preset_cli.cli.superset.sql.PromptSession")
    session = PromptSession()
    session.prompt.side_effect = [
        """SELECT 'foo\nbar' AS "the\nanswer";""",
        "",
        EOFError(),
    ]

    run_session(
        client=client,
        database_id=1,
        database_name="GSheets",
        schema=None,
        url=URL("https://superset.example.org/"),
    )
    result = stdout.getvalue()
    assert (
        result
        == """the
answer
--------
foo
bar
Goodbye!
"""
    )


def test_run_session_ctrl_c(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test that ``CTRL-C`` does not exit the REPL.
    """
    history = Path("/path/to/.config/preset-cli/")
    os = mocker.patch("preset_cli.cli.superset.sql.os")
    os.path.expanduser.return_value = str(history)

    client = mocker.MagicMock()
    client.run_query.return_value = pd.DataFrame([{"answer": 42}])

    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("preset_cli.cli.superset.sql.PromptSession")
    session = PromptSession()
    session.prompt.side_effect = [KeyboardInterrupt(), "SELECT 1;", EOFError()]

    run_session(
        client=client,
        database_id=1,
        database_name="GSheets",
        schema=None,
        url=URL("https://superset.example.org/"),
    )
    result = stdout.getvalue()
    assert (
        result
        == """  answer
--------
      42
Goodbye!
"""
    )


def test_run_session_history_exists(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``run_session``.
    """
    history = Path("/path/to/.config/preset-cli/")
    os = mocker.patch("preset_cli.cli.superset.sql.os")
    os.path.expanduser.return_value = str(history)
    history.mkdir(parents=True)

    client = mocker.MagicMock()
    client.run_query.return_value = pd.DataFrame([{"answer": 42}])

    stdout = mocker.patch("sys.stdout", new_callable=StringIO)
    PromptSession = mocker.patch("preset_cli.cli.superset.sql.PromptSession")
    session = PromptSession()
    session.prompt.side_effect = ["SELECT 42 AS answer;", "", EOFError()]

    run_session(
        client=client,
        database_id=1,
        database_name="GSheets",
        schema=None,
        url=URL("https://superset.example.org/"),
    )
    result = stdout.getvalue()
    assert (
        result
        == """  answer
--------
      42
Goodbye!
"""
    )


def test_sql_run_query(mocker: MockerFixture) -> None:
    """
    Test the ``sql`` command in programmatic mode (run single query).
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.sql.SupersetClient")
    client = SupersetClient()
    client.get_databases.return_value = [{"id": 1, "database_name": "GSheets"}]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    run_query = mocker.patch("preset_cli.cli.superset.sql.run_query")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sql",
            "-e",
            "SELECT 1",
            "--database-id",
            "1",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    run_query.assert_called_with(client, 1, None, "SELECT 1")


def test_sql_run_session(mocker: MockerFixture) -> None:
    """
    Test the ``sql`` command in session mode (REPL).
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.sql.SupersetClient")
    client = SupersetClient()
    client.get_databases.return_value = [{"id": 1, "database_name": "GSheets"}]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    run_session = mocker.patch("preset_cli.cli.superset.sql.run_session")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sql",
            "--database-id",
            "1",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    run_session.assert_called_with(
        client,
        1,
        "GSheets",
        None,
        URL("https://superset.example.org/"),
    )


def test_sql_run_query_no_databases(mocker: MockerFixture) -> None:
    """
    Test the ``sql`` command when no databases are found.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.sql.SupersetClient")
    client = SupersetClient()
    client.get_databases.return_value = []
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    mocker.patch("preset_cli.cli.superset.sql.run_query")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sql",
            "-e",
            "SELECT 1",
            "--database-id",
            "1",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.output == "No databases available\n"


def test_sql_choose_database(mocker: MockerFixture) -> None:
    """
    Test the ``sql`` command choosing a DB interactively.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.sql.SupersetClient")
    client = SupersetClient()
    client.get_databases.return_value = [
        {"id": 1, "database_name": "GSheets"},
        {"id": 2, "database_name": "Trino"},
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    mocker.patch("preset_cli.cli.superset.sql.input", side_effect=["3", "invalid", "1"])
    run_query = mocker.patch("preset_cli.cli.superset.sql.run_query")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sql",
            "-e",
            "SELECT 1",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    run_query.assert_called_with(client, 1, None, "SELECT 1")


def test_sql_single_database(mocker: MockerFixture) -> None:
    """
    Test the ``sql`` command when there's a single database available.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.sql.SupersetClient")
    client = SupersetClient()
    client.get_databases.return_value = [
        {"id": 1, "database_name": "GSheets"},
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    run_query = mocker.patch("preset_cli.cli.superset.sql.run_query")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sql",
            "-e",
            "SELECT 1",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    run_query.assert_called_with(client, 1, None, "SELECT 1")
