"""
Run SQL queries on Superset.
"""
import os.path
import traceback
from operator import itemgetter
from pathlib import Path
from typing import List, Optional, Tuple

import click
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.history import FileHistory
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles.pygments import style_from_pygments_cls
from pygments.lexers.sql import SqlLexer
from pygments.styles import get_style_by_name
from sqlparse.keywords import KEYWORDS
from tabulate import tabulate
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.exceptions import SupersetError

sql_completer = WordCompleter(list(KEYWORDS))
style = style_from_pygments_cls(get_style_by_name("stata-dark"))


@click.command()
@click.option(
    "--database-id",
    default=None,
    help="Database ID (leave empty for options)",
    type=click.INT,
)
@click.option(
    "--schema",
    default=None,
    help="Schema",
)
@click.option("-e", "--execute", default=None, help="Run query non-interactively")
@click.pass_context
def sql(  # pylint: disable=too-many-arguments
    ctx: click.core.Context,
    database_id: Optional[int],
    schema: Optional[str] = None,
    execute: Optional[str] = None,
) -> None:
    """
    Run SQL against an Apache Superset database.
    """
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    databases = client.get_databases()
    if not databases:
        click.echo("No databases available")
        return None

    if len(databases) == 1 and database_id is None:
        database_id = databases[0]["id"]

    if database_id is None:
        click.echo("Choose the ID of a database to connect to:")
        for database in sorted(databases, key=itemgetter("id")):
            click.echo(f'({database["id"]}) {database["database_name"]}')
    while database_id is None:
        try:
            choice = int(input("> "))
            if any(database["id"] == choice for database in databases):
                database_id = choice
                break
        except ValueError:
            pass
        click.echo("Invalid choice")

    database_name = [
        database["database_name"]
        for database in databases
        if database["id"] == database_id
    ][0]

    if execute:
        return run_query(client, database_id, schema, execute)

    return run_session(client, database_id, database_name, schema, url)


def run_query(
    client: SupersetClient,
    database_id: int,
    schema: Optional[str],
    query: str,
) -> None:
    """
    Run a query in a given database.
    """
    try:
        results = client.run_query(database_id, query, schema)
        click.echo(tabulate(results, headers=results.columns, showindex=False))
    except SupersetError as ex:
        click.echo(
            click.style(
                "\n".join(error["message"] for error in ex.errors),
                fg="bright_red",
            ),
        )
    except Exception:  # pylint: disable=broad-except
        traceback.print_exc()


def run_session(
    client: SupersetClient,
    database_id: int,
    database_name: str,
    schema: Optional[str],
    url: URL,
) -> None:
    """
    Run SQL queries in an interactive session.
    """
    history = Path(os.path.expanduser("~/.config/preset-cli/"))
    if not history.exists():
        history.mkdir(parents=True)

    session = PromptSession(
        lexer=PygmentsLexer(SqlLexer),
        completer=sql_completer,
        style=style,
        history=FileHistory(history / f"sql-{url.host}-{database_id}.history"),
    )

    lines: List[str] = []
    quote_context = " "
    padding = " " * (len(database_name) - 1)
    while True:
        prompt = f"{database_name}> " if not lines else f"{padding}{quote_context}. "
        try:
            line = session.prompt(prompt)
        except KeyboardInterrupt:
            lines = []
            quote_context = " "
            continue  # Control-C pressed. Try again.
        except EOFError:
            break  # Control-D pressed.

        lines.append(line)
        query = "\n".join(lines)

        is_terminated, quote_context = get_query_termination(query)
        if is_terminated:
            run_query(client, database_id, schema, query)
            lines = []
            quote_context = " "

    click.echo("Goodbye!")


def get_query_termination(query: str) -> Tuple[bool, str]:
    """
    Check if a query is ended or if a new line should be created.

    This function looks for a semicolon at the end, making sure no quotation mark must be
    closed.
    """
    quote_context = " "
    quote_chars = ('"', "'", "`")

    for query_char in query:
        if quote_context == query_char:
            quote_context = " "
        else:
            for quote in quote_chars:
                if quote_context == " " and quote == query_char:
                    quote_context = quote

    is_terminated = quote_context == " " and query.endswith(";")

    return is_terminated, quote_context
