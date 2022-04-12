"""
Test for ``preset_cli.cli.superset.sync.dbt.lib``.
"""

import pytest

from preset_cli.cli.superset.sync.dbt.lib import build_sqlalchemy_uri


def test_build_sqlalchemy_uri_postgres() -> None:
    """
    Test ``build_sqlalchemy_uri`` for PostgreSQL.
    """
    config = {
        "type": "postgres",
        "user": "username",
        "pass": "password123",
        "host": "localhost",
        "port": 5432,
        "dbname": "db",
    }
    assert (
        str(build_sqlalchemy_uri(config))
        == "postgresql+psycopg2://username:password123@localhost:5432/db"
    )


def test_build_sqlalchemy_uri_bigquery() -> None:
    """
    Test ``build_sqlalchemy_uri`` for BigQuery.
    """
    config = {
        "type": "bigquery",
        "project": "my_project",
        "keyfile": "/path/to/credentials.json",
    }
    assert (
        str(build_sqlalchemy_uri(config))
        == "bigquery://my_project?credentials_path=%2Fpath%2Fto%2Fcredentials.json"
    )


def test_build_sqlalchemy_uri_unsupported() -> None:
    """
    Test ``build_sqlalchemy_uri`` for databases currently unsupported.
    """
    config = {"type": "mysql"}
    with pytest.raises(Exception) as excinfo:
        build_sqlalchemy_uri(config)
    assert str(excinfo.value) == (
        "Unable to build a SQLAlchemy URI for a target of type mysql. Please file "
        "an issue at https://github.com/preset-io/backend-sdk/issues/new?"
        "labels=enhancement&title=Backend+for+mysql."
    )
