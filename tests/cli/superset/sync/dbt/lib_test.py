"""
Test for ``preset_cli.cli.superset.sync.dbt.lib``.
"""
# pylint: disable=invalid-name

import json

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem

from preset_cli.cli.superset.sync.dbt.lib import build_sqlalchemy_params, env_var


def test_build_sqlalchemy_params_postgres() -> None:
    """
    Test ``build_sqlalchemy_params`` for PostgreSQL.
    """
    config = {
        "type": "postgres",
        "user": "username",
        "pass": "password123",
        "host": "localhost",
        "port": 5432,
        "dbname": "db",
    }
    assert build_sqlalchemy_params(config) == {
        "sqlalchemy_uri": "postgresql+psycopg2://username:password123@localhost:5432/db",
    }


def test_build_sqlalchemy_params_bigquery(fs: FakeFilesystem) -> None:
    """
    Test ``build_sqlalchemy_params`` for BigQuery.
    """
    fs.create_file(
        "/path/to/credentials.json",
        contents=json.dumps({"Hello": "World!"}),
    )
    config = {
        "type": "bigquery",
        "project": "my_project",
        "keyfile": "/path/to/credentials.json",
    }
    assert build_sqlalchemy_params(config) == {
        "sqlalchemy_uri": "bigquery://my_project/",
        "encrypted_extra": json.dumps({"credentials_info": {"Hello": "World!"}}),
    }


def test_build_sqlalchemy_params_bigquery_with_priority(fs: FakeFilesystem) -> None:
    """
    Test ``build_sqlalchemy_params`` for BigQuery with priority parameter.

    Parameter should be uppercase.
    """
    fs.create_file(
        "/path/to/credentials.json",
        contents=json.dumps({"Hello": "World!"}),
    )
    config = {
        "type": "bigquery",
        "project": "my_project",
        "keyfile": "/path/to/credentials.json",
        "priority": "interactive",
    }
    assert build_sqlalchemy_params(config) == {
        "sqlalchemy_uri": "bigquery://my_project/?priority=INTERACTIVE",
        "encrypted_extra": json.dumps({"credentials_info": {"Hello": "World!"}}),
    }


def test_build_sqlalchemy_params_bigquery_no_keyfile() -> None:
    """
    Test ``build_sqlalchemy_params`` for BigQuery with priority parameter.

    Parameter should be uppercase.
    """
    config = {
        "type": "bigquery",
        "project": "my_project",
    }
    with pytest.raises(Exception) as excinfo:
        build_sqlalchemy_params(config)
    assert (
        str(excinfo.value)
        == "Only service account auth is supported, you MUST pass `keyfile`."
    )


def test_build_sqlalchemy_params_unsupported() -> None:
    """
    Test ``build_sqlalchemy_params`` for databases currently unsupported.
    """
    config = {"type": "mysql"}
    with pytest.raises(Exception) as excinfo:
        build_sqlalchemy_params(config)
    assert str(excinfo.value) == (
        "Unable to build a SQLAlchemy URI for a target of type mysql. Please file "
        "an issue at https://github.com/preset-io/backend-sdk/issues/new?"
        "labels=enhancement&title=Backend+for+mysql."
    )


def test_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test the ``env_var`` implementation.
    """
    monkeypatch.setenv("MY_USER", "Nanna")

    assert env_var("MY_USER") == "Nanna"
    assert env_var("YOUR_USER", "Jane Doe") == "Jane Doe"
    with pytest.raises(Exception) as excinfo:
        env_var("YOUR_USER")
    assert str(excinfo.value) == "Env var required but not provided: 'YOUR_USER'"
