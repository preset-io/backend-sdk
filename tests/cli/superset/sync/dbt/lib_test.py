"""
Test for ``preset_cli.cli.superset.sync.dbt.lib``.
"""
# pylint: disable=invalid-name

import json
import math
from pathlib import Path

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.cli.superset.sync.dbt.lib import (
    as_number,
    build_sqlalchemy_params,
    env_var,
    load_profiles,
)


def test_build_sqlalchemy_params_postgres(mocker: MockerFixture) -> None:
    """
    Test ``build_sqlalchemy_params`` for PostgreSQL.
    """
    _logger = mocker.patch("preset_cli.cli.superset.sync.dbt.lib._logger")
    config = {
        "type": "postgres",
        "user": "username",
        "password": "password123",
        "host": "localhost",
        "port": 5432,
        "dbname": "db",
    }
    assert build_sqlalchemy_params(config) == {
        "sqlalchemy_uri": "postgresql+psycopg2://username:password123@localhost:5432/db",
    }
    _logger.warning.assert_not_called()
    config["search_path"] = "test_schema"
    build_sqlalchemy_params(config)
    _logger.warning.assert_called_with(
        "Specifying a search path is not supported in Apache Superset",
    )


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


def test_build_snowflake_sqlalchemy_params() -> None:
    """
    Test ``build_snowflake_sqlalchemy_params`` for Snowflake.
    """
    config = {
        "type": "snowflake",
        "account": "abc123.eu-west-1.aws",
        "user": "jdoe",
        "password": "secret",
        "role": "admin",
        "database": "default",
        "warehouse": "dunder-mifflin",
    }
    assert build_sqlalchemy_params(config) == {
        "sqlalchemy_uri": (
            "snowflake://jdoe:secret@abc123.eu-west-1.aws/default?"
            "role=admin&warehouse=dunder-mifflin"
        ),
    }


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


def test_as_number() -> None:
    """
    Test ``as_number`` macro.
    """
    assert as_number("1.0") == 1
    assert as_number("1.1") == 1.1
    assert as_number("2") == 2
    assert math.isnan(as_number("nan"))
    with pytest.raises(ValueError) as excinfo:
        as_number("invalid")
    assert str(excinfo.value) == "could not convert string to float: 'invalid'"


def test_load_profiles(monkeypatch: pytest.MonkeyPatch, fs: FakeFilesystem) -> None:
    """
    Test ``load_profiles``.
    """
    monkeypatch.setenv("REDSHIFT_HOST", "127.0.0.1")
    monkeypatch.setenv("REDSHIFT_PORT", "1234")
    monkeypatch.setenv("REDSHIFT_USER", "username")
    monkeypatch.setenv("REDSHIFT_PASSWORD", "password123")
    monkeypatch.setenv("REDSHIFT_DATABASE", "db")
    monkeypatch.setenv("THREADS", "3")

    path = Path("/path/to/profiles.yml")
    fs.create_file(
        path,
        contents="""
jaffle_shop:
  outputs:
    dev:
      host: "{{ env_var('REDSHIFT_HOST') | as_text }}"
      port: "{{ env_var('REDSHIFT_PORT') | as_number }}"
      user: "{{ env_var('REDSHIFT_USER') }}"
      pass: "{{ env_var('REDSHIFT_PASSWORD') }}"
      dbname: "{{ env_var('REDSHIFT_DATABASE') }}"
      schema: public
      threads: "{{ env_var('THREADS') | as_native }}"
      type: postgres
      enabled: "{{ (target.name == 'prod') | as_bool }}"
      a_list: [1, 2, 3]
      a_value: 10
  target: dev
    """,
    )

    assert load_profiles(path, "jaffle_shop", "dev") == {
        "jaffle_shop": {
            "outputs": {
                "dev": {
                    "host": "127.0.0.1",
                    "port": 1234,
                    "user": "username",
                    "pass": "password123",
                    "dbname": "db",
                    "schema": "public",
                    "threads": 3,
                    "type": "postgres",
                    "enabled": False,
                    "a_list": [1, 2, 3],
                    "a_value": 10,
                },
            },
            "target": "dev",
        },
    }
