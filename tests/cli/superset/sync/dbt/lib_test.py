"""
Test for ``preset_cli.cli.superset.sync.dbt.lib``.
"""
# pylint: disable=invalid-name

import json
import math
from pathlib import Path
from typing import List

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.api.clients.dbt import ModelSchema
from preset_cli.cli.superset.sync.dbt.lib import (
    apply_select,
    as_number,
    build_sqlalchemy_params,
    env_var,
    filter_models,
    list_failed_models,
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


def test_build_sqlalchemy_params_redshift(mocker: MockerFixture) -> None:
    """
    Test ``build_sqlalchemy_params`` for Redshift.
    """
    _logger = mocker.patch("preset_cli.cli.superset.sync.dbt.lib._logger")
    config = {
        "type": "redshift",
        "user": "username",
        "password": "password123",
        "host": "localhost",
        "port": 5432,
        "dbname": "db",
    }
    assert build_sqlalchemy_params(config) == {
        "sqlalchemy_uri": "redshift+psycopg2://username:password123@localhost:5432/db",
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


def test_build_snowflake_sqlalchemy_params_pk(fs: FakeFilesystem) -> None:
    """
    Test ``build_snowflake_sqlalchemy_params`` for Snowflake with private keys.
    """
    fs.create_file("/path/to/key", contents="-----BEGIN ENCRYPTED PRIVATE KEY")

    config = {
        "type": "snowflake",
        "account": "abc123.eu-west-1.aws",
        "user": "jdoe",
        "password": "secret",
        "role": "admin",
        "database": "default",
        "warehouse": "dunder-mifflin",
        "private_key_path": "/path/to/key",
        "private_key_passphrase": "XXX",
    }
    assert build_sqlalchemy_params(config) == {
        "sqlalchemy_uri": (
            "snowflake://jdoe:secret@abc123.eu-west-1.aws/default?"
            "role=admin&warehouse=dunder-mifflin"
        ),
        "encrypted_extra": json.dumps(
            {
                "auth_method": "keypair",
                "auth_params": {
                    "privatekey_body": "-----BEGIN ENCRYPTED PRIVATE KEY",
                    "privatekey_pass": "XXX",
                },
            },
        ),
    }


def test_build_snowflake_sqlalchemy_params_mfa() -> None:
    """
    Test ``build_snowflake_sqlalchemy_params`` for Snowflake with MFA.
    """
    config = {
        "type": "snowflake",
        "account": "abc123.eu-west-1.aws",
        "user": "jdoe",
        "password": "secret",
        "authenticator": "DUO code",
        "role": "admin",
        "database": "default",
        "warehouse": "dunder-mifflin",
    }
    assert build_sqlalchemy_params(config) == {
        "sqlalchemy_uri": (
            "snowflake://jdoe:secret@abc123.eu-west-1.aws/default?"
            "role=admin&warehouse=dunder-mifflin"
        ),
        "extra": json.dumps(
            {"engine_params": {"connect_args": {"passcode": "DUO code"}}},
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

    assert load_profiles(path, "jaffle_shop", "jaffle_shop", "dev") == {
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


def test_load_profiles_default_target(
    monkeypatch: pytest.MonkeyPatch,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``load_profiles`` when no target is specified.
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

    assert load_profiles(path, "jaffle_shop", "jaffle_shop", None) == {
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


def test_filter_models() -> None:
    """
    Test ``filter_models``.
    """
    one = {
        "name": "one",
        "tags": ["test"],
        "unique_id": "model.one",
        "depends_on": ["source.zero"],
        "children": ["model.two"],
    }
    two = {
        "name": "two",
        "tags": [],
        "unique_id": "model.two",
        "depends_on": ["model.one", "model.three"],
    }
    three = {
        "name": "three",
        "tags": [],
        "unique_id": "model.three",
        "depends_on": ["source.zero"],
        "children": ["model.two"],
        "config": {
            "materialized": "view",
            "persist_docs": {},
            "quoting": {},
            "column_types": {},
        },
    }
    models: List[ModelSchema] = [one, two, three]  # type: ignore

    assert {model["name"] for model in filter_models(models, "one")} == {"one"}
    assert {model["name"] for model in filter_models(models, "one+")} == {
        "one",
        "two",
    }
    assert {model["name"] for model in filter_models(models, "+two")} == {
        "one",
        "two",
        "three",
    }
    assert {model["name"] for model in filter_models(models, "tag:test")} == {"one"}
    assert {model["name"] for model in filter_models(models, "@one")} == {
        "one",
        "two",
        "three",
    }

    # testing config filtering
    assert {
        model["name"] for model in filter_models(models, "config.materialized:view")
    } == {"three"}

    with pytest.raises(NotImplementedError) as excinfo:
        filter_models(models, "invalid")
    assert str(excinfo.value) == (
        "Unable to parse the selection invalid. Please file an issue at "
        "https://github.com/preset-io/backend-sdk/issues/new?"
        "labels=enhancement&title=dbt+select+invalid."
    )


def test_filter_models_seen() -> None:
    """
    Test that ``filter_models`` dedupes models.
    """
    one = {
        "name": "one",
        "tags": [],
        "unique_id": "model.one",
        "depends_on": ["source.zero"],
        "children": ["model.two", "model.three"],
    }
    two = {
        "name": "two",
        "tags": [],
        "unique_id": "model.two",
        "depends_on": ["model.one"],
        "children": ["model.four"],
    }
    three = {
        "name": "three",
        "tags": [],
        "unique_id": "model.three",
        "depends_on": ["model.one"],
        "children": ["model.four"],
    }
    four = {
        "name": "four",
        "tags": [],
        "unique_id": "model.four",
        "depends_on": ["model.two", "model.three"],
        "children": [],
    }
    models: List[ModelSchema] = [one, two, three, four]  # type: ignore

    assert {model["name"] for model in filter_models(models, "+four")} == {
        "one",
        "two",
        "three",
        "four",
    }
    assert {model["name"] for model in filter_models(models, "one+")} == {
        "one",
        "two",
        "three",
        "four",
    }
    assert {model["name"] for model in filter_models(models, "1+four")} == {
        "two",
        "three",
        "four",
    }
    assert {model["name"] for model in filter_models(models, "one+1")} == {
        "one",
        "two",
        "three",
    }


def test_apply_select() -> None:
    """
    Test ``apply_select``.
    """
    one = {
        "name": "one",
        "tags": ["test"],
        "unique_id": "model.one",
        "depends_on": ["source.zero"],
        "children": ["model.two"],
    }
    two = {
        "name": "two",
        "tags": [],
        "unique_id": "model.two",
        "depends_on": ["model.one", "model.three"],
        "children": [],
    }
    three = {
        "name": "three",
        "tags": [],
        "unique_id": "model.three",
        "depends_on": ["source.zero"],
        "children": ["model.two"],
    }
    models: List[ModelSchema] = [one, two, three]  # type: ignore

    assert {model["name"] for model in apply_select(models, ("one", "two"), ())} == {
        "one",
        "two",
    }
    assert {model["name"] for model in apply_select(models, ("+two+",), ())} == {
        "one",
        "two",
        "three",
    }
    assert {
        model["name"] for model in apply_select(models, ("+two+,tag:test",), ())
    } == {
        "one",
    }
    assert {
        model["name"] for model in apply_select(models, ("tag:test,+two+",), ())
    } == {
        "one",
    }

    assert {
        model["name"]
        for model in apply_select(models, ("+two+",), ("three", "tag:test"))
    } == {
        "two",
    }


def test_apply_select_exclude() -> None:
    """
    Custom tests for the ``exclude`` option.
    """
    a = dict(name="a", tags=[], unique_id="a", depends_on=[], children=["b", "c"])
    b = dict(name="b", tags=[], unique_id="b", depends_on=["a"], children=["d"])
    c = dict(name="c", tags=[], unique_id="c", depends_on=["a"], children=["d"])
    d = dict(name="d", tags=[], unique_id="d", depends_on=["b", "c"], children=[])
    models: List[ModelSchema] = [a, b, c, d]  # type: ignore

    assert {model["name"] for model in apply_select(models, (), ("d",))} == {
        "a",
        "b",
        "c",
    }
    assert {model["name"] for model in apply_select(models, (), ("b+", "c+"))} == {"a"}
    assert {model["name"] for model in apply_select(models, ("a",), ("d",))} == {"a"}


# pylint: disable=unused-argument
def test_apply_select_using_path(fs: FakeFilesystem) -> None:
    """
    Test ``apply_select`` using directory/path arguments.
    """
    one = {
        "name": "one",
        "tags": ["test"],
        "unique_id": "model.one",
        "depends_on": ["source.zero"],
        "children": ["model.two"],
    }
    two = {
        "name": "two",
        "tags": [],
        "unique_id": "model.two",
        "depends_on": ["model.one", "model.three"],
        "children": [],
    }
    three = {
        "name": "three",
        "tags": [],
        "unique_id": "model.three",
        "depends_on": ["source.zero"],
        "children": ["model.two"],
    }
    models: List[ModelSchema] = [one, two, three]  # type: ignore

    base_dir = Path("models")
    base_dir.mkdir(exist_ok=True)
    (base_dir / "one.sql").write_text(json.dumps(one))

    test_folder = base_dir / "test_folder"
    test_folder.mkdir(exist_ok=True)
    (test_folder / "two.sql").write_text(json.dumps(two))

    test_second_folder = test_folder / "test_second_folder"
    test_second_folder.mkdir(exist_ok=True)
    (test_second_folder / "three.sql").write_text(json.dumps(three))

    assert {model["name"] for model in apply_select(models, ("models",), ())} == {
        "one",
        "two",
        "three",
    }
    assert {
        model["name"] for model in apply_select(models, ("models/one.sql",), ())
    } == {
        "one",
    }
    assert {model["name"] for model in apply_select(models, ("models/",), ())} == {
        "one",
        "two",
        "three",
    }
    assert {
        model["name"] for model in apply_select(models, ("models/test_folder/*",), ())
    } == {
        "two",
        "three",
    }


def test_list_failed_models_single_model() -> None:
    """
    Test ``list_failed_models()`` with a single failed model
    """
    error_list = list_failed_models(["single_failure"])
    assert error_list == "Below model(s) failed to sync:\n - single_failure"


def test_list_failed_models_multiple_models() -> None:
    """
    Test ``list_failed_models()`` with multiple failed models
    """
    error_list = list_failed_models(["single_failure", "another_failure"])
    assert (
        error_list
        == "Below model(s) failed to sync:\n - single_failure\n - another_failure"
    )
