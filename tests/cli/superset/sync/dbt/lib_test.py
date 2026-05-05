"""
Test for ``preset_cli.cli.superset.sync.dbt.lib``.
"""

# pylint: disable=invalid-name, too-many-lines
import copy
import json
import math
from pathlib import Path
from typing import Any, Dict, List

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from sqlalchemy.engine.url import URL

from preset_cli.cli.superset.sync.dbt.lib import (
    apply_select,
    as_number,
    build_sqlalchemy_params,
    create_engine_with_check,
    env_var,
    filter_models,
    get_og_metric_from_config,
    list_failed_models,
    load_profiles,
)
from preset_cli.cli.superset.sync.dbt.schemas import ModelSchema, parse_meta_properties
from preset_cli.exceptions import CLIError

base_metric_config: Dict[str, Any] = {
    "name": "revenue_metric",
    "expression": "price_each",
    "description": "revenue.",
    "calculation_method": "sum",
    "unique_id": "metric.postgres.revenue_verbose_name_from_dbt",
    "label": "Sales Revenue Metric and this is the dbt label",
    "depends_on": {
        "nodes": ["model.postgres.vehicle_sales"],
    },
    "metrics": [],
    "created_at": 1701101973.269536,
    "resource_type": "metric",
    "fqn": ["postgres", "revenue_verbose_name_from_dbt"],
    "model": "ref('vehicle_sales')",
    "path": "schema.yml",
    "package_name": "postgres",
    "original_file_path": "models/schema.yml",
    "refs": [{"name": "vehicle_sales", "package": None, "version": None}],
    "time_grains": [],
    "model_unique_id": None,
    "meta": {
        "superset": {
            "d3format": ",.2f",
        },
    },
}


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


def test_create_engine_with_check(mocker: MockerFixture) -> None:
    """
    Test the ``create_engine_with_check`` method.
    """
    mock_engine = mocker.patch("preset_cli.cli.superset.sync.dbt.lib.create_engine")
    test = create_engine_with_check(URL("blah://blah"))
    assert test == mock_engine.return_value


def test_create_engine_with_check_missing_snowflake() -> None:
    """
    Test the ``create_engine_with_check`` method when the Snowflake driver is
    not installed.
    """
    with pytest.raises(CLIError) as excinfo:
        create_engine_with_check(URL("snowflake://blah"))
    assert 'run ``pip install "preset-cli[snowflake]"``' in str(excinfo.value)


def test_create_engine_with_check_missing_unknown_driver() -> None:
    """
    Test the ``create_engine_with_check`` method when a SQLAlchemy driver is
    not installed.
    """
    with pytest.raises(NotImplementedError) as excinfo:
        create_engine_with_check(URL("mssql+odbc://blah"))
    assert "Unable to build a SQLAlchemy Engine for the mssql+odbc connection" in str(
        excinfo.value,
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


def test_filter_models_config_meta() -> None:
    """
    Test ``filter_models`` with config.meta selectors.
    """
    model_schema = ModelSchema()
    one = model_schema.load(
        {
            "name": "one",
            "tags": [],
            "unique_id": "model.one",
            "config": {
                "meta": {
                    "connection": "oauth",
                    "priority": "high",
                    "team": "analytics",
                },
            },
        },
    )
    two = model_schema.load(
        {
            "name": "two",
            "tags": [],
            "unique_id": "model.two",
            "config": {
                "meta": {
                    "connection": "service_account",
                    "priority": "low",
                },
            },
        },
    )
    three = model_schema.load(
        {
            "name": "three",
            "tags": [],
            "unique_id": "model.three",
            "config": {
                "meta": {
                    "connection": "oauth",
                    "team": "data_eng",
                },
            },
        },
    )
    four = model_schema.load(
        {
            "name": "four",
            "tags": [],
            "unique_id": "model.four",
            "config": {
                "materialized": "table",
            },
        },
    )
    five = model_schema.load(
        {
            "name": "five",
            "tags": [],
            "unique_id": "model.five",
        },
    )
    models = [one, two, three, four, five]

    assert {
        model["name"] for model in filter_models(models, "config.meta.connection:oauth")
    } == {"one", "three"}

    assert {
        model["name"]
        for model in filter_models(models, "config.meta.connection:service_account")
    } == {"two"}

    assert {
        model["name"] for model in filter_models(models, "config.meta.priority:high")
    } == {"one"}

    assert {
        model["name"] for model in filter_models(models, "config.meta.priority:low")
    } == {"two"}

    assert {
        model["name"] for model in filter_models(models, "config.meta.team:analytics")
    } == {"one"}

    assert {
        model["name"] for model in filter_models(models, "config.meta.team:data_eng")
    } == {"three"}

    assert {
        model["name"]
        for model in filter_models(models, "config.meta.nonexistent:value")
    } == set()

    assert {
        model["name"] for model in filter_models(models, "config.meta.connection:none")
    } == set()

    assert {
        model["name"] for model in filter_models(models, "config.materialized:table")
    } == {"four"}


def test_filter_models_deep_nesting() -> None:
    """
    Test ``filter_models`` with deeply nested config properties (3+ levels).
    """
    model_schema = ModelSchema()

    # Model with 3-level nesting
    model_one = model_schema.load(
        {
            "name": "model_one",
            "tags": [],
            "unique_id": "model.one",
            "config": {
                "foo": {
                    "bar": {
                        "baz": "value1",
                    },
                },
            },
        },
    )

    # Model with 4-level nesting
    model_two = model_schema.load(
        {
            "name": "model_two",
            "tags": [],
            "unique_id": "model.two",
            "config": {
                "foo": {
                    "bar": {
                        "baz": {
                            "qux": "deep_value",
                        },
                    },
                },
            },
        },
    )

    # Model with 5-level nesting
    model_three = model_schema.load(
        {
            "name": "model_three",
            "tags": [],
            "unique_id": "model.three",
            "config": {
                "level1": {
                    "level2": {
                        "level3": {
                            "level4": {
                                "level5": "very_deep",
                            },
                        },
                    },
                },
            },
        },
    )

    # Model with different 3-level structure
    model_four = model_schema.load(
        {
            "name": "model_four",
            "tags": [],
            "unique_id": "model.four",
            "config": {
                "foo": {
                    "bar": {
                        "baz": "value2",
                    },
                },
            },
        },
    )

    # Model with partial path
    model_five = model_schema.load(
        {
            "name": "model_five",
            "tags": [],
            "unique_id": "model.five",
            "config": {
                "foo": {
                    "bar": "simple_value",
                },
            },
        },
    )

    models = [model_one, model_two, model_three, model_four, model_five]

    # Test 3-level nesting
    assert {
        model["name"] for model in filter_models(models, "config.foo.bar.baz:value1")
    } == {"model_one"}

    assert {
        model["name"] for model in filter_models(models, "config.foo.bar.baz:value2")
    } == {"model_four"}

    # Test 4-level nesting
    assert {
        model["name"]
        for model in filter_models(models, "config.foo.bar.baz.qux:deep_value")
    } == {"model_two"}

    # Test 5-level nesting
    assert {
        model["name"]
        for model in filter_models(
            models,
            "config.level1.level2.level3.level4.level5:very_deep",
        )
    } == {"model_three"}

    # Test non-existent deep path
    assert {
        model["name"]
        for model in filter_models(models, "config.foo.bar.baz.qux.quux:nonexistent")
    } == set()

    # Test partial match (should not match if full path doesn't exist)
    assert {
        model["name"] for model in filter_models(models, "config.foo.bar:simple_value")
    } == {"model_five"}

    # Test looking for a nested value at wrong level
    assert {
        model["name"] for model in filter_models(models, "config.foo.bar:value1")
    } == set()


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


def test_apply_select_with_config_meta() -> None:
    """
    Test ``apply_select`` with config.meta selectors.
    """
    model_schema = ModelSchema()
    a = model_schema.load(
        {
            "name": "a",
            "tags": [],
            "unique_id": "a",
            "depends_on": [],
            "children": ["b"],
            "config": {"meta": {"db": "oauth"}},
        },
    )
    b = model_schema.load(
        {
            "name": "b",
            "tags": [],
            "unique_id": "b",
            "depends_on": ["a"],
            "children": [],
            "config": {"meta": {"db": "service_account"}},
        },
    )
    c = model_schema.load(
        {
            "name": "c",
            "tags": ["production"],
            "unique_id": "c",
            "depends_on": [],
            "children": [],
            "config": {"meta": {"db": "oauth"}},
        },
    )
    d = model_schema.load(
        {
            "name": "d",
            "tags": [],
            "unique_id": "d",
            "depends_on": [],
            "children": [],
            "config": {"materialized": "table", "meta": {"db": "oauth"}},
        },
    )
    models = [a, b, c, d]

    assert {
        model["name"] for model in apply_select(models, ("config.meta.db:oauth",), ())
    } == {"a", "c", "d"}

    assert {
        model["name"]
        for model in apply_select(
            models,
            ("config.meta.db:oauth",),
            ("tag:production",),
        )
    } == {"a", "d"}

    assert {
        model["name"]
        for model in apply_select(
            models,
            ("config.meta.db:oauth,config.materialized:table",),
            (),
        )
    } == {"d"}


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
    Test ``list_failed_models`` with a single failed model
    """
    error_list = list_failed_models(["single_failure"])
    assert error_list == "Below model(s) failed to sync:\n - single_failure"


def test_list_failed_models_multiple_models() -> None:
    """
    Test ``list_failed_models`` with multiple failed models
    """
    error_list = list_failed_models(["single_failure", "another_failure"])
    assert (
        error_list
        == "Below model(s) failed to sync:\n - single_failure\n - another_failure"
    )


def test_get_og_metric_from_config() -> None:
    """
    Test ``get_og_metric_from_config`` method.
    """
    metric_config = copy.deepcopy(base_metric_config)
    assert get_og_metric_from_config(metric_config, "my_dialect") == {
        "depends_on": ["model.postgres.vehicle_sales"],
        "description": "revenue.",
        "meta": {},
        "superset_meta": {"d3format": ",.2f"},
        "name": "revenue_metric",
        "label": "Sales Revenue Metric and this is the dbt label",
        "unique_id": "metric.postgres.revenue_verbose_name_from_dbt",
        "calculation_method": "sum",
        "expression": "price_each",
        "dialect": "my_dialect",
        "metrics": [],
        "created_at": 1701101973.269536,
        "resource_type": "metric",
        "fqn": ["postgres", "revenue_verbose_name_from_dbt"],
        "model": "ref('vehicle_sales')",
        "path": "schema.yml",
        "package_name": "postgres",
        "original_file_path": "models/schema.yml",
        "refs": [{"name": "vehicle_sales", "package": None, "version": None}],
        "time_grains": [],
        "model_unique_id": None,
    }


def test_get_og_metric_from_config_older_dbt_version() -> None:
    """
    Test ``get_og_metric_from_config`` when passing a metric built using an
    older dbt version (< 1.3).
    """
    metric_config = copy.deepcopy(base_metric_config)
    metric_config["type"] = metric_config.pop("calculation_method")
    metric_config["sql"] = metric_config.pop("expression")
    assert get_og_metric_from_config(metric_config, "other_dialect") == {
        "depends_on": ["model.postgres.vehicle_sales"],
        "description": "revenue.",
        "meta": {},
        "superset_meta": {"d3format": ",.2f"},
        "name": "revenue_metric",
        "label": "Sales Revenue Metric and this is the dbt label",
        "unique_id": "metric.postgres.revenue_verbose_name_from_dbt",
        "type": "sum",
        "sql": "price_each",
        "dialect": "other_dialect",
        "metrics": [],
        "created_at": 1701101973.269536,
        "resource_type": "metric",
        "fqn": ["postgres", "revenue_verbose_name_from_dbt"],
        "model": "ref('vehicle_sales')",
        "path": "schema.yml",
        "package_name": "postgres",
        "original_file_path": "models/schema.yml",
        "refs": [{"name": "vehicle_sales", "package": None, "version": None}],
        "time_grains": [],
        "model_unique_id": None,
    }


def test_get_og_metric_from_config_ready_metric() -> None:
    """
    Test ``get_og_metric_from_config`` when passing a metric that already
    contains the model's ``unique_id`` and its ``sql``.
    """
    metric_config = copy.deepcopy(base_metric_config)
    metric_config["meta"]["superset"]["model"] = "model.postgres.vehicle_sales"

    # Make sure that:
    #   - depends_on is empty (so that ``skip_parsing`` gets later set to True)
    #   - ``expression`` gets the value of ``sql``
    #   - ``calculation_method`` is set to ``derived``
    assert get_og_metric_from_config(
        metric_config,
        "preset_sql",
        depends_on=[],
        sql="SUM(price_each) - max(cost)",
    ) == {
        "depends_on": [],
        "description": "revenue.",
        "meta": {},
        "superset_meta": {"d3format": ",.2f", "model": "model.postgres.vehicle_sales"},
        "name": "revenue_metric",
        "label": "Sales Revenue Metric and this is the dbt label",
        "unique_id": "metric.postgres.revenue_verbose_name_from_dbt",
        "calculation_method": "derived",
        "expression": "SUM(price_each) - max(cost)",
        "dialect": "preset_sql",
        "metrics": [],
        "created_at": 1701101973.269536,
        "resource_type": "metric",
        "fqn": ["postgres", "revenue_verbose_name_from_dbt"],
        "model": "ref('vehicle_sales')",
        "path": "schema.yml",
        "package_name": "postgres",
        "original_file_path": "models/schema.yml",
        "refs": [{"name": "vehicle_sales", "package": None, "version": None}],
        "time_grains": [],
        "model_unique_id": None,
    }


def test_parse_meta_properties() -> None:
    """
    Test the ``parse_meta_properties`` helper.
    """
    data = {
        "name": "test metric",
        "label": "Test Metric",
        "description": "This is a test metric",
        "meta": {
            "superset": {
                "d3format": ",.2f",
                "metric_name": "revenue_metric",
            },
            "airflow": "other_id",
        },
    }
    parse_meta_properties(data)
    assert data == {
        "name": "test metric",
        "label": "Test Metric",
        "description": "This is a test metric",
        "meta": {"airflow": "other_id"},
        "superset_meta": {
            "d3format": ",.2f",
            "metric_name": "revenue_metric",
        },
    }

    data = {
        "name": "Sabe but using config",
        "label": "Meta inside config",
        "description": "",
        "meta": {
            "superset": {
                "d3format": ",.2f",
                "metric_name": "revenue_metric",
            },
            "airflow": "other_id",
        },
    }
    parse_meta_properties(data, preserve_dbt_meta=False)
    assert data == {
        "name": "Sabe but using config",
        "label": "Meta inside config",
        "description": "",
        "superset_meta": {
            "d3format": ",.2f",
            "metric_name": "revenue_metric",
        },
    }

    data = {
        "name": "Sabe but using config",
        "label": "Meta inside config",
        "description": "",
        "meta": {},
    }
    parse_meta_properties(data)
    assert data == {
        "name": "Sabe but using config",
        "label": "Meta inside config",
        "description": "",
        "meta": {},
        "superset_meta": {},
    }

    data = {
        "name": "test metric",
        "label": "Test Metric",
        "description": "This is a test metric",
        "config": {
            "meta": {
                "superset": {
                    "d3format": ",.2f",
                    "metric_name": "revenue_metric",
                },
                "airflow": "other_id",
            },
        },
    }
    parse_meta_properties(data)
    assert data == {
        "name": "test metric",
        "label": "Test Metric",
        "description": "This is a test metric",
        "meta": {"airflow": "other_id"},
        "superset_meta": {
            "d3format": ",.2f",
            "metric_name": "revenue_metric",
        },
        "config": {
            "meta": {"airflow": "other_id"},
        },
    }
