"""
Tests for the native import command.
"""

# pylint: disable=redefined-outer-name, invalid-name, too-many-lines, too-many-locals

import json
from pathlib import Path
from typing import Any, Dict, List
from unittest import mock
from zipfile import ZipFile

import pytest
import requests
import yaml
from click.testing import CliRunner
from freezegun import freeze_time
from jinja2 import Template
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from sqlalchemy.engine.url import URL

from preset_cli.cli.superset.main import superset_cli
from preset_cli.cli.superset.sync.native.command import (
    ResourceType,
    add_password_to_config,
    import_resources,
    import_resources_individually,
    load_user_modules,
    raise_helper,
    verify_db_connectivity,
)
from preset_cli.exceptions import ErrorLevel, ErrorPayload, SupersetError


def test_add_password_to_config_new_connection(mocker: MockerFixture) -> None:
    """
    Test ``add_password_to_config`` for new connections.
    """
    getpass_mock = mocker.patch("preset_cli.cli.superset.sync.native.command.getpass")
    mock_verify_conn = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.verify_db_connectivity",
    )

    # New connection, no pwd set -- should prompt
    config = {
        "sqlalchemy_uri": "postgresql://user:XXXXXXXXXX@host:5432/db",
        "uuid": "uuid1",
    }
    path = Path("/path/to/root/databases/psql.yaml")
    add_password_to_config(path, config, {}, True)

    getpass_mock.getpass.assert_called_with(f"Please provide the password for {path}: ")
    mock_verify_conn.assert_called_once_with(config)

    # New connection, pwd set -- should not prompt
    getpass_mock.reset_mock()
    mock_verify_conn.reset_mock()
    config["password"] = "password123"
    add_password_to_config(path, config, {}, True)

    getpass_mock.getpass.assert_not_called()
    mock_verify_conn.assert_called_once_with(config)

    # db_password takes precedence over config["password"]
    getpass_mock.reset_mock()
    mock_verify_conn.reset_mock()
    add_password_to_config(path, config, {"uuid1": "password321"}, True)

    assert config == {
        "sqlalchemy_uri": "postgresql://user:XXXXXXXXXX@host:5432/db",
        "uuid": "uuid1",
        "password": "password321",
    }
    getpass_mock.getpass.assert_not_called()
    mock_verify_conn.assert_called_once_with(config)


def test_add_password_to_config_existing_connection(mocker: MockerFixture) -> None:
    """
    Test ``add_password_to_config`` for existing connections.
    """
    getpass_mock = mocker.patch("preset_cli.cli.superset.sync.native.command.getpass")
    mock_verify_conn = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.verify_db_connectivity",
    )

    # Existing connection with no override
    config = {
        "sqlalchemy_uri": "postgresql://user:XXXXXXXXXX@host:5432/db",
        "uuid": "uuid1",
    }
    path = Path("/path/to/root/databases/psql.yaml")
    add_password_to_config(path, config, {}, False)

    getpass_mock.getpass.assert_not_called()
    mock_verify_conn.assert_not_called()

    # Existing connection with override
    getpass_mock.reset_mock()
    mock_verify_conn.reset_mock()
    config["password"] = "password123"
    add_password_to_config(path, config, {}, False)

    getpass_mock.getpass.assert_not_called()
    mock_verify_conn.assert_called_once_with(config)

    # db_password takes precedence over config["password"]
    getpass_mock.reset_mock()
    mock_verify_conn.reset_mock()
    add_password_to_config(path, config, {"uuid1": "password321"}, False)

    assert config == {
        "sqlalchemy_uri": "postgresql://user:XXXXXXXXXX@host:5432/db",
        "uuid": "uuid1",
        "password": "password321",
    }
    getpass_mock.getpass.assert_not_called()
    mock_verify_conn.assert_called_once_with(config)


def test_import_resources(mocker: MockerFixture) -> None:
    """
    Test ``import_resources``.
    """
    client = mocker.MagicMock()

    contents = {"bundle/databases/gsheets.yaml": "GSheets"}
    with freeze_time("2022-01-01T00:00:00Z"):
        import_resources(contents, client, False, ResourceType.ASSET)

    call = client.import_zip.mock_calls[0]
    assert call.kwargs == {"overwrite": False}

    resource, buf = call.args
    assert resource == "assets"
    with ZipFile(buf) as bundle:
        assert bundle.namelist() == [
            "bundle/databases/gsheets.yaml",
            "bundle/metadata.yaml",
        ]
        assert (
            bundle.read("bundle/metadata.yaml").decode()
            == "timestamp: '2022-01-01T00:00:00+00:00'\ntype: assets\nversion: 1.0.0\n"
        )
        assert bundle.read("bundle/databases/gsheets.yaml").decode() == "GSheets"


@pytest.mark.parametrize("resource_type", list(ResourceType))
def test_import_resources_asset_types(
    mocker: MockerFixture,
    resource_type: ResourceType,
) -> None:
    """
    Test ``import_resources`` when a resource_type value is specified.
    """
    client = mocker.MagicMock()

    contents = {"bundle/databases/gsheets.yaml": "GSheets"}
    with freeze_time("2022-01-01T00:00:00Z"):
        import_resources(contents, client, False, resource_type)

    call = client.import_zip.mock_calls[0]
    assert call.kwargs == {"overwrite": False}

    resource, buf = call.args
    assert resource == resource_type.resource_name
    with ZipFile(buf) as bundle:
        assert bundle.namelist() == [
            "bundle/databases/gsheets.yaml",
            "bundle/metadata.yaml",
        ]
        assert bundle.read("bundle/metadata.yaml").decode() == (
            "timestamp: '2022-01-01T00:00:00+00:00'\n"
            f"type: {resource_type.metadata_type}\nversion: 1.0.0\n"
        )
        assert bundle.read("bundle/databases/gsheets.yaml").decode() == "GSheets"


def test_import_resources_overwrite_needed(mocker: MockerFixture) -> None:
    """
    Test ``import_resources`` when an overwrite error is raised.
    """
    click = mocker.patch("preset_cli.cli.superset.sync.native.command.click")
    client = mocker.MagicMock()
    errors: List[ErrorPayload] = [
        {
            "message": "Error importing database",
            "error_type": "GENERIC_COMMAND_ERROR",
            "level": ErrorLevel.WARNING,
            "extra": {
                "databases/gsheets.yaml": (
                    "Database already exists and `overwrite=true` was not passed"
                ),
                "issue_codes": [
                    {
                        "code": 1010,
                        "message": (
                            "Issue 1010 - Superset encountered an "
                            "error while running a command."
                        ),
                    },
                ],
            },
        },
    ]
    client.import_zip.side_effect = SupersetError(errors)

    contents = {"bundle/databases/gsheets.yaml": "GSheets"}
    import_resources(contents, client, False, ResourceType.ASSET)

    assert click.style.called_with(
        "The following file(s) already exist. Pass ``--overwrite`` to replace them.\n"
        "databases/gsheets.yaml",
        fg="bright_red",
    )


def test_import_resources_error(mocker: MockerFixture) -> None:
    """
    Test ``import_resources`` when an unexpected error is raised.
    """
    client = mocker.MagicMock()
    errors: List[ErrorPayload] = [
        {
            "message": "Error importing database",
            "error_type": "GENERIC_COMMAND_ERROR",
            "level": ErrorLevel.WARNING,
            "extra": {
                "issue_codes": [
                    {
                        "code": 1010,
                        "message": (
                            "Issue 1010 - Superset encountered an "
                            "error while running a command."
                        ),
                    },
                ],
            },
        },
    ]
    client.import_zip.side_effect = SupersetError(errors)

    contents = {"bundle/databases/gsheets.yaml": "GSheets"}
    with pytest.raises(SupersetError) as excinfo:
        import_resources(contents, client, False, ResourceType.ASSET)
    assert excinfo.value.errors == errors


def test_native(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    # pylint: disable=line-too-long
    """
    Test the ``native`` command.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    database_config = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "is_managed_externally": False,
        "uuid": "uuid1",
    }
    dataset_config = {"table_name": "test", "is_managed_externally": False}
    chart_config = {
        "slice_name": "test",
        "viz_type": "big_number_total",
        "params": {
            "datasource": "1__table",
            "viz_type": "big_number_total",
            "slice_id": 1,
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "{{ '{% if' }} from_dttm {{ '%}' }} count(*) {{ '{% else %}' }} count(*) {{ '{% endif %}' }}",
                "column": None,
                "aggregate": None,
                "datasourceWarning": False,
                "hasCustomLabel": True,
                "label": "custom_calculation",
                "optionName": "metric_6aq7h4t8b3t_jbp2rak398o",
            },
            "adhoc_filters": [],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "y_axis_format": "SMART_NUMBER",
            "time_format": "smart_date",
            "extra_form_data": {},
            "dashboards": [],
        },
        "query_context": json.loads(
            """
{"datasource":{"id":1,"type":"table"},"force":false,"queries":[{"filters":[],"extras":{"having":"","where":""},"applied_time_extras":{},"columns":[],
"metrics":[{"expressionType":"SQL","sqlExpression":"{{ '{% if' }} from_dttm {{ '%}' }} count(*) {{ '{% else %}' }} count(*) {{ '{% endif %}' }}",
"column":null,"aggregate":null,"datasourceWarning":false,"hasCustomLabel":true,"label":"custom_calculation","optionName":"metric_6aq7h4t8b3t_jbp2rak398o"}],
"annotation_layers":[],"series_limit":0,"order_desc":true,"url_params":{},"custom_params":{},"custom_form_data":{}}],"form_data":{"datasource":"1__table",
"viz_type":"big_number_total","slice_id":1,"metric":{"expressionType":"SQL","sqlExpression":"{{ '{% if' }} from_dttm {{ '%}' }} count(*) {{ '{% else %}' }} count(*) {{ '{% endif %}' }}",
"column":null,"aggregate":null,"datasourceWarning":false,"hasCustomLabel":true,"label":"custom_calculation","optionName":"metric_6aq7h4t8b3t_jbp2rak398o"},
"adhoc_filters":[],"header_font_size":0.4,"subheader_font_size":0.15,"y_axis_format":"SMART_NUMBER","time_format":"smart_date",
"extra_form_data":{},"dashboards":[],"force":false,"result_format":"json","result_type":"full"},"result_format":"json","result_type":"full"}""",
        ),
    }

    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(database_config),
    )
    fs.create_file(
        root / "datasets/gsheets/test.yaml",
        contents=yaml.dump(dataset_config),
    )
    fs.create_file(
        root / "datasets/gsheets/test.overrides.yaml",
        contents="table_name: {{ 'Hello' }}",
    )
    fs.create_file(
        root / "charts/test_01.yaml",
        contents=yaml.dump(chart_config),
    )
    fs.create_file(
        root / "README.txt",
        contents="Hello, world",
    )
    fs.create_file(
        root / "tmp/file.yaml",
        contents=yaml.dump([1, 2, 3]),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = []
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "sync", "native", str(root)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    contents = {
        "bundle/charts/test_01.yaml": yaml.dump(
            {
                "slice_name": "test",
                "viz_type": "big_number_total",
                "params": {
                    "datasource": "1__table",
                    "viz_type": "big_number_total",
                    "slice_id": 1,
                    "metric": {
                        "expressionType": "SQL",
                        "sqlExpression": "{% if from_dttm %} count(*) {% else %} count(*) {% endif %}",
                        "column": None,
                        "aggregate": None,
                        "datasourceWarning": False,
                        "hasCustomLabel": True,
                        "label": "custom_calculation",
                        "optionName": "metric_6aq7h4t8b3t_jbp2rak398o",
                    },
                    "adhoc_filters": [],
                    "header_font_size": 0.4,
                    "subheader_font_size": 0.15,
                    "y_axis_format": "SMART_NUMBER",
                    "time_format": "smart_date",
                    "extra_form_data": {},
                    "dashboards": [],
                },
                "query_context": json.loads(
                    """
{"datasource":{"id":1,"type":"table"},"force":false,"queries":[{"filters":[],"extras":{"having":"","where":""},"applied_time_extras":{},
"columns":[],"metrics":[{"expressionType":"SQL","sqlExpression":"{% if from_dttm %} count(*) {% else %} count(*) {% endif %}","column":null,"aggregate":null,
"datasourceWarning":false,"hasCustomLabel":true,"label":"custom_calculation","optionName":"metric_6aq7h4t8b3t_jbp2rak398o"}],"annotation_layers":[],
"series_limit":0,"order_desc":true,"url_params":{},"custom_params":{},"custom_form_data":{}}],"form_data":{"datasource":"1__table","viz_type":"big_number_total",
"slice_id":1,"metric":{"expressionType":"SQL","sqlExpression":"{% if from_dttm %} count(*) {% else %} count(*) {% endif %}","column":null,"aggregate":null,
"datasourceWarning":false,"hasCustomLabel":true,"label":"custom_calculation","optionName":"metric_6aq7h4t8b3t_jbp2rak398o"},"adhoc_filters":[],"header_font_size":0.4,
"subheader_font_size":0.15,"y_axis_format":"SMART_NUMBER","time_format":"smart_date","extra_form_data":{},"dashboards":[],"force":false,"result_format":"json","result_type":"full"},
"result_format":"json","result_type":"full"}""",
                ),
                "is_managed_externally": False,
            },
        ),
        "bundle/databases/gsheets.yaml": yaml.dump(database_config),
        "bundle/datasets/gsheets/test.yaml": yaml.dump(
            {
                "table_name": "Hello",
                "is_managed_externally": False,
            },
        ),
    }

    import_resources.assert_called_once_with(
        contents,
        client,
        False,
        ResourceType.ASSET,
    )
    client.get_uuids.assert_not_called()


def test_native_params_as_str(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``native`` command when dataset ``params`` are a string.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    database_config = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "is_managed_externally": False,
        "uuid": "uuid1",
    }
    dataset_config = {
        "table_name": "test",
        "is_managed_externally": False,
        "params": '{"hello": "world"}',
    }
    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(database_config),
    )
    fs.create_file(
        root / "datasets/gsheets/test.yaml",
        contents=yaml.dump(dataset_config),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = []
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "sync", "native", str(root)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    contents = {
        "bundle/databases/gsheets.yaml": yaml.dump(database_config),
        "bundle/datasets/gsheets/test.yaml": yaml.dump(
            {
                "table_name": "test",
                "is_managed_externally": False,
                "params": {"hello": "world"},
            },
        ),
    }
    import_resources.assert_called_once_with(
        contents,
        client,
        False,
        ResourceType.ASSET,
    )
    client.get_uuids.assert_not_called()


def test_native_password_prompt(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``native`` command with databases that have masked passwords.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    database_config = {
        "database_name": "Postgres",
        "sqlalchemy_uri": "postgresql://user:XXXXXXXXXX@host:5432/dbname",
        "is_managed_externally": False,
        "uuid": "uuid1",
    }
    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(database_config),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = []
    mocker.patch("preset_cli.cli.superset.sync.native.command.import_resources")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    add_password_to_config = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.add_password_to_config",
    )

    runner = CliRunner()

    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "sync", "native", str(root)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    add_password_to_config.assert_called_once_with(
        Path("databases/gsheets.yaml"),
        database_config,
        {},
        True,
    )

    add_password_to_config.reset_mock()
    client.get_databases.return_value = [
        {"uuid": "uuid1"},
    ]
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "sync", "native", str(root)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    add_password_to_config.assert_called_once_with(
        Path("databases/gsheets.yaml"),
        database_config,
        {},
        False,
    )
    client.get_uuids.assert_not_called()


def test_native_load_env(
    mocker: MockerFixture,
    fs: FakeFilesystem,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test the ``native`` command with the ``--load-env`` option.
    """
    monkeypatch.setenv("SQLALCHEMY_URI", "postgres://host1")

    root = Path("/path/to/root")
    fs.create_dir(root)
    database_config = {
        "database_name": "Postgres",
        "sqlalchemy_uri": '{{ env["SQLALCHEMY_URI"] }}',
        "is_managed_externally": False,
        "uuid": "uuid1",
    }
    fs.create_file(
        root / "databases/postgres.yaml",
        contents=yaml.dump(database_config),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = []
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "native",
            str(root),
            "-e",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    contents = {
        "bundle/databases/postgres.yaml": yaml.dump(
            {
                "database_name": "Postgres",
                "sqlalchemy_uri": "postgres://host1",
                "is_managed_externally": False,
                "uuid": "uuid1",
            },
        ),
    }
    import_resources.assert_called_once_with(
        contents,
        client,
        False,
        ResourceType.ASSET,
    )
    client.get_uuids.assert_not_called()


def test_native_external_url(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``native`` command with an external URL.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    database_config = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "external_url": "https://repo.example.com/databases/gsheets.yaml",
        "is_managed_externally": True,
        "uuid": "uuid1",
    }
    dataset_config = {
        "table_name": "test",
        "external_url": "https://repo.example.com/datasets/gsheets/test.yaml",
        "is_managed_externally": True,
    }
    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(database_config),
    )
    fs.create_file(
        root / "datasets/gsheets/test.yaml",
        contents=yaml.dump(dataset_config),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = []
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "native",
            str(root),
            "--external-url-prefix",
            "https://repo.example.com/",
            "--disallow-edits",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    database_config["external_url"] = "https://repo.example.com/databases/gsheets.yaml"
    dataset_config["external_url"] = (
        "https://repo.example.com/datasets/gsheets/test.yaml"
    )
    contents = {
        "bundle/databases/gsheets.yaml": yaml.dump(database_config),
        "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
    }
    import_resources.assert_called_once_with(
        contents,
        client,
        False,
        ResourceType.ASSET,
    )
    client.get_uuids.assert_not_called()


def test_native_legacy_instance(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``native`` command for legacy instances that don't expose the
    DB connection ``uuid`` in the API response.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    database_config = {
        "database_name": "Postgres",
        "sqlalchemy_uri": "postgresql://user:XXXXXXXXXX@host:5432/dbname",
        "is_managed_externally": False,
        "uuid": "uuid1",
    }
    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(database_config),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = [
        {
            "connection_name": "Test",
        },
    ]
    client.get_uuids.return_value = {1: "uuid1"}
    mocker.patch("preset_cli.cli.superset.sync.native.command.import_resources")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    add_password_to_config = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.add_password_to_config",
    )

    runner = CliRunner()

    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "sync", "native", str(root)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    add_password_to_config.assert_called_once_with(
        Path("databases/gsheets.yaml"),
        database_config,
        {},
        False,
    )


def test_load_user_modules(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``load_user_modules``.
    """
    importlib = mocker.patch("preset_cli.cli.superset.sync.native.command.importlib")
    spec = mocker.MagicMock()
    importlib.util.spec_from_file_location.side_effect = [spec, None]

    root = Path("/path/to/root")
    fs.create_dir(root)
    fs.create_file(root / "test.py")
    fs.create_file(root / "invalid.py")
    fs.create_file(root / "test.txt")

    load_user_modules(root)

    importlib.util.spec_from_file_location.assert_has_calls(
        [
            mock.call("test", root / "test.py"),
            mock.call("invalid", root / "invalid.py"),
        ],
    )
    assert importlib.util.module_from_spec.call_count == 1


def test_raise_helper() -> None:
    """
    Test the ``raise_helper`` macro.
    """
    template = Template(
        """
{% if i > 0  %}
{{ i }}
{% else %}
{{ raise("Invalid number: %d", i) }}
{% endif %}
    """,
    )

    assert template.render(i=1, **{"raise": raise_helper}) == "\n\n1\n\n    "
    with pytest.raises(Exception) as excinfo:
        template.render(i=-1, **{"raise": raise_helper})
    assert str(excinfo.value) == "Invalid number: -1"


def test_template_in_environment(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test that the underlying template is passed to the Jinja renderer.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    database_config = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "test": "{{ filepath }}",
        "uuid": "uuid1",
    }
    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(database_config),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = []
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "sync", "native", str(root)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    contents = {
        "bundle/databases/gsheets.yaml": yaml.dump(
            {
                "database_name": "GSheets",
                "is_managed_externally": False,
                "sqlalchemy_uri": "gsheets://",
                "test": "/path/to/root/databases/gsheets.yaml",
                "uuid": "uuid1",
            },
        ),
    }
    import_resources.assert_called_once_with(
        contents,
        client,
        False,
        ResourceType.ASSET,
    )
    client.get_uuids.assert_not_called()


def test_verify_db_connectivity(mocker: MockerFixture) -> None:
    """
    Test ``verify_db_connectivity``.
    """
    create_engine = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.create_engine",
    )

    config = {
        "sqlalchemy_uri": "postgresql://username:XXXXXXXXXX@localhost:5432/examples",
        "password": "SECRET",
    }
    verify_db_connectivity(config)

    create_engine.assert_called_with(
        URL(
            "postgresql",
            username="username",
            password="SECRET",
            host="localhost",
            port=5432,
            database="examples",
        ),
    )


def test_verify_db_connectivity_no_password(mocker: MockerFixture) -> None:
    """
    Test ``verify_db_connectivity`` without passwords.
    """
    create_engine = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.create_engine",
    )

    config = {
        "sqlalchemy_uri": "gsheets://",
    }
    verify_db_connectivity(config)

    create_engine.assert_called_with(
        URL("gsheets"),
    )


def test_verify_db_connectivity_error(mocker: MockerFixture) -> None:
    """
    Test ``verify_db_connectivity`` errors.
    """
    _logger = mocker.patch("preset_cli.cli.superset.sync.native.command._logger")
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command.create_engine",
        side_effect=Exception("Unable to connect"),
    )

    config = {
        "sqlalchemy_uri": "postgresql://username:XXXXXXXXXX@localhost:5432/examples",
        "password": "SECRET",
    }
    verify_db_connectivity(config)

    _logger.warning.assert_called_with(
        "Cannot connect to database %s",
        "postgresql://username:***@localhost:5432/examples",
    )


def test_native_split(  # pylint: disable=too-many-locals
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``native`` command with split imports.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    database_config = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "is_managed_externally": False,
        "uuid": "1",
    }
    dataset_config = {
        "table_name": "test",
        "is_managed_externally": False,
        "database_uuid": "1",
        "uuid": "2",
    }
    chart_config = {
        "dataset_uuid": "2",
        "is_managed_externally": False,
        "slice_name": "Some chart",
        "uuid": "3",
    }
    dashboard_config = {
        "dashboard_title": "Some dashboard",
        "is_managed_externally": False,
        "position": {
            "DASHBOARD_VERSION_KEY": "v2",
            "CHART-BVI44PWH": {
                "type": "CHART",
                "meta": {
                    "uuid": "3",
                },
            },
        },
        "metadata": {},
        "uuid": "4",
    }
    dataset_filter_config = {
        "table_name": "filter_test",
        "is_managed_externally": False,
        "database_uuid": "1",
        "uuid": "5",
    }
    dashboard_with_filter_config = {
        "dashboard_title": "Some dashboard",
        "is_managed_externally": False,
        "position": {},
        "metadata": {
            "native_filter_configuration": [
                {
                    "type": "NATIVE_FILTER",
                    "targets": [
                        {
                            "column": "some_column",
                            "datasetUuid": "5",
                        },
                    ],
                },
                {
                    "type": "NATIVE_FILTER",
                    "targets": [
                        {
                            "column": "other_column",
                            "datasetUuid": "5",
                        },
                    ],
                },
                {
                    "type": "NATIVE_FILTER",
                    "targets": [
                        {
                            "column": "blah",
                            "datasetUuid": "2",
                        },
                    ],
                },
            ],
        },
        "uuid": "6",
    }
    dashboard_with_temporal_filter = {
        "dashboard_title": "Some dashboard",
        "is_managed_externally": False,
        "position": {},
        "metadata": {
            "native_filter_configuration": [
                {
                    "type": "NATIVE_FILTER",
                    "targets": [],
                },
            ],
        },
        "uuid": "6",
    }
    dashboard_deleted_dataset = {
        "dashboard_title": "Some dashboard",
        "is_managed_externally": False,
        "position": {},
        "metadata": {
            "native_filter_configuration": [
                {
                    "type": "NATIVE_FILTER",
                    "targets": [
                        {
                            "column": "some_column",
                        },
                    ],
                },
            ],
        },
        "uuid": "6",
    }

    dashboard_with_filter_divider_config = {
        "dashboard_title": "Some dashboard",
        "is_managed_externally": False,
        "position": {},
        "metadata": {
            "native_filter_configuration": [
                {
                    "type": "NATIVE_FILTER",
                    "targets": [
                        {
                            "column": "some_column",
                            "datasetUuid": "5",
                        },
                    ],
                },
                {"type": "DIVIDER", "targets": {}},
            ],
        },
        "uuid": "7",
    }
    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(database_config),
    )
    fs.create_file(
        root / "datasets/gsheets/test.yaml",
        contents=yaml.dump(dataset_config),
    )
    fs.create_file(
        root / "charts/chart.yaml",
        contents=yaml.dump(chart_config),
    )
    fs.create_file(
        root / "dashboards/dashboard.yaml",
        contents=yaml.dump(dashboard_config),
    )
    fs.create_file(
        root / "datasets/gsheets/filter_test.yaml",
        contents=yaml.dump(dataset_filter_config),
    )
    fs.create_file(
        root / "dashboards/dashboard_with_filter_config.yaml",
        contents=yaml.dump(dashboard_with_filter_config),
    )
    fs.create_file(
        root / "dashboards/dashboard_with_temporal_filter.yaml",
        contents=yaml.dump(dashboard_with_temporal_filter),
    )
    fs.create_file(
        root / "dashboards/dashboard_deleted_dataset.yaml",
        contents=yaml.dump(dashboard_deleted_dataset),
    )
    fs.create_file(
        root / "dashboards/dashboard_with_filter_divider_config.yaml",
        contents=yaml.dump(dashboard_with_filter_divider_config),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "sync", "native", str(root), "--split"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    import_resources.assert_has_calls(
        [
            mock.call(
                {
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
            mock.call(
                {
                    "bundle/datasets/gsheets/filter_test.yaml": yaml.dump(
                        dataset_filter_config,
                    ),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
            mock.call(
                {
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
            mock.call(
                {
                    "bundle/charts/chart.yaml": yaml.dump(chart_config),
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
            mock.call(
                {
                    "bundle/dashboards/dashboard_deleted_dataset.yaml": yaml.dump(
                        dashboard_deleted_dataset,
                    ),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
            mock.call(
                {
                    "bundle/dashboards/dashboard_with_temporal_filter.yaml": yaml.dump(
                        dashboard_with_temporal_filter,
                    ),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
            mock.call(
                {
                    "bundle/dashboards/dashboard_with_filter_config.yaml": yaml.dump(
                        dashboard_with_filter_config,
                    ),
                    "bundle/datasets/gsheets/filter_test.yaml": yaml.dump(
                        dataset_filter_config,
                    ),
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
            mock.call(
                {
                    "bundle/dashboards/dashboard.yaml": yaml.dump(dashboard_config),
                    "bundle/charts/chart.yaml": yaml.dump(chart_config),
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
            mock.call(
                {
                    "bundle/dashboards/dashboard_with_filter_divider_config.yaml": yaml.dump(
                        dashboard_with_filter_divider_config,
                    ),
                    "bundle/datasets/gsheets/filter_test.yaml": yaml.dump(
                        dataset_filter_config,
                    ),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
        ],
        any_order=True,
    )


def test_native_split_continue(  # pylint: disable=too-many-locals
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``native`` command with split imports and the continue flag.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    database_config = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "is_managed_externally": False,
        "uuid": "1",
    }
    dataset_config = {
        "table_name": "test",
        "is_managed_externally": False,
        "database_uuid": "1",
        "uuid": "2",
    }
    chart_config = {
        "dataset_uuid": "2",
        "is_managed_externally": False,
        "slice_name": "Some chart",
        "uuid": "3",
    }
    dashboard_config = {
        "dashboard_title": "Some dashboard",
        "is_managed_externally": False,
        "position": {
            "DASHBOARD_VERSION_KEY": "v2",
            "CHART-BVI44PWH": {
                "type": "CHART",
                "meta": {
                    "uuid": "3",
                },
            },
        },
        "metadata": {},
        "uuid": "4",
    }
    dashboard_deleted_dataset = {
        "dashboard_title": "Some dashboard",
        "is_managed_externally": False,
        "position": {},
        "metadata": {
            "native_filter_configuration": [
                {
                    "type": "NATIVE_FILTER",
                    "targets": [
                        {
                            "column": "some_column",
                        },
                    ],
                },
            ],
        },
        "uuid": "5",
    }
    dashboard_deleted_chart = {
        "dashboard_title": "Dashboard with deleted chart",
        "is_managed_externally": False,
        "position": {
            "DASHBOARD_VERSION_KEY": "v2",
            "CHART-BVI44PWH": {
                "type": "CHART",
                "meta": {
                    "uuid": "3",
                },
            },
            "CHART-BLAH": {
                "type": "CHART",
                "meta": {
                    "uuid": None,
                },
            },
        },
        "metadata": {},
        "uuid": "6",
    }

    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(database_config),
    )
    fs.create_file(
        root / "datasets/gsheets/test.yaml",
        contents=yaml.dump(dataset_config),
    )
    fs.create_file(
        root / "charts/chart.yaml",
        contents=yaml.dump(chart_config),
    )
    fs.create_file(
        root / "dashboards/dashboard.yaml",
        contents=yaml.dump(dashboard_config),
    )
    fs.create_file(
        root / "dashboards/dashboard_deleted_dataset.yaml",
        contents=yaml.dump(dashboard_deleted_dataset),
    )
    fs.create_file(
        root / "dashboards/dashboard_deleted_chart.yaml",
        contents=yaml.dump(dashboard_deleted_chart),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))

    assert not Path("progress.log").exists()

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "native",
            str(root),
            "--split",
            "--continue-on-error",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    import_resources.assert_has_calls(
        [
            mock.call(
                {
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
            mock.call(
                {
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
            mock.call(
                {
                    "bundle/charts/chart.yaml": yaml.dump(chart_config),
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
            mock.call(
                {
                    "bundle/dashboards/dashboard_deleted_dataset.yaml": yaml.dump(
                        dashboard_deleted_dataset,
                    ),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
            mock.call(
                {
                    "bundle/dashboards/dashboard.yaml": yaml.dump(dashboard_config),
                    "bundle/charts/chart.yaml": yaml.dump(chart_config),
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.ASSET,
            ),
        ],
        any_order=True,
    )

    with open("progress.log", encoding="utf-8") as log:
        content = yaml.load(log, Loader=yaml.SafeLoader)

    assert content["ownership"] == []
    assert content["assets"] == [
        {
            "path": "bundle/databases/gsheets.yaml",
            "uuid": "1",
            "status": "SUCCESS",
        },
        {
            "path": "bundle/datasets/gsheets/test.yaml",
            "uuid": "2",
            "status": "SUCCESS",
        },
        {
            "path": "bundle/charts/chart.yaml",
            "uuid": "3",
            "status": "SUCCESS",
        },
        {
            "path": "bundle/dashboards/dashboard_deleted_chart.yaml",
            "uuid": "6",
            "status": "FAILED",
        },
        {
            "path": "bundle/dashboards/dashboard_deleted_dataset.yaml",
            "uuid": "5",
            "status": "SUCCESS",
        },
        {
            "path": "bundle/dashboards/dashboard.yaml",
            "uuid": "4",
            "status": "SUCCESS",
        },
    ]


def test_native_continue_without_split(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test the ``native`` command with split imports and the continue flag.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    database_config = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "is_managed_externally": False,
        "uuid": "1",
    }

    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(database_config),
    )
    import_resources_individually = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources_individually",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "native",
            str(root),
            "--continue-on-error",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    import_resources_individually.assert_called()


def test_import_resources_individually_retries(
    mocker: MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test retries in ``import_resources_individually``.
    """
    # prevent test from actually waiting between tries
    monkeypatch.setattr("time.sleep", lambda x: None)

    client = mocker.MagicMock()

    client.import_zip.side_effect = [
        requests.exceptions.ConnectionError("Connection aborted."),
        requests.exceptions.ConnectionError("Connection aborted."),
        None,
    ]
    configs = {
        Path("bundle/databases/gsheets.yaml"): {"name": "my database", "uuid": "uuid1"},
    }
    import_resources_individually(configs, client, True, ResourceType.ASSET)

    client.import_zip.side_effect = [
        requests.exceptions.ConnectionError("Connection aborted."),
        requests.exceptions.ConnectionError("Connection aborted."),
        requests.exceptions.ConnectionError("Connection aborted."),
        requests.exceptions.ConnectionError("Connection aborted."),
        requests.exceptions.ConnectionError("Connection aborted."),
    ]
    with pytest.raises(Exception) as excinfo:
        import_resources_individually(configs, client, True, ResourceType.ASSET)
    assert str(excinfo.value) == "Connection aborted."


def test_import_resources_individually_checkpoint(
    mocker: MockerFixture,
    fs: FakeFilesystem,  # pylint: disable=unused-argument
) -> None:
    """
    Test checkpoint in ``import_resources_individually``.
    """
    client = mocker.MagicMock()
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    configs = {
        Path("bundle/databases/gsheets.yaml"): {"name": "my database", "uuid": "uuid1"},
        Path("bundle/databases/psql.yaml"): {
            "name": "my other database",
            "uuid": "uuid2",
        },
    }
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    import_resources.side_effect = [None, Exception("An error occurred!"), None]

    with pytest.raises(Exception) as excinfo:
        import_resources_individually(configs, client, True, ResourceType.ASSET)
    assert str(excinfo.value) == "An error occurred!"

    import_resources.assert_has_calls(
        [
            mocker.call(
                {
                    "bundle/databases/gsheets.yaml": yaml.dump(
                        {"name": "my database", "uuid": "uuid1"},
                    ),
                },
                client,
                True,
                ResourceType.ASSET,
            ),
            mocker.call(
                {
                    "bundle/databases/psql.yaml": yaml.dump(
                        {"name": "my other database", "uuid": "uuid2"},
                    ),
                },
                client,
                True,
                ResourceType.ASSET,
            ),
        ],
    )

    with open("progress.log", encoding="utf-8") as log:
        content = yaml.load(log, Loader=yaml.SafeLoader)

    assert content == {
        "assets": [
            {
                "path": "bundle/databases/gsheets.yaml",
                "uuid": "uuid1",
                "status": "SUCCESS",
            },
        ],
        "ownership": [],
    }

    # retry
    import_resources.reset_mock()
    import_resources_individually(configs, client, True, ResourceType.ASSET)
    import_resources.assert_called_once_with(
        {
            "bundle/databases/psql.yaml": yaml.dump(
                {"name": "my other database", "uuid": "uuid2"},
            ),
        },
        client,
        True,
        ResourceType.ASSET,
    )

    assert not Path("progress.log").exists()


def test_import_resources_individually_continue(
    mocker: MockerFixture,
    fs: FakeFilesystem,  # pylint: disable=unused-argument
) -> None:
    """
    Test the ``import_resources_individually`` flow with ``continue_on_error``.
    """
    client = mocker.MagicMock()
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    configs = {
        Path("bundle/databases/gsheets.yaml"): {"name": "my database", "uuid": "uuid1"},
        Path("bundle/databases/gsheets_two.yaml"): {"name": "other", "uuid": "uuid2"},
        Path("bundle/databases/psql.yaml"): {
            "name": "my other database",
            "uuid": "uuid3",
        },
    }
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    import_resources.side_effect = [
        None,
        Exception("An error occurred!"),
        None,
        None,
        None,
    ]

    assert not Path("progress.log").exists()
    import_resources_individually(
        configs,
        client,
        True,
        ResourceType.ASSET,
        continue_on_error=True,
    )

    with open("progress.log", encoding="utf-8") as log:
        content = yaml.load(log, Loader=yaml.SafeLoader)

    assert content == {
        "assets": [
            {
                "path": "bundle/databases/gsheets.yaml",
                "uuid": "uuid1",
                "status": "SUCCESS",
            },
            {
                "path": "bundle/databases/gsheets_two.yaml",
                "uuid": "uuid2",
                "status": "FAILED",
            },
            {
                "path": "bundle/databases/psql.yaml",
                "uuid": "uuid3",
                "status": "SUCCESS",
            },
        ],
        "ownership": [],
    }

    # retry should succeed and delete the log file
    import_resources_individually(
        configs,
        client,
        True,
        ResourceType.ASSET,
        continue_on_error=True,
    )
    import_resources.assert_has_calls(
        [
            mocker.call(
                {
                    "bundle/databases/gsheets.yaml": yaml.dump(
                        {"name": "my database", "uuid": "uuid1"},
                    ),
                },
                client,
                True,
                ResourceType.ASSET,
            ),
            mocker.call(
                {
                    "bundle/databases/gsheets_two.yaml": yaml.dump(
                        {"name": "other", "uuid": "uuid2"},
                    ),
                },
                client,
                True,
                ResourceType.ASSET,
            ),
            mocker.call(
                {
                    "bundle/databases/psql.yaml": yaml.dump(
                        {"name": "my other database", "uuid": "uuid3"},
                    ),
                },
                client,
                True,
                ResourceType.ASSET,
            ),
            mocker.call(
                {
                    "bundle/databases/gsheets_two.yaml": yaml.dump(
                        {"name": "other", "uuid": "uuid2"},
                    ),
                },
                client,
                True,
                ResourceType.ASSET,
            ),
        ],
    )
    assert not Path("progress.log").exists()


def test_import_resources_individually_debug(mocker: MockerFixture) -> None:
    """
    Test the ``import_resources_individually`` method with logger set
    to debug mode.
    """
    db_config: Dict[str, Any] = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "is_managed_externally": False,
        "uuid": "uuid1",
    }
    dataset_config: Dict[str, Any] = {
        "table_name": "test",
        "is_managed_externally": False,
        "uuid": "uuid2",
        "database_uuid": "uuid1",
    }
    chart_config: Dict[str, Any] = {
        "slice_name": "test",
        "viz_type": "big_number_total",
        "params": {
            "datasource": "1__table",
            "viz_type": "big_number_total",
            "slice_id": 1,
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "column": None,
                "aggregate": None,
                "datasourceWarning": False,
                "hasCustomLabel": True,
                "label": "custom_calculation",
                "optionName": "metric_6aq7h4t8b3t_jbp2rak398o",
            },
            "adhoc_filters": [],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "y_axis_format": "SMART_NUMBER",
            "time_format": "smart_date",
            "extra_form_data": {},
            "dashboards": [],
        },
        "query_context": None,
        "is_managed_externally": False,
        "uuid": "uuid3",
        "dataset_uuid": "uuid2",
    }

    configs = {
        Path("bundle/databases/gsheets.yaml"): db_config,
        Path("bundle/datasets/gsheets/test.yaml"): dataset_config,
        Path("bundle/charts/test_01.yaml"): chart_config,
    }

    _logger = mocker.patch("preset_cli.cli.superset.sync.native.command._logger")
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    client = mocker.MagicMock()

    import_resources_individually(
        configs,
        client,
        True,
        ResourceType.CHART,
    )

    _logger.debug.assert_has_calls(
        [
            mock.call("Processing %s for import", Path("databases/gsheets.yaml")),
            mock.call("Processing %s for import", Path("datasets/gsheets/test.yaml")),
            mock.call("Processing %s for import", Path("charts/test_01.yaml")),
        ],
    )
    _logger.info.assert_called_once_with(
        "Importing %s",
        Path("charts/test_01.yaml"),
    )


def test_sync_native_jinja_templating_disabled(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``native`` command with ``--disable-jinja-templating``.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)

    database_config = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "is_managed_externally": False,
        "uuid": "uuid1",
    }
    dataset_config = {
        "table_name": "test",
        "is_managed_externally": False,
        "sql": """
SELECT action, count(*) as times
FROM logs
{% if filter_values('action_type')|length %}
    WHERE action is null
    {% for action in filter_values('action_type') %}
        or action = '{{ action }}'
    {% endfor %}
{% endif %}
GROUP BY action""",
    }
    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(database_config),
    )
    fs.create_file(
        root / "datasets/gsheets/test.yaml",
        contents=yaml.dump(dataset_config),
    )
    fs.create_file(
        root / "datasets/gsheets/test.overrides.yaml",
        contents=yaml.dump(dataset_config),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = []
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "native",
            str(root),
            "--disable-jinja-templating",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    contents = {
        "bundle/databases/gsheets.yaml": yaml.dump(database_config),
        "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
    }
    import_resources.assert_called_once_with(
        contents,
        client,
        False,
        ResourceType.ASSET,
    )
    client.get_uuids.assert_not_called()


@pytest.mark.parametrize("resource_type", list(ResourceType))
def test_native_asset_types(
    mocker: MockerFixture,
    fs: FakeFilesystem,
    resource_type: ResourceType,
) -> None:
    """
    Test the ``native`` command while specifying an asset type.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    database_config = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "is_managed_externally": False,
        "uuid": "uuid1",
    }
    dataset_config = {"table_name": "test", "is_managed_externally": False}
    chart_config = {
        "slice_name": "test",
        "viz_type": "big_number_total",
        "params": {
            "datasource": "1__table",
            "viz_type": "big_number_total",
            "slice_id": 1,
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "column": None,
                "aggregate": None,
                "datasourceWarning": False,
                "hasCustomLabel": True,
                "label": "custom_calculation",
                "optionName": "metric_6aq7h4t8b3t_jbp2rak398o",
            },
            "adhoc_filters": [],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "y_axis_format": "SMART_NUMBER",
            "time_format": "smart_date",
            "extra_form_data": {},
            "dashboards": [],
        },
        "query_context": None,
        "is_managed_externally": False,
    }
    dashboard_config = {
        "dashboard_title": "Some dashboard",
        "is_managed_externally": False,
        "position": {
            "DASHBOARD_VERSION_KEY": "v2",
            "CHART-BVI44PWH": {
                "type": "CHART",
                "meta": {
                    "uuid": "3",
                },
            },
        },
        "metadata": {},
        "uuid": "4",
    }

    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(database_config),
    )
    fs.create_file(
        root / "datasets/gsheets/test.yaml",
        contents=yaml.dump(dataset_config),
    )
    fs.create_file(
        root / "charts/test_01.yaml",
        contents=yaml.dump(chart_config),
    )
    fs.create_file(
        root / "dashboards/dashboard.yaml",
        contents=yaml.dump(dashboard_config),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = []
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "native",
            str(root),
            "--asset-type",
            resource_type.resource_name,
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    contents = {
        "bundle/charts/test_01.yaml": yaml.dump(chart_config),
        "bundle/databases/gsheets.yaml": yaml.dump(database_config),
        "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
        "bundle/dashboards/dashboard.yaml": yaml.dump(dashboard_config),
    }

    import_resources.assert_called_once_with(contents, client, False, resource_type)
    client.get_uuids.assert_not_called()


@pytest.mark.parametrize("resource_type", list(ResourceType))
def test_native_split_asset_types(
    mocker: MockerFixture,
    fs: FakeFilesystem,
    resource_type: ResourceType,
) -> None:
    """
    Test the ``native`` command while specifying an asset type and the `split` flag.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    first_db_config = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "is_managed_externally": False,
        "uuid": "uuid1",
    }
    first_dataset_config = {
        "table_name": "test",
        "is_managed_externally": False,
        "uuid": "uuid2",
        "database_uuid": "uuid1",
    }
    first_chart_config = {
        "slice_name": "test",
        "viz_type": "big_number_total",
        "params": {
            "datasource": "1__table",
            "viz_type": "big_number_total",
            "slice_id": 1,
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "column": None,
                "aggregate": None,
                "datasourceWarning": False,
                "hasCustomLabel": True,
                "label": "custom_calculation",
                "optionName": "metric_6aq7h4t8b3t_jbp2rak398o",
            },
            "adhoc_filters": [],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "y_axis_format": "SMART_NUMBER",
            "time_format": "smart_date",
            "extra_form_data": {},
            "dashboards": [],
        },
        "query_context": None,
        "is_managed_externally": False,
        "uuid": "uuid3",
        "dataset_uuid": "uuid2",
    }
    first_dash_config = {
        "dashboard_title": "Some dashboard",
        "is_managed_externally": False,
        "position": {
            "DASHBOARD_VERSION_KEY": "v2",
            "CHART-BVI44PWH": {
                "type": "CHART",
                "meta": {
                    "uuid": "uuid3",
                },
            },
        },
        "metadata": {},
        "uuid": "4",
    }

    second_db_config = {
        "database_name": "Other DB",
        "sqlalchemy_uri": "gsheets://",
        "is_managed_externally": False,
        "uuid": "uuid5",
    }
    second_dataset_config = {
        "table_name": "test_new",
        "is_managed_externally": False,
        "uuid": "uuid6",
        "database_uuid": "uuid5",
    }
    second_chart_config = {
        "slice_name": "test other",
        "viz_type": "big_number_total",
        "params": {
            "datasource": "6__table",
            "viz_type": "big_number_total",
            "slice_id": 7,
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "column": None,
                "aggregate": None,
                "datasourceWarning": False,
                "hasCustomLabel": True,
                "label": "custom_calculation",
                "optionName": "metric_6aq7h4t8b3t_jbp2rak398o",
            },
            "adhoc_filters": [],
            "header_font_size": 0.4,
            "subheader_font_size": 0.15,
            "y_axis_format": "SMART_NUMBER",
            "time_format": "smart_date",
            "extra_form_data": {},
            "dashboards": [],
        },
        "query_context": None,
        "is_managed_externally": False,
        "uuid": "uuid7",
        "dataset_uuid": "uuid6",
    }
    second_dash_config = {
        "dashboard_title": "other dashboard",
        "is_managed_externally": False,
        "position": {
            "DASHBOARD_VERSION_KEY": "v2",
            "CHART-BVI44PWH": {
                "type": "CHART",
                "meta": {
                    "uuid": "uuid7",
                },
            },
        },
        "metadata": {},
        "uuid": "uuid8",
    }

    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(first_db_config),
    )
    fs.create_file(
        root / "databases/Other_DB.yaml",
        contents=yaml.dump(second_db_config),
    )
    fs.create_file(
        root / "datasets/gsheets/test.yaml",
        contents=yaml.dump(first_dataset_config),
    )
    fs.create_file(
        root / "datasets/Other_DB/test_new.yaml",
        contents=yaml.dump(second_dataset_config),
    )
    fs.create_file(
        root / "charts/test_01.yaml",
        contents=yaml.dump(first_chart_config),
    )
    fs.create_file(
        root / "charts/test_other_07.yaml",
        contents=yaml.dump(second_chart_config),
    )
    fs.create_file(
        root / "dashboards/dashboard.yaml",
        contents=yaml.dump(first_dash_config),
    )
    fs.create_file(
        root / "dashboards/other_dashboard.yaml",
        contents=yaml.dump(second_dash_config),
    )

    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = []
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "native",
            str(root),
            "--asset-type",
            resource_type.resource_name,
            "--split",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    expected_contents: List[Dict[str, str]] = []

    if resource_type in {ResourceType.DATABASE, ResourceType.ASSET}:
        expected_contents += [
            {
                "bundle/databases/gsheets.yaml": yaml.dump(first_db_config),
            },
            {
                "bundle/databases/Other_DB.yaml": yaml.dump(second_db_config),
            },
        ]
    if resource_type in {ResourceType.DATASET, ResourceType.ASSET}:
        expected_contents += [
            {
                "bundle/datasets/gsheets/test.yaml": yaml.dump(first_dataset_config),
                "bundle/databases/gsheets.yaml": yaml.dump(first_db_config),
            },
            {
                "bundle/datasets/Other_DB/test_new.yaml": yaml.dump(
                    second_dataset_config,
                ),
                "bundle/databases/Other_DB.yaml": yaml.dump(second_db_config),
            },
        ]
    if resource_type in {ResourceType.CHART, ResourceType.ASSET}:
        expected_contents += [
            {
                "bundle/charts/test_01.yaml": yaml.dump(first_chart_config),
                "bundle/datasets/gsheets/test.yaml": yaml.dump(first_dataset_config),
                "bundle/databases/gsheets.yaml": yaml.dump(first_db_config),
            },
            {
                "bundle/charts/test_other_07.yaml": yaml.dump(second_chart_config),
                "bundle/datasets/Other_DB/test_new.yaml": yaml.dump(
                    second_dataset_config,
                ),
                "bundle/databases/Other_DB.yaml": yaml.dump(second_db_config),
            },
        ]
    if resource_type in {ResourceType.DASHBOARD, ResourceType.ASSET}:
        expected_contents += [
            {
                "bundle/dashboards/dashboard.yaml": yaml.dump(first_dash_config),
                "bundle/charts/test_01.yaml": yaml.dump(first_chart_config),
                "bundle/datasets/gsheets/test.yaml": yaml.dump(first_dataset_config),
                "bundle/databases/gsheets.yaml": yaml.dump(first_db_config),
            },
            {
                "bundle/dashboards/other_dashboard.yaml": yaml.dump(second_dash_config),
                "bundle/charts/test_other_07.yaml": yaml.dump(second_chart_config),
                "bundle/datasets/Other_DB/test_new.yaml": yaml.dump(
                    second_dataset_config,
                ),
                "bundle/databases/Other_DB.yaml": yaml.dump(second_db_config),
            },
        ]

    import_resources.assert_has_calls(
        [
            mock.call(
                content,
                client,
                False,
                resource_type,
            )
            for content in expected_contents
        ],
        any_order=True,
    )
    client.get_uuids.assert_not_called()
    assert not Path("progress.log").exists()


def test_native_invalid_asset_type(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``native`` command while specifying an invalid asset type.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "native",
            str(root),
            "--asset-type",
            "datasource",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 2
    assert "Invalid value for '--asset-type'" in result.output


def test_native_with_db_passwords(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``native`` command while passing db passwords in the command.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    sqlalchemy_uri = {
        "sqlalchemy_uri": "postgresql://user:XXXXXXXXXX@host:5432/db",
    }
    unmasked_uri = {
        "sqlalchemy_uri": "postgresql://user:unmasked@host:5432/db",
    }

    db_configs = {
        "db_config_masked_no_password": {
            "uuid": "uuid1",
            **sqlalchemy_uri,
        },
        "other_db_config_masked_no_password": {
            "uuid": "uuid2",
            **sqlalchemy_uri,
        },
        "db_config_masked_with_password": {
            "uuid": "uuid3",
            "password": "directpwd!",
            **sqlalchemy_uri,
        },
        "other_db_config_masked_with_password": {
            "uuid": "uuid4",
            "password": "directpwd!again",
            **sqlalchemy_uri,
        },
        "db_config_unmasked": {
            "uuid": "uuid5",
            **unmasked_uri,
        },
        "other_db_config_unmasked": {
            "uuid": "uuid6",
            **unmasked_uri,
        },
        "db_config_unmasked_with_password": {
            "uuid": "uuid7",
            "password": "unmaskedpwd!",
            **unmasked_uri,
        },
        "final_db_config_unmasked_with_password": {
            "uuid": "uuid8",
            "password": "unmaskedpwd!again",
            **unmasked_uri,
        },
    }

    for file_name, content in db_configs.items():
        fs.create_file(
            root / f"databases/{file_name}.yaml",
            contents=yaml.dump(content),
        )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = []
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    mocker.patch("preset_cli.cli.superset.sync.native.command.verify_db_connectivity")
    getpass = mocker.patch("preset_cli.cli.superset.sync.native.command.getpass")
    getpass.getpass.return_value = "pwd_from_prompt"

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "sync",
            "native",
            str(root),
            "--db-password",
            "uuid1=pwd_from_command=1",
            "--db-password",
            "uuid3=pwd_from_command=2",
            "--db-password",
            "uuid5=pwd_from_command=3",
            "--db-password",
            "uuid7=pwd_from_command=4",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    contents = {
        "bundle/databases/db_config_masked_no_password.yaml": yaml.dump(
            {
                "uuid": "uuid1",
                **sqlalchemy_uri,
                "password": "pwd_from_command=1",
                "is_managed_externally": False,
            },
        ),
        "bundle/databases/other_db_config_masked_no_password.yaml": yaml.dump(
            {
                "uuid": "uuid2",
                **sqlalchemy_uri,
                "password": "pwd_from_prompt",
                "is_managed_externally": False,
            },
        ),
        "bundle/databases/db_config_masked_with_password.yaml": yaml.dump(
            {
                "uuid": "uuid3",
                **sqlalchemy_uri,
                "password": "pwd_from_command=2",
                "is_managed_externally": False,
            },
        ),
        "bundle/databases/other_db_config_masked_with_password.yaml": yaml.dump(
            {
                "uuid": "uuid4",
                **sqlalchemy_uri,
                "password": "directpwd!again",
                "is_managed_externally": False,
            },
        ),
        "bundle/databases/db_config_unmasked.yaml": yaml.dump(
            {
                "uuid": "uuid5",
                **unmasked_uri,
                "password": "pwd_from_command=3",
                "is_managed_externally": False,
            },
        ),
        "bundle/databases/other_db_config_unmasked.yaml": yaml.dump(
            {
                "uuid": "uuid6",
                **unmasked_uri,
                "is_managed_externally": False,
            },
        ),
        "bundle/databases/db_config_unmasked_with_password.yaml": yaml.dump(
            {
                "uuid": "uuid7",
                **unmasked_uri,
                "password": "pwd_from_command=4",
                "is_managed_externally": False,
            },
        ),
        "bundle/databases/final_db_config_unmasked_with_password.yaml": yaml.dump(
            {
                "uuid": "uuid8",
                **unmasked_uri,
                "password": "unmaskedpwd!again",
                "is_managed_externally": False,
            },
        ),
    }

    import_resources.assert_called_once_with(
        contents,
        client,
        False,
        ResourceType.ASSET,
    )
    getpass.getpass.assert_called_once_with(
        "Please provide the password for databases/other_db_config_masked_no_password.yaml: ",
    )
