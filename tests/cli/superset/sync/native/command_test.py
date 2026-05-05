"""
Tests for the native import command.
"""

# pylint: disable=redefined-outer-name, invalid-name, too-many-lines, too-many-locals

import json
from pathlib import Path
from typing import Dict, List, Tuple, cast
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
    _build_chart_contents,
    _build_dashboard_contents,
    _build_dataset_contents,
    _filter_payload_to_schema,
    _find_config_by_uuid,
    _prepare_chart_update_payload,
    _prepare_dashboard_update_payload,
    _prune_existing_dependency_configs,
    _resolve_input_root,
    _resolve_uuid_to_id,
    _safe_extract_zip,
    _safe_json_loads,
    _set_integer_list_payload_field,
    _set_json_payload_field,
    _update_chart_datasource_refs,
    _update_chart_no_cascade,
    _update_dashboard_no_cascade,
    add_password_to_config,
    get_charts_uuids,
    get_dataset_filter_uuids,
    import_resources,
    import_resources_individually,
    load_user_modules,
    raise_helper,
    verify_db_connectivity,
)
from preset_cli.cli.superset.sync.native.types import AssetConfig
from preset_cli.exceptions import CLIError, ErrorLevel, ErrorPayload, SupersetError


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

    click.style.assert_called_with(
        "The following file(s) already exist. Pass ``--overwrite`` to replace them.\n"
        "- databases/gsheets.yaml",
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


def test_native_zip_input(mocker: MockerFixture, tmp_path: Path) -> None:
    """
    Test the ``native`` command when input is a ZIP bundle.
    """
    bundle_root = tmp_path / "bundle"
    (bundle_root / "databases").mkdir(parents=True)
    db_config = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "uuid": "uuid1",
    }
    (bundle_root / "databases" / "gsheets.yaml").write_text(
        yaml.dump(db_config),
        encoding="utf-8",
    )

    zip_path = tmp_path / "assets.zip"
    with ZipFile(zip_path, "w") as bundle:
        for file_path in bundle_root.rglob("*"):
            if file_path.is_file():
                bundle.write(file_path, file_path.relative_to(tmp_path))

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = []
    import_resources_mock = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "sync", "native", str(zip_path)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    import_resources_mock.assert_called_once()
    contents = import_resources_mock.call_args.args[0]
    assert "bundle/databases/gsheets.yaml" in contents


def test_native_zip_rejects_unsafe_paths(mocker: MockerFixture, tmp_path: Path) -> None:
    """
    Test the ``native`` command rejects ZIP bundles with path traversal entries.
    """
    zip_path = tmp_path / "unsafe.zip"
    with ZipFile(zip_path, "w") as bundle:
        bundle.writestr("../evil.yaml", "uuid: bad")

    mocker.patch("preset_cli.cli.superset.sync.native.command.SupersetClient")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "sync", "native", str(zip_path)],
    )
    assert result.exit_code != 0
    assert "unsafe zip path detected" in result.output.lower()


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
                ResourceType.DATABASE,
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
                ResourceType.DATASET,
            ),
            mock.call(
                {
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.DATASET,
            ),
            mock.call(
                {
                    "bundle/charts/chart.yaml": yaml.dump(chart_config),
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.CHART,
            ),
            mock.call(
                {
                    "bundle/dashboards/dashboard_deleted_dataset.yaml": yaml.dump(
                        dashboard_deleted_dataset,
                    ),
                },
                client,
                False,
                ResourceType.DASHBOARD,
            ),
            mock.call(
                {
                    "bundle/dashboards/dashboard_with_temporal_filter.yaml": yaml.dump(
                        dashboard_with_temporal_filter,
                    ),
                },
                client,
                False,
                ResourceType.DASHBOARD,
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
                ResourceType.DASHBOARD,
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
                ResourceType.DASHBOARD,
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
                ResourceType.DASHBOARD,
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

    def import_resources_side_effect(contents, *_args, **_kwargs):
        if "bundle/dashboards/dashboard_deleted_chart.yaml" in contents:
            raise Exception("An error occurred!")

    import_resources.side_effect = import_resources_side_effect
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
                ResourceType.DATABASE,
            ),
            mock.call(
                {
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.DATASET,
            ),
            mock.call(
                {
                    "bundle/charts/chart.yaml": yaml.dump(chart_config),
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
                ResourceType.CHART,
            ),
            mock.call(
                {
                    "bundle/dashboards/dashboard_deleted_dataset.yaml": yaml.dump(
                        dashboard_deleted_dataset,
                    ),
                },
                client,
                False,
                ResourceType.DASHBOARD,
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
                ResourceType.DASHBOARD,
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
    configs: Dict[Path, AssetConfig] = {
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


def test_import_resources_individually_resource_endpoints(
    mocker: MockerFixture,
    fs: FakeFilesystem,  # pylint: disable=unused-argument
) -> None:
    """
    Test ``import_resources_individually`` uses per-resource endpoints.
    """
    client = mocker.MagicMock()
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {"uuid": "db1"},
        Path("bundle/datasets/ds.yaml"): {"uuid": "ds1", "database_uuid": "db1"},
        Path("bundle/charts/chart.yaml"): {"uuid": "chart1", "dataset_uuid": "ds1"},
    }

    import_resources_individually(configs, client, True, ResourceType.ASSET)

    types = [call.args[3] for call in import_resources.call_args_list]
    assert types == [
        ResourceType.DATABASE,
        ResourceType.DATASET,
        ResourceType.CHART,
    ]
    assert import_resources.call_count == 3


def test_import_resources_individually_skips_existing_database(
    mocker: MockerFixture,
    fs: FakeFilesystem,  # pylint: disable=unused-argument
) -> None:
    """
    Test existing databases are skipped when importing dashboards.
    """
    client = mocker.MagicMock()
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {"uuid": "db1"},
        Path("bundle/datasets/ds.yaml"): {"uuid": "ds1", "database_uuid": "db1"},
        Path("bundle/charts/chart.yaml"): {"uuid": "chart1", "dataset_uuid": "ds1"},
    }

    import_resources_individually(
        configs,
        client,
        True,
        ResourceType.DASHBOARD,
        existing_databases={"db1"},
    )

    types = {call.args[3] for call in import_resources.call_args_list}
    assert ResourceType.DATABASE not in types
    assert ResourceType.DATASET in types
    assert ResourceType.CHART in types
    dataset_calls = [
        call
        for call in import_resources.call_args_list
        if call.args[3] == ResourceType.DATASET
    ]
    assert dataset_calls
    for call in dataset_calls:
        contents = call.args[0]
        assert "bundle/databases/db.yaml" in contents


def test_import_resources_individually_checkpoint(
    mocker: MockerFixture,
    fs: FakeFilesystem,  # pylint: disable=unused-argument
) -> None:
    """
    Test checkpoint in ``import_resources_individually``.
    """
    client = mocker.MagicMock()
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    configs: Dict[Path, AssetConfig] = {
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
                False,
                ResourceType.DATABASE,
            ),
            mocker.call(
                {
                    "bundle/databases/psql.yaml": yaml.dump(
                        {"name": "my other database", "uuid": "uuid2"},
                    ),
                },
                client,
                False,
                ResourceType.DATABASE,
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
        False,
        ResourceType.DATABASE,
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
    configs: Dict[Path, AssetConfig] = {
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
                False,
                ResourceType.DATABASE,
            ),
            mocker.call(
                {
                    "bundle/databases/gsheets_two.yaml": yaml.dump(
                        {"name": "other", "uuid": "uuid2"},
                    ),
                },
                client,
                False,
                ResourceType.DATABASE,
            ),
            mocker.call(
                {
                    "bundle/databases/psql.yaml": yaml.dump(
                        {"name": "my other database", "uuid": "uuid3"},
                    ),
                },
                client,
                False,
                ResourceType.DATABASE,
            ),
            mocker.call(
                {
                    "bundle/databases/gsheets_two.yaml": yaml.dump(
                        {"name": "other", "uuid": "uuid2"},
                    ),
                },
                client,
                False,
                ResourceType.DATABASE,
            ),
        ],
    )
    assert not Path("progress.log").exists()


def test_import_resources_individually_debug(mocker: MockerFixture) -> None:
    """
    Test the ``import_resources_individually`` method with logger set
    to debug mode.
    """
    db_config: Dict[str, object] = {
        "database_name": "GSheets",
        "sqlalchemy_uri": "gsheets://",
        "is_managed_externally": False,
        "uuid": "uuid1",
    }
    dataset_config: Dict[str, object] = {
        "table_name": "test",
        "is_managed_externally": False,
        "uuid": "uuid2",
        "database_uuid": "uuid1",
    }
    chart_config: Dict[str, object] = {
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
    _logger.info.assert_has_calls(
        [
            mock.call("Importing %s", Path("databases/gsheets.yaml")),
            mock.call("Importing %s", Path("datasets/gsheets/test.yaml")),
            mock.call("Importing %s", Path("charts/test_01.yaml")),
        ],
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

    expected_calls: List[Tuple[Dict[str, str], ResourceType]] = []

    if resource_type in {ResourceType.DATABASE, ResourceType.ASSET}:
        expected_calls += [
            (
                {
                    "bundle/databases/gsheets.yaml": yaml.dump(first_db_config),
                },
                ResourceType.DATABASE,
            ),
            (
                {
                    "bundle/databases/Other_DB.yaml": yaml.dump(second_db_config),
                },
                ResourceType.DATABASE,
            ),
        ]
    if resource_type in {ResourceType.DATASET, ResourceType.ASSET}:
        expected_calls += [
            (
                {
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(
                        first_dataset_config,
                    ),
                    "bundle/databases/gsheets.yaml": yaml.dump(first_db_config),
                },
                ResourceType.DATASET,
            ),
            (
                {
                    "bundle/datasets/Other_DB/test_new.yaml": yaml.dump(
                        second_dataset_config,
                    ),
                    "bundle/databases/Other_DB.yaml": yaml.dump(second_db_config),
                },
                ResourceType.DATASET,
            ),
        ]
    if resource_type in {ResourceType.CHART, ResourceType.ASSET}:
        expected_calls += [
            (
                {
                    "bundle/charts/test_01.yaml": yaml.dump(first_chart_config),
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(
                        first_dataset_config,
                    ),
                    "bundle/databases/gsheets.yaml": yaml.dump(first_db_config),
                },
                ResourceType.CHART,
            ),
            (
                {
                    "bundle/charts/test_other_07.yaml": yaml.dump(second_chart_config),
                    "bundle/datasets/Other_DB/test_new.yaml": yaml.dump(
                        second_dataset_config,
                    ),
                    "bundle/databases/Other_DB.yaml": yaml.dump(second_db_config),
                },
                ResourceType.CHART,
            ),
        ]
    if resource_type in {ResourceType.DASHBOARD, ResourceType.ASSET}:
        expected_calls += [
            (
                {
                    "bundle/dashboards/dashboard.yaml": yaml.dump(first_dash_config),
                    "bundle/charts/test_01.yaml": yaml.dump(first_chart_config),
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(
                        first_dataset_config,
                    ),
                    "bundle/databases/gsheets.yaml": yaml.dump(first_db_config),
                },
                ResourceType.DASHBOARD,
            ),
            (
                {
                    "bundle/dashboards/other_dashboard.yaml": yaml.dump(
                        second_dash_config,
                    ),
                    "bundle/charts/test_other_07.yaml": yaml.dump(second_chart_config),
                    "bundle/datasets/Other_DB/test_new.yaml": yaml.dump(
                        second_dataset_config,
                    ),
                    "bundle/databases/Other_DB.yaml": yaml.dump(second_db_config),
                },
                ResourceType.DASHBOARD,
            ),
        ]

    import_resources.assert_has_calls(
        [
            mock.call(
                content,
                client,
                False,
                call_type,
            )
            for content, call_type in expected_calls
        ],
        any_order=True,
    )
    client.get_uuids.assert_not_called()


def test_native_cascade_default(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test default cascade behavior uses overwrite for all resources.
    """
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    client = mocker.MagicMock()
    root = Path("/path/to/root")
    fs.create_dir(root)

    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {
            "uuid": "db-uuid",
            "database_name": "db",
            "sqlalchemy_uri": "sqlite://",
        },
        Path("bundle/datasets/ds.yaml"): {
            "uuid": "ds-uuid",
            "database_uuid": "db-uuid",
            "table_name": "test",
        },
    }

    import_resources_individually(
        configs,
        client,
        overwrite=True,
        asset_type=ResourceType.ASSET,
        continue_on_error=False,
        cascade=True,
    )

    assert import_resources.call_count == 2
    for call in import_resources.mock_calls:
        contents = call.args[0]
        if "bundle/datasets/ds.yaml" in contents:
            assert call.args[2] is True
            assert call.args[3] is ResourceType.DATASET
        elif "bundle/databases/db.yaml" in contents:
            assert call.args[2] is False
            assert call.args[3] is ResourceType.DATABASE
        else:
            assert False, "Unexpected import_resources call"


def test_native_no_cascade(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test no-cascade imports dependencies without overwrite.
    """
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    update_dashboard_no_cascade = mocker.patch(
        "preset_cli.cli.superset.sync.native.command._update_dashboard_no_cascade",
    )
    client = mocker.MagicMock()
    root = Path("/path/to/root")
    fs.create_dir(root)

    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {
            "uuid": "db-uuid",
            "database_name": "db",
            "sqlalchemy_uri": "sqlite://",
        },
        Path("bundle/datasets/ds.yaml"): {
            "uuid": "ds-uuid",
            "database_uuid": "db-uuid",
            "table_name": "test",
        },
        Path("bundle/charts/chart.yaml"): {
            "uuid": "chart-uuid",
            "dataset_uuid": "ds-uuid",
        },
        Path("bundle/dashboards/dash.yaml"): {
            "uuid": "dash-uuid",
            "position": {},
            "metadata": {},
        },
    }

    import_resources_individually(
        configs,
        client,
        overwrite=True,
        asset_type=ResourceType.DASHBOARD,
        continue_on_error=False,
        cascade=False,
    )

    overwrite_by_resource = {}
    for call in import_resources.mock_calls:
        contents = call.args[0]
        resource = next(iter(contents)).split("/")[1]
        overwrite_by_resource[resource] = call.args[2]

    assert overwrite_by_resource["charts"] is False
    assert overwrite_by_resource["datasets"] is False
    assert overwrite_by_resource["databases"] is False
    update_dashboard_no_cascade.assert_called_once()


def test_native_no_cascade_forces_split(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test no-cascade forces split imports.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    fs.create_dir(root / "databases")
    fs.create_file(
        root / "databases/db.yaml",
        contents=yaml.dump(
            {
                "database_name": "db",
                "sqlalchemy_uri": "sqlite://",
                "uuid": "db-uuid",
            },
        ),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = [{"uuid": "db-uuid"}]
    import_resources_individually_mock = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources_individually",
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
            "--no-cascade",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    import_resources_individually_mock.assert_called_once()
    assert import_resources_individually_mock.call_args.kwargs["cascade"] is False
    assert not Path("progress.log").exists()


def test_native_cascade_chart_does_not_force_split(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test cascade chart imports do not force split mode.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    fs.create_dir(root / "databases")
    fs.create_file(
        root / "databases/db.yaml",
        contents=yaml.dump(
            {
                "database_name": "db",
                "sqlalchemy_uri": "sqlite://",
                "uuid": "db-uuid",
            },
        ),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    client.get_databases.return_value = [{"uuid": "db-uuid"}]
    import_resources_mock = mocker.patch(
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
            "chart",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    import_resources_mock.assert_called_once()
    assert not Path("progress.log").exists()


def test_update_chart_no_cascade_updates_chart(mocker: MockerFixture) -> None:
    """
    Test updating a chart via the no-cascade path.
    """
    client = mocker.MagicMock()
    client.get_dataset.return_value = {"kind": "table"}
    client.get_resource_endpoint_info.return_value = {
        "edit_columns": [
            {"name": "slice_name"},
            {"name": "viz_type"},
            {"name": "params"},
            {"name": "query_context"},
            {"name": "datasource_id"},
            {"name": "datasource_type"},
        ],
    }

    def resolve_side_effect(_client, resource_name, _uuid):
        if resource_name == "chart":
            return 11
        if resource_name == "dataset":
            return 24
        return None

    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=resolve_side_effect,
    )

    chart_config: AssetConfig = {
        "uuid": "chart-uuid",
        "dataset_uuid": "ds-uuid",
        "slice_name": "Chart Name",
        "viz_type": "table",
        "params": {"datasource": "1__table"},
        "query_context": json.dumps(
            {
                "datasource": {"id": 1, "type": "table"},
                "queries": [{"datasource": {"id": 1, "type": "table"}}],
                "form_data": {"datasource": "1__table"},
            },
        ),
    }
    configs: Dict[Path, AssetConfig] = {Path("bundle/charts/chart.yaml"): chart_config}

    _update_chart_no_cascade(
        Path("bundle/charts/chart.yaml"),
        chart_config,
        configs,
        client,
        overwrite=True,
    )

    client.update_chart.assert_called_once()
    chart_id = client.update_chart.call_args.args[0]
    payload = client.update_chart.call_args.kwargs
    assert chart_id == 11

    params = json.loads(payload["params"])
    query_context = json.loads(payload["query_context"])
    assert params["datasource"] == "24__table"
    assert query_context["datasource"]["id"] == 24
    assert query_context["queries"][0]["datasource"]["id"] == 24
    assert query_context["form_data"]["datasource"] == "24__table"


def test_update_chart_no_cascade_skips_when_overwrite_false(
    mocker: MockerFixture,
) -> None:
    """
    Test no-cascade update skips when overwrite is false.
    """
    client = mocker.MagicMock()

    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        return_value=11,
    )

    chart_config: AssetConfig = {
        "uuid": "chart-uuid",
        "dataset_uuid": "ds-uuid",
        "slice_name": "Chart Name",
        "viz_type": "table",
        "params": {"datasource": "1__table"},
    }
    configs: Dict[Path, AssetConfig] = {Path("bundle/charts/chart.yaml"): chart_config}

    _update_chart_no_cascade(
        Path("bundle/charts/chart.yaml"),
        chart_config,
        configs,
        client,
        overwrite=False,
    )

    client.update_chart.assert_not_called()


def test_update_chart_no_cascade_creates_missing_dataset(
    mocker: MockerFixture,
) -> None:
    """
    Test dataset creation fallback for no-cascade chart updates.
    """
    client = mocker.MagicMock()
    client.get_dataset.return_value = {"kind": "table"}
    client.get_resource_endpoint_info.return_value = {"edit_columns": []}

    call_state = {"dataset_calls": 0}

    def resolve_side_effect(_client, resource_name, _uuid):
        if resource_name == "chart":
            return 11
        if resource_name == "dataset":
            call_state["dataset_calls"] += 1
            return None if call_state["dataset_calls"] == 1 else 24
        return None

    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=resolve_side_effect,
    )
    import_resources_mock = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )

    chart_config: AssetConfig = {
        "uuid": "chart-uuid",
        "dataset_uuid": "ds-uuid",
        "slice_name": "Chart Name",
        "viz_type": "table",
        "params": {"datasource": "1__table"},
    }
    dataset_config: AssetConfig = {
        "uuid": "ds-uuid",
        "database_uuid": "db-uuid",
        "params": {},
    }
    database_config: AssetConfig = {
        "uuid": "db-uuid",
        "sqlalchemy_uri": "sqlite://",
        "database_name": "db",
    }
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/charts/chart.yaml"): chart_config,
        Path("bundle/datasets/db/ds.yaml"): dataset_config,
        Path("bundle/databases/db.yaml"): database_config,
    }

    _update_chart_no_cascade(
        Path("bundle/charts/chart.yaml"),
        chart_config,
        configs,
        client,
        overwrite=True,
    )

    import_resources_mock.assert_called_once()
    called_asset_type = import_resources_mock.call_args.kwargs.get("asset_type")
    if called_asset_type is None:
        called_asset_type = import_resources_mock.call_args.args[3]
    assert called_asset_type == ResourceType.DATASET
    client.update_chart.assert_called_once()


def test_update_chart_no_cascade_creates_missing_chart(
    mocker: MockerFixture,
) -> None:
    """
    Test chart creation fallback for no-cascade chart updates.
    """
    client = mocker.MagicMock()
    client.get_dataset.return_value = {"kind": "table"}
    client.get_resource_endpoint_info.return_value = {"edit_columns": []}

    call_state = {"chart_calls": 0}

    def resolve_side_effect(_client, resource_name, _uuid):
        if resource_name == "chart":
            call_state["chart_calls"] += 1
            return None if call_state["chart_calls"] == 1 else 11
        if resource_name == "dataset":
            return 24
        return None

    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=resolve_side_effect,
    )
    import_resources_mock = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )

    chart_config: AssetConfig = {
        "uuid": "chart-uuid",
        "dataset_uuid": "ds-uuid",
        "slice_name": "Chart Name",
        "viz_type": "table",
        "params": {"datasource": "1__table"},
    }
    dataset_config: AssetConfig = {
        "uuid": "ds-uuid",
        "database_uuid": "db-uuid",
        "params": {},
    }
    database_config: AssetConfig = {
        "uuid": "db-uuid",
        "sqlalchemy_uri": "sqlite://",
        "database_name": "db",
    }
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/charts/chart.yaml"): chart_config,
        Path("bundle/datasets/db/ds.yaml"): dataset_config,
        Path("bundle/databases/db.yaml"): database_config,
    }

    _update_chart_no_cascade(
        Path("bundle/charts/chart.yaml"),
        chart_config,
        configs,
        client,
        overwrite=True,
    )

    import_resources_mock.assert_called_once()
    called_asset_type = import_resources_mock.call_args.kwargs.get("asset_type")
    if called_asset_type is None:
        called_asset_type = import_resources_mock.call_args.args[3]
    assert called_asset_type == ResourceType.CHART
    client.update_chart.assert_called_once()


def test_no_cascade_skips_existing_dependencies(
    mocker: MockerFixture,
) -> None:
    """
    Test no-cascade skips imports for existing dependent assets.
    """
    client = mocker.MagicMock()
    import_resources_mock = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._update_chart_no_cascade",
    )

    def resolve_side_effect(_client, resource_name, _uuid):
        if resource_name in {"dataset", "chart", "database"}:
            return 1
        return None

    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=resolve_side_effect,
    )

    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {
            "uuid": "db-uuid",
        },
        Path("bundle/datasets/db/ds.yaml"): {
            "uuid": "ds-uuid",
            "database_uuid": "db-uuid",
        },
        Path("bundle/charts/chart.yaml"): {
            "uuid": "chart-uuid",
            "dataset_uuid": "ds-uuid",
        },
    }

    import_resources_individually(
        configs,
        client,
        overwrite=True,
        asset_type=ResourceType.CHART,
        continue_on_error=False,
        cascade=False,
        existing_databases={"db-uuid"},
    )

    import_resources_mock.assert_not_called()


def test_native_no_cascade_dashboard_uses_update_helper(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test dashboard no-cascade routes primary imports through update helper.
    """
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    update_dashboard_no_cascade = mocker.patch(
        "preset_cli.cli.superset.sync.native.command._update_dashboard_no_cascade",
    )
    client = mocker.MagicMock()
    root = Path("/path/to/root")
    fs.create_dir(root)

    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=lambda _client, resource_name, _uuid: (
            1 if resource_name in {"chart", "dataset", "database"} else None
        ),
    )

    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {
            "uuid": "db-uuid",
            "database_name": "db",
            "sqlalchemy_uri": "sqlite://",
        },
        Path("bundle/datasets/ds.yaml"): {
            "uuid": "ds-uuid",
            "database_uuid": "db-uuid",
            "table_name": "test",
        },
        Path("bundle/charts/chart.yaml"): {
            "uuid": "chart-uuid",
            "dataset_uuid": "ds-uuid",
            "slice_name": "chart",
        },
        Path("bundle/dashboards/dash.yaml"): {
            "uuid": "dash-uuid",
            "position": {
                "CHART-1": {
                    "type": "CHART",
                    "meta": {"uuid": "chart-uuid"},
                },
            },
            "metadata": {
                "native_filter_configuration": [
                    {"targets": [{"datasetUuid": "ds-uuid"}]},
                ],
            },
        },
    }

    import_resources_individually(
        configs,
        client,
        overwrite=True,
        asset_type=ResourceType.DASHBOARD,
        continue_on_error=False,
        cascade=False,
    )

    import_resources.assert_not_called()
    update_dashboard_no_cascade.assert_called_once()


def test_build_dashboard_contents_returns_none_when_dashboard_missing() -> None:
    """
    Test dashboard bundle builder returns None when dashboard config is missing.
    """
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/charts/chart.yaml"): {
            "uuid": "chart-uuid",
            "dataset_uuid": "ds-uuid",
        },
    }
    assert _build_dashboard_contents(configs, "dash-uuid") is None


def test_build_dashboard_contents_includes_all_dependencies() -> None:
    """
    Test dashboard bundle builder includes chart, dataset, and database assets.
    """
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {"uuid": "db-uuid", "database_name": "db"},
        Path("bundle/datasets/ds.yaml"): {
            "uuid": "ds-uuid",
            "database_uuid": "db-uuid",
        },
        Path("bundle/charts/chart.yaml"): {
            "uuid": "chart-uuid",
            "dataset_uuid": "ds-uuid",
        },
        Path("bundle/dashboards/dash.yaml"): {
            "uuid": "dash-uuid",
            "position": {
                "CHART-1": {
                    "type": "CHART",
                    "meta": {"uuid": "chart-uuid"},
                },
            },
            "metadata": {
                "native_filter_configuration": [
                    {"targets": [{"datasetUuid": "ds-uuid"}]},
                ],
            },
        },
    }

    contents = _build_dashboard_contents(configs, "dash-uuid")
    assert contents is not None
    assert sorted(contents.keys()) == [
        "bundle/charts/chart.yaml",
        "bundle/dashboards/dash.yaml",
        "bundle/databases/db.yaml",
        "bundle/datasets/ds.yaml",
    ]


def test_build_dashboard_contents_missing_only_skips_existing_dependencies(
    mocker: MockerFixture,
) -> None:
    """
    Test missing-only dashboard bundle excludes dependencies that already exist.
    """
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {"uuid": "db-uuid", "database_name": "db"},
        Path("bundle/datasets/ds.yaml"): {
            "uuid": "ds-uuid",
            "database_uuid": "db-uuid",
        },
        Path("bundle/charts/chart.yaml"): {
            "uuid": "chart-uuid",
            "dataset_uuid": "ds-uuid",
        },
        Path("bundle/dashboards/dash.yaml"): {
            "uuid": "dash-uuid",
            "position": {
                "CHART-1": {
                    "type": "CHART",
                    "meta": {"uuid": "chart-uuid"},
                },
            },
            "metadata": {
                "native_filter_configuration": [
                    {"targets": [{"datasetUuid": "ds-uuid"}]},
                ],
            },
        },
    }

    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=lambda _client, resource_name, _uuid: (
            1 if resource_name in {"chart", "dataset", "database"} else None
        ),
    )
    client = mocker.MagicMock()

    contents = _build_dashboard_contents(
        configs,
        "dash-uuid",
        client=client,
        missing_only=True,
    )
    assert contents is not None
    assert sorted(contents.keys()) == ["bundle/dashboards/dash.yaml"]


def test_build_dashboard_contents_missing_only_with_no_client_includes_dependencies() -> (
    None
):
    """
    Test missing-only dashboard bundle includes dependencies when no client is provided.
    """
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {"uuid": "db-uuid", "database_name": "db"},
        Path("bundle/datasets/ds.yaml"): {
            "uuid": "ds-uuid",
            "database_uuid": "db-uuid",
        },
        Path("bundle/charts/chart.yaml"): {
            "uuid": "chart-uuid",
            "dataset_uuid": "ds-uuid",
        },
        Path("bundle/dashboards/dash.yaml"): {
            "uuid": "dash-uuid",
            "position": {
                "CHART-1": {
                    "type": "CHART",
                    "meta": {"uuid": "chart-uuid"},
                },
            },
            "metadata": {"native_filter_configuration": []},
        },
    }

    contents = _build_dashboard_contents(
        configs,
        "dash-uuid",
        client=None,
        missing_only=True,
    )
    assert contents is not None
    assert sorted(contents.keys()) == [
        "bundle/charts/chart.yaml",
        "bundle/dashboards/dash.yaml",
        "bundle/databases/db.yaml",
        "bundle/datasets/ds.yaml",
    ]


def test_build_dashboard_contents_tolerates_missing_dependency_references() -> None:
    """
    Test dashboard bundle builder skips missing chart, dataset, and database references.
    """
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/datasets/ds-without-db.yaml"): {
            "uuid": "ds-without-db",
            "database_uuid": "missing-db",
        },
        Path("bundle/charts/chart-with-missing-db.yaml"): {
            "uuid": "chart-has-dataset-with-missing-db",
            "dataset_uuid": "ds-without-db",
        },
        Path("bundle/dashboards/dash.yaml"): {
            "uuid": "dash-uuid",
            "position": {
                "CHART-missing": {
                    "type": "CHART",
                    "meta": {"uuid": "missing-chart"},
                },
                "CHART-existing": {
                    "type": "CHART",
                    "meta": {"uuid": "chart-has-dataset-with-missing-db"},
                },
            },
            "metadata": {
                "native_filter_configuration": [
                    {"targets": [{"datasetUuid": "missing-dataset"}]},
                ],
            },
        },
    }

    contents = _build_dashboard_contents(configs, "dash-uuid")
    assert contents is not None
    assert sorted(contents.keys()) == [
        "bundle/charts/chart-with-missing-db.yaml",
        "bundle/dashboards/dash.yaml",
        "bundle/datasets/ds-without-db.yaml",
    ]


def test_prepare_dashboard_update_payload_handles_non_integer_roles_and_json_fields(
    mocker: MockerFixture,
) -> None:
    """
    Test dashboard payload skips non-integer owner/role values and serializes dict JSON.
    """
    client = mocker.MagicMock()
    client.get_resource_endpoint_info.return_value = {
        "edit_columns": [
            {"name": "dashboard_title"},
            {"name": "position_json"},
            {"name": "json_metadata"},
        ],
    }
    warning_mock = mocker.patch(
        "preset_cli.cli.superset.sync.native.command._logger.warning",
    )

    payload = _prepare_dashboard_update_payload(
        {
            "dashboard_title": "Dashboard Name",
            "owners": ["owner@example.org"],
            "roles": ["Gamma"],
            "position_json": {"ROOT_ID": {"type": "ROOT"}},
            "json_metadata": {"native_filter_configuration": []},
        },
        client,
    )

    assert "owners" not in payload
    assert "roles" not in payload
    assert json.loads(cast(str, payload["position_json"])) == {
        "ROOT_ID": {"type": "ROOT"},
    }
    assert json.loads(cast(str, payload["json_metadata"])) == {
        "native_filter_configuration": [],
    }
    assert warning_mock.call_count == 2


def test_prepare_dashboard_update_payload_uses_serialized_json_fallbacks(
    mocker: MockerFixture,
) -> None:
    """
    Test dashboard payload keeps pre-serialized JSON fields when provided as strings.
    """
    client = mocker.MagicMock()
    client.get_resource_endpoint_info.return_value = {
        "edit_columns": [
            {"name": "position_json"},
            {"name": "json_metadata"},
        ],
    }

    payload = _prepare_dashboard_update_payload(
        {
            "position_json": '{"ROOT_ID":{"type":"ROOT"}}',
            "json_metadata": '{"color_scheme":"supersetColors"}',
        },
        client,
    )

    assert payload["position_json"] == '{"ROOT_ID":{"type":"ROOT"}}'
    assert payload["json_metadata"] == '{"color_scheme":"supersetColors"}'


def test_prepare_dashboard_update_payload_includes_integer_owners_and_roles(
    mocker: MockerFixture,
) -> None:
    """
    Test dashboard payload keeps owners/roles when they are integer IDs.
    """
    client = mocker.MagicMock()
    client.get_resource_endpoint_info.return_value = {
        "edit_columns": [
            {"name": "owners"},
            {"name": "roles"},
        ],
    }

    payload = _prepare_dashboard_update_payload(
        {
            "owners": [1, 2],
            "roles": [3],
        },
        client,
    )

    assert payload["owners"] == [1, 2]
    assert payload["roles"] == [3]


def test_update_dashboard_no_cascade_updates_dashboard(
    mocker: MockerFixture,
) -> None:
    """
    Test updating a dashboard via the no-cascade path.
    """
    client = mocker.MagicMock()
    client.get_resource_endpoint_info.return_value = {
        "edit_columns": [
            {"name": "dashboard_title"},
            {"name": "slug"},
            {"name": "position_json"},
            {"name": "json_metadata"},
            {"name": "published"},
            {"name": "is_managed_externally"},
            {"name": "external_url"},
        ],
    }
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        return_value=9,
    )

    dashboard_config = {
        "uuid": "dash-uuid",
        "dashboard_title": "Dashboard Name",
        "slug": "dashboard-name",
        "published": True,
        "position": {"ROOT_ID": {"type": "ROOT"}},
        "metadata": {"native_filter_configuration": []},
        "is_managed_externally": True,
        "external_url": "https://example.org/dashboard",
    }
    configs = {Path("bundle/dashboards/dash.yaml"): dashboard_config}

    _update_dashboard_no_cascade(
        Path("bundle/dashboards/dash.yaml"),
        dashboard_config,
        configs,
        client,
        overwrite=True,
    )

    client.update_dashboard.assert_called_once()
    dashboard_id = client.update_dashboard.call_args.args[0]
    payload = client.update_dashboard.call_args.kwargs
    assert dashboard_id == 9
    assert payload["dashboard_title"] == "Dashboard Name"
    assert payload["slug"] == "dashboard-name"
    assert payload["published"] is True
    assert payload["is_managed_externally"] is True
    assert payload["external_url"] == "https://example.org/dashboard"
    assert json.loads(payload["position_json"]) == {"ROOT_ID": {"type": "ROOT"}}
    assert json.loads(payload["json_metadata"]) == {"native_filter_configuration": []}


def test_update_dashboard_no_cascade_skips_when_overwrite_false(
    mocker: MockerFixture,
) -> None:
    """
    Test no-cascade dashboard update skips when overwrite is false.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        return_value=9,
    )
    import_resources_mock = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )

    dashboard_config: AssetConfig = {
        "uuid": "dash-uuid",
        "dashboard_title": "Dashboard Name",
        "position": {"ROOT_ID": {"type": "ROOT"}},
        "metadata": {"native_filter_configuration": []},
    }
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/dashboards/dash.yaml"): dashboard_config,
    }

    _update_dashboard_no_cascade(
        Path("bundle/dashboards/dash.yaml"),
        dashboard_config,
        configs,
        client,
        overwrite=False,
    )

    import_resources_mock.assert_not_called()
    client.update_dashboard.assert_not_called()


def test_update_dashboard_no_cascade_creates_missing_dashboard(
    mocker: MockerFixture,
) -> None:
    """
    Test dashboard creation fallback for no-cascade dashboard updates.
    """
    client = mocker.MagicMock()
    client.get_resource_endpoint_info.return_value = {"edit_columns": []}
    import_resources_mock = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )

    call_state = {"dashboard_calls": 0}

    def resolve_side_effect(_client, resource_name, _uuid):
        if resource_name == "dashboard":
            call_state["dashboard_calls"] += 1
            return None if call_state["dashboard_calls"] == 1 else 42
        return None

    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=resolve_side_effect,
    )

    dashboard_config: AssetConfig = {
        "uuid": "dash-uuid",
        "dashboard_title": "Dashboard Name",
        "position": {},
        "metadata": {},
    }
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/dashboards/dash.yaml"): dashboard_config,
    }

    _update_dashboard_no_cascade(
        Path("bundle/dashboards/dash.yaml"),
        dashboard_config,
        configs,
        client,
        overwrite=True,
    )

    import_resources_mock.assert_called_once()
    called_asset_type = import_resources_mock.call_args.kwargs.get("asset_type")
    if called_asset_type is None:
        called_asset_type = import_resources_mock.call_args.args[3]
    assert called_asset_type == ResourceType.DASHBOARD
    client.update_dashboard.assert_called_once()


def test_update_dashboard_no_cascade_raises_for_missing_uuid(
    mocker: MockerFixture,
) -> None:
    """
    Test dashboard no-cascade update fails fast when UUID is missing.
    """
    client = mocker.MagicMock()
    with pytest.raises(CLIError, match="Dashboard config missing UUID"):
        _update_dashboard_no_cascade(
            Path("bundle/dashboards/dash.yaml"),
            {},
            {},
            client,
            overwrite=True,
        )


def test_update_dashboard_no_cascade_raises_when_missing_dashboard_bundle(
    mocker: MockerFixture,
) -> None:
    """
    Test dashboard no-cascade update fails when dashboard cannot be created.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        return_value=None,
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._build_dashboard_contents",
        return_value=None,
    )

    with pytest.raises(
        CLIError,
        match="Dashboard not found and no dashboard config available for import.",
    ):
        _update_dashboard_no_cascade(
            Path("bundle/dashboards/dash.yaml"),
            {"uuid": "dash-uuid"},
            {},
            client,
            overwrite=True,
        )


def test_update_dashboard_no_cascade_raises_when_create_does_not_materialize(
    mocker: MockerFixture,
) -> None:
    """
    Test dashboard no-cascade update fails when create flow doesn't produce an ID.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=[None, None],
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._build_dashboard_contents",
        return_value={"bundle/dashboards/dash.yaml": "dashboard_title: test"},
    )
    import_resources_mock = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )

    with pytest.raises(CLIError, match="Unable to create dashboard"):
        _update_dashboard_no_cascade(
            Path("bundle/dashboards/dash.yaml"),
            {"uuid": "dash-uuid"},
            {},
            client,
            overwrite=True,
        )
    import_resources_mock.assert_called_once()


def test_update_dashboard_no_cascade_create_then_skip_when_overwrite_false(
    mocker: MockerFixture,
) -> None:
    """
    Test dashboard no-cascade create path does not update when overwrite is false.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=[None, 42],
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._build_dashboard_contents",
        return_value={"bundle/dashboards/dash.yaml": "dashboard_title: test"},
    )
    import_resources_mock = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )

    _update_dashboard_no_cascade(
        Path("bundle/dashboards/dash.yaml"),
        {"uuid": "dash-uuid"},
        {},
        client,
        overwrite=False,
    )

    import_resources_mock.assert_called_once()
    client.update_dashboard.assert_not_called()


def test_native_no_cascade_dataset_keeps_existing_database_in_primary_bundle(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test dataset no-cascade keeps existing databases in the primary bundle.
    """
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    client = mocker.MagicMock()
    root = Path("/path/to/root")
    fs.create_dir(root)

    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=lambda _client, resource_name, _uuid: (
            1 if resource_name == "database" else None
        ),
    )

    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {
            "uuid": "db-uuid",
            "database_name": "db",
            "sqlalchemy_uri": "sqlite://",
        },
        Path("bundle/datasets/ds.yaml"): {
            "uuid": "ds-uuid",
            "database_uuid": "db-uuid",
            "table_name": "test",
        },
    }

    import_resources_individually(
        configs,
        client,
        overwrite=True,
        asset_type=ResourceType.DATASET,
        continue_on_error=False,
        cascade=False,
    )

    import_resources.assert_called_once()
    call = import_resources.mock_calls[0]
    assert call.args[3] is ResourceType.DATASET
    assert call.args[2] is True
    assert sorted(call.args[0].keys()) == [
        "bundle/databases/db.yaml",
        "bundle/datasets/ds.yaml",
    ]


def test_native_no_cascade_dataset_keeps_missing_database(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test dataset no-cascade keeps missing database configs for creation.
    """
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    client = mocker.MagicMock()
    root = Path("/path/to/root")
    fs.create_dir(root)

    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        return_value=None,
    )

    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {
            "uuid": "db-uuid",
            "database_name": "db",
            "sqlalchemy_uri": "sqlite://",
        },
        Path("bundle/datasets/ds.yaml"): {
            "uuid": "ds-uuid",
            "database_uuid": "db-uuid",
            "table_name": "test",
        },
    }

    import_resources_individually(
        configs,
        client,
        overwrite=True,
        asset_type=ResourceType.DATASET,
        continue_on_error=False,
        cascade=False,
    )

    # First call imports the missing database, second call imports dataset + dependency.
    assert import_resources.call_count == 2
    dataset_call = next(
        call
        for call in import_resources.mock_calls
        if call.args[3] is ResourceType.DATASET
    )
    assert sorted(dataset_call.args[0].keys()) == [
        "bundle/databases/db.yaml",
        "bundle/datasets/ds.yaml",
    ]


def test_prune_existing_dependency_configs_keeps_non_existing_dependencies() -> None:
    """
    Test dependency prune keeps assets when dependency does not exist in target.
    """
    dashboard_path = Path("bundle/dashboards/dash.yaml")
    chart_path = Path("bundle/charts/chart.yaml")
    asset_configs: Dict[Path, AssetConfig] = {
        dashboard_path: {"uuid": "dash-uuid"},
        chart_path: {"uuid": "chart-uuid"},
    }

    _prune_existing_dependency_configs(
        asset_configs=asset_configs,
        primary_path=dashboard_path,
        resource_name="dashboards",
        dependency_resource_map={"dashboards": {"charts": "chart"}},
        resource_exists=lambda _resource_name, _config: False,
    )

    assert set(asset_configs.keys()) == {dashboard_path, chart_path}


def test_native_no_cascade_dataset(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test no-cascade with dataset asset type: database imported without overwrite.
    """
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    client = mocker.MagicMock()
    root = Path("/path/to/root")
    fs.create_dir(root)

    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {
            "uuid": "db-uuid",
            "database_name": "db",
            "sqlalchemy_uri": "sqlite://",
        },
        Path("bundle/datasets/ds.yaml"): {
            "uuid": "ds-uuid",
            "database_uuid": "db-uuid",
            "table_name": "test",
        },
    }

    import_resources_individually(
        configs,
        client,
        overwrite=True,
        asset_type=ResourceType.DATASET,
        continue_on_error=False,
        cascade=False,
    )

    overwrite_by_resource = {}
    for call in import_resources.mock_calls:
        contents = call.args[0]
        resource = next(iter(contents)).split("/")[1]
        overwrite_by_resource[resource] = call.args[2]

    assert overwrite_by_resource["datasets"] is True
    assert overwrite_by_resource["databases"] is False


def test_native_no_cascade_database(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test no-cascade with database asset type: database imported with overwrite.
    """
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    client = mocker.MagicMock()
    root = Path("/path/to/root")
    fs.create_dir(root)

    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {
            "uuid": "db-uuid",
            "database_name": "db",
            "sqlalchemy_uri": "sqlite://",
        },
    }

    import_resources_individually(
        configs,
        client,
        overwrite=True,
        asset_type=ResourceType.DATABASE,
        continue_on_error=False,
        cascade=False,
    )

    assert import_resources.call_count == 1
    call = import_resources.mock_calls[0]
    assert call.args[2] is True
    assert call.args[3] is ResourceType.DATABASE


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


def test_resolve_uuid_to_id_with_prefetched_resources() -> None:
    """
    Test ``_resolve_uuid_to_id`` skips the API fetch when resources are provided.
    """
    client = mock.MagicMock()
    resources = [
        {"id": 10, "uuid": "aaa-bbb"},
        {"id": 20, "uuid": "ccc-ddd"},
    ]

    result = _resolve_uuid_to_id(client, "chart", "ccc-ddd", resources=resources)
    assert result == 20
    client.get_resources.assert_not_called()

    # Not found in prefetched, falls back to UUID map
    client.get_uuids.return_value = {30: "eee-fff"}
    result = _resolve_uuid_to_id(client, "chart", "eee-fff", resources=resources)
    assert result == 30


def test_resolve_uuid_to_id_skips_non_integer_matching_id() -> None:
    """
    Test ``_resolve_uuid_to_id`` keeps scanning when a matching UUID has a non-int ID.
    """
    client = mock.MagicMock()
    resources: List[AssetConfig] = [
        {"id": "not-an-int", "uuid": "same-uuid"},
        {"id": 42, "uuid": "same-uuid"},
    ]
    assert _resolve_uuid_to_id(client, "chart", "same-uuid", resources=resources) == 42


def test_get_charts_uuids_handles_non_dashboard_nodes() -> None:
    """
    Test ``get_charts_uuids`` ignores invalid position structures and non-chart nodes.
    """
    assert list(get_charts_uuids({"position": []})) == []
    assert list(
        get_charts_uuids(
            {
                "position": {
                    "A": "not-a-dict",
                    "B": {"type": "ROW"},
                    "C": {"type": "CHART", "meta": {"uuid": "chart-1"}},
                },
            },
        ),
    ) == ["chart-1"]


def test_get_dataset_filter_uuids_handles_invalid_filter_shapes() -> None:
    """
    Test ``get_dataset_filter_uuids`` ignores malformed metadata/targets entries.
    """
    assert get_dataset_filter_uuids({"metadata": "invalid"}) == set()
    assert (
        get_dataset_filter_uuids({"metadata": {"native_filter_configuration": {}}})
        == set()
    )
    assert get_dataset_filter_uuids(
        {
            "metadata": {
                "native_filter_configuration": [
                    "not-a-dict",
                    {"targets": "not-a-list"},
                    {"targets": [None, {"datasetUuid": "dataset-1"}]},
                ],
            },
        },
    ) == {"dataset-1"}


def test_import_resources_individually_dashboard_uses_multiple_related_configs(
    mocker: MockerFixture,
    fs: FakeFilesystem,  # pylint: disable=unused-argument
) -> None:
    """
    Test dashboard import picks up both chart and dataset related configs.
    """
    client = mocker.MagicMock()
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {"uuid": "db-1"},
        Path("bundle/datasets/ds.yaml"): {"uuid": "ds-1", "database_uuid": "db-1"},
        Path("bundle/charts/chart.yaml"): {"uuid": "chart-1", "dataset_uuid": "ds-1"},
        Path("bundle/dashboards/dash.yaml"): {
            "uuid": "dash-1",
            "position": {
                "CHART-0": {"type": "CHART", "meta": {"uuid": "chart-missing"}},
                "CHART-1": {"type": "CHART", "meta": {"uuid": "chart-1"}},
            },
            "metadata": {
                "native_filter_configuration": [
                    {"targets": [{"datasetUuid": "ds-1"}]},
                ],
            },
        },
    }

    import_resources_individually(configs, client, True, ResourceType.DASHBOARD)

    dashboard_calls = [
        call
        for call in import_resources.call_args_list
        if call.args[3] == ResourceType.DASHBOARD
    ]
    assert len(dashboard_calls) == 1
    dashboard_contents = dashboard_calls[0].args[0]
    assert "bundle/dashboards/dash.yaml" in dashboard_contents
    assert "bundle/charts/chart.yaml" in dashboard_contents
    assert "bundle/datasets/ds.yaml" in dashboard_contents
    assert "bundle/databases/db.yaml" in dashboard_contents


def test_safe_json_loads_decode_failure() -> None:
    """
    Test ``_safe_json_loads`` returns None for invalid JSON strings.
    """
    assert _safe_json_loads(None, "test") is None
    assert _safe_json_loads({"key": "val"}, "test") == {"key": "val"}
    assert _safe_json_loads('{"valid": true}', "test") == {"valid": True}
    assert _safe_json_loads("not-json", "test") is None
    assert _safe_json_loads(12345, "test") is None


def test_no_cascade_wrapper_helpers() -> None:
    """
    Test wrapper helpers delegated from ``command.py`` into ``no_cascade`` module.
    """
    params, query_context = _update_chart_datasource_refs(
        {"datasource": "1__table"},
        {"datasource": {"id": 1, "type": "table"}},
        datasource_id=42,
        datasource_type="table",
    )
    assert params == {"datasource": "42__table"}
    assert query_context == {"datasource": {"id": 42, "type": "table"}}

    payload: Dict[str, object] = {}
    _set_integer_list_payload_field(
        payload,
        {"owners": [1, 2]},
        "owners",
        "warning",
    )
    assert payload["owners"] == [1, 2]

    _set_json_payload_field(
        payload,
        {"position": {"CHART-1": {"type": "CHART"}}},
        preferred_field="position",
        fallback_field="position_json",
        payload_field="position_json",
    )
    assert payload["position_json"] == '{"CHART-1": {"type": "CHART"}}'


def test_safe_extract_zip_creates_directories(tmp_path: Path) -> None:
    """
    Test ZIP extraction handles directory members.
    """
    zip_path = tmp_path / "assets.zip"
    with ZipFile(zip_path, "w") as bundle:
        bundle.writestr("bundle/charts/", "")
        bundle.writestr("bundle/charts/chart.yaml", "uuid: c1")

    output_dir = tmp_path / "out"
    _safe_extract_zip(zip_path, output_dir)
    assert (output_dir / "bundle/charts").is_dir()
    assert (output_dir / "bundle/charts/chart.yaml").exists()


def test_safe_extract_zip_rejects_commonpath_escape(
    mocker: MockerFixture,
    tmp_path: Path,
) -> None:
    """
    Test ZIP extraction rejects entries escaping the target path by commonpath check.
    """
    zip_path = tmp_path / "assets.zip"
    with ZipFile(zip_path, "w") as bundle:
        bundle.writestr("bundle/charts/chart.yaml", "uuid: c1")

    output_dir = tmp_path / "out"
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command.os.path.commonpath",
        return_value="/outside",
    )
    with pytest.raises(Exception, match="escapes target directory"):
        _safe_extract_zip(zip_path, output_dir)


def test_resolve_input_root_rejects_non_zip_file(tmp_path: Path) -> None:
    """
    Test ``_resolve_input_root`` rejects non-zip files.
    """
    file_path = tmp_path / "not-a-zip.txt"
    file_path.write_text("content", encoding="utf-8")
    with pytest.raises(Exception, match="directory or a .zip bundle"):
        _resolve_input_root(file_path)


def test_resolve_input_root_rejects_invalid_zip_bundle(tmp_path: Path) -> None:
    """
    Test ``_resolve_input_root`` rejects zip files that do not contain assets.
    """
    zip_path = tmp_path / "invalid.zip"
    with ZipFile(zip_path, "w") as bundle:
        bundle.writestr("README.txt", "not a bundle")

    with pytest.raises(Exception, match="does not contain a valid assets bundle"):
        _resolve_input_root(zip_path)


def test_resolve_input_root_handles_duplicate_candidates(tmp_path: Path) -> None:
    """
    Test ``_resolve_input_root`` de-duplicates candidate paths while scanning bundles.
    """
    zip_path = tmp_path / "nested.zip"
    with ZipFile(zip_path, "w") as bundle:
        bundle.writestr("bundle/bundle/databases/db.yaml", "uuid: db1")

    root, temp_dir = _resolve_input_root(zip_path)
    assert root.name == "bundle"
    assert (root / "databases" / "db.yaml").exists()
    if temp_dir:
        temp_dir.cleanup()


def test_resolve_uuid_to_id_with_empty_uuid_returns_none() -> None:
    """
    Test ``_resolve_uuid_to_id`` returns ``None`` for empty UUID values.
    """
    client = mock.MagicMock()
    assert _resolve_uuid_to_id(client, "chart", None) is None
    client.get_resources.assert_not_called()


def test_resolve_uuid_to_id_returns_none_when_not_found() -> None:
    """
    Test ``_resolve_uuid_to_id`` returns ``None`` when fallback UUID map has no match.
    """
    client = mock.MagicMock()
    resources = [{"id": 1, "uuid": "a"}]
    client.get_uuids.return_value = {1: "a"}
    assert _resolve_uuid_to_id(client, "chart", "missing", resources=resources) is None


def test_find_config_by_uuid_none_and_not_found() -> None:
    """
    Test ``_find_config_by_uuid`` for empty UUID and missing entries.
    """
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/charts/chart.yaml"): {"uuid": "chart-1"},
    }
    assert _find_config_by_uuid(configs, "charts", None) is None
    assert _find_config_by_uuid(configs, "charts", "missing") is None


def test_build_dataset_contents_missing_entries() -> None:
    """
    Test ``_build_dataset_contents`` returns None when dataset/database configs are absent.
    """
    assert _build_dataset_contents({}, "ds-uuid") is None
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/datasets/ds.yaml"): {
            "uuid": "ds-uuid",
            "database_uuid": "db-uuid",
        },
    }
    assert _build_dataset_contents(configs, "ds-uuid") is None


def test_build_chart_contents_missing_entries() -> None:
    """
    Test ``_build_chart_contents`` returns None when chart or dependencies are absent.
    """
    assert _build_chart_contents({}, "chart-uuid") is None
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/charts/chart.yaml"): {
            "uuid": "chart-uuid",
            "dataset_uuid": "ds-uuid",
        },
    }
    assert _build_chart_contents(configs, "chart-uuid") is None


def test_filter_payload_to_schema_falls_back_on_superset_error(
    mocker: MockerFixture,
) -> None:
    """
    Test payload filtering falls back to allowlist when schema lookup fails.
    """
    client = mocker.MagicMock()
    client.get_resource_endpoint_info.side_effect = SupersetError(
        errors=[{"message": "boom"}],
    )
    payload: Dict[str, object] = {"slice_name": "Chart", "not_allowed": "x"}
    result = _filter_payload_to_schema(client, "chart", payload, {"slice_name"})
    assert result == {"slice_name": "Chart"}


def test_prepare_chart_update_payload_raw_json_and_non_numeric_meta(
    mocker: MockerFixture,
) -> None:
    """
    Test chart payload keeps raw JSON strings and skips owners/tags without IDs.
    """
    client = mocker.MagicMock()
    client.get_resource_endpoint_info.return_value = {"edit_columns": []}
    warning = mocker.patch(
        "preset_cli.cli.superset.sync.native.command._logger.warning",
    )
    payload = _prepare_chart_update_payload(
        {
            "slice_name": "Chart",
            "viz_type": "table",
            "owners": ["owner@example.org"],
            "tags": ["tag-name"],
            "params": "{invalid-json}",
            "query_context": "{invalid-json}",
        },
        datasource_id=7,
        datasource_type="table",
        client=client,
    )
    assert payload["params"] == "{invalid-json}"
    assert payload["query_context"] == "{invalid-json}"
    assert "owners" not in payload
    assert "tags" not in payload
    assert warning.call_count >= 2


def test_prepare_chart_update_payload_with_numeric_owners_and_tags(
    mocker: MockerFixture,
) -> None:
    """
    Test chart payload includes owners/tags when values are numeric IDs.
    """
    client = mocker.MagicMock()
    client.get_resource_endpoint_info.return_value = {"edit_columns": []}
    payload = _prepare_chart_update_payload(
        {
            "slice_name": "Chart",
            "viz_type": "table",
            "owners": [1, 2],
            "tags": [3, 4],
            "params": "{}",
            "query_context": "{}",
        },
        datasource_id=7,
        datasource_type="table",
        client=client,
    )
    assert payload["owners"] == [1, 2]
    assert payload["tags"] == [3, 4]


def test_update_chart_no_cascade_missing_chart_uuid_raises(
    mocker: MockerFixture,
) -> None:
    """
    Test no-cascade chart update raises for configs missing UUID.
    """
    client = mocker.MagicMock()
    with pytest.raises(Exception, match="Chart config missing UUID"):
        _update_chart_no_cascade(
            Path("bundle/charts/chart.yaml"),
            {"dataset_uuid": "ds-uuid"},
            {},
            client,
            overwrite=True,
        )


def test_update_chart_no_cascade_missing_chart_bundle_raises(
    mocker: MockerFixture,
) -> None:
    """
    Test no-cascade chart update raises when chart is missing and no bundle is available.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        return_value=None,
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._build_chart_contents",
        return_value=None,
    )
    with pytest.raises(Exception, match="no dataset/database configs available"):
        _update_chart_no_cascade(
            Path("bundle/charts/chart.yaml"),
            {"uuid": "chart-uuid", "dataset_uuid": "ds-uuid"},
            {},
            client,
            overwrite=True,
        )


def test_update_chart_no_cascade_unable_to_create_chart_raises(
    mocker: MockerFixture,
) -> None:
    """
    Test no-cascade chart update raises when chart creation does not produce an ID.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=[None, None],
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._build_chart_contents",
        return_value={"bundle/charts/chart.yaml": "uuid: chart-uuid"},
    )
    mocker.patch("preset_cli.cli.superset.sync.native.command.import_resources")
    with pytest.raises(Exception, match="Unable to create chart"):
        _update_chart_no_cascade(
            Path("bundle/charts/chart.yaml"),
            {"uuid": "chart-uuid", "dataset_uuid": "ds-uuid"},
            {},
            client,
            overwrite=True,
        )


def test_update_chart_no_cascade_create_then_skip_update_when_overwrite_false(
    mocker: MockerFixture,
) -> None:
    """
    Test no-cascade chart path skips update after create when overwrite is false.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=[None, 11],
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._build_chart_contents",
        return_value={"bundle/charts/chart.yaml": "uuid: chart-uuid"},
    )
    mocker.patch("preset_cli.cli.superset.sync.native.command.import_resources")
    _update_chart_no_cascade(
        Path("bundle/charts/chart.yaml"),
        {"uuid": "chart-uuid", "dataset_uuid": "ds-uuid"},
        {},
        client,
        overwrite=False,
    )
    client.update_chart.assert_not_called()


def test_update_chart_no_cascade_missing_dataset_uuid_raises(
    mocker: MockerFixture,
) -> None:
    """
    Test no-cascade chart update raises when dataset UUID is absent.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        return_value=11,
    )
    with pytest.raises(Exception, match="missing dataset_uuid"):
        _update_chart_no_cascade(
            Path("bundle/charts/chart.yaml"),
            {"uuid": "chart-uuid"},
            {},
            client,
            overwrite=True,
        )


def test_update_chart_no_cascade_missing_dataset_bundle_raises(
    mocker: MockerFixture,
) -> None:
    """
    Test no-cascade chart update raises when dataset is missing and cannot be built.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=[11, None],
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._build_dataset_contents",
        return_value=None,
    )
    with pytest.raises(Exception, match="no dataset/database configs available"):
        _update_chart_no_cascade(
            Path("bundle/charts/chart.yaml"),
            {"uuid": "chart-uuid", "dataset_uuid": "ds-uuid"},
            {},
            client,
            overwrite=True,
        )


def test_update_chart_no_cascade_unable_to_create_dataset_raises(
    mocker: MockerFixture,
) -> None:
    """
    Test no-cascade chart update raises when dataset creation fails to produce an ID.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        side_effect=[11, None, None],
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._build_dataset_contents",
        return_value={"bundle/datasets/ds.yaml": "uuid: ds-uuid"},
    )
    mocker.patch("preset_cli.cli.superset.sync.native.command.import_resources")
    with pytest.raises(Exception, match="Unable to create dataset"):
        _update_chart_no_cascade(
            Path("bundle/charts/chart.yaml"),
            {"uuid": "chart-uuid", "dataset_uuid": "ds-uuid"},
            {},
            client,
            overwrite=True,
        )


def test_import_resources_individually_handles_none_uuid_dependency_check(
    mocker: MockerFixture,
    fs: FakeFilesystem,  # pylint: disable=unused-argument
) -> None:
    """
    Test no-cascade dependency existence check handles configs with ``uuid=None``.
    """
    client = mocker.MagicMock()
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    import_resources_mock = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.native.command._resolve_uuid_to_id",
        return_value=None,
    )
    configs: Dict[Path, AssetConfig] = {
        Path("bundle/databases/db.yaml"): {"uuid": None, "database_name": "db"},
    }

    import_resources_individually(
        configs,
        client,
        overwrite=True,
        asset_type=ResourceType.DASHBOARD,
        continue_on_error=False,
        cascade=False,
    )

    import_resources_mock.assert_called_once()


def test_prepare_chart_update_payload_without_params_updates_query_context_only(
    mocker: MockerFixture,
) -> None:
    """
    Test payload generation when ``params`` is absent and query list has mixed entries.
    """
    client = mocker.MagicMock()
    client.get_resource_endpoint_info.return_value = {"edit_columns": []}
    payload = _prepare_chart_update_payload(
        {
            "slice_name": "Chart",
            "viz_type": "table",
            "params": None,
            "query_context": json.dumps(
                {
                    "datasource": {"id": 1, "type": "table"},
                    "queries": ["not-a-dict", {"foo": "bar"}],
                    "form_data": {"datasource": "1__table"},
                },
            ),
        },
        datasource_id=24,
        datasource_type="table",
        client=client,
    )
    assert "params" not in payload
    query_context = json.loads(cast(str, payload["query_context"]))
    assert query_context["datasource"]["id"] == 24
    assert query_context["form_data"]["datasource"] == "24__table"
