"""
Tests for the native import command.
"""
# pylint: disable=redefined-outer-name, invalid-name

from pathlib import Path
from typing import List
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
    import_resources,
    import_resources_individually,
    load_user_modules,
    prompt_for_passwords,
    raise_helper,
    verify_db_connectivity,
)
from preset_cli.exceptions import ErrorLevel, ErrorPayload, SupersetError


def test_prompt_for_passwords(mocker: MockerFixture) -> None:
    """
    Test ``prompt_for_passwords``.
    """
    getpass = mocker.patch("preset_cli.cli.superset.sync.native.command.getpass")

    config = {"sqlalchemy_uri": "postgresql://user:XXXXXXXXXX@host:5432/db"}
    path = Path("/path/to/root/databases/psql.yaml")
    prompt_for_passwords(path, config)

    getpass.getpass.assert_called_with(f"Please provide the password for {path}: ")

    config["password"] = "password123"
    getpass.reset_mock()
    prompt_for_passwords(path, config)
    getpass.getpass.assert_not_called()


def test_import_resources(mocker: MockerFixture) -> None:
    """
    Test ``import_resources``.
    """
    client = mocker.MagicMock()

    contents = {"bundle/databases/gsheets.yaml": "GSheets"}
    with freeze_time("2022-01-01T00:00:00Z"):
        import_resources(contents=contents, client=client, overwrite=False)

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
    import_resources(contents=contents, client=client, overwrite=False)

    assert click.style.called_with(
        "The following file(s) already exist. Pass --overwrite to replace them.\n"
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
        import_resources(contents=contents, client=client, overwrite=False)
    assert excinfo.value.errors == errors


def test_native(mocker: MockerFixture, fs: FakeFilesystem) -> None:
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
    client.get_uuids.return_value = {}
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
                "table_name": "Hello",
                "is_managed_externally": False,
            },
        ),
    }
    import_resources.assert_has_calls([mock.call(contents, client, False)])


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
    client.get_uuids.return_value = {}
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
    import_resources.assert_has_calls([mock.call(contents, client, False)])


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
    client.get_uuids.return_value = {}
    mocker.patch("preset_cli.cli.superset.sync.native.command.import_resources")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    prompt_for_passwords = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.prompt_for_passwords",
    )

    runner = CliRunner()

    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "sync", "native", str(root)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    prompt_for_passwords.assert_called()

    prompt_for_passwords.reset_mock()
    client.get_uuids.return_value = {"1": "uuid1"}
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "sync", "native", str(root)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    prompt_for_passwords.assert_not_called()


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
    client.get_uuids.return_value = {}
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
    import_resources.assert_has_calls([mock.call(contents, client, False)])


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
    client.get_uuids.return_value = {}
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
    dataset_config[
        "external_url"
    ] = "https://repo.example.com/datasets/gsheets/test.yaml"
    contents = {
        "bundle/databases/gsheets.yaml": yaml.dump(database_config),
        "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
    }
    import_resources.assert_has_calls([mock.call(contents, client, False)])


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
    client.get_uuids.return_value = {}
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
    import_resources.assert_has_calls([mock.call(contents, client, False)])


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


def test_native_split(mocker: MockerFixture, fs: FakeFilesystem) -> None:
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
        root / "charts/chart.yaml",
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
    import_resources = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resources",
    )
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

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
            ),
            mock.call(
                {
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
            ),
            mock.call(
                {
                    "bundle/charts/chart.yaml": yaml.dump(chart_config),
                    "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
                    "bundle/databases/gsheets.yaml": yaml.dump(database_config),
                },
                client,
                False,
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
            ),
        ],
    )


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
    import_resources_individually(configs, client, overwrite=True)

    client.import_zip.side_effect = [
        requests.exceptions.ConnectionError("Connection aborted."),
        requests.exceptions.ConnectionError("Connection aborted."),
        requests.exceptions.ConnectionError("Connection aborted."),
        requests.exceptions.ConnectionError("Connection aborted."),
        requests.exceptions.ConnectionError("Connection aborted."),
    ]
    with pytest.raises(Exception) as excinfo:
        import_resources_individually(configs, client, overwrite=True)
    assert str(excinfo.value) == "Connection aborted."


def test_import_resources_individually_checkpoint(
    mocker: MockerFixture,
    fs: FakeFilesystem,  # pylint: disable=unused-argument
) -> None:
    """
    Test checkpoint in ``import_resources_individually``.
    """
    client = mocker.MagicMock()
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
        import_resources_individually(configs, client, overwrite=True)
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
            ),
            mocker.call(
                {
                    "bundle/databases/psql.yaml": yaml.dump(
                        {"name": "my other database", "uuid": "uuid2"},
                    ),
                },
                client,
                True,
            ),
        ],
    )

    with open("checkpoint.log", encoding="utf-8") as log:
        assert log.read() == "bundle/databases/gsheets.yaml\n"

    # retry
    import_resources.mock_reset()
    import_resources_individually(configs, client, overwrite=True)
    import_resources.assert_has_calls(
        [
            mock.call(
                {
                    "bundle/databases/psql.yaml": yaml.dump(
                        {"name": "my other database", "uuid": "uuid2"},
                    ),
                },
                client,
                True,
            ),
        ],
    )

    assert not Path("checkpoint.log").exists()


def test_sync_native_jinja_templating_disabled(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``native`` command with --disable-jinja-templating.
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
    client.get_uuids.return_value = {}
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
    import_resources.assert_has_calls([mock.call(contents, client, False)])
