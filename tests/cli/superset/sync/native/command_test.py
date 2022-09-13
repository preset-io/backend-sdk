"""
Tests for the native import command.
"""
# pylint: disable=redefined-outer-name, invalid-name

from pathlib import Path
from typing import List
from unittest import mock
from zipfile import ZipFile

import pytest
import yaml
from click.testing import CliRunner
from freezegun import freeze_time
from jinja2 import Template
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.cli.superset.main import superset_cli
from preset_cli.cli.superset.sync.native.command import (
    import_resource,
    load_user_modules,
    prompt_for_passwords,
    raise_helper,
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


def test_import_resource(mocker: MockerFixture) -> None:
    """
    Test ``import_resource``.
    """
    client = mocker.MagicMock()

    contents = {"bundle/databases/gsheets.yaml": "GSheets"}
    with freeze_time("2022-01-01T00:00:00Z"):
        import_resource("database", contents=contents, client=client, overwrite=False)

    call = client.import_zip.mock_calls[0]
    assert call.kwargs == {"overwrite": False}

    resource, buf = call.args
    assert resource == "database"
    with ZipFile(buf) as bundle:
        assert bundle.namelist() == [
            "bundle/databases/gsheets.yaml",
            "bundle/metadata.yaml",
        ]
        assert (
            bundle.read("bundle/metadata.yaml").decode()
            == "timestamp: '2022-01-01T00:00:00+00:00'\ntype: Database\nversion: 1.0.0\n"
        )
        assert bundle.read("bundle/databases/gsheets.yaml").decode() == "GSheets"


def test_import_resource_overwrite_needed(mocker: MockerFixture) -> None:
    """
    Test ``import_resource`` when an overwrite error is raised.
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
    import_resource("database", contents=contents, client=client, overwrite=False)

    assert click.style.called_with(
        "The following file(s) already exist. Pass --overwrite to replace them.\n"
        "databases/gsheets.yaml",
        fg="bright_red",
    )


def test_import_resource_error(mocker: MockerFixture) -> None:
    """
    Test ``import_resource`` when an unexpected error is raised.
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
        import_resource("database", contents=contents, client=client, overwrite=False)
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
        root / "README.txt",
        contents="Hello, world",
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    import_resource = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resource",
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
        "bundle/datasets/gsheets/test.yaml": yaml.dump(dataset_config),
    }
    import_resource.assert_has_calls(
        [
            mock.call("database", contents, client, False),
            mock.call("dataset", contents, client, False),
            mock.call("chart", contents, client, False),
            mock.call("dashboard", contents, client, False),
        ],
    )


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
    }
    fs.create_file(
        root / "databases/postgres.yaml",
        contents=yaml.dump(database_config),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    import_resource = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resource",
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
            },
        ),
    }
    import_resource.assert_has_calls(
        [
            mock.call("database", contents, client, False),
            mock.call("dataset", contents, client, False),
            mock.call("chart", contents, client, False),
            mock.call("dashboard", contents, client, False),
        ],
    )


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
    import_resource = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resource",
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
    import_resource.assert_has_calls(
        [
            mock.call("database", contents, client, False),
            mock.call("dataset", contents, client, False),
            mock.call("chart", contents, client, False),
            mock.call("dashboard", contents, client, False),
        ],
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
    }
    fs.create_file(
        root / "databases/gsheets.yaml",
        contents=yaml.dump(database_config),
    )

    SupersetClient = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.SupersetClient",
    )
    client = SupersetClient()
    import_resource = mocker.patch(
        "preset_cli.cli.superset.sync.native.command.import_resource",
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
            },
        ),
    }
    import_resource.assert_has_calls(
        [
            mock.call("database", contents, client, False),
            mock.call("dataset", contents, client, False),
            mock.call("chart", contents, client, False),
            mock.call("dashboard", contents, client, False),
        ],
    )
