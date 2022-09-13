"""
Tests for the export commands.
"""
# pylint: disable=redefined-outer-name, invalid-name, unused-argument

from io import BytesIO
from pathlib import Path
from unittest import mock
from uuid import UUID
from zipfile import ZipFile

import pytest
import yaml
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.auth.main import Auth
from preset_cli.cli.superset.export import export_resource
from preset_cli.cli.superset.main import superset_cli


@pytest.fixture
def dataset_export() -> BytesIO:
    """
    Fixture for the contents of a simple database export.
    """
    contents = {
        "dashboard_export/metadata.yaml": "Metadata",
        "dashboard_export/databases/gsheets.yaml": yaml.dump(
            {
                "database_name": "GSheets",
                "sqlalchemy_uri": "gsheets://",
            },
        ),
        "dashboard_export/datasets/gsheets/test.yaml": yaml.dump(
            {
                "table_name": "test",
                "sql": """
SELECT action, count(*) as times
FROM logs
WHERE
    action in {{ filter_values('action_type')|where_in }}
GROUP BY action""",
            },
        ),
    }

    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        for file_name, file_contents in contents.items():
            with bundle.open(file_name, "w") as output:
                output.write(file_contents.encode())
    buf.seek(0)
    return buf


def test_export_resource(
    mocker: MockerFixture,
    fs: FakeFilesystem,
    dataset_export: BytesIO,
) -> None:
    """
    Test ``export_resource``.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)

    client = mocker.MagicMock()
    client.export_zip.return_value = dataset_export

    export_resource(resource_name="database", root=root, client=client, overwrite=False)
    with open(root / "databases/gsheets.yaml", encoding="utf-8") as input_:
        assert input_.read() == "database_name: GSheets\nsqlalchemy_uri: gsheets://\n"

    # check that Jinja2 was escaped
    export_resource(resource_name="dataset", root=root, client=client, overwrite=False)
    with open(root / "datasets/gsheets/test.yaml", encoding="utf-8") as input_:
        assert yaml.load(input_.read(), Loader=yaml.SafeLoader) == {
            "table_name": "test",
            "sql": """
SELECT action, count(*) as times
FROM logs
WHERE
    action in {{ '{{' }} filter_values('action_type')|where_in {{ '}}' }}
GROUP BY action""",
        }

    # metadata file should be ignored
    assert not (root / "metadata.yaml").exists()


def test_export_resource_overwrite(
    mocker: MockerFixture,
    fs: FakeFilesystem,
    dataset_export: BytesIO,
) -> None:
    """
    Test that we need to confirm overwrites.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)

    client = mocker.MagicMock()
    client.export_zip.return_value = dataset_export

    export_resource(resource_name="database", root=root, client=client, overwrite=False)
    with pytest.raises(Exception) as excinfo:
        export_resource(
            resource_name="database",
            root=root,
            client=client,
            overwrite=False,
        )
    assert str(excinfo.value) == (
        "File already exists and --overwrite was not specified: "
        "/path/to/root/databases/gsheets.yaml"
    )

    export_resource(resource_name="database", root=root, client=client, overwrite=True)


def test_export_assets(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export_assets`` command.
    """
    # root must exist for command to succeed
    root = Path("/path/to/root")
    fs.create_dir(root)

    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    export_resource = mocker.patch("preset_cli.cli.superset.export.export_resource")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export", "/path/to/root"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    export_resource.assert_has_calls(
        [
            mock.call("database", Path("/path/to/root"), client, False),
            mock.call("dataset", Path("/path/to/root"), client, False),
            mock.call("chart", Path("/path/to/root"), client, False),
            mock.call("dashboard", Path("/path/to/root"), client, False),
        ],
    )


def test_export_with_custom_auth(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export`` command.
    """
    # root must exist for command to succeed
    root = Path("/path/to/root")
    fs.create_dir(root)

    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    export_resource = mocker.patch("preset_cli.cli.superset.export.export_resource")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export", "/path/to/root"],
        catch_exceptions=False,
        obj={"AUTH": Auth()},
    )
    assert result.exit_code == 0
    export_resource.assert_has_calls(
        [
            mock.call("database", Path("/path/to/root"), client, False),
            mock.call("dataset", Path("/path/to/root"), client, False),
            mock.call("chart", Path("/path/to/root"), client, False),
            mock.call("dashboard", Path("/path/to/root"), client, False),
        ],
    )


def test_export_users(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export_users`` command.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_users.return_value = [
        {
            "first_name": "admin",
            "last_name": "admin",
            "username": "admin",
            "email": "admin@example.com",
            "role": ["Admin"],
        },
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export-users", "users.yaml"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    with open("users.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == [
        {
            "first_name": "admin",
            "last_name": "admin",
            "username": "admin",
            "email": "admin@example.com",
            "role": ["Admin"],
        },
    ]


def test_export_rls(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export_users`` command.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_rls.return_value = [
        {
            "clause": "client_id = 9",
            "description": "This is a rule. There are many others like it, but this one is mine.",
            "filter_type": "Regular",
            "group_key": "department",
            "name": "My rule",
            "roles": ["Gamma"],
            "tables": ["main.test_table"],
        },
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export-rls", "rls.yaml"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    with open("rls.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == [
        {
            "clause": "client_id = 9",
            "description": "This is a rule. There are many others like it, but this one is mine.",
            "filter_type": "Regular",
            "group_key": "department",
            "name": "My rule",
            "roles": ["Gamma"],
            "tables": ["main.test_table"],
        },
    ]


def test_export_ownership(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export_ownership`` command.
    """
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")
    SupersetClient = mocker.patch("preset_cli.cli.superset.export.SupersetClient")
    client = SupersetClient()
    client.export_ownership.side_effect = [
        [],
        [
            {
                "name": "My chart",
                "uuid": UUID("e0d20af0-cef9-4bdb-80b4-745827f441bf"),
                "owners": ["adoe@example.com", "bdoe@example.com"],
            },
        ],
        [],
    ]

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "export-ownership"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    with open("ownership.yaml", encoding="utf-8") as input_:
        contents = yaml.load(input_, Loader=yaml.SafeLoader)
    assert contents == {
        "chart": [
            {
                "name": "My chart",
                "uuid": "e0d20af0-cef9-4bdb-80b4-745827f441bf",
                "owners": ["adoe@example.com", "bdoe@example.com"],
            },
        ],
    }
