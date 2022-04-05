"""
Tests for the export command.
"""
# pylint: disable=redefined-outer-name, invalid-name

from io import BytesIO
from pathlib import Path
from unittest import mock
from zipfile import ZipFile

import pytest
from click.testing import CliRunner
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.auth.main import Auth
from preset_cli.cli.superset.export import export_resource
from preset_cli.cli.superset.main import superset


@pytest.fixture
def database_export() -> BytesIO:
    """
    Fixture for the contents of a simple database export.
    """
    contents = {
        "dashboard_export/metadata.yaml": "Metadata",
        "dashboard_export/databases/gsheets.yaml": "GSheets",
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
    database_export: BytesIO,
) -> None:
    """
    Test ``export_resource``.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)

    client = mocker.MagicMock()
    client.export_zip.return_value = database_export

    export_resource(resource="database", root=root, client=client, overwrite=False)

    # check that the database was written to the directory
    with open(root / "databases/gsheets.yaml", encoding="utf-8") as input_:
        assert input_.read() == "GSheets"

    # metadata file should be ignored
    assert not (root / "metadata.yaml").exists()


def test_export_resource_overwrite(
    mocker: MockerFixture,
    fs: FakeFilesystem,
    database_export: BytesIO,
) -> None:
    """
    Test that we need to confirm overwrites.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)

    client = mocker.MagicMock()
    client.export_zip.return_value = database_export

    export_resource(resource="database", root=root, client=client, overwrite=False)
    with pytest.raises(Exception) as excinfo:
        export_resource(resource="database", root=root, client=client, overwrite=False)
    assert str(excinfo.value) == (
        "File already exists and --overwrite was not specified: "
        "/path/to/root/databases/gsheets.yaml"
    )

    export_resource(resource="database", root=root, client=client, overwrite=True)


def test_export(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``export`` command.
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
        superset,
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
        superset,
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
