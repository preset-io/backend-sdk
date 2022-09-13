"""
Tests for ``preset_cli.cli.superset.sync.dbt.databases``.
"""
# pylint: disable=invalid-name

from pathlib import Path

import pytest
import yaml
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.cli.superset.sync.dbt.databases import sync_database
from preset_cli.exceptions import DatabaseNotFoundError


def test_sync_database_new(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``sync_database`` when we want to import a new DB.
    """
    fs.create_file(
        "/path/to/.dbt/profiles.yml",
        contents=yaml.dump({"my_project": {"outputs": {"dev": {}}}}),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.databases.build_sqlalchemy_params",
        return_value={"sqlalchemy_uri": "dummy://"},
    )
    client = mocker.MagicMock()
    client.get_databases.return_value = []

    sync_database(
        client=client,
        profiles_path=Path("/path/to/.dbt/profiles.yml"),
        project_name="my_project",
        target_name="dev",
        import_db=True,
        disallow_edits=False,
        external_url_prefix="",
    )

    client.create_database.assert_called_with(
        database_name="my_project_dev",
        sqlalchemy_uri="dummy://",
        is_managed_externally=False,
    )


def test_sync_database_new_custom_sqlalchemy_uri(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``sync_database`` when we want to import a new DB.
    """
    fs.create_file(
        "/path/to/.dbt/profiles.yml",
        contents=yaml.dump(
            {
                "my_project": {
                    "outputs": {
                        "dev": {
                            "meta": {
                                "superset": {
                                    "connection_params": {
                                        "sqlalchemy_uri": "sqlite://",
                                    },
                                    "database_name": "my_database",
                                },
                            },
                        },
                    },
                },
            },
        ),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.databases.build_sqlalchemy_params",
        return_value={"sqlalchemy_uri": "dummy://"},
    )
    client = mocker.MagicMock()
    client.get_databases.return_value = []

    sync_database(
        client=client,
        profiles_path=Path("/path/to/.dbt/profiles.yml"),
        project_name="my_project",
        target_name="dev",
        import_db=True,
        disallow_edits=False,
        external_url_prefix="",
    )

    client.create_database.assert_called_with(
        database_name="my_database",
        sqlalchemy_uri="sqlite://",
        is_managed_externally=False,
    )


def test_sync_database_env_var(
    mocker: MockerFixture,
    fs: FakeFilesystem,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test ``sync_database`` when the profiles file uses ``env_var``.
    """
    monkeypatch.setenv("dsn", "sqlite://")

    fs.create_file(
        "/path/to/.dbt/profiles.yml",
        contents=yaml.dump(
            {
                "my_project": {
                    "outputs": {
                        "dev": {
                            "meta": {
                                "superset": {
                                    "connection_params": {
                                        "sqlalchemy_uri": '{{ env_var("dsn") }}',
                                    },
                                    "database_name": "my_database",
                                },
                            },
                        },
                    },
                },
            },
        ),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.databases.build_sqlalchemy_params",
        return_value={"sqlalchemy_uri": "dummy://"},
    )
    client = mocker.MagicMock()
    client.get_databases.return_value = []

    sync_database(
        client=client,
        profiles_path=Path("/path/to/.dbt/profiles.yml"),
        project_name="my_project",
        target_name="dev",
        import_db=True,
        disallow_edits=False,
        external_url_prefix="",
    )

    client.create_database.assert_called_with(
        database_name="my_database",
        sqlalchemy_uri="sqlite://",
        is_managed_externally=False,
    )


def test_sync_database_no_project(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``sync_database`` when the project is invalid.
    """
    fs.create_file(
        "/path/to/.dbt/profiles.yml",
        contents=yaml.dump({"my_project": {"outputs": {"dev": {}}}}),
    )
    client = mocker.MagicMock()
    client.get_databases.return_value = []

    with pytest.raises(Exception) as excinfo:
        sync_database(
            client=client,
            profiles_path=Path("/path/to/.dbt/profiles.yml"),
            project_name="my_other_project",
            target_name="dev",
            import_db=True,
            disallow_edits=False,
            external_url_prefix="",
        )
    assert (
        str(excinfo.value)
        == "Project my_other_project not found in /path/to/.dbt/profiles.yml"
    )


def test_sync_database_no_target(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``sync_database`` when the target is invalid.
    """
    fs.create_file(
        "/path/to/.dbt/profiles.yml",
        contents=yaml.dump({"my_project": {"outputs": {"dev": {}}}}),
    )
    client = mocker.MagicMock()
    client.get_databases.return_value = []

    with pytest.raises(Exception) as excinfo:
        sync_database(
            client=client,
            profiles_path=Path("/path/to/.dbt/profiles.yml"),
            project_name="my_project",
            target_name="prod",
            import_db=True,
            disallow_edits=False,
            external_url_prefix="",
        )
    assert (
        str(excinfo.value)
        == "Target prod not found in the outputs of /path/to/.dbt/profiles.yml"
    )


def test_sync_database_multiple_databases(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``sync_database`` when multiple databases are found.

    This should not happen, since database names are unique.
    """
    fs.create_file(
        "/path/to/.dbt/profiles.yml",
        contents=yaml.dump({"my_project": {"outputs": {"dev": {}}}}),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.databases.build_sqlalchemy_params",
        return_value={"sqlalchemy_uri": "dummy://"},
    )
    client = mocker.MagicMock()
    client.get_databases.return_value = [
        {"id": 1, "database_name": "my_project_dev", "sqlalchemy_uri": "dummy://"},
        {"id": 2, "database_name": "my_project_dev", "sqlalchemy_uri": "dummy://"},
    ]

    with pytest.raises(Exception) as excinfo:
        sync_database(
            client=client,
            profiles_path=Path("/path/to/.dbt/profiles.yml"),
            project_name="my_project",
            target_name="dev",
            import_db=True,
            disallow_edits=False,
            external_url_prefix="",
        )
    assert str(excinfo.value) == "More than one database with the same name found"


def test_sync_database_external_url_prefix(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``sync_database`` with an external URL prefix.
    """
    fs.create_file(
        "/path/to/.dbt/profiles.yml",
        contents=yaml.dump({"my_project": {"outputs": {"dev": {}}}}),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.databases.build_sqlalchemy_params",
        return_value={"sqlalchemy_uri": "dummy://"},
    )
    client = mocker.MagicMock()
    client.get_databases.return_value = []

    sync_database(
        client=client,
        profiles_path=Path("/path/to/.dbt/profiles.yml"),
        project_name="my_project",
        target_name="dev",
        import_db=True,
        disallow_edits=True,
        external_url_prefix="https://dbt.example.org/",
    )

    client.create_database.assert_called_with(
        database_name="my_project_dev",
        sqlalchemy_uri="dummy://",
        external_url="https://dbt.example.org/#!/overview",
        is_managed_externally=True,
    )


def test_sync_database_existing(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``sync_database`` when we want to import an existing DB.
    """
    fs.create_file(
        "/path/to/.dbt/profiles.yml",
        contents=yaml.dump({"my_project": {"outputs": {"dev": {}}}}),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.databases.build_sqlalchemy_params",
        return_value={"sqlalchemy_uri": "dummy://"},
    )
    client = mocker.MagicMock()
    client.get_databases.return_value = [
        {"id": 1, "database_name": "my_project_dev", "sqlalchemy_uri": "dummy://"},
    ]

    sync_database(
        client=client,
        profiles_path=Path("/path/to/.dbt/profiles.yml"),
        project_name="my_project",
        target_name="dev",
        import_db=True,
        disallow_edits=False,
        external_url_prefix="",
    )

    client.update_database.assert_called_with(
        database_id=1,
        database_name="my_project_dev",
        is_managed_externally=False,
        masked_encrypted_extra=None,
        sqlalchemy_uri="dummy://",
    )


def test_sync_database_new_no_import(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``sync_database`` when we want to import a new DB.
    """
    fs.create_file(
        "/path/to/.dbt/profiles.yml",
        contents=yaml.dump({"my_project": {"outputs": {"dev": {}}}}),
    )
    mocker.patch(
        "preset_cli.cli.superset.sync.dbt.databases.build_sqlalchemy_params",
        return_value={"sqlalchemy_uri": "dummy://"},
    )
    client = mocker.MagicMock()
    client.get_databases.return_value = []

    with pytest.raises(DatabaseNotFoundError):
        sync_database(
            client=client,
            profiles_path=Path("/path/to/.dbt/profiles.yml"),
            project_name="my_project",
            target_name="dev",
            import_db=False,
            disallow_edits=False,
            external_url_prefix="",
        )
