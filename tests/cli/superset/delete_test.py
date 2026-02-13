"""
Tests for delete assets command.
"""

# pylint: disable=redefined-outer-name, invalid-name, unused-argument, too-many-lines

import tempfile
from io import BytesIO
from typing import Dict
from zipfile import ZipFile

import yaml
from click.testing import CliRunner
from pytest_mock import MockerFixture

from preset_cli.cli.superset.delete import _apply_db_passwords_to_backup, _dataset_db_id
from preset_cli.cli.superset.main import superset_cli
from preset_cli.exceptions import SupersetError


def make_export_zip(
    chart_uuid: str = "chart-uuid",
    dataset_uuid: str = "dataset-uuid",
    database_uuid: str = "db-uuid",
) -> BytesIO:
    """
    Build a minimal export zip with chart, dataset, and database.
    """
    contents: Dict[str, str] = {
        "bundle/charts/chart.yaml": yaml.dump(
            {"uuid": chart_uuid, "dataset_uuid": dataset_uuid},
        ),
        "bundle/datasets/ds.yaml": yaml.dump(
            {"uuid": dataset_uuid, "database_uuid": database_uuid},
        ),
        "bundle/databases/db.yaml": yaml.dump(
            {"uuid": database_uuid},
        ),
    }
    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        for file_name, file_contents in contents.items():
            with bundle.open(file_name, "w") as output:
                output.write(file_contents.encode())
    buf.seek(0)
    return buf


def test_delete_assets_dry_run(mocker: MockerFixture) -> None:
    """
    Test delete assets default dry-run.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [
        {"id": 1, "dashboard_title": "Test", "slug": "test"},
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "no changes will be made" in result.output.lower()
    client.delete_resource.assert_not_called()


def test_delete_assets_no_matches(mocker: MockerFixture) -> None:
    """
    Test delete assets with no matches.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = []
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=missing",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "no dashboards match the specified filters" in result.output.lower()
    client.delete_resource.assert_not_called()


def test_delete_assets_confirm_required(mocker: MockerFixture) -> None:
    """
    Test delete assets require confirm when not dry-run.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--no-dry-run",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "confirm=delete" in result.output.lower()
    client.delete_resource.assert_not_called()


def test_delete_assets_confirm_wrong_value(mocker: MockerFixture) -> None:
    """
    Test delete assets reject wrong confirm value.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--no-dry-run",
            "--confirm",
            "WRONG",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "confirm=delete" in result.output.lower()
    client.delete_resource.assert_not_called()


def test_delete_assets_execute(mocker: MockerFixture) -> None:
    """
    Test delete assets execution.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [
        {"id": 1, "dashboard_title": "Test", "slug": "test"},
    ]
    client.export_zip.return_value = make_export_zip()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    client.delete_resource.assert_called_once_with("dashboard", 1)


def test_delete_assets_execute_with_dry_run_false(mocker: MockerFixture) -> None:
    """
    Test delete assets execution with --dry-run=false alias.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [
        {"id": 1, "dashboard_title": "Test", "slug": "test"},
    ]
    client.export_zip.return_value = make_export_zip()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--dry-run=false",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    client.delete_resource.assert_called_once_with("dashboard", 1)


def test_delete_assets_chart_by_id_dry_run(mocker: MockerFixture) -> None:
    """
    Test delete chart assets by id with dry-run default.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_resources.return_value = [
        {"id": 10, "slice_name": "Revenue Chart"},
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "chart",
            "--filter",
            "id=10",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "charts (1):" in result.output.lower()
    assert "id: 10" in result.output.lower()
    client.delete_resource.assert_not_called()


def test_delete_assets_chart_by_id_execute(mocker: MockerFixture) -> None:
    """
    Test delete chart assets by id with execution.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_resources.return_value = [
        {"id": 10, "slice_name": "Revenue Chart"},
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "chart",
            "--filter",
            "id=10",
            "--dry-run=false",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    client.delete_resource.assert_called_once_with("chart", 10)


def test_delete_assets_non_dashboard_ignores_db_password_parse(
    mocker: MockerFixture,
) -> None:
    """
    Test non-dashboard deletes ignore ``--db-password`` parsing.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_resources.return_value = [
        {"id": 10, "slice_name": "Revenue Chart"},
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "chart",
            "--filter",
            "id=10",
            "--db-password",
            "invalid-value-without-equals",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    client.delete_resource.assert_not_called()


def test_delete_assets_chart_invalid_filter_key(mocker: MockerFixture) -> None:
    """
    Test delete chart assets invalid filter key.
    """
    mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "chart",
            "--filter",
            "slug=test",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code != 0


def test_delete_assets_database_local_filter(mocker: MockerFixture) -> None:
    """
    Test database delete uses local filtering by id.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_resources.return_value = [
        {"id": 2, "database_name": "examples"},
        {"id": 3, "database_name": "other"},
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "database",
            "--filter",
            "id=2",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    client.get_resources.assert_called_once_with("database")
    client.delete_resource.assert_not_called()
    assert "databases (1):" in result.output.lower()
    assert "id: 2" in result.output.lower()


def test_delete_assets_cascade_charts(mocker: MockerFixture) -> None:
    """
    Test delete assets with cascade charts.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.return_value = make_export_zip()
    client.get_resources.side_effect = [
        [{"id": 1}],
        [{"id": 1, "uuid": "chart-uuid"}],
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-charts",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    client.delete_resource.assert_any_call("dashboard", 1)
    client.delete_resource.assert_any_call("chart", 1)


def test_delete_assets_cascade_datasets(mocker: MockerFixture) -> None:
    """
    Test delete assets with cascade datasets.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.return_value = make_export_zip()
    client.get_resources.side_effect = [
        [{"id": 1}],
        [{"id": 1, "uuid": "chart-uuid"}],
        [{"id": 2, "uuid": "dataset-uuid"}],
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-charts",
            "--cascade-datasets",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    client.delete_resource.assert_any_call("chart", 1)
    client.delete_resource.assert_any_call("dataset", 2)


def test_delete_assets_cascade_full(mocker: MockerFixture) -> None:
    """
    Test delete assets with full cascade.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.return_value = make_export_zip()

    def get_resources(resource_name: str, **kwargs: str):
        if resource_name == "chart":
            return [{"id": 1, "uuid": "chart-uuid"}]
        if resource_name == "dataset" and "database_id" not in kwargs:
            return [{"id": 2, "uuid": "dataset-uuid"}]
        if resource_name == "dataset" and "database_id" in kwargs:
            return [{"id": 2, "database_id": 3}]
        if resource_name == "database":
            return [{"id": 3, "uuid": "db-uuid"}]
        return [{"id": 1}]

    client.get_resources.side_effect = get_resources
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-charts",
            "--cascade-datasets",
            "--cascade-databases",
            "--db-password",
            "db-uuid=secret",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    client.delete_resource.assert_any_call("chart", 1)
    client.delete_resource.assert_any_call("dataset", 2)
    client.delete_resource.assert_any_call("database", 3)


def test_delete_assets_cascade_hierarchy_datasets_without_charts(
    mocker: MockerFixture,
) -> None:
    """
    Test cascade hierarchy for datasets without charts.
    """
    mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-datasets",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code != 0


def test_delete_assets_cascade_hierarchy_databases_without_datasets(
    mocker: MockerFixture,
) -> None:
    """
    Test cascade hierarchy for databases without datasets.
    """
    mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-databases",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code != 0


def test_delete_assets_filter_required(mocker: MockerFixture) -> None:
    """
    Test delete assets requires filter.
    """
    mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        ["https://superset.example.org/", "delete-assets", "--asset-type", "dashboard"],
        catch_exceptions=False,
    )
    assert result.exit_code != 0


def test_delete_assets_deletion_error_partial(mocker: MockerFixture) -> None:
    """
    Test delete assets with partial failures.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [
        {"id": 1, "dashboard_title": "One", "slug": "one"},
        {"id": 2, "dashboard_title": "Two", "slug": "two"},
    ]
    client.export_zip.return_value = make_export_zip()
    client.delete_resource.side_effect = [Exception("boom"), None]
    client.import_zip.return_value = True
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "some deletions failed" in result.output.lower()
    assert "rollback succeeded" in result.output.lower()
    client.import_zip.assert_called_once()
    assert client.delete_resource.call_count == 2


def test_delete_assets_rollback_on_failure(mocker: MockerFixture) -> None:
    """
    Test rollback is attempted when a deletion fails after a success.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [
        {"id": 1, "dashboard_title": "Test", "slug": "test"},
    ]
    client.export_zip.return_value = make_export_zip()

    def get_resources(resource_name: str, **_: str):
        if resource_name == "chart":
            return [{"id": 1, "uuid": "chart-uuid"}]
        return []

    client.get_resources.side_effect = get_resources
    client.delete_resource.side_effect = [None, Exception("boom")]
    client.import_zip.return_value = True
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-charts",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "rollback succeeded" in result.output.lower()
    client.import_zip.assert_called_once()


def test_delete_assets_rollback_not_triggered_if_no_deletes_succeeded(
    mocker: MockerFixture,
) -> None:
    """
    Test rollback is not attempted when all deletions fail.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [
        {"id": 1, "dashboard_title": "Test", "slug": "test"},
    ]
    client.export_zip.return_value = make_export_zip()

    def get_resources(resource_name: str, **_: str):
        if resource_name == "chart":
            return [{"id": 1, "uuid": "chart-uuid"}]
        return []

    client.get_resources.side_effect = get_resources
    client.delete_resource.side_effect = [Exception("boom"), Exception("boom")]
    client.import_zip.return_value = True
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-charts",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "rollback" not in result.output.lower()
    client.import_zip.assert_not_called()


def test_delete_assets_backup_created(mocker: MockerFixture) -> None:
    """
    Test pre-delete backup is always created.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [
        {"id": 1, "dashboard_title": "Test", "slug": "test"},
    ]
    client.export_zip.return_value = make_export_zip()
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "backup saved to:" in result.output.lower()
    assert "preset-cli-backup-delete-" in result.output
    assert tempfile.gettempdir() in result.output
    assert "import-assets" in result.output
    assert "--overwrite" in result.output


def test_delete_assets_requires_db_password_for_rollback(
    mocker: MockerFixture,
) -> None:
    """
    Test rollback requires db password when deleting databases.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.return_value = make_export_zip()

    def get_resources(resource_name: str, **_: str):
        if resource_name == "chart":
            return [{"id": 1, "uuid": "chart-uuid"}]
        if resource_name == "dataset":
            return [{"id": 2, "uuid": "dataset-uuid"}]
        if resource_name == "database":
            return [{"id": 3, "uuid": "db-uuid"}]
        return []

    client.get_resources.side_effect = get_resources
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-charts",
            "--cascade-datasets",
            "--cascade-databases",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code != 0
    assert "db password" in result.output.lower()
    client.delete_resource.assert_not_called()


def test_delete_assets_preflight_blocks_database_with_extra_dataset(
    mocker: MockerFixture,
) -> None:
    """
    Test preflight blocks deletion when DB has datasets outside the cascade set.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.return_value = make_export_zip()

    def get_resources(resource_name: str, **kwargs: str):
        if resource_name == "chart":
            return [{"id": 1, "uuid": "chart-uuid"}]
        if resource_name == "dataset" and "database_id" not in kwargs:
            return [{"id": 2, "uuid": "dataset-uuid"}]
        if resource_name == "dataset" and "database_id" in kwargs:
            return [
                {"id": 2, "database_id": 3},
                {"id": 999, "database_id": 3},
            ]
        if resource_name == "database":
            return [{"id": 3, "uuid": "db-uuid"}]
        return []

    client.get_resources.side_effect = get_resources
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-charts",
            "--cascade-datasets",
            "--cascade-databases",
            "--db-password",
            "db-uuid=secret",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code != 0
    assert "datasets not in cascade set" in result.output.lower()
    client.delete_resource.assert_not_called()


def test_delete_assets_preflight_fallback_blocks_known_extra_dataset(
    mocker: MockerFixture,
) -> None:
    """
    Test fallback preflight still blocks deletion when known extra datasets exist.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.return_value = make_export_zip()

    def get_resources(resource_name: str, **kwargs: str):
        if resource_name == "chart":
            return [{"id": 1, "uuid": "chart-uuid"}]
        if resource_name == "dataset" and "database_id" in kwargs:
            raise SupersetError(
                errors=[
                    {
                        "message": "Filter column: database_id not allowed to filter",
                    },
                ],
            )
        if resource_name == "dataset":
            return [
                {"id": 2, "uuid": "dataset-uuid"},
                {"id": 2, "database": {"id": 3}},
                {"id": 999, "database": {"id": 3}},
                {"id": 1000},
            ]
        if resource_name == "database":
            return [{"id": 3, "uuid": "db-uuid"}]
        return []

    client.get_resources.side_effect = get_resources
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-charts",
            "--cascade-datasets",
            "--cascade-databases",
            "--db-password",
            "db-uuid=secret",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code != 0
    assert "cannot verify all datasets" in result.output.lower()
    assert "datasets not in cascade set" in result.output.lower()
    client.delete_resource.assert_not_called()


def test_delete_assets_shared_dep_skipped(mocker: MockerFixture) -> None:
    """
    Test shared dependency skip behavior.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.side_effect = [make_export_zip(), make_export_zip()]
    client.get_resources.side_effect = [
        [{"id": 1}, {"id": 2}],
        [{"id": 1, "uuid": "chart-uuid"}],
        [{"id": 2, "uuid": "dataset-uuid"}],
        [{"id": 3, "uuid": "db-uuid"}],
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-charts",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "shared (skipped)" in result.output.lower()


def test_delete_assets_skip_shared_check(mocker: MockerFixture) -> None:
    """
    Test delete assets skip shared dependency check.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.return_value = make_export_zip()

    def get_resources(resource_name: str, **kwargs: str):
        if resource_name == "chart":
            return [{"id": 1, "uuid": "chart-uuid"}]
        if resource_name == "dataset" and "database_id" not in kwargs:
            return [{"id": 2, "uuid": "dataset-uuid"}]
        if resource_name == "dataset" and "database_id" in kwargs:
            return [{"id": 2, "database_id": 3}]
        if resource_name == "database":
            return [{"id": 3, "uuid": "db-uuid"}]
        return []

    client.get_resources.side_effect = get_resources
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-charts",
            "--cascade-datasets",
            "--cascade-databases",
            "--skip-shared-check",
            "--db-password",
            "db-uuid=secret",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "shared dependency check skipped" in result.output.lower()
    client.delete_resource.assert_any_call("chart", 1)
    client.delete_resource.assert_any_call("dataset", 2)
    client.delete_resource.assert_any_call("database", 3)


def test_delete_assets_filter_api_error(mocker: MockerFixture) -> None:
    """
    Test delete assets handles filter API error gracefully.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.side_effect = Exception("400 Bad Request")
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "dashboard_title=test",
        ],
    )
    assert result.exit_code != 0
    assert "may not be supported" in result.output


def test_delete_assets_dry_run_shows_cascade_ids(mocker: MockerFixture) -> None:
    """
    Test that dry-run summary shows individual cascade IDs.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.return_value = make_export_zip()
    client.get_resources.side_effect = [
        [{"id": 1}],
        [{"id": 1, "uuid": "chart-uuid"}],
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dashboard",
            "--filter",
            "slug=test",
            "--cascade-charts",
            "--skip-shared-check",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "[ID: 1]" in result.output


def test_apply_db_passwords_to_backup() -> None:
    """
    Test ``_apply_db_passwords_to_backup`` injects passwords into database YAML files.
    """
    db_config = {"uuid": "db-uuid-1", "database_name": "mydb"}
    contents: Dict[str, str] = {
        "bundle/databases/db.yaml": yaml.dump(db_config),
        "bundle/charts/chart.yaml": yaml.dump({"uuid": "chart-uuid"}),
    }
    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        for name, data in contents.items():
            bundle.writestr(name, data)
    backup_data = buf.getvalue()

    # No passwords → unchanged
    result_buf = _apply_db_passwords_to_backup(backup_data, {})
    with ZipFile(result_buf) as zf:
        db_yaml = yaml.load(
            zf.read("bundle/databases/db.yaml"),
            Loader=yaml.SafeLoader,
        )
    assert "password" not in db_yaml

    # With password → injected
    result_buf = _apply_db_passwords_to_backup(backup_data, {"db-uuid-1": "secret123"})
    with ZipFile(result_buf) as zf:
        db_yaml = yaml.load(
            zf.read("bundle/databases/db.yaml"),
            Loader=yaml.SafeLoader,
        )
    assert db_yaml["password"] == "secret123"

    # Non-matching UUID → not injected
    result_buf = _apply_db_passwords_to_backup(backup_data, {"other-uuid": "secret123"})
    with ZipFile(result_buf) as zf:
        db_yaml = yaml.load(
            zf.read("bundle/databases/db.yaml"),
            Loader=yaml.SafeLoader,
        )
    assert "password" not in db_yaml


def test_dataset_db_id_nested_dict() -> None:
    """
    Test ``_dataset_db_id`` with nested dict fallback for database field.
    """
    # Direct database_id
    assert _dataset_db_id({"database_id": 5}) == 5

    # Nested dict fallback
    assert _dataset_db_id({"database": {"id": 7}}) == 7

    # Neither field
    assert _dataset_db_id({}) is None

    # database field is not a dict
    assert _dataset_db_id({"database": "not-a-dict"}) is None
