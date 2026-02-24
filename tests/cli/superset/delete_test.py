"""
Tests for delete assets command.
"""

# pylint: disable=redefined-outer-name, invalid-name, unused-argument, too-many-lines

import tempfile
from io import BytesIO
from pathlib import Path
from typing import Dict
from zipfile import ZipFile

import click
import pytest
import yaml
from click.testing import CliRunner
from pytest_mock import MockerFixture

from preset_cli.cli.superset.dependency_utils import (
    build_uuid_map,
    compute_shared_uuids,
    extract_backup_uuids_by_type,
    extract_dependency_maps,
    resolve_ids,
)
from preset_cli.cli.superset.asset_utils import (
    RESOURCE_CHART,
    RESOURCE_CHARTS,
    RESOURCE_DATABASES,
    RESOURCE_DATASET,
    RESOURCE_DATASETS,
)
from preset_cli.cli.superset.delete_cascade import (
    _dataset_db_id,
    _fetch_preflight_datasets,
    _filter_datasets_for_database_ids,
    _resolve_chart_targets,
    _warn_missing_uuids,
)
from preset_cli.cli.superset.delete import (
    _delete_non_dashboard_assets,
    _execute_dashboard_delete_plan,
    _parse_delete_command_options,
    _resolve_rollback_settings,
    _validate_delete_option_combinations,
)
from preset_cli.cli.superset.delete_display import (
    _echo_cascade_section,
    _echo_dry_run_hint,
    _echo_shared_summary,
    _format_resource_summary,
    _format_restore_command,
)
from preset_cli.cli.superset.delete_rollback import (
    _apply_db_passwords_to_backup,
    _expected_dashboard_rollback_types,
    _parse_db_passwords,
    _rollback_dashboard_deletion,
    _rollback_non_dashboard_deletion,
    _verify_rollback_restoration,
    _write_backup,
)
from preset_cli.cli.superset.delete_types import (
    CascadeDependencies,
    DashboardCascadeOptions,
    _CascadeResolution,
    _DashboardDeletePlan,
    _DashboardExecutionOptions,
    _DashboardSelection,
    _DeleteSummaryData,
    _NonDashboardDeleteOptions,
)
from preset_cli.cli.superset.main import superset_cli
from preset_cli.exceptions import SupersetError


def make_export_zip(
    chart_uuid: str = "chart-uuid",
    dataset_uuid: str = "dataset-uuid",
    database_uuid: str = "db-uuid",
    dashboard_title: str = "Test Dashboard",
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
        "bundle/dashboards/dashboard.yaml": yaml.dump(
            {
                "dashboard_title": dashboard_title,
                "position": {
                    "DASHBOARD_VERSION_KEY": "v2",
                    "CHART-1": {
                        "type": "CHART",
                        "meta": {
                            "uuid": chart_uuid,
                        },
                    },
                },
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


def make_dashboard_delete_plan() -> _DashboardDeletePlan:
    """Build a minimal dashboard delete plan for execution flow tests."""
    dashboard = {"id": 1, "dashboard_title": "Test", "slug": "test"}
    return _DashboardDeletePlan(
        selection=_DashboardSelection(
            dashboards=[dashboard],
            dashboard_ids={1},
        ),
        resolution=_CascadeResolution(
            ids={
                RESOURCE_CHARTS: {11},
                RESOURCE_DATASETS: {22},
                RESOURCE_DATABASES: {33},
            },
            names={
                RESOURCE_CHARTS: {},
                RESOURCE_DATASETS: {},
                RESOURCE_DATABASES: {},
            },
            chart_dashboard_context={},
            flags={
                RESOURCE_CHARTS: True,
                RESOURCE_DATASETS: True,
                RESOURCE_DATABASES: True,
            },
        ),
        summary=_DeleteSummaryData(
            dashboards=[dashboard],
            cascade_ids={
                RESOURCE_CHARTS: {11},
                RESOURCE_DATASETS: {22},
                RESOURCE_DATABASES: {33},
            },
            cascade_names={
                RESOURCE_CHARTS: {},
                RESOURCE_DATASETS: {},
                RESOURCE_DATABASES: {},
            },
            chart_dashboard_context={},
            shared={
                RESOURCE_CHARTS: set(),
                RESOURCE_DATASETS: set(),
                RESOURCE_DATABASES: set(),
            },
            cascade_flags={
                RESOURCE_CHARTS: True,
                RESOURCE_DATASETS: True,
                RESOURCE_DATABASES: True,
            },
        ),
    )


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
    assert result.output.count("(not cascading)") == 3
    assert "--dry-run=false" in result.output
    assert "--confirm=DELETE" in result.output


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
    client.export_zip.return_value = make_export_zip()
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


def test_delete_assets_database_execute_without_db_password_disables_rollback(
    mocker: MockerFixture,
) -> None:
    """
    Test database execute path disables rollback when no db password is provided.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_resources.return_value = [
        {"id": 2, "database_name": "examples"},
    ]
    client.export_zip.return_value = make_export_zip()
    client.delete_resource.side_effect = [Exception("boom")]
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
            "--dry-run=false",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code != 0
    assert "proceeding without rollback" in result.output.lower()
    client.import_zip.assert_not_called()


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
            "certified_by=",
            "--no-dry-run",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code != 0
    assert "some deletions failed" in result.output.lower()
    assert "best-effort rollback attempted" in result.output.lower()
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
    assert result.exit_code != 0
    assert "best-effort rollback attempted" in result.output.lower()
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
    assert result.exit_code != 0
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


def test_delete_assets_dry_run_shows_cascade_names(mocker: MockerFixture) -> None:
    """
    Test that dry-run summary shows resource names for cascade targets.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.return_value = make_export_zip()
    client.get_resources.return_value = [
        {"id": 5, "uuid": "chart-uuid", "slice_name": "Revenue Chart"},
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
    assert "[ID: 5] Revenue Chart" in result.output


def test_delete_assets_dry_run_chart_context_single_dashboard(
    mocker: MockerFixture,
) -> None:
    """
    Test chart lines include single dashboard context in dry-run summary.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.return_value = make_export_zip(
        dashboard_title="Quarterly Sales",
    )
    client.get_resources.return_value = [
        {"id": 5, "uuid": "chart-uuid", "slice_name": "Revenue Chart"},
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
    assert "[ID: 5] Revenue Chart (dashboard: Quarterly Sales)" in result.output


def test_delete_assets_dry_run_chart_context_multiple_dashboards(
    mocker: MockerFixture,
) -> None:
    """
    Test chart lines include multi-dashboard context in dry-run summary.
    """
    contents: Dict[str, str] = {
        "bundle/charts/chart.yaml": yaml.dump(
            {"uuid": "chart-uuid", "dataset_uuid": "dataset-uuid"},
        ),
        "bundle/datasets/ds.yaml": yaml.dump(
            {"uuid": "dataset-uuid", "database_uuid": "db-uuid"},
        ),
        "bundle/databases/db.yaml": yaml.dump({"uuid": "db-uuid"}),
        "bundle/dashboards/dash_a.yaml": yaml.dump(
            {
                "dashboard_title": "Alpha",
                "position": {
                    "CHART-A": {
                        "type": "CHART",
                        "meta": {"uuid": "chart-uuid"},
                    },
                },
            },
        ),
        "bundle/dashboards/dash_b.yaml": yaml.dump(
            {
                "dashboard_title": "Beta",
                "position": {
                    "CHART-B": {
                        "type": "CHART",
                        "meta": {"uuid": "chart-uuid"},
                    },
                },
            },
        ),
    }
    export_buf = BytesIO()
    with ZipFile(export_buf, "w") as bundle:
        for file_name, file_contents in contents.items():
            with bundle.open(file_name, "w") as output:
                output.write(file_contents.encode())
    export_buf.seek(0)

    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [{"id": 1}]
    client.export_zip.return_value = export_buf
    client.get_resources.return_value = [
        {"id": 5, "uuid": "chart-uuid", "slice_name": "Revenue Chart"},
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
    assert "[ID: 5] Revenue Chart (dashboards: Alpha, Beta)" in result.output


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


def test_delete_assets_dataset_by_id_dry_run(mocker: MockerFixture) -> None:
    """
    Test delete dataset assets by id with dry-run default.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_resources.return_value = [
        {"id": 20, "table_name": "Sales Dataset"},
    ]
    mocker.patch("preset_cli.cli.superset.main.UsernamePasswordAuth")

    runner = CliRunner()
    result = runner.invoke(
        superset_cli,
        [
            "https://superset.example.org/",
            "delete-assets",
            "--asset-type",
            "dataset",
            "--filter",
            "id=20",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "datasets (1):" in result.output.lower()
    assert "id: 20" in result.output.lower()
    client.delete_resource.assert_not_called()


def test_delete_assets_dataset_by_id_execute(mocker: MockerFixture) -> None:
    """
    Test delete dataset assets by id with execution.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_resources.return_value = [
        {"id": 20, "table_name": "Sales Dataset"},
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
            "dataset",
            "--filter",
            "id=20",
            "--dry-run=false",
            "--confirm",
            "DELETE",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    client.delete_resource.assert_called_once_with("dataset", 20)


def test_delete_assets_chart_execute_partial_failure(mocker: MockerFixture) -> None:
    """
    Test non-dashboard primary asset deletion with partial failure.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_resources.return_value = [
        {"id": 10, "slice_name": "Chart A"},
        {"id": 11, "slice_name": "Chart B"},
    ]
    client.export_zip.return_value = make_export_zip()
    client.delete_resource.side_effect = [Exception("boom"), None]
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
    assert result.exit_code != 0
    assert "some deletions failed" in result.output.lower()
    client.delete_resource.assert_called_once_with("chart", 10)


def test_format_restore_command_quotes_path() -> None:
    """
    Test restore command quoting for paths with spaces.
    """
    command = _format_restore_command("/tmp/my backup.zip", "dashboard")
    assert command == (
        "preset-cli superset import-assets '/tmp/my backup.zip' "
        "--overwrite --asset-type dashboard"
    )


def test_delete_assets_multiple_filters(mocker: MockerFixture) -> None:
    """
    Test delete assets with multiple ``--filter`` flags (ANDed).
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [
        {"id": 1, "dashboard_title": "Test", "slug": "test", "certified_by": "Alice"},
        {"id": 2, "dashboard_title": "Other", "slug": "other", "certified_by": "Bob"},
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
            "--filter",
            "certified_by=Alice",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "dashboards (1):" in result.output.lower()
    assert "[ID: 1]" in result.output
    assert "[ID: 2]" not in result.output
    client.delete_resource.assert_not_called()


def test_delete_assets_prd_example_local_filters(mocker: MockerFixture) -> None:
    """
    Test delete with LOCAL_FILTER_KEYS: managed_externally and certified_by.
    """
    SupersetClient = mocker.patch("preset_cli.cli.superset.delete.SupersetClient")
    client = SupersetClient()
    client.get_dashboards.return_value = [
        {
            "id": 1,
            "dashboard_title": "Managed",
            "slug": "managed",
            "is_managed_externally": False,
            "certified_by": "",
        },
        {
            "id": 2,
            "dashboard_title": "External",
            "slug": "ext",
            "is_managed_externally": True,
            "certified_by": "",
        },
        {
            "id": 3,
            "dashboard_title": "Certified",
            "slug": "cert",
            "is_managed_externally": False,
            "certified_by": "Alice",
        },
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
            "managed_externally=false",
            "--filter",
            "certified_by=",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "dashboards (1):" in result.output.lower()
    assert "[ID: 1]" in result.output
    assert "[ID: 2]" not in result.output
    assert "[ID: 3]" not in result.output
    client.delete_resource.assert_not_called()


def test_extract_backup_uuids_by_type_skips_unrelated_entries() -> None:
    """
    Test backup UUID extraction ignores non-yaml and unrelated paths.
    """
    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        bundle.writestr("bundle/charts/chart.yaml", yaml.dump({"uuid": "chart-uuid"}))
        bundle.writestr("bundle/datasets/ds.yaml", yaml.dump({"uuid": "dataset-uuid"}))
        bundle.writestr("bundle/databases/db.yaml", yaml.dump({"uuid": "db-uuid"}))
        bundle.writestr("bundle/dashboards/dash.yaml", yaml.dump({"uuid": "dash-uuid"}))
        bundle.writestr("bundle/notes.txt", "ignore")
        bundle.writestr("bundle/unknown/asset.yaml", yaml.dump({"uuid": "ignored"}))

    result = extract_backup_uuids_by_type(buf.getvalue())
    assert result == {
        "dashboard": {"dash-uuid"},
        "chart": {"chart-uuid"},
        "dataset": {"dataset-uuid"},
        "database": {"db-uuid"},
    }


def test_verify_rollback_restoration_collects_unresolved_and_missing(
    mocker: MockerFixture,
) -> None:
    """
    Test rollback verification tracks unresolved and missing types.
    """
    mocker.patch(
        "preset_cli.cli.superset.dependency_utils.extract_backup_uuids_by_type",
        return_value={
            "dashboard": set(),
            "chart": {"chart-uuid"},
            "dataset": {"dataset-uuid"},
            "database": set(),
        },
    )
    mocker.patch(
        "preset_cli.cli.superset.dependency_utils.resolve_ids",
        side_effect=[
            (set(), {}, [], False),
            ({1}, {1: "dataset"}, ["dataset-uuid"], True),
        ],
    )

    unresolved, missing = _verify_rollback_restoration(
        mocker.MagicMock(),
        b"zip-data",
        {"chart", "dataset"},
    )
    assert unresolved == ["chart"]
    assert missing == ["dataset"]


def test_extract_dependency_maps_handles_invalid_dashboard_nodes() -> None:
    """
    Test dependency extraction tolerates malformed dashboard position data.
    """
    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        bundle.writestr("bundle/README.txt", "ignore")
        bundle.writestr(
            "bundle/dashboards/bad.yaml",
            yaml.dump({"position": "not-a-dict"}),
        )
        bundle.writestr(
            "bundle/dashboards/mixed.yaml",
            yaml.dump(
                {
                    "dashboard_title": "Mixed",
                    "position": {
                        "a": "not-dict",
                        "b": {"type": "ROW"},
                        "c": {"type": "CHART", "meta": "not-dict"},
                        "d": {"type": "CHART", "meta": {"uuid": "chart-uuid"}},
                    },
                },
            ),
        )

    _, _, _, _, _, chart_context = extract_dependency_maps(buf)
    assert chart_context == {"chart-uuid": {"Mixed"}}


def test_extract_dependency_maps_full_dependency_graph() -> None:
    """
    Test dependency extraction returns all UUID sets and relationship maps.
    """
    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        bundle.writestr(
            "bundle/charts/chart_a.yaml",
            yaml.dump({"uuid": "chart-a", "dataset_uuid": "dataset-a"}),
        )
        bundle.writestr(
            "bundle/charts/chart_b.yaml",
            yaml.dump({"uuid": "chart-b", "dataset_uuid": "dataset-b"}),
        )
        bundle.writestr(
            "bundle/datasets/dataset_a.yaml",
            yaml.dump({"uuid": "dataset-a", "database_uuid": "db-a"}),
        )
        bundle.writestr(
            "bundle/datasets/dataset_b.yaml",
            yaml.dump({"uuid": "dataset-b", "database_uuid": "db-b"}),
        )
        bundle.writestr("bundle/databases/db_a.yaml", yaml.dump({"uuid": "db-a"}))
        bundle.writestr("bundle/databases/db_b.yaml", yaml.dump({"uuid": "db-b"}))
        bundle.writestr(
            "bundle/dashboards/dash_one.yaml",
            yaml.dump(
                {
                    "dashboard_title": "Sales",
                    "position": {
                        "CHART-1": {"type": "CHART", "meta": {"uuid": "chart-a"}},
                        "CHART-2": {"type": "CHART", "meta": {"uuid": "chart-b"}},
                    },
                },
            ),
        )
        bundle.writestr(
            "bundle/dashboards/dash_two.yaml",
            yaml.dump(
                {
                    "title": "Marketing",
                    "position": {
                        "CHART-1": {"type": "CHART", "meta": {"uuid": "chart-a"}},
                    },
                },
            ),
        )

    (
        chart_uuids,
        dataset_uuids,
        database_uuids,
        chart_dataset_map,
        dataset_database_map,
        chart_context,
    ) = extract_dependency_maps(buf)

    assert chart_uuids == {"chart-a", "chart-b"}
    assert dataset_uuids == {"dataset-a", "dataset-b"}
    assert database_uuids == {"db-a", "db-b"}
    assert chart_dataset_map == {
        "chart-a": "dataset-a",
        "chart-b": "dataset-b",
    }
    assert dataset_database_map == {
        "dataset-a": "db-a",
        "dataset-b": "db-b",
    }
    assert chart_context == {
        "chart-a": {"Marketing", "Sales"},
        "chart-b": {"Sales"},
    }


def test_parse_delete_command_options_normalizes_dry_run_false() -> None:
    """
    Test delete command parser normalizes string dry_run option to bool.
    """
    options = _parse_delete_command_options(
        {
            "asset_type": "dashboard",
            "filters": ("slug=test",),
            "cascade_charts": True,
            "cascade_datasets": False,
            "cascade_databases": False,
            "dry_run": "false",
            "skip_shared_check": True,
            "confirm": "DELETE",
            "rollback": True,
            "db_password": ("db-uuid=secret",),
        },
    )

    assert options.execution_options.dry_run is False


def test_parse_delete_command_options_defaults_dry_run_true() -> None:
    """
    Test delete command parser maps None dry_run option to True.
    """
    options = _parse_delete_command_options(
        {
            "asset_type": "chart",
            "filters": ("id=1",),
            "cascade_charts": False,
            "cascade_datasets": False,
            "cascade_databases": False,
            "dry_run": None,
            "skip_shared_check": False,
            "confirm": None,
            "rollback": True,
            "db_password": (),
        },
    )

    assert options.execution_options.dry_run is True


def test_parse_delete_command_options_invalid_dry_run_raises() -> None:
    """
    Test delete command parser rejects invalid dry_run values.
    """
    with pytest.raises(click.BadParameter, match="Invalid value for dry_run"):
        _parse_delete_command_options(
            {
                "asset_type": "chart",
                "filters": ("id=1",),
                "cascade_charts": False,
                "cascade_datasets": False,
                "cascade_databases": False,
                "dry_run": "invalid",
                "skip_shared_check": False,
                "confirm": None,
                "rollback": True,
                "db_password": (),
            },
        )


def test_parse_db_passwords_invalid_value_raises() -> None:
    """
    Test invalid db-password format raises a click exception.
    """
    with pytest.raises(Exception, match="Invalid --db-password value"):
        _parse_db_passwords(("invalid",))


def test_write_backup_ignores_chmod_error(mocker: MockerFixture) -> None:
    """
    Test backup creation still succeeds when chmod fails.
    """
    mocker.patch("pathlib.Path.chmod", side_effect=OSError("denied"))
    backup_path = _write_backup(b"data")
    path = Path(backup_path)
    assert path.exists()
    path.unlink()


def test_build_uuid_map_fallback_and_missing_id_entries(mocker: MockerFixture) -> None:
    """
    Test UUID map fallback path uses ``get_uuids`` and skips resources without IDs.
    """
    client = mocker.MagicMock()
    client.get_resources.return_value = [
        {"uuid": "uuid-without-id"},
        {"id": 2, "name": "resource-name"},
    ]
    client.get_uuids.return_value = {2: "fallback-uuid"}

    uuid_map, name_map, resolved = build_uuid_map(client, "chart")
    assert resolved is True
    assert uuid_map == {"fallback-uuid": 2}
    assert name_map == {2: "resource-name"}


def test_resolve_ids_unresolved_map(mocker: MockerFixture) -> None:
    """
    Test ``resolve_ids`` reports unresolved state when UUID mapping fails.
    """
    mocker.patch(
        "preset_cli.cli.superset.dependency_utils.build_uuid_map",
        return_value=({}, {}, False),
    )
    ids, names, missing, resolved = resolve_ids(
        mocker.MagicMock(),
        "chart",
        {"a"},
    )
    assert ids == set()
    assert names == {}
    assert missing == ["a"]
    assert resolved is False


def test_format_resource_summary_without_label() -> None:
    """
    Test summary formatting falls back to ID-only when no label is available.
    """
    assert _format_resource_summary("chart", [{"id": 1}]) == ["- [ID: 1]"]


def test_echo_shared_summary_and_dry_run_hint(capsys) -> None:
    """
    Test shared summary sections and dry-run hint are rendered.
    """
    _echo_shared_summary(
        {
            "charts": {"c1"},
            "datasets": {"d1"},
            "databases": {"db1"},
        },
    )
    _echo_dry_run_hint(True)
    output = capsys.readouterr().out
    assert "Shared (skipped)" in output
    assert "Charts (1): c1" in output
    assert "Datasets (1): d1" in output
    assert "Databases (1): db1" in output
    assert "--dry-run=false --confirm=DELETE" in output


def test_validate_delete_option_combinations_rejects_non_dashboard_cascade() -> None:
    """
    Test non-dashboard deletes reject cascade options.
    """
    cascade = DashboardCascadeOptions(
        charts=True,
        datasets=False,
        databases=False,
        skip_shared_check=False,
    )
    with pytest.raises(Exception, match="Cascade options are only supported"):
        _validate_delete_option_combinations("chart", cascade)


def test_resolve_rollback_settings_database_path_with_no_rollback() -> None:
    """
    Test rollback settings keep parsed passwords when rollback is disabled.
    """
    db_passwords, rollback = _resolve_rollback_settings("database", False, ("u=pw",))
    assert db_passwords == {"u": "pw"}
    assert rollback is False


def test_rollback_non_dashboard_deletion_import_failure(mocker: MockerFixture) -> None:
    """
    Test non-dashboard rollback raises when import fails.
    """
    client = mocker.MagicMock()
    client.import_zip.side_effect = Exception("boom")

    with pytest.raises(Exception, match="Rollback failed"):
        _rollback_non_dashboard_deletion(client, "chart", b"zip", {})


def test_rollback_non_dashboard_deletion_warns_unresolved(
    mocker: MockerFixture,
    capsys,
) -> None:
    """
    Test non-dashboard rollback warns for unresolved verification types.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.delete_rollback._verify_rollback_restoration",
        return_value=(["chart"], []),
    )
    _rollback_non_dashboard_deletion(client, "chart", b"zip", {})
    output = capsys.readouterr().out
    assert "unable to verify rollback for: chart" in output
    assert "Rollback succeeded." in output


def test_rollback_non_dashboard_deletion_raises_on_missing_types(
    mocker: MockerFixture,
) -> None:
    """
    Test non-dashboard rollback raises when restored resources are missing.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.delete_rollback._verify_rollback_restoration",
        return_value=([], ["chart"]),
    )
    with pytest.raises(Exception, match="Rollback verification failed for: chart"):
        _rollback_non_dashboard_deletion(client, "chart", b"zip", {})


def test_delete_non_dashboard_assets_attempts_rollback_on_partial_failure(
    mocker: MockerFixture,
) -> None:
    """
    Test non-dashboard helper attempts rollback after partial deletion failures.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.delete._fetch_non_dashboard_resources",
        return_value=[{"id": 10}, {"id": 11}],
    )
    client.export_zip.return_value = make_export_zip()
    client.delete_resource.side_effect = [None, Exception("boom")]
    rollback = mocker.patch(
        "preset_cli.cli.superset.delete._rollback_non_dashboard_deletion",
    )
    options = _NonDashboardDeleteOptions(
        resource_name="chart",
        dry_run=False,
        confirm="DELETE",
        rollback=True,
        db_passwords={},
    )

    with pytest.raises(Exception, match="Deletion completed with failures"):
        _delete_non_dashboard_assets(client, {}, options)

    rollback.assert_called_once()


def test_delete_non_dashboard_assets_no_matches_returns_early(
    mocker: MockerFixture,
) -> None:
    """
    Test non-dashboard helper returns early when no resources match.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.delete._fetch_non_dashboard_resources",
        return_value=[],
    )
    options = _NonDashboardDeleteOptions(
        resource_name="chart",
        dry_run=False,
        confirm="DELETE",
        rollback=True,
        db_passwords={},
    )
    _delete_non_dashboard_assets(client, {}, options)
    client.delete_resource.assert_not_called()


def test_delete_non_dashboard_assets_requires_confirm_for_execute(
    mocker: MockerFixture,
) -> None:
    """
    Test non-dashboard helper aborts execute path without confirm token.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.delete._fetch_non_dashboard_resources",
        return_value=[{"id": 10, "slice_name": "Chart"}],
    )
    options = _NonDashboardDeleteOptions(
        resource_name="chart",
        dry_run=False,
        confirm=None,
        rollback=True,
        db_passwords={},
    )
    _delete_non_dashboard_assets(client, {}, options)
    client.delete_resource.assert_not_called()


def test_compute_shared_uuids_propagates_dataset_and_database() -> None:
    """
    Test shared chart dependencies propagate dataset/database protection.
    """
    dependencies = CascadeDependencies(
        chart_uuids={"chart-1"},
        dataset_uuids={"dataset-1"},
        database_uuids={"db-1"},
        chart_dataset_map={"chart-1": "dataset-2"},
        dataset_database_map={"dataset-1": "db-1", "dataset-2": "db-2"},
        chart_dashboard_titles_by_uuid={},
    )
    shared = compute_shared_uuids(
        dependencies,
        {"charts": {"chart-1"}, "datasets": set(), "databases": set()},
    )
    assert shared["charts"] == {"chart-1"}
    assert shared["datasets"] == {"dataset-2"}
    assert shared["databases"] == {"db-2"}


def test_resolve_chart_targets_skips_unknown_and_non_target_context(
    mocker: MockerFixture,
) -> None:
    """
    Test chart context mapping ignores unknown or non-target chart UUIDs.
    """
    dependencies = CascadeDependencies(
        chart_uuids={"chart-a"},
        dataset_uuids=set(),
        database_uuids=set(),
        chart_dataset_map={},
        dataset_database_map={},
        chart_dashboard_titles_by_uuid={
            "missing-chart": {"Missing"},
            "chart-b": {"NotTarget"},
            "chart-a": {"Target"},
        },
    )
    mocker.patch(
        "preset_cli.cli.superset.dependency_utils.build_uuid_map",
        return_value=(
            {"chart-a": 1, "chart-b": 2},
            {1: "A", 2: "B"},
            True,
        ),
    )

    chart_ids, _, context, missing, resolved = _resolve_chart_targets(
        mocker.MagicMock(),
        dependencies,
    )
    assert resolved is True
    assert chart_ids == {1}
    assert context == {1: ["Target"]}
    assert missing == []


def test_warn_missing_uuids_emits_messages(capsys) -> None:
    """
    Test missing UUID warnings are printed.
    """
    _warn_missing_uuids("dataset", ["a", "b"])
    output = capsys.readouterr().out
    assert "Warning: dataset UUID not found: a" in output
    assert "Warning: dataset UUID not found: b" in output


def test_fetch_preflight_datasets_raises_on_unexpected_error(
    mocker: MockerFixture,
) -> None:
    """
    Test preflight dataset fetch wraps unexpected errors.
    """
    client = mocker.MagicMock()
    client.get_resources.side_effect = RuntimeError("boom")
    mocker.patch(
        "preset_cli.cli.superset.delete_cascade.is_filter_not_allowed_error",
        return_value=False,
    )
    with pytest.raises(Exception, match="Failed to preflight datasets"):
        _fetch_preflight_datasets(client, {1})


def test_expected_dashboard_rollback_types_full_cascade() -> None:
    """
    Test rollback type set includes all cascade levels when enabled.
    """
    cascade = DashboardCascadeOptions(
        charts=True,
        datasets=True,
        databases=True,
        skip_shared_check=False,
    )
    assert _expected_dashboard_rollback_types(cascade) == {
        "dashboard",
        "chart",
        "dataset",
        "database",
    }


def test_rollback_dashboard_deletion_import_failure(mocker: MockerFixture) -> None:
    """
    Test dashboard rollback raises when re-import fails.
    """
    client = mocker.MagicMock()
    client.import_zip.side_effect = Exception("boom")
    cascade = DashboardCascadeOptions(
        charts=True,
        datasets=False,
        databases=False,
        skip_shared_check=False,
    )
    with pytest.raises(Exception, match="Rollback failed"):
        _rollback_dashboard_deletion(client, b"zip", {}, cascade)


def test_rollback_dashboard_deletion_warns_unresolved(
    mocker: MockerFixture,
    capsys,
) -> None:
    """
    Test dashboard rollback warns unresolved verification types and succeeds.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.delete_rollback._verify_rollback_restoration",
        return_value=(["dataset"], []),
    )
    cascade = DashboardCascadeOptions(
        charts=True,
        datasets=True,
        databases=True,
        skip_shared_check=False,
    )
    _rollback_dashboard_deletion(client, b"zip", {}, cascade)
    output = capsys.readouterr().out
    assert "unable to verify rollback for: dataset" in output
    assert "Rollback succeeded." in output


def test_rollback_dashboard_deletion_raises_on_missing_types(
    mocker: MockerFixture,
) -> None:
    """
    Test dashboard rollback raises when verification reports missing resources.
    """
    client = mocker.MagicMock()
    mocker.patch(
        "preset_cli.cli.superset.delete_rollback._verify_rollback_restoration",
        return_value=([], ["database"]),
    )
    cascade = DashboardCascadeOptions(
        charts=True,
        datasets=True,
        databases=True,
        skip_shared_check=False,
    )
    with pytest.raises(Exception, match="Rollback verification failed for: database"):
        _rollback_dashboard_deletion(client, b"zip", {}, cascade)


def test_execute_dashboard_delete_plan_stops_after_failing_stage(
    mocker: MockerFixture,
) -> None:
    """
    Test dashboard execution stops after first stage that records failures.
    """
    client = mocker.MagicMock()
    plan = make_dashboard_delete_plan()
    cascade_options = DashboardCascadeOptions(
        charts=True,
        datasets=True,
        databases=True,
        skip_shared_check=False,
    )
    execution_options = _DashboardExecutionOptions(
        dry_run=False,
        confirm="DELETE",
        rollback=False,
    )

    mocker.patch("preset_cli.cli.superset.delete._echo_summary")
    mocker.patch(
        "preset_cli.cli.superset.delete._read_dashboard_backup_data",
        return_value=b"zip",
    )
    mocker.patch(
        "preset_cli.cli.superset.delete._write_backup",
        return_value="/tmp/backup.zip",
    )
    mocker.patch("preset_cli.cli.superset.delete._echo_backup_restore_details")
    rollback = mocker.patch("preset_cli.cli.superset.delete._rollback_dashboard_deletion")

    stage_calls = []

    def delete_resources_side_effect(
        _client,
        resource_name: str,
        _resource_ids,
        failures,
    ) -> bool:
        stage_calls.append(resource_name)
        if resource_name == RESOURCE_CHART:
            failures.append("chart:11 (403)")
        return False

    mocker.patch(
        "preset_cli.cli.superset.delete._delete_resources",
        side_effect=delete_resources_side_effect,
    )

    with pytest.raises(click.ClickException, match="Deletion completed with failures"):
        _execute_dashboard_delete_plan(
            client,
            plan,
            cascade_options,
            execution_options,
            {},
        )

    assert stage_calls == [RESOURCE_CHART]
    rollback.assert_not_called()


def test_execute_dashboard_delete_plan_rolls_back_after_partial_stage_failure(
    mocker: MockerFixture,
) -> None:
    """
    Test dashboard execution rolls back when a failing stage already deleted assets.
    """
    client = mocker.MagicMock()
    plan = make_dashboard_delete_plan()
    cascade_options = DashboardCascadeOptions(
        charts=True,
        datasets=True,
        databases=True,
        skip_shared_check=False,
    )
    execution_options = _DashboardExecutionOptions(
        dry_run=False,
        confirm="DELETE",
        rollback=True,
    )

    mocker.patch("preset_cli.cli.superset.delete._echo_summary")
    mocker.patch(
        "preset_cli.cli.superset.delete._read_dashboard_backup_data",
        return_value=b"zip",
    )
    mocker.patch(
        "preset_cli.cli.superset.delete._write_backup",
        return_value="/tmp/backup.zip",
    )
    mocker.patch("preset_cli.cli.superset.delete._echo_backup_restore_details")
    rollback = mocker.patch("preset_cli.cli.superset.delete._rollback_dashboard_deletion")

    stage_calls = []

    def delete_resources_side_effect(
        _client,
        resource_name: str,
        _resource_ids,
        failures,
    ) -> bool:
        stage_calls.append(resource_name)
        if resource_name == RESOURCE_CHART:
            failures.append("chart:11 (403)")
            return True
        return False

    mocker.patch(
        "preset_cli.cli.superset.delete._delete_resources",
        side_effect=delete_resources_side_effect,
    )

    with pytest.raises(click.ClickException, match="Deletion completed with failures"):
        _execute_dashboard_delete_plan(
            client,
            plan,
            cascade_options,
            execution_options,
            {},
        )

    assert stage_calls == [RESOURCE_CHART]
    rollback.assert_called_once_with(client, b"zip", {}, cascade_options)


def test_execute_dashboard_delete_plan_stops_after_dataset_stage_failure(
    mocker: MockerFixture,
) -> None:
    """
    Test dashboard execution stops before DB/dashboard stages after dataset failure.
    """
    client = mocker.MagicMock()
    plan = make_dashboard_delete_plan()
    cascade_options = DashboardCascadeOptions(
        charts=True,
        datasets=True,
        databases=True,
        skip_shared_check=False,
    )
    execution_options = _DashboardExecutionOptions(
        dry_run=False,
        confirm="DELETE",
        rollback=False,
    )

    mocker.patch("preset_cli.cli.superset.delete._echo_summary")
    mocker.patch(
        "preset_cli.cli.superset.delete._read_dashboard_backup_data",
        return_value=b"zip",
    )
    mocker.patch(
        "preset_cli.cli.superset.delete._write_backup",
        return_value="/tmp/backup.zip",
    )
    mocker.patch("preset_cli.cli.superset.delete._echo_backup_restore_details")
    rollback = mocker.patch("preset_cli.cli.superset.delete._rollback_dashboard_deletion")

    stage_calls = []

    def delete_resources_side_effect(
        _client,
        resource_name: str,
        _resource_ids,
        failures,
    ) -> bool:
        stage_calls.append(resource_name)
        if resource_name == RESOURCE_CHART:
            return True
        if resource_name == RESOURCE_DATASET:
            failures.append("dataset:22 (403)")
        return False

    mocker.patch(
        "preset_cli.cli.superset.delete._delete_resources",
        side_effect=delete_resources_side_effect,
    )

    with pytest.raises(click.ClickException, match="Deletion completed with failures"):
        _execute_dashboard_delete_plan(
            client,
            plan,
            cascade_options,
            execution_options,
            {},
        )

    assert stage_calls == [RESOURCE_CHART, RESOURCE_DATASET]
    rollback.assert_not_called()


def test_extract_dependency_maps_covers_optional_uuid_paths() -> None:
    """
    Test dependency extraction handles yaml assets missing optional UUID fields.
    """
    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        bundle.writestr("bundle/charts/no_uuid.yaml", yaml.dump({"dataset_uuid": "ds"}))
        bundle.writestr(
            "bundle/charts/no_dataset.yaml",
            yaml.dump({"uuid": "chart-no-dataset"}),
        )
        bundle.writestr(
            "bundle/datasets/no_uuid.yaml",
            yaml.dump({"database_uuid": "db"}),
        )
        bundle.writestr(
            "bundle/datasets/no_database.yaml",
            yaml.dump({"uuid": "dataset-no-database"}),
        )
        bundle.writestr("bundle/databases/no_uuid.yaml", yaml.dump({"name": "db"}))
        bundle.writestr("bundle/other/unknown.yaml", yaml.dump({"uuid": "ignored"}))
        bundle.writestr(
            "bundle/dashboards/no_chart_uuid.yaml",
            yaml.dump(
                {
                    "dashboard_title": "No UUID Dashboard",
                    "position": {"CHART-1": {"type": "CHART", "meta": {}}},
                },
            ),
        )

    chart_uuids, dataset_uuids, database_uuids, *_ = extract_dependency_maps(buf)
    assert "chart-no-dataset" in chart_uuids
    assert "dataset-no-database" in dataset_uuids
    assert database_uuids == set()


def test_apply_db_passwords_to_backup_skips_database_without_uuid() -> None:
    """
    Test DB password injection ignores database yaml files with no UUID.
    """
    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        bundle.writestr("bundle/databases/db.yaml", yaml.dump({"name": "db"}))

    result = _apply_db_passwords_to_backup(buf.getvalue(), {"db-uuid": "secret"})
    with ZipFile(result) as bundle:
        config = yaml.load(
            bundle.read("bundle/databases/db.yaml"),
            Loader=yaml.SafeLoader,
        )
    assert "password" not in config


def test_build_uuid_map_unresolved_when_no_ids(mocker: MockerFixture) -> None:
    """
    Test UUID mapping reports unresolved state when no IDs are available.
    """
    client = mocker.MagicMock()
    client.get_resources.return_value = [{"uuid": "only-uuid"}]
    uuid_map, name_map, resolved = build_uuid_map(client, "chart")
    assert uuid_map == {}
    assert name_map == {}
    assert resolved is False


def test_echo_cascade_section_handles_empty_dashboard_context(capsys) -> None:
    """
    Test cascade section renders entries without dashboard context labels.
    """
    _echo_cascade_section(
        "charts",
        {10},
        {10: "Chart"},
        enabled=True,
        chart_dashboard_context={},
    )
    output = capsys.readouterr().out
    assert "[ID: 10] Chart" in output


def test_echo_shared_summary_only_databases(capsys) -> None:
    """
    Test shared summary handles empty chart/dataset sections.
    """
    _echo_shared_summary(
        {
            "charts": set(),
            "datasets": set(),
            "databases": {"db-1"},
        },
    )
    output = capsys.readouterr().out
    assert "Databases (1): db-1" in output
    assert "Charts" not in output
    assert "Datasets" not in output


def test_compute_shared_uuids_shared_chart_without_dataset_mapping() -> None:
    """
    Test shared chart UUIDs without dataset mappings do not add extra dependencies.
    """
    dependencies = CascadeDependencies(
        chart_uuids={"chart-1"},
        dataset_uuids=set(),
        database_uuids=set(),
        chart_dataset_map={},
        dataset_database_map={},
        chart_dashboard_titles_by_uuid={},
    )
    shared = compute_shared_uuids(
        dependencies,
        {"charts": {"chart-1"}, "datasets": set(), "databases": set()},
    )
    assert shared == {"charts": {"chart-1"}, "datasets": set(), "databases": set()}


def test_filter_datasets_for_database_ids_ignores_other_databases(capsys) -> None:
    """
    Test dataset filtering drops rows for non-target DB IDs without warnings.
    """
    datasets = [{"id": 1, "database_id": 99}]
    filtered = _filter_datasets_for_database_ids(datasets, {1, 2})
    assert filtered == []
    assert "Cannot verify all datasets" not in capsys.readouterr().out
