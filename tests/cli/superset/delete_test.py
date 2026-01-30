"""
Tests for delete assets command.
"""

# pylint: disable=redefined-outer-name, invalid-name, unused-argument

from io import BytesIO
from typing import Dict
from zipfile import ZipFile

import yaml
from click.testing import CliRunner
from pytest_mock import MockerFixture

from preset_cli.cli.superset.main import superset_cli


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
    client.get_resources.side_effect = [
        [{"id": 1}],
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
            "--cascade-datasets",
            "--cascade-databases",
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
    client.delete_resource.side_effect = [Exception("boom"), None]
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
    assert client.delete_resource.call_count == 2


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
    client.get_resources.side_effect = [
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
            "--cascade-datasets",
            "--cascade-databases",
            "--skip-shared-check",
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
