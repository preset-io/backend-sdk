"""
Delete Superset assets.
"""

from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, Iterable, List, Set, Tuple
from zipfile import ZipFile

import click
import yaml
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.lib import DASHBOARD_FILTER_KEYS, parse_filters
from preset_cli.lib import remove_root


def _extract_uuids_from_export(buf: BytesIO) -> Dict[str, Set[str]]:
    """Extract UUID sets from a dashboard export ZIP."""
    chart_uuids, dataset_uuids, database_uuids, _, _ = _extract_dependency_maps(buf)
    return {
        "charts": chart_uuids,
        "datasets": dataset_uuids,
        "databases": database_uuids,
    }


def _extract_dependency_maps(buf: BytesIO) -> Tuple[Set[str], Set[str], Set[str], Dict[str, str], Dict[str, str]]:
    chart_uuids: Set[str] = set()
    dataset_uuids: Set[str] = set()
    database_uuids: Set[str] = set()
    chart_dataset_map: Dict[str, str] = {}
    dataset_database_map: Dict[str, str] = {}

    with ZipFile(buf) as bundle:
        for file_name in bundle.namelist():
            relative = remove_root(file_name)
            if not relative.endswith((".yaml", ".yml")):
                continue
            config = yaml.load(bundle.read(file_name), Loader=yaml.SafeLoader) or {}
            if relative.startswith("charts/"):
                if uuid := config.get("uuid"):
                    chart_uuids.add(uuid)
                    if dataset_uuid := config.get("dataset_uuid"):
                        chart_dataset_map[uuid] = dataset_uuid
                        dataset_uuids.add(dataset_uuid)
            elif relative.startswith("datasets/"):
                if uuid := config.get("uuid"):
                    dataset_uuids.add(uuid)
                    if database_uuid := config.get("database_uuid"):
                        dataset_database_map[uuid] = database_uuid
                        database_uuids.add(database_uuid)
            elif relative.startswith("databases/"):
                if uuid := config.get("uuid"):
                    database_uuids.add(uuid)

    return (
        chart_uuids,
        dataset_uuids,
        database_uuids,
        chart_dataset_map,
        dataset_database_map,
    )


def _build_uuid_map(
    client: SupersetClient,
    resource_name: str,
) -> Tuple[Dict[str, int], bool]:
    resources = client.get_resources(resource_name)
    uuid_map = {
        resource["uuid"]: resource["id"]
        for resource in resources
        if "uuid" in resource and "id" in resource
    }
    if uuid_map:
        return uuid_map, True

    ids = {resource["id"] for resource in resources if "id" in resource}
    if ids:
        uuid_map = {
            str(uuid): id_ for id_, uuid in client.get_uuids(resource_name, ids).items()
        }
        if uuid_map:
            return uuid_map, True

    return {}, False


def _resolve_ids(
    client: SupersetClient,
    resource_name: str,
    uuids: Set[str],
) -> Tuple[Set[int], List[str], bool]:
    if not uuids:
        return set(), [], True

    uuid_map, resolved = _build_uuid_map(client, resource_name)
    if not resolved:
        return set(), list(uuids), False

    ids = {uuid_map[uuid] for uuid in uuids if uuid in uuid_map}
    missing = [uuid for uuid in uuids if uuid not in uuid_map]
    return ids, missing, True


def _format_dashboard_summary(dashboards: Iterable[Dict[str, Any]]) -> List[str]:
    lines = []
    for dashboard in dashboards:
        title = dashboard.get("dashboard_title") or dashboard.get("title") or "Unknown"
        slug = dashboard.get("slug", "n/a")
        lines.append(f"- [ID: {dashboard['id']}] {title} (slug: {slug})")
    return lines


def _echo_summary(
    dashboards: List[Dict[str, Any]],
    chart_ids: Set[int],
    dataset_ids: Set[int],
    database_ids: Set[int],
    shared: Dict[str, Set[str]],
    cascade_flags: Dict[str, bool],
    dry_run: bool,
) -> None:
    if dry_run:
        click.echo("No changes will be made. Assets to be deleted:\n")
    else:
        click.echo("Assets to be deleted:\n")

    click.echo(f"Dashboards ({len(dashboards)}):")
    for line in _format_dashboard_summary(dashboards):
        click.echo(f"  {line}")

    if cascade_flags["charts"]:
        click.echo(f"\nCharts ({len(chart_ids)}):")
        for cid in sorted(chart_ids):
            click.echo(f"  - [ID: {cid}]")
    else:
        click.echo("\nCharts (0): (not cascading)")

    if cascade_flags["datasets"]:
        click.echo(f"\nDatasets ({len(dataset_ids)}):")
        for did in sorted(dataset_ids):
            click.echo(f"  - [ID: {did}]")
    else:
        click.echo("\nDatasets (0): (not cascading)")

    if cascade_flags["databases"]:
        click.echo(f"\nDatabases ({len(database_ids)}):")
        for bid in sorted(database_ids):
            click.echo(f"  - [ID: {bid}]")
    else:
        click.echo("\nDatabases (0): (not cascading)")

    if any(shared.values()):
        click.echo("\nShared (skipped):")
        if shared["charts"]:
            click.echo(f"  Charts ({len(shared['charts'])}): {', '.join(sorted(shared['charts']))}")
        if shared["datasets"]:
            click.echo(f"  Datasets ({len(shared['datasets'])}): {', '.join(sorted(shared['datasets']))}")
        if shared["databases"]:
            click.echo(f"  Databases ({len(shared['databases'])}): {', '.join(sorted(shared['databases']))}")

    if dry_run:
        click.echo(
            "\nTo proceed with deletion, run with: --no-dry-run --confirm=DELETE",
        )


@click.command()
@click.option(
    "--asset-type",
    required=True,
    type=click.Choice(["dashboard"], case_sensitive=False),
    help="Asset type to delete",
)
@click.option(
    "--filter",
    "-t",
    "filters",
    multiple=True,
    required=True,
    help="Filter key=value (repeatable, ANDed). At least one required.",
)
@click.option(
    "--cascade-charts",
    "-c",
    is_flag=True,
    default=False,
    help="Also delete associated charts",
)
@click.option(
    "--cascade-datasets",
    "-d",
    is_flag=True,
    default=False,
    help="Also delete associated datasets (requires --cascade-charts)",
)
@click.option(
    "--cascade-databases",
    "-b",
    is_flag=True,
    default=False,
    help="Also delete associated databases (requires --cascade-datasets)",
)
@click.option(
    "--dry-run/--no-dry-run",
    "-r",
    default=True,
    help="Preview without making changes (default: dry-run)",
)
@click.option(
    "--skip-shared-check",
    is_flag=True,
    default=False,
    help="Skip checking for shared dependencies (faster, use on large instances)",
)
@click.option(
    "--confirm",
    default=None,
    help='Must be "DELETE" to proceed with actual deletion',
)
@click.pass_context
def delete_assets(  # pylint: disable=too-many-locals, too-many-arguments, too-many-branches
    ctx: click.core.Context,
    asset_type: str,
    filters: Tuple[str, ...],
    cascade_charts: bool,
    cascade_datasets: bool,
    cascade_databases: bool,
    dry_run: bool,
    skip_shared_check: bool,
    confirm: str | None,
) -> None:
    """
    Delete assets by filters.
    """
    if asset_type.lower() != "dashboard":
        raise click.UsageError("Only dashboard assets are supported.")
    if cascade_datasets and not cascade_charts:
        raise click.UsageError(
            "--cascade-datasets requires --cascade-charts.",
        )
    if cascade_databases and not cascade_datasets:
        raise click.UsageError(
            "--cascade-databases requires --cascade-datasets.",
        )

    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    client = SupersetClient(url, auth)

    parsed_filters = parse_filters(filters, DASHBOARD_FILTER_KEYS)
    try:
        dashboards = client.get_dashboards(**parsed_filters)
    except Exception as exc:  # pylint: disable=broad-except
        filter_keys = ", ".join(parsed_filters.keys())
        raise click.ClickException(
            "Filter key(s) "
            f"{filter_keys} may not be supported by this Superset version. "
            "Supported fields vary by version.",
        ) from exc
    if not dashboards:
        click.echo("No dashboards match the specified filters.")
        return

    dashboard_ids = {dashboard["id"] for dashboard in dashboards}
    chart_uuids: Set[str] = set()
    dataset_uuids: Set[str] = set()
    database_uuids: Set[str] = set()
    chart_dataset_map: Dict[str, str] = {}
    dataset_database_map: Dict[str, str] = {}

    if cascade_charts:
        buf = client.export_zip("dashboard", list(dashboard_ids))
        (
            chart_uuids,
            dataset_uuids,
            database_uuids,
            chart_dataset_map,
            dataset_database_map,
        ) = _extract_dependency_maps(buf)

        if not cascade_datasets:
            dataset_uuids = set()
            dataset_database_map = {}
        if not cascade_databases:
            database_uuids = set()

    shared_uuids = {"charts": set(), "datasets": set(), "databases": set()}
    if cascade_charts and not skip_shared_check:
        all_dashboards = client.get_resources("dashboard")
        other_ids = {d["id"] for d in all_dashboards} - dashboard_ids
        if other_ids:
            other_buf = client.export_zip("dashboard", list(other_ids))
            protected = _extract_uuids_from_export(other_buf)
            shared_uuids["charts"] = chart_uuids & protected["charts"]
            protected_datasets = dataset_uuids & protected["datasets"]
            protected_databases = database_uuids & protected["databases"]

            for chart_uuid in shared_uuids["charts"]:
                if dataset_uuid := chart_dataset_map.get(chart_uuid):
                    protected_datasets.add(dataset_uuid)
                    if database_uuid := dataset_database_map.get(dataset_uuid):
                        protected_databases.add(database_uuid)

            for dataset_uuid in protected_datasets:
                if database_uuid := dataset_database_map.get(dataset_uuid):
                    protected_databases.add(database_uuid)

            shared_uuids["datasets"] = protected_datasets
            shared_uuids["databases"] = protected_databases

            chart_uuids -= shared_uuids["charts"]
            dataset_uuids -= shared_uuids["datasets"]
            database_uuids -= shared_uuids["databases"]
    elif cascade_charts and skip_shared_check:
        click.echo(
            "Shared dependency check skipped — cascade targets may be used by other dashboards",
        )

    chart_ids: Set[int] = set()
    dataset_ids: Set[int] = set()
    database_ids: Set[int] = set()

    cascade_flags = {
        "charts": cascade_charts,
        "datasets": cascade_datasets,
        "databases": cascade_databases,
    }

    if cascade_charts:
        chart_ids, missing_chart_uuids, charts_resolved = _resolve_ids(
            client,
            "chart",
            chart_uuids,
        )
        dataset_ids, missing_dataset_uuids, datasets_resolved = _resolve_ids(
            client,
            "dataset",
            dataset_uuids,
        )
        database_ids, missing_database_uuids, databases_resolved = _resolve_ids(
            client,
            "database",
            database_uuids,
        )

        if not (charts_resolved and datasets_resolved and databases_resolved):
            click.echo(
                "Cannot resolve cascade targets on this Superset version — "
                "skipping cascade deletion.",
            )
            chart_ids = set()
            dataset_ids = set()
            database_ids = set()
        else:
            for missing in missing_chart_uuids:
                click.echo(f"Warning: chart UUID not found: {missing}")
            for missing in missing_dataset_uuids:
                click.echo(f"Warning: dataset UUID not found: {missing}")
            for missing in missing_database_uuids:
                click.echo(f"Warning: database UUID not found: {missing}")

    if dry_run:
        _echo_summary(
            dashboards,
            chart_ids,
            dataset_ids,
            database_ids,
            shared_uuids,
            cascade_flags,
            dry_run=True,
        )
        return

    if confirm != "DELETE":
        click.echo(
            'Deletion aborted. Pass --confirm=DELETE to proceed with deletion.',
        )
        return

    _echo_summary(
        dashboards,
        chart_ids,
        dataset_ids,
        database_ids,
        shared_uuids,
        cascade_flags,
        dry_run=False,
    )

    failures: List[str] = []

    def _delete(resource_name: str, resource_ids: Iterable[int]) -> None:
        for resource_id in resource_ids:
            try:
                client.delete_resource(resource_name, resource_id)
            except Exception as exc:  # pylint: disable=broad-except
                failures.append(f"{resource_name}:{resource_id} ({exc})")

    _delete("dashboard", dashboard_ids)
    _delete("chart", chart_ids)
    _delete("dataset", dataset_ids)
    _delete("database", database_ids)

    if failures:
        click.echo("Some deletions failed:")
        for failure in failures:
            click.echo(f"  {failure}")
