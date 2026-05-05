"""
Cascade planning helpers for delete assets.
"""

from __future__ import annotations

from io import BytesIO
from typing import Dict, List, Mapping, Sequence, Set, Tuple, cast

import click

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.api.operators import In
from preset_cli.cli.superset import dependency_utils as dep_utils
from preset_cli.cli.superset.asset_utils import (
    RESOURCE_CHART,
    RESOURCE_CHARTS,
    RESOURCE_DASHBOARD,
    RESOURCE_DASHBOARDS,
    RESOURCE_DATABASE,
    RESOURCE_DATABASES,
    RESOURCE_DATASET,
    RESOURCE_DATASETS,
)
from preset_cli.cli.superset.delete_types import (
    CascadeDependencies,
    DashboardCascadeOptions,
    _CascadeResolution,
    _DashboardDeletePlan,
    _DashboardExecutionOptions,
    _DashboardSelection,
    _DashboardSummaryRow,
    _DeleteSummaryData,
)
from preset_cli.cli.superset.lib import (
    ParsedFilterValue,
    fetch_with_filter_fallback,
    is_filter_not_allowed_error,
)


def _extract_uuids_from_export(buf: BytesIO) -> Dict[str, Set[str]]:
    """Extract UUID sets from a dashboard export ZIP."""
    dependencies = dep_utils.extract_dependency_maps(buf)
    return {
        RESOURCE_CHARTS: dependencies.chart_uuids,
        RESOURCE_DATASETS: dependencies.dataset_uuids,
        RESOURCE_DATABASES: dependencies.database_uuids,
    }


def _dataset_db_id(dataset: Mapping[str, object]) -> int | None:
    if "database_id" in dataset:
        database_id = dataset["database_id"]
        return database_id if isinstance(database_id, int) else None
    database = dataset.get("database")
    if isinstance(database, dict):
        database_id = database.get("id")
        return database_id if isinstance(database_id, int) else None
    return None


def _fetch_dashboard_selection(
    client: SupersetClient,
    parsed_filters: Dict[str, ParsedFilterValue],
) -> _DashboardSelection | None:
    dashboards = fetch_with_filter_fallback(
        client.get_dashboards,
        client.get_dashboards,
        parsed_filters,
        RESOURCE_DASHBOARDS,
    )
    if not dashboards:
        click.echo("No dashboards match the specified filters.")
        return None
    summary_dashboards = cast(List[_DashboardSummaryRow], dashboards)
    return _DashboardSelection(
        dashboards=summary_dashboards,
        dashboard_ids={dashboard["id"] for dashboard in summary_dashboards},
    )


def _load_cascade_dependencies(
    client: SupersetClient,
    selection: _DashboardSelection,
    cascade_options: DashboardCascadeOptions,
) -> CascadeDependencies:
    dependencies = CascadeDependencies(
        chart_uuids=set(),
        dataset_uuids=set(),
        database_uuids=set(),
        chart_dataset_map={},
        dataset_database_map={},
        chart_dashboard_titles_by_uuid={},
    )

    if cascade_options.charts:
        selection.cascade_buf = client.export_zip(
            RESOURCE_DASHBOARD,
            list(selection.dashboard_ids),
        )
        dependencies = dep_utils.extract_dependency_maps(selection.cascade_buf)

        if not cascade_options.datasets:
            dependencies.dataset_uuids = set()
            dependencies.dataset_database_map = {}
        if not cascade_options.databases:
            dependencies.database_uuids = set()

    return dependencies


def _protect_shared_dependencies(
    client: SupersetClient,
    selection: _DashboardSelection,
    dependencies: CascadeDependencies,
    cascade_options: DashboardCascadeOptions,
) -> Dict[str, Set[str]]:
    shared_uuids = _empty_shared_uuids()
    if not cascade_options.charts:
        return shared_uuids
    if cascade_options.skip_shared_check:
        click.echo(
            "Shared dependency check skipped — cascade targets may be used by other dashboards",
        )
        return shared_uuids

    other_ids = _find_other_dashboard_ids(client, selection.dashboard_ids)
    if not other_ids:
        return shared_uuids

    other_buf = client.export_zip(RESOURCE_DASHBOARD, list(other_ids))
    protected = _extract_uuids_from_export(other_buf)
    shared_uuids = dep_utils.compute_shared_uuids(dependencies, protected)
    _remove_shared_dependencies(dependencies, shared_uuids)
    return shared_uuids


def _empty_shared_uuids() -> Dict[str, Set[str]]:
    return {
        RESOURCE_CHARTS: set(),
        RESOURCE_DATASETS: set(),
        RESOURCE_DATABASES: set(),
    }


def _find_other_dashboard_ids(
    client: SupersetClient,
    dashboard_ids: Set[int],
) -> Set[int]:
    all_dashboards = client.get_resources(RESOURCE_DASHBOARD)
    return {dashboard["id"] for dashboard in all_dashboards} - dashboard_ids


def _remove_shared_dependencies(
    dependencies: CascadeDependencies,
    shared_uuids: Dict[str, Set[str]],
) -> None:
    dependencies.chart_uuids -= shared_uuids[RESOURCE_CHARTS]
    dependencies.chart_dashboard_titles_by_uuid = {
        uuid: titles
        for uuid, titles in dependencies.chart_dashboard_titles_by_uuid.items()
        if uuid in dependencies.chart_uuids
    }
    dependencies.dataset_uuids -= shared_uuids[RESOURCE_DATASETS]
    dependencies.database_uuids -= shared_uuids[RESOURCE_DATABASES]


def _resolve_cascade_targets(
    client: SupersetClient,
    dependencies: CascadeDependencies,
    cascade_options: DashboardCascadeOptions,
) -> _CascadeResolution:
    ids: Dict[str, Set[int]] = {
        RESOURCE_CHARTS: set(),
        RESOURCE_DATASETS: set(),
        RESOURCE_DATABASES: set(),
    }
    names: Dict[str, Dict[int, str]] = {}
    chart_dashboard_context: Dict[int, List[str]] = {}
    cascade_flags = {
        RESOURCE_CHARTS: cascade_options.charts,
        RESOURCE_DATASETS: cascade_options.datasets,
        RESOURCE_DATABASES: cascade_options.databases,
    }
    if not cascade_options.charts:
        return _CascadeResolution(
            ids=ids,
            names=names,
            chart_dashboard_context=chart_dashboard_context,
            flags=cascade_flags,
        )

    (
        ids[RESOURCE_CHARTS],
        chart_names,
        chart_dashboard_context,
        missing_chart_uuids,
        charts_resolved,
    ) = _resolve_chart_targets(client, dependencies)
    dataset_result = dep_utils.resolve_ids(
        client,
        RESOURCE_DATASET,
        dependencies.dataset_uuids,
    )
    database_result = dep_utils.resolve_ids(
        client,
        RESOURCE_DATABASE,
        dependencies.database_uuids,
    )
    ids[RESOURCE_DATASETS] = dataset_result[0]
    ids[RESOURCE_DATABASES] = database_result[0]
    names = {
        RESOURCE_CHARTS: chart_names,
        RESOURCE_DATASETS: dataset_result[1],
        RESOURCE_DATABASES: database_result[1],
    }
    all_resolved = charts_resolved and dataset_result[3] and database_result[3]
    if not all_resolved:
        click.echo(
            "Cannot resolve cascade targets on this Superset version — "
            "skipping cascade deletion.",
        )
        ids = {
            RESOURCE_CHARTS: set(),
            RESOURCE_DATASETS: set(),
            RESOURCE_DATABASES: set(),
        }
    else:
        _warn_missing_uuids(RESOURCE_CHART, missing_chart_uuids)
        _warn_missing_uuids(RESOURCE_DATASET, dataset_result[2])
        _warn_missing_uuids(RESOURCE_DATABASE, database_result[2])

    return _CascadeResolution(
        ids=ids,
        names=names,
        chart_dashboard_context=chart_dashboard_context,
        flags=cascade_flags,
    )


def _resolve_chart_targets(
    client: SupersetClient,
    dependencies: CascadeDependencies,
) -> Tuple[Set[int], Dict[int, str], Dict[int, List[str]], List[str], bool]:
    chart_dashboard_context: Dict[int, List[str]] = {}
    chart_uuid_map, chart_names, charts_resolved = dep_utils.build_uuid_map(
        client,
        RESOURCE_CHART,
    )
    if not charts_resolved:
        return (
            set(),
            chart_names,
            chart_dashboard_context,
            list(dependencies.chart_uuids),
            False,
        )

    chart_ids = {
        chart_uuid_map[chart_uuid]
        for chart_uuid in dependencies.chart_uuids
        if chart_uuid in chart_uuid_map
    }
    missing_chart_uuids = [
        chart_uuid
        for chart_uuid in dependencies.chart_uuids
        if chart_uuid not in chart_uuid_map
    ]
    for chart_uuid, titles in dependencies.chart_dashboard_titles_by_uuid.items():
        chart_id = chart_uuid_map.get(chart_uuid)
        if chart_id is None:
            continue
        if chart_id not in chart_ids:
            continue
        chart_dashboard_context[chart_id] = sorted(titles)

    return (
        chart_ids,
        chart_names,
        chart_dashboard_context,
        missing_chart_uuids,
        True,
    )


def _warn_missing_uuids(resource_name: str, missing_uuids: List[str]) -> None:
    for missing in missing_uuids:
        click.echo(f"Warning: {resource_name} UUID not found: {missing}")


def _preflight_database_deletion(
    client: SupersetClient,
    database_ids: Set[int],
    dataset_ids: Set[int],
) -> None:
    datasets, filtered_by_db = _fetch_preflight_datasets(
        client,
        database_ids,
    )
    if not filtered_by_db:
        datasets = _filter_datasets_for_database_ids(
            datasets,
            database_ids,
        )

    _raise_if_extra_preflight_datasets(datasets, dataset_ids)


def _fetch_preflight_datasets(
    client: SupersetClient,
    database_ids: Set[int],
) -> Tuple[List[Dict[str, object]], bool]:
    try:
        return (
            cast(
                List[Dict[str, object]],
                client.get_resources(
                    RESOURCE_DATASET,
                    database_id=In(list(database_ids)),
                ),
            ),
            True,
        )
    except Exception as exc:  # pylint: disable=broad-except
        if is_filter_not_allowed_error(exc):
            return (
                cast(List[Dict[str, object]], client.get_resources(RESOURCE_DATASET)),
                False,
            )
        raise click.ClickException(
            f"Failed to preflight datasets ({exc}).",
        ) from exc


def _filter_datasets_for_database_ids(
    datasets: Sequence[Mapping[str, object]],
    database_ids: Set[int],
) -> List[Dict[str, object]]:
    filtered_datasets = []
    missing_db_info = False
    for dataset in datasets:
        db_id = _dataset_db_id(dataset)
        if db_id is None:
            missing_db_info = True
            continue
        if db_id in database_ids:
            if isinstance(dataset, dict):
                filtered_datasets.append(dataset)
            else:
                filtered_datasets.append(dict(dataset))
    if missing_db_info:
        click.echo(
            "Warning: Cannot verify all datasets for target databases; "
            "skipping preflight check.",
        )
    return filtered_datasets


def _raise_if_extra_preflight_datasets(
    datasets: Sequence[Mapping[str, object]],
    dataset_ids: Set[int],
) -> None:
    extra = [dataset for dataset in datasets if dataset.get("id") not in dataset_ids]
    if extra:
        extra_ids = ", ".join(str(dataset.get("id")) for dataset in extra)
        raise click.ClickException(
            "Aborting deletion: databases have datasets not in cascade set. "
            f"Extra dataset IDs: {extra_ids}",
        )


def _build_dashboard_summary(
    selection: _DashboardSelection,
    resolution: _CascadeResolution,
    shared_uuids: Dict[str, Set[str]],
) -> _DeleteSummaryData:
    return _DeleteSummaryData(
        dashboards=selection.dashboards,
        cascade_ids=resolution.ids,
        cascade_names=resolution.names,
        chart_dashboard_context=resolution.chart_dashboard_context,
        shared=shared_uuids,
        cascade_flags=resolution.flags,
    )


def _prepare_dashboard_delete_plan(
    client: SupersetClient,
    parsed_filters: Dict[str, ParsedFilterValue],
    cascade_options: DashboardCascadeOptions,
) -> _DashboardDeletePlan | None:
    selection = _fetch_dashboard_selection(client, parsed_filters)
    if selection is None:
        return None

    dependencies = _load_cascade_dependencies(client, selection, cascade_options)
    shared_uuids = _protect_shared_dependencies(
        client,
        selection,
        dependencies,
        cascade_options,
    )
    resolution = _resolve_cascade_targets(client, dependencies, cascade_options)
    summary = _build_dashboard_summary(selection, resolution, shared_uuids)
    return _DashboardDeletePlan(
        selection=selection,
        resolution=resolution,
        summary=summary,
    )


def _validate_dashboard_delete_execution(
    client: SupersetClient,
    plan: _DashboardDeletePlan,
    cascade_options: DashboardCascadeOptions,
    execution_options: _DashboardExecutionOptions,
    db_passwords: Dict[str, str],
) -> bool:
    if execution_options.confirm != "DELETE":
        click.echo(
            "Deletion aborted. Pass --confirm=DELETE to proceed with deletion.",
        )
        return False

    if (
        execution_options.rollback
        and cascade_options.databases
        and plan.resolution.ids[RESOURCE_DATABASES]
        and not db_passwords
    ):
        raise click.ClickException(
            "Rollback requires a DB password (--db-password) when deleting databases.",
        )

    if cascade_options.databases and plan.resolution.ids[RESOURCE_DATABASES]:
        _preflight_database_deletion(
            client,
            plan.resolution.ids[RESOURCE_DATABASES],
            plan.resolution.ids[RESOURCE_DATASETS],
        )

    return True
