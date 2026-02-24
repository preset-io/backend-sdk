"""
Delete Superset assets.
"""

# pylint: disable=too-many-lines

from __future__ import annotations

from io import BytesIO
from typing import Any, Dict, Iterable, List, Set, Tuple, cast

import click
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.api.operators import In
from preset_cli.cli.superset.asset_utils import (
    RESOURCE_CHART,
    RESOURCE_CHARTS,
    RESOURCE_DATABASE,
    RESOURCE_DATABASES,
    RESOURCE_DASHBOARD,
    RESOURCE_DASHBOARDS,
    RESOURCE_DATASET,
    RESOURCE_DATASETS,
)
from preset_cli.cli.superset import dependency_utils as dep_utils
from preset_cli.cli.superset.delete_display import (
    _echo_backup_restore_details,
    _echo_resource_summary,
    _echo_summary,
)
from preset_cli.cli.superset.delete_rollback import (
    _parse_db_passwords,
    _rollback_dashboard_deletion,
    _rollback_non_dashboard_deletion,
    _write_backup,
)
from preset_cli.cli.superset.delete_types import (
    CascadeDependencies,
    _CascadeResolution,
    DashboardCascadeOptions,
    _DashboardDeletePlan,
    _DashboardExecutionOptions,
    _DashboardSelection,
    _DeleteAssetsCommandOptions,
    _DeleteAssetsRawOptions,
    _DeleteSummaryData,
    _NonDashboardDeleteOptions,
)
from preset_cli.cli.superset.lib import (
    DELETE_FILTER_KEYS,
    coerce_bool_option,
    fetch_with_filter_fallback,
    filter_resources_locally,
    is_filter_not_allowed_error,
    parse_filters,
)


def _extract_uuids_from_export(buf: BytesIO) -> Dict[str, Set[str]]:
    """Extract UUID sets from a dashboard export ZIP."""
    chart_uuids, dataset_uuids, database_uuids, _, _, _ = dep_utils.extract_dependency_maps(
        buf,
    )
    return {
        RESOURCE_CHARTS: chart_uuids,
        RESOURCE_DATASETS: dataset_uuids,
        RESOURCE_DATABASES: database_uuids,
    }


def _dataset_db_id(dataset: Dict[str, Any]) -> int | None:
    if "database_id" in dataset:
        return dataset["database_id"]
    database = dataset.get("database")
    if isinstance(database, dict):
        return database.get("id")
    return None


@click.command()
@click.option(
    "--asset-type",
    required=True,
    type=click.Choice(
        [RESOURCE_DASHBOARD, RESOURCE_CHART, RESOURCE_DATASET, RESOURCE_DATABASE],
        case_sensitive=False,
    ),
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
    "--dry-run",
    "-r",
    default=None,
    help="Preview without making changes (default: true)",
)
@click.option(
    "--no-dry-run",
    "dry_run",
    flag_value="false",
    default=None,
    help="Alias for --dry-run=false",
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
@click.option(
    "--rollback/--no-rollback",
    default=True,
    help="Attempt best-effort rollback if any deletion fails",
)
@click.option(
    "--db-password",
    multiple=True,
    help="Password for DB connections during rollback (eg, uuid1=my_db_password)",
)
@click.pass_context
def delete_assets(
    ctx: click.core.Context,
    **raw_options: Any,
) -> None:
    """
    Delete assets by filters.
    """
    command_options = _parse_delete_command_options(
        cast(_DeleteAssetsRawOptions, raw_options),
    )
    _run_delete_assets(ctx, command_options)


def _parse_delete_command_options(
    raw_options: _DeleteAssetsRawOptions,
) -> _DeleteAssetsCommandOptions:
    cascade_options = DashboardCascadeOptions(
        charts=raw_options["cascade_charts"],
        datasets=raw_options["cascade_datasets"],
        databases=raw_options["cascade_databases"],
        skip_shared_check=raw_options["skip_shared_check"],
    )
    execution_options = _DashboardExecutionOptions(
        dry_run=_normalize_dry_run(raw_options["dry_run"]),
        confirm=raw_options["confirm"],
        rollback=raw_options["rollback"],
    )
    return _DeleteAssetsCommandOptions(
        asset_type=raw_options["asset_type"],
        filters=raw_options["filters"],
        cascade_options=cascade_options,
        execution_options=execution_options,
        db_password=raw_options["db_password"],
    )


def _run_delete_assets(
    ctx: click.core.Context,
    command_options: _DeleteAssetsCommandOptions,
) -> None:
    cascade_options = command_options.cascade_options
    execution_options = command_options.execution_options
    resource_name = command_options.asset_type.lower()
    dry_run = execution_options.dry_run
    _validate_delete_option_combinations(resource_name, cascade_options)
    db_passwords, rollback = _resolve_rollback_settings(
        resource_name,
        execution_options.rollback,
        command_options.db_password,
    )
    client = _build_superset_client(ctx)

    parsed_filters = parse_filters(
        command_options.filters,
        DELETE_FILTER_KEYS[resource_name],
    )
    if resource_name != RESOURCE_DASHBOARD:
        non_dashboard_options = _NonDashboardDeleteOptions(
            resource_name=resource_name,
            dry_run=dry_run,
            confirm=execution_options.confirm,
            rollback=rollback,
            db_passwords=db_passwords,
        )
        _delete_non_dashboard_assets(
            client,
            parsed_filters,
            non_dashboard_options,
        )
        return

    dashboard_execution_options = _DashboardExecutionOptions(
        dry_run=dry_run,
        confirm=execution_options.confirm,
        rollback=rollback,
    )
    _delete_dashboard_assets(
        client,
        parsed_filters,
        cascade_options,
        dashboard_execution_options,
        db_passwords,
    )


def _normalize_dry_run(raw_dry_run: bool | str | None) -> bool:
    dry_run = True if raw_dry_run is None else raw_dry_run
    return coerce_bool_option(dry_run, "dry_run")


def _validate_delete_option_combinations(
    resource_name: str,
    cascade_options: DashboardCascadeOptions,
) -> None:
    if resource_name != RESOURCE_DASHBOARD and any(
        [
            cascade_options.charts,
            cascade_options.datasets,
            cascade_options.databases,
            cascade_options.skip_shared_check,
        ],
    ):
        raise click.UsageError(
            "Cascade options are only supported for dashboard assets.",
        )
    if cascade_options.datasets and not cascade_options.charts:
        raise click.UsageError(
            "--cascade-datasets requires --cascade-charts.",
        )
    if cascade_options.databases and not cascade_options.datasets:
        raise click.UsageError(
            "--cascade-databases requires --cascade-datasets.",
        )


def _resolve_rollback_settings(
    resource_name: str,
    rollback: bool,
    db_password: Tuple[str, ...],
) -> Tuple[Dict[str, str], bool]:
    if resource_name == RESOURCE_DASHBOARD:
        return _parse_db_passwords(db_password), rollback
    if resource_name != RESOURCE_DATABASE:
        return {}, rollback

    db_passwords = _parse_db_passwords(db_password)
    if rollback and not db_passwords:
        click.echo(
            "Warning: rollback for database deletes requires "
            "--db-password; proceeding without rollback.",
        )
        return db_passwords, False
    return db_passwords, rollback


def _build_superset_client(ctx: click.core.Context) -> SupersetClient:
    auth = ctx.obj["AUTH"]
    url = URL(ctx.obj["INSTANCE"])
    return SupersetClient(url, auth)


def _delete_resources(
    client: SupersetClient,
    resource_name: str,
    resource_ids: Iterable[int],
    failures: List[str],
) -> bool:
    deleted_any = False
    for resource_id in sorted(resource_ids):
        try:
            client.delete_resource(resource_name, resource_id)
            deleted_any = True
        except Exception as exc:  # pylint: disable=broad-except
            failures.append(f"{resource_name}:{resource_id} ({exc})")
    return deleted_any


def _fetch_non_dashboard_resources(
    client: SupersetClient,
    resource_name: str,
    parsed_filters: Dict[str, Any],
) -> List[Dict[str, Any]]:
    if resource_name == RESOURCE_DATABASE:
        return filter_resources_locally(
            client.get_resources(resource_name),
            parsed_filters,
        )
    return fetch_with_filter_fallback(
        lambda **kw: client.get_resources(resource_name, **kw),
        lambda: client.get_resources(resource_name),
        parsed_filters,
        f"{resource_name}s",
    )


def _delete_non_dashboard_assets(
    client: SupersetClient,
    parsed_filters: Dict[str, Any],
    options: _NonDashboardDeleteOptions,
) -> None:
    resource_name = options.resource_name
    resources = _fetch_non_dashboard_resources(
        client,
        resource_name,
        parsed_filters,
    )
    if not resources:
        click.echo(f"No {resource_name}s match the specified filters.")
        return

    resource_ids = {resource["id"] for resource in resources}
    if options.dry_run:
        _echo_resource_summary(resource_name, resources, dry_run=True)
        return

    if options.confirm != "DELETE":
        click.echo(
            "Deletion aborted. Pass --confirm=DELETE to proceed with deletion.",
        )
        return

    _echo_resource_summary(resource_name, resources, dry_run=False)

    # Pre-delete backup for manual restore (or rollback when enabled).
    backup_buf = client.export_zip(resource_name, sorted(resource_ids))
    backup_data = backup_buf.read()
    backup_path = _write_backup(backup_data)
    _echo_backup_restore_details(backup_path, resource_name)

    resource_failures: List[str] = []
    deleted_any = _delete_resources(
        client,
        resource_name,
        resource_ids,
        resource_failures,
    )

    if resource_failures:
        click.echo("Some deletions failed:")
        for failure in resource_failures:
            click.echo(f"  {failure}")
        if options.rollback and deleted_any:
            click.echo("\nBest-effort rollback attempted...")
            _rollback_non_dashboard_deletion(
                client,
                resource_name,
                backup_data,
                options.db_passwords,
            )

        raise click.ClickException(
            "Deletion completed with failures. See errors above.",
        )


def _fetch_dashboard_selection(
    client: SupersetClient,
    parsed_filters: Dict[str, Any],
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
    return _DashboardSelection(
        dashboards=dashboards,
        dashboard_ids={dashboard["id"] for dashboard in dashboards},
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
        (
            dependencies.chart_uuids,
            dependencies.dataset_uuids,
            dependencies.database_uuids,
            dependencies.chart_dataset_map,
            dependencies.dataset_database_map,
            dependencies.chart_dashboard_titles_by_uuid,
        ) = dep_utils.extract_dependency_maps(selection.cascade_buf)

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
) -> Tuple[List[Dict[str, Any]], bool]:
    try:
        return (
            client.get_resources(
                RESOURCE_DATASET,
                database_id=In(list(database_ids)),
            ),
            True,
        )
    except Exception as exc:  # pylint: disable=broad-except
        if is_filter_not_allowed_error(exc):
            return client.get_resources(RESOURCE_DATASET), False
        raise click.ClickException(
            f"Failed to preflight datasets ({exc}).",
        ) from exc


def _filter_datasets_for_database_ids(
    datasets: List[Dict[str, Any]],
    database_ids: Set[int],
) -> List[Dict[str, Any]]:
    filtered_datasets = []
    missing_db_info = False
    for dataset in datasets:
        db_id = _dataset_db_id(dataset)
        if db_id is None:
            missing_db_info = True
            continue
        if db_id in database_ids:
            filtered_datasets.append(dataset)
    if missing_db_info:
        click.echo(
            "Warning: Cannot verify all datasets for target databases; "
            "skipping preflight check.",
        )
    return filtered_datasets


def _raise_if_extra_preflight_datasets(
    datasets: List[Dict[str, Any]],
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


def _read_dashboard_backup_data(
    client: SupersetClient,
    selection: _DashboardSelection,
) -> bytes:
    if selection.cascade_buf is None:
        backup_buf = client.export_zip(RESOURCE_DASHBOARD, list(selection.dashboard_ids))
        return backup_buf.read()
    selection.cascade_buf.seek(0)
    return selection.cascade_buf.read()


def _prepare_dashboard_delete_plan(
    client: SupersetClient,
    parsed_filters: Dict[str, Any],
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


def _execute_dashboard_delete_plan(
    client: SupersetClient,
    plan: _DashboardDeletePlan,
    cascade_options: DashboardCascadeOptions,
    execution_options: _DashboardExecutionOptions,
    db_passwords: Dict[str, str],
) -> None:
    selection = plan.selection
    resolution = plan.resolution
    _echo_summary(plan.summary, dry_run=False)

    backup_data = _read_dashboard_backup_data(client, plan.selection)
    backup_path = _write_backup(backup_data)
    _echo_backup_restore_details(backup_path, RESOURCE_DASHBOARD)

    failures: List[str] = []
    deleted_any = False
    stages = [
        (RESOURCE_CHART, resolution.ids[RESOURCE_CHARTS]),
        (RESOURCE_DATASET, resolution.ids[RESOURCE_DATASETS]),
        (RESOURCE_DATABASE, resolution.ids[RESOURCE_DATABASES]),
        (RESOURCE_DASHBOARD, selection.dashboard_ids),
    ]
    for resource_name, resource_ids in stages:
        failure_count_before_stage = len(failures)
        deleted_any = (
            _delete_resources(
                client,
                resource_name,
                resource_ids,
                failures,
            )
            or deleted_any
        )
        if len(failures) > failure_count_before_stage:
            break

    if failures:
        click.echo("Some deletions failed:")
        for failure in failures:
            click.echo(f"  {failure}")

        if execution_options.rollback and deleted_any:
            click.echo("\nBest-effort rollback attempted...")
            _rollback_dashboard_deletion(
                client,
                backup_data,
                db_passwords,
                cascade_options,
            )
        raise click.ClickException(
            "Deletion completed with failures. See errors above.",
        )


def _delete_dashboard_assets(
    client: SupersetClient,
    parsed_filters: Dict[str, Any],
    cascade_options: DashboardCascadeOptions,
    execution_options: _DashboardExecutionOptions,
    db_passwords: Dict[str, str],
) -> None:
    plan = _prepare_dashboard_delete_plan(client, parsed_filters, cascade_options)
    if plan is None:
        return

    if execution_options.dry_run:
        _echo_summary(plan.summary, dry_run=True)
        return

    if not _validate_dashboard_delete_execution(
        client,
        plan,
        cascade_options,
        execution_options,
        db_passwords,
    ):
        return

    _execute_dashboard_delete_plan(
        client,
        plan,
        cascade_options,
        execution_options,
        db_passwords,
    )
