"""
Delete Superset assets.
"""

# pylint: disable=too-many-lines,too-many-locals

from __future__ import annotations

from typing import Dict, Iterable, List, Tuple, cast

import click
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.asset_utils import (
    RESOURCE_CHART,
    RESOURCE_CHARTS,
    RESOURCE_DASHBOARD,
    RESOURCE_DATABASE,
    RESOURCE_DATABASES,
    RESOURCE_DATASET,
    RESOURCE_DATASETS,
)
from preset_cli.cli.superset.delete_cascade import (
    _prepare_dashboard_delete_plan,
    _validate_dashboard_delete_execution,
)
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
    DashboardCascadeOptions,
    _DashboardDeletePlan,
    _DashboardExecutionOptions,
    _DashboardSelection,
    _DeleteAssetsCommandOptions,
    _DeleteAssetsRawOptions,
    _DeleteResourceName,
    _NonDashboardDeleteOptions,
    _ResourceSummaryRow,
)
from preset_cli.cli.superset.lib import (
    DELETE_FILTER_KEYS,
    coerce_bool_option,
    fetch_with_filter_fallback,
    filter_resources_locally,
    parse_filters,
)


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
    **raw_options: object,
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

    parsed_filters = cast(
        Dict[str, object],
        parse_filters(
            command_options.filters,
            DELETE_FILTER_KEYS[resource_name],
        ),
    )
    if resource_name != RESOURCE_DASHBOARD:
        non_dashboard_options = _NonDashboardDeleteOptions(
            resource_name=cast(_DeleteResourceName, resource_name),
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
    resource_name: _DeleteResourceName,
    parsed_filters: Dict[str, object],
) -> List[_ResourceSummaryRow]:
    if resource_name == RESOURCE_DATABASE:
        return cast(
            List[_ResourceSummaryRow],
            filter_resources_locally(
                client.get_resources(resource_name),
                parsed_filters,
            ),
        )
    return cast(
        List[_ResourceSummaryRow],
        fetch_with_filter_fallback(
            lambda **kw: client.get_resources(resource_name, **kw),
            lambda: client.get_resources(resource_name),
            parsed_filters,
            f"{resource_name}s",
        ),
    )


def _delete_non_dashboard_assets(
    client: SupersetClient,
    parsed_filters: Dict[str, object],
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


def _read_dashboard_backup_data(
    client: SupersetClient,
    selection: _DashboardSelection,
) -> bytes:
    if selection.cascade_buf is None:
        backup_buf = client.export_zip(
            RESOURCE_DASHBOARD,
            list(selection.dashboard_ids),
        )
        return backup_buf.read()
    selection.cascade_buf.seek(0)
    return selection.cascade_buf.read()


def _execute_dashboard_delete_plan(
    client: SupersetClient,
    plan: _DashboardDeletePlan,
    cascade_options: DashboardCascadeOptions,
    execution_options: _DashboardExecutionOptions,
    db_passwords: Dict[str, str],
) -> None:  # pylint: disable=too-many-locals
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
    parsed_filters: Dict[str, object],
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
