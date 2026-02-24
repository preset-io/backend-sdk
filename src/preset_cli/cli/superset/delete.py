"""
Delete Superset assets.
"""

# pylint: disable=too-many-lines

from __future__ import annotations

import shlex
import tempfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple, cast
from zipfile import ZipFile

import click
import yaml
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.api.operators import In
from preset_cli.cli.superset.delete_types import (
    _CascadeDependencies,
    _CascadeResolution,
    _DashboardCascadeOptions,
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
from preset_cli.lib import remove_root


def _extract_uuids_from_export(buf: BytesIO) -> Dict[str, Set[str]]:
    """Extract UUID sets from a dashboard export ZIP."""
    chart_uuids, dataset_uuids, database_uuids, _, _, _ = _extract_dependency_maps(
        buf,
    )
    return {
        "charts": chart_uuids,
        "datasets": dataset_uuids,
        "databases": database_uuids,
    }


def _extract_backup_uuids_by_type(backup_data: bytes) -> Dict[str, Set[str]]:
    uuids: Dict[str, Set[str]] = {
        "dashboard": set(),
        "chart": set(),
        "dataset": set(),
        "database": set(),
    }

    with ZipFile(BytesIO(backup_data)) as bundle:
        for file_name in bundle.namelist():
            relative = remove_root(file_name)
            if not relative.endswith((".yaml", ".yml")):
                continue

            if relative.startswith("dashboards/"):
                resource_name = "dashboard"
            elif relative.startswith("charts/"):
                resource_name = "chart"
            elif relative.startswith("datasets/"):
                resource_name = "dataset"
            elif relative.startswith("databases/"):
                resource_name = "database"
            else:
                continue

            config = yaml.load(bundle.read(file_name), Loader=yaml.SafeLoader) or {}
            if uuid := config.get("uuid"):
                uuids[resource_name].add(str(uuid))

    return uuids


def _verify_rollback_restoration(
    client: SupersetClient,
    backup_data: bytes,
    expected_types: Set[str],
) -> Tuple[List[str], List[str]]:
    """
    Verify rollback restored UUIDs for expected resource types.

    Returns:
      - list of unresolved resource types (cannot verify on this Superset version)
      - list of resource types with missing UUIDs
    """
    backup_uuids = _extract_backup_uuids_by_type(backup_data)
    unresolved: List[str] = []
    missing_types: List[str] = []

    for resource_name in sorted(expected_types):
        expected_uuids = backup_uuids.get(resource_name, set())
        if not expected_uuids:
            continue

        _, _, missing_uuids, resolved = _resolve_ids(
            client,
            resource_name,
            expected_uuids,
        )
        if not resolved:
            unresolved.append(resource_name)
            continue
        if missing_uuids:
            missing_types.append(resource_name)

    return unresolved, missing_types


def _extract_dependency_maps(  # pylint: disable=too-many-locals, too-many-branches
    buf: BytesIO,
) -> Tuple[
    Set[str],
    Set[str],
    Set[str],
    Dict[str, str],
    Dict[str, str],
    Dict[str, Set[str]],
]:
    chart_uuids: Set[str] = set()
    dataset_uuids: Set[str] = set()
    database_uuids: Set[str] = set()
    chart_dataset_map: Dict[str, str] = {}
    dataset_database_map: Dict[str, str] = {}
    chart_dashboard_titles: Dict[str, Set[str]] = {}

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
            elif relative.startswith("dashboards/"):
                dashboard_title = (
                    config.get("dashboard_title") or config.get("title") or "Unknown"
                )
                position = config.get("position")
                if not isinstance(position, dict):
                    continue
                for node in position.values():
                    if not isinstance(node, dict):
                        continue
                    if node.get("type") != "CHART":
                        continue
                    meta = node.get("meta")
                    if not isinstance(meta, dict):
                        continue
                    chart_uuid = meta.get("uuid")
                    if chart_uuid:
                        chart_dashboard_titles.setdefault(str(chart_uuid), set()).add(
                            str(dashboard_title),
                        )

    return (
        chart_uuids,
        dataset_uuids,
        database_uuids,
        chart_dataset_map,
        dataset_database_map,
        chart_dashboard_titles,
    )


def _parse_db_passwords(db_password: Tuple[str, ...]) -> Dict[str, str]:
    passwords: Dict[str, str] = {}
    for pair in db_password:
        if "=" not in pair:
            raise click.ClickException(
                "Invalid --db-password value. Use the format uuid=password.",
            )
        uuid, password = pair.split("=", 1)
        passwords[uuid] = password
    return passwords


def _write_backup(backup_data: bytes) -> str:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    with tempfile.NamedTemporaryFile(
        mode="wb",
        prefix=f"preset-cli-backup-delete-{timestamp}-",
        suffix=".zip",
        delete=False,
        dir=tempfile.gettempdir(),
    ) as output:
        output.write(backup_data)
        backup_path = Path(output.name)

    # Enforce private backup file permissions where supported.
    try:
        backup_path.chmod(0o600)
    except OSError:
        pass
    return str(backup_path)


def _format_restore_command(backup_path: str, asset_type: str) -> str:
    quoted_path = shlex.quote(backup_path)
    return (
        "preset-cli superset import-assets "
        f"{quoted_path} --overwrite --asset-type {asset_type}"
    )


def _apply_db_passwords_to_backup(
    backup_data: bytes,
    db_passwords: Dict[str, str],
) -> BytesIO:
    if not db_passwords:
        buf = BytesIO(backup_data)
        buf.seek(0)
        return buf

    input_buf = BytesIO(backup_data)
    output_buf = BytesIO()
    with ZipFile(input_buf) as src, ZipFile(output_buf, "w") as dest:
        for file_name in src.namelist():
            data = src.read(file_name)
            relative = remove_root(file_name)
            if relative.startswith("databases/") and file_name.endswith(
                (".yaml", ".yml"),
            ):
                config = yaml.load(data, Loader=yaml.SafeLoader) or {}
                if uuid := config.get("uuid"):
                    if uuid in db_passwords:
                        config["password"] = db_passwords[uuid]
                        data = yaml.dump(config).encode()
            dest.writestr(file_name, data)
    output_buf.seek(0)
    return output_buf


def _dataset_db_id(dataset: Dict[str, Any]) -> int | None:
    if "database_id" in dataset:
        return dataset["database_id"]
    database = dataset.get("database")
    if isinstance(database, dict):
        return database.get("id")
    return None


def _build_uuid_map(
    client: SupersetClient,
    resource_name: str,
) -> Tuple[Dict[str, int], Dict[int, str], bool]:
    resources = client.get_resources(resource_name)
    name_keys = RESOURCE_NAME_KEYS.get(resource_name, ("name",))
    name_map: Dict[int, str] = {}
    for resource in resources:
        if "id" not in resource:
            continue
        name = next(
            (resource.get(k) for k in name_keys if resource.get(k)),
            None,
        )
        if name:
            name_map[resource["id"]] = name

    uuid_map = {
        resource["uuid"]: resource["id"]
        for resource in resources
        if "uuid" in resource and "id" in resource
    }
    if uuid_map:
        return uuid_map, name_map, True

    ids = {resource["id"] for resource in resources if "id" in resource}
    if ids:
        uuid_map = {
            str(uuid): id_ for id_, uuid in client.get_uuids(resource_name, ids).items()
        }
        if uuid_map:
            return uuid_map, name_map, True

    return {}, name_map, False


def _resolve_ids(
    client: SupersetClient,
    resource_name: str,
    uuids: Set[str],
) -> Tuple[Set[int], Dict[int, str], List[str], bool]:
    if not uuids:
        return set(), {}, [], True

    uuid_map, name_map, resolved = _build_uuid_map(client, resource_name)
    if not resolved:
        return set(), {}, list(uuids), False

    ids = {uuid_map[uuid] for uuid in uuids if uuid in uuid_map}
    missing = [uuid for uuid in uuids if uuid not in uuid_map]
    return ids, name_map, missing, True


def _format_dashboard_summary(dashboards: Iterable[Dict[str, Any]]) -> List[str]:
    lines = []
    for dashboard in dashboards:
        title = dashboard.get("dashboard_title") or dashboard.get("title") or "Unknown"
        slug = dashboard.get("slug", "n/a")
        lines.append(f"- [ID: {dashboard['id']}] {title} (slug: {slug})")
    return lines


RESOURCE_NAME_KEYS = {
    "chart": ("slice_name", "name"),
    "dataset": ("table_name", "name"),
    "database": ("database_name", "name"),
}


def _format_resource_summary(
    resource_name: str,
    resources: Iterable[Dict[str, Any]],
) -> List[str]:
    lines = []
    keys = RESOURCE_NAME_KEYS.get(resource_name, ("name",))
    for resource in resources:
        label = next(
            (resource.get(key) for key in keys if resource.get(key)),
            None,
        )
        if label:
            lines.append(f"- [ID: {resource['id']}] {label}")
        else:
            lines.append(f"- [ID: {resource['id']}]")
    return lines


def _echo_resource_summary(
    resource_name: str,
    resources: List[Dict[str, Any]],
    dry_run: bool,
) -> None:
    if dry_run:
        click.echo("No changes will be made. Assets to be deleted:\n")
    else:
        click.echo("Assets to be deleted:\n")

    title = f"{resource_name.title()}s"
    click.echo(f"{title} ({len(resources)}):")
    for line in _format_resource_summary(resource_name, resources):
        click.echo(f"  {line}")

    if dry_run:
        click.echo(
            "\nTo proceed with deletion, run with: --dry-run=false --confirm=DELETE",
        )


def _echo_delete_header(dry_run: bool) -> None:
    if dry_run:
        click.echo("No changes will be made. Assets to be deleted:\n")
        return
    click.echo("Assets to be deleted:\n")


def _echo_cascade_section(
    resource_name: str,
    ids: Set[int],
    names: Dict[int, str],
    enabled: bool,
    chart_dashboard_context: Dict[int, List[str]] | None = None,
) -> None:
    title = resource_name.title()
    if not enabled:
        click.echo(f"\n{title} (0): (not cascading)")
        return

    click.echo(f"\n{title} ({len(ids)}):")
    for resource_id in sorted(ids):
        name = names.get(resource_id)
        label = f" {name}" if name else ""
        context = ""
        if chart_dashboard_context is not None:
            dashboard_titles = chart_dashboard_context.get(resource_id, [])
            if len(dashboard_titles) == 1:
                context = f" (dashboard: {dashboard_titles[0]})"
            elif dashboard_titles:
                context = f" (dashboards: {', '.join(dashboard_titles)})"
        click.echo(f"  - [ID: {resource_id}]{label}{context}")


def _echo_shared_summary(shared: Dict[str, Set[str]]) -> None:
    if not any(shared.values()):
        return

    click.echo("\nShared (skipped):")
    if shared["charts"]:
        charts = ", ".join(sorted(shared["charts"]))
        click.echo(
            f"  Charts ({len(shared['charts'])}): {charts}",
        )
    if shared["datasets"]:
        datasets = ", ".join(sorted(shared["datasets"]))
        click.echo(
            f"  Datasets ({len(shared['datasets'])}): {datasets}",
        )
    if shared["databases"]:
        databases = ", ".join(sorted(shared["databases"]))
        click.echo(
            f"  Databases ({len(shared['databases'])}): {databases}",
        )


def _echo_dry_run_hint(dry_run: bool) -> None:
    if not dry_run:
        return

    click.echo(
        "\nTo proceed with deletion, run with: --dry-run=false --confirm=DELETE",
    )


def _echo_summary(summary: _DeleteSummaryData, dry_run: bool) -> None:
    _echo_delete_header(dry_run)

    dashboards = summary.dashboards
    click.echo(f"Dashboards ({len(dashboards)}):")
    for line in _format_dashboard_summary(dashboards):
        click.echo(f"  {line}")

    _echo_cascade_section(
        "charts",
        summary.cascade_ids["charts"],
        summary.cascade_names.get("charts", {}),
        summary.cascade_flags["charts"],
        chart_dashboard_context=summary.chart_dashboard_context,
    )
    _echo_cascade_section(
        "datasets",
        summary.cascade_ids["datasets"],
        summary.cascade_names.get("datasets", {}),
        summary.cascade_flags["datasets"],
    )
    _echo_cascade_section(
        "databases",
        summary.cascade_ids["databases"],
        summary.cascade_names.get("databases", {}),
        summary.cascade_flags["databases"],
    )

    _echo_shared_summary(summary.shared)
    _echo_dry_run_hint(dry_run)


@click.command()
@click.option(
    "--asset-type",
    required=True,
    type=click.Choice(
        ["dashboard", "chart", "dataset", "database"],
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
    cascade_options = _DashboardCascadeOptions(
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
    if resource_name != "dashboard":
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
    cascade_options: _DashboardCascadeOptions,
) -> None:
    if resource_name != "dashboard" and any(
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
    if resource_name == "dashboard":
        return _parse_db_passwords(db_password), rollback
    if resource_name != "database":
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


def _echo_backup_restore_details(backup_path: str, resource_name: str) -> None:
    click.echo(f"\nBackup saved to: {backup_path}")
    click.echo(
        f"To restore, run: {_format_restore_command(backup_path, resource_name)}\n",
    )


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
    if resource_name == "database":
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


def _rollback_non_dashboard_deletion(
    client: SupersetClient,
    resource_name: str,
    backup_data: bytes,
    db_passwords: Dict[str, str],
) -> None:
    try:
        rollback_buf = _apply_db_passwords_to_backup(
            backup_data,
            db_passwords if resource_name == "database" else {},
        )
        client.import_zip(resource_name, rollback_buf, overwrite=True)
    except Exception as exc:  # pylint: disable=broad-except
        click.echo(f"Rollback failed: {exc}")
        raise click.ClickException(
            "Rollback failed. A backup zip is available for manual restore.",
        ) from exc

    unresolved, missing_types = _verify_rollback_restoration(
        client,
        backup_data,
        {resource_name},
    )
    if unresolved:
        labels = ", ".join(unresolved)
        click.echo(
            f"Warning: unable to verify rollback for: {labels}.",
        )
    if missing_types:
        labels = ", ".join(missing_types)
        raise click.ClickException(
            f"Rollback verification failed for: {labels}. "
            "A backup zip is available for manual restore.",
        )
    click.echo("Rollback succeeded.")


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
        "dashboards",
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
    cascade_options: _DashboardCascadeOptions,
) -> _CascadeDependencies:
    dependencies = _CascadeDependencies(
        chart_uuids=set(),
        dataset_uuids=set(),
        database_uuids=set(),
        chart_dataset_map={},
        dataset_database_map={},
        chart_dashboard_titles_by_uuid={},
    )

    if cascade_options.charts:
        selection.cascade_buf = client.export_zip(
            "dashboard",
            list(selection.dashboard_ids),
        )
        (
            dependencies.chart_uuids,
            dependencies.dataset_uuids,
            dependencies.database_uuids,
            dependencies.chart_dataset_map,
            dependencies.dataset_database_map,
            dependencies.chart_dashboard_titles_by_uuid,
        ) = _extract_dependency_maps(selection.cascade_buf)

        if not cascade_options.datasets:
            dependencies.dataset_uuids = set()
            dependencies.dataset_database_map = {}
        if not cascade_options.databases:
            dependencies.database_uuids = set()

    return dependencies


def _protect_shared_dependencies(
    client: SupersetClient,
    selection: _DashboardSelection,
    dependencies: _CascadeDependencies,
    cascade_options: _DashboardCascadeOptions,
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

    other_buf = client.export_zip("dashboard", list(other_ids))
    protected = _extract_uuids_from_export(other_buf)
    shared_uuids = _compute_shared_uuids(dependencies, protected)
    _remove_shared_dependencies(dependencies, shared_uuids)
    return shared_uuids


def _empty_shared_uuids() -> Dict[str, Set[str]]:
    return {
        "charts": set(),
        "datasets": set(),
        "databases": set(),
    }


def _find_other_dashboard_ids(
    client: SupersetClient,
    dashboard_ids: Set[int],
) -> Set[int]:
    all_dashboards = client.get_resources("dashboard")
    return {dashboard["id"] for dashboard in all_dashboards} - dashboard_ids


def _compute_shared_uuids(
    dependencies: _CascadeDependencies,
    protected: Dict[str, Set[str]],
) -> Dict[str, Set[str]]:
    shared_charts = dependencies.chart_uuids & protected["charts"]
    protected_datasets = dependencies.dataset_uuids & protected["datasets"]
    protected_databases = dependencies.database_uuids & protected["databases"]

    for chart_uuid in shared_charts:
        if dataset_uuid := dependencies.chart_dataset_map.get(chart_uuid):
            protected_datasets.add(dataset_uuid)
            if database_uuid := dependencies.dataset_database_map.get(dataset_uuid):
                protected_databases.add(database_uuid)

    for dataset_uuid in protected_datasets:
        if database_uuid := dependencies.dataset_database_map.get(dataset_uuid):
            protected_databases.add(database_uuid)

    return {
        "charts": shared_charts,
        "datasets": protected_datasets,
        "databases": protected_databases,
    }


def _remove_shared_dependencies(
    dependencies: _CascadeDependencies,
    shared_uuids: Dict[str, Set[str]],
) -> None:
    dependencies.chart_uuids -= shared_uuids["charts"]
    dependencies.chart_dashboard_titles_by_uuid = {
        uuid: titles
        for uuid, titles in dependencies.chart_dashboard_titles_by_uuid.items()
        if uuid in dependencies.chart_uuids
    }
    dependencies.dataset_uuids -= shared_uuids["datasets"]
    dependencies.database_uuids -= shared_uuids["databases"]


def _resolve_cascade_targets(
    client: SupersetClient,
    dependencies: _CascadeDependencies,
    cascade_options: _DashboardCascadeOptions,
) -> _CascadeResolution:
    ids: Dict[str, Set[int]] = {
        "charts": set(),
        "datasets": set(),
        "databases": set(),
    }
    names: Dict[str, Dict[int, str]] = {}
    chart_dashboard_context: Dict[int, List[str]] = {}
    cascade_flags = {
        "charts": cascade_options.charts,
        "datasets": cascade_options.datasets,
        "databases": cascade_options.databases,
    }
    if not cascade_options.charts:
        return _CascadeResolution(
            ids=ids,
            names=names,
            chart_dashboard_context=chart_dashboard_context,
            flags=cascade_flags,
        )

    (
        ids["charts"],
        chart_names,
        chart_dashboard_context,
        missing_chart_uuids,
        charts_resolved,
    ) = _resolve_chart_targets(client, dependencies)
    dataset_result = _resolve_ids(
        client,
        "dataset",
        dependencies.dataset_uuids,
    )
    database_result = _resolve_ids(
        client,
        "database",
        dependencies.database_uuids,
    )
    ids["datasets"] = dataset_result[0]
    ids["databases"] = database_result[0]
    names = {
        "charts": chart_names,
        "datasets": dataset_result[1],
        "databases": database_result[1],
    }
    all_resolved = charts_resolved and dataset_result[3] and database_result[3]
    if not all_resolved:
        click.echo(
            "Cannot resolve cascade targets on this Superset version — "
            "skipping cascade deletion.",
        )
        ids = {
            "charts": set(),
            "datasets": set(),
            "databases": set(),
        }
    else:
        _warn_missing_uuids("chart", missing_chart_uuids)
        _warn_missing_uuids("dataset", dataset_result[2])
        _warn_missing_uuids("database", database_result[2])

    return _CascadeResolution(
        ids=ids,
        names=names,
        chart_dashboard_context=chart_dashboard_context,
        flags=cascade_flags,
    )


def _resolve_chart_targets(
    client: SupersetClient,
    dependencies: _CascadeDependencies,
) -> Tuple[Set[int], Dict[int, str], Dict[int, List[str]], List[str], bool]:
    chart_dashboard_context: Dict[int, List[str]] = {}
    chart_uuid_map, chart_names, charts_resolved = _build_uuid_map(client, "chart")
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
                "dataset",
                database_id=In(list(database_ids)),
            ),
            True,
        )
    except Exception as exc:  # pylint: disable=broad-except
        if is_filter_not_allowed_error(exc):
            return client.get_resources("dataset"), False
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
        backup_buf = client.export_zip("dashboard", list(selection.dashboard_ids))
        return backup_buf.read()
    selection.cascade_buf.seek(0)
    return selection.cascade_buf.read()


def _expected_dashboard_rollback_types(
    cascade_options: _DashboardCascadeOptions,
) -> Set[str]:
    expected_types = {"dashboard"}
    if cascade_options.charts:
        expected_types.add("chart")
    if cascade_options.datasets:
        expected_types.add("dataset")
    if cascade_options.databases:
        expected_types.add("database")
    return expected_types


def _rollback_dashboard_deletion(
    client: SupersetClient,
    backup_data: bytes,
    db_passwords: Dict[str, str],
    cascade_options: _DashboardCascadeOptions,
) -> None:
    try:
        rollback_buf = _apply_db_passwords_to_backup(
            backup_data,
            db_passwords,
        )
        client.import_zip("dashboard", rollback_buf, overwrite=True)
    except Exception as exc:  # pylint: disable=broad-except
        click.echo(f"Rollback failed: {exc}")
        raise click.ClickException(
            "Rollback failed. A backup zip is available for manual restore.",
        ) from exc

    unresolved, missing_types = _verify_rollback_restoration(
        client,
        backup_data,
        _expected_dashboard_rollback_types(cascade_options),
    )
    if unresolved:
        labels = ", ".join(unresolved)
        click.echo(
            f"Warning: unable to verify rollback for: {labels}.",
        )
    if missing_types:
        labels = ", ".join(missing_types)
        raise click.ClickException(
            f"Rollback verification failed for: {labels}. "
            "A backup zip is available for manual restore.",
        )
    click.echo("Rollback succeeded.")


def _prepare_dashboard_delete_plan(
    client: SupersetClient,
    parsed_filters: Dict[str, Any],
    cascade_options: _DashboardCascadeOptions,
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
    cascade_options: _DashboardCascadeOptions,
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
        and plan.resolution.ids["databases"]
        and not db_passwords
    ):
        raise click.ClickException(
            "Rollback requires a DB password (--db-password) when deleting databases.",
        )

    if cascade_options.databases and plan.resolution.ids["databases"]:
        _preflight_database_deletion(
            client,
            plan.resolution.ids["databases"],
            plan.resolution.ids["datasets"],
        )

    return True


def _execute_dashboard_delete_plan(
    client: SupersetClient,
    plan: _DashboardDeletePlan,
    cascade_options: _DashboardCascadeOptions,
    execution_options: _DashboardExecutionOptions,
    db_passwords: Dict[str, str],
) -> None:
    selection = plan.selection
    resolution = plan.resolution
    _echo_summary(plan.summary, dry_run=False)

    backup_data = _read_dashboard_backup_data(client, plan.selection)
    backup_path = _write_backup(backup_data)
    _echo_backup_restore_details(backup_path, "dashboard")

    failures: List[str] = []
    deleted_any = _delete_resources(
        client,
        "chart",
        resolution.ids["charts"],
        failures,
    )
    deleted_any = (
        _delete_resources(
            client,
            "dataset",
            resolution.ids["datasets"],
            failures,
        )
        or deleted_any
    )
    deleted_any = (
        _delete_resources(
            client,
            "database",
            resolution.ids["databases"],
            failures,
        )
        or deleted_any
    )
    deleted_any = (
        _delete_resources(
            client,
            "dashboard",
            selection.dashboard_ids,
            failures,
        )
        or deleted_any
    )

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
    cascade_options: _DashboardCascadeOptions,
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
