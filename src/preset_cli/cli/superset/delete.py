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
from typing import Any, Dict, Iterable, List, Set, Tuple
from zipfile import ZipFile

import click
import yaml
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.api.operators import In
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


def _echo_summary(  # pylint: disable=too-many-arguments, too-many-branches, too-many-locals
    dashboards: List[Dict[str, Any]],
    chart_ids: Set[int],
    dataset_ids: Set[int],
    database_ids: Set[int],
    cascade_names: Dict[str, Dict[int, str]],
    chart_dashboard_context: Dict[int, List[str]],
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
            name = cascade_names.get("charts", {}).get(cid)
            label = f" {name}" if name else ""
            dashboard_titles = chart_dashboard_context.get(cid, [])
            context = ""
            if dashboard_titles:
                if len(dashboard_titles) == 1:
                    context = f" (dashboard: {dashboard_titles[0]})"
                else:
                    joined = ", ".join(dashboard_titles)
                    context = f" (dashboards: {joined})"
            click.echo(f"  - [ID: {cid}]{label}{context}")
    else:
        click.echo("\nCharts (0): (not cascading)")

    if cascade_flags["datasets"]:
        click.echo(f"\nDatasets ({len(dataset_ids)}):")
        for did in sorted(dataset_ids):
            name = cascade_names.get("datasets", {}).get(did)
            label = f" {name}" if name else ""
            click.echo(f"  - [ID: {did}]{label}")
    else:
        click.echo("\nDatasets (0): (not cascading)")

    if cascade_flags["databases"]:
        click.echo(f"\nDatabases ({len(database_ids)}):")
        for bid in sorted(database_ids):
            name = cascade_names.get("databases", {}).get(bid)
            label = f" {name}" if name else ""
            click.echo(f"  - [ID: {bid}]{label}")
    else:
        click.echo("\nDatabases (0): (not cascading)")

    if any(shared.values()):
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

    if dry_run:
        click.echo(
            "\nTo proceed with deletion, run with: --dry-run=false --confirm=DELETE",
        )


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
def delete_assets(  # pylint: disable=too-many-arguments, too-many-locals
    ctx: click.core.Context,
    asset_type: str,
    filters: Tuple[str, ...],
    cascade_charts: bool,
    cascade_datasets: bool,
    cascade_databases: bool,
    dry_run: bool,
    skip_shared_check: bool,
    confirm: str | None,
    rollback: bool,
    db_password: Tuple[str, ...],
) -> None:
    """
    Delete assets by filters.
    """
    resource_name = asset_type.lower()
    if dry_run is None:
        dry_run = True
    dry_run = coerce_bool_option(dry_run, "dry_run")
    db_passwords: Dict[str, str] = {}

    if resource_name != "dashboard":
        if any(
            [cascade_charts, cascade_datasets, cascade_databases, skip_shared_check],
        ):
            raise click.UsageError(
                "Cascade options are only supported for dashboard assets.",
            )
        if resource_name == "database":
            db_passwords = _parse_db_passwords(db_password)
            if rollback and not db_passwords:
                click.echo(
                    "Warning: rollback for database deletes requires "
                    "--db-password; proceeding without rollback.",
                )
                rollback = False
    else:
        db_passwords = _parse_db_passwords(db_password)
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

    parsed_filters = parse_filters(filters, DELETE_FILTER_KEYS[resource_name])
    if resource_name != "dashboard":
        _delete_non_dashboard_assets(
            client,
            resource_name,
            parsed_filters,
            dry_run,
            confirm,
            rollback,
            db_passwords,
        )
        return

    _delete_dashboard_assets(
        client,
        parsed_filters,
        dry_run,
        confirm,
        cascade_charts,
        cascade_datasets,
        cascade_databases,
        skip_shared_check,
        rollback,
        db_passwords,
    )


def _delete_non_dashboard_assets(  # pylint: disable=too-many-arguments, too-many-locals, too-many-branches
    client: SupersetClient,
    resource_name: str,
    parsed_filters: Dict[str, Any],
    dry_run: bool,
    confirm: str | None,
    rollback: bool,
    db_passwords: Dict[str, str],
) -> None:
    if resource_name == "database":
        resources = filter_resources_locally(
            client.get_resources(resource_name),
            parsed_filters,
        )
    else:
        resources = fetch_with_filter_fallback(
            lambda **kw: client.get_resources(resource_name, **kw),
            lambda: client.get_resources(resource_name),
            parsed_filters,
            f"{resource_name}s",
        )
    if not resources:
        click.echo(f"No {resource_name}s match the specified filters.")
        return

    resource_ids = {resource["id"] for resource in resources}
    if dry_run:
        _echo_resource_summary(resource_name, resources, dry_run=True)
        return

    if confirm != "DELETE":
        click.echo(
            "Deletion aborted. Pass --confirm=DELETE to proceed with deletion.",
        )
        return

    _echo_resource_summary(resource_name, resources, dry_run=False)

    # Pre-delete backup for manual restore (or rollback when enabled).
    backup_buf = client.export_zip(resource_name, sorted(resource_ids))
    backup_data = backup_buf.read()
    backup_path = _write_backup(backup_data)
    click.echo(f"\nBackup saved to: {backup_path}")
    click.echo(
        f"To restore, run: {_format_restore_command(backup_path, resource_name)}\n",
    )

    resource_failures: List[str] = []
    deleted_any = False

    for resource_id in sorted(resource_ids):
        try:
            client.delete_resource(resource_name, resource_id)
            deleted_any = True
        except Exception as exc:  # pylint: disable=broad-except
            resource_failures.append(f"{resource_name}:{resource_id} ({exc})")

    if resource_failures:
        click.echo("Some deletions failed:")
        for failure in resource_failures:
            click.echo(f"  {failure}")
        if rollback and deleted_any:
            click.echo("\nBest-effort rollback attempted...")
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

        raise click.ClickException(
            "Deletion completed with failures. See errors above.",
        )


def _delete_dashboard_assets(  # pylint: disable=too-many-arguments, too-many-locals, too-many-branches, too-many-statements
    client: SupersetClient,
    parsed_filters: Dict[str, Any],
    dry_run: bool,
    confirm: str | None,
    cascade_charts: bool,
    cascade_datasets: bool,
    cascade_databases: bool,
    skip_shared_check: bool,
    rollback: bool,
    db_passwords: Dict[str, str],
) -> None:
    dashboards = fetch_with_filter_fallback(
        client.get_dashboards,
        client.get_dashboards,
        parsed_filters,
        "dashboards",
    )
    if not dashboards:
        click.echo("No dashboards match the specified filters.")
        return

    dashboard_ids = {dashboard["id"] for dashboard in dashboards}
    chart_uuids: Set[str] = set()
    dataset_uuids: Set[str] = set()
    database_uuids: Set[str] = set()
    chart_dataset_map: Dict[str, str] = {}
    dataset_database_map: Dict[str, str] = {}
    chart_dashboard_titles_by_uuid: Dict[str, Set[str]] = {}
    cascade_buf: BytesIO | None = None

    if cascade_charts:
        cascade_buf = client.export_zip("dashboard", list(dashboard_ids))
        (
            chart_uuids,
            dataset_uuids,
            database_uuids,
            chart_dataset_map,
            dataset_database_map,
            chart_dashboard_titles_by_uuid,
        ) = _extract_dependency_maps(cascade_buf)

        if not cascade_datasets:
            dataset_uuids = set()
            dataset_database_map = {}
        if not cascade_databases:
            database_uuids = set()

    shared_uuids: Dict[str, Set[str]] = {
        "charts": set(),
        "datasets": set(),
        "databases": set(),
    }
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
            chart_dashboard_titles_by_uuid = {
                uuid: titles
                for uuid, titles in chart_dashboard_titles_by_uuid.items()
                if uuid in chart_uuids
            }
            dataset_uuids -= shared_uuids["datasets"]
            database_uuids -= shared_uuids["databases"]
    elif cascade_charts and skip_shared_check:
        click.echo(
            "Shared dependency check skipped — cascade targets may be used by other dashboards",
        )

    chart_ids: Set[int] = set()
    dataset_ids: Set[int] = set()
    database_ids: Set[int] = set()
    chart_dashboard_context: Dict[int, List[str]] = {}
    cascade_names: Dict[str, Dict[int, str]] = {}

    cascade_flags = {
        "charts": cascade_charts,
        "datasets": cascade_datasets,
        "databases": cascade_databases,
    }

    if cascade_charts:
        chart_uuid_map, chart_names, charts_resolved = _build_uuid_map(client, "chart")
        if charts_resolved:
            chart_ids = {
                chart_uuid_map[chart_uuid]
                for chart_uuid in chart_uuids
                if chart_uuid in chart_uuid_map
            }
            missing_chart_uuids = [
                chart_uuid
                for chart_uuid in chart_uuids
                if chart_uuid not in chart_uuid_map
            ]
            for chart_uuid, titles in chart_dashboard_titles_by_uuid.items():
                chart_id = chart_uuid_map.get(chart_uuid)
                if chart_id is None:
                    continue
                if chart_id not in chart_ids:
                    continue
                chart_dashboard_context[chart_id] = sorted(titles)
        else:
            chart_ids = set()
            missing_chart_uuids = list(chart_uuids)
        dataset_ids, dataset_names, missing_dataset_uuids, datasets_resolved = (
            _resolve_ids(
                client,
                "dataset",
                dataset_uuids,
            )
        )
        database_ids, database_names, missing_database_uuids, databases_resolved = (
            _resolve_ids(
                client,
                "database",
                database_uuids,
            )
        )
        cascade_names = {
            "charts": chart_names,
            "datasets": dataset_names,
            "databases": database_names,
        }

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
            cascade_names,
            chart_dashboard_context,
            shared_uuids,
            cascade_flags,
            dry_run=True,
        )
        return

    if confirm != "DELETE":
        click.echo(
            "Deletion aborted. Pass --confirm=DELETE to proceed with deletion.",
        )
        return

    if rollback and cascade_databases and database_ids and not db_passwords:
        raise click.ClickException(
            "Rollback requires a DB password (--db-password) when deleting databases.",
        )

    if cascade_databases and database_ids:
        filtered_by_db = False
        try:
            datasets = client.get_resources(
                "dataset",
                database_id=In(list(database_ids)),
            )
            filtered_by_db = True
        except Exception as exc:  # pylint: disable=broad-except
            if is_filter_not_allowed_error(exc):
                datasets = client.get_resources("dataset")
            else:
                raise click.ClickException(
                    f"Failed to preflight datasets ({exc}).",
                ) from exc

        if not filtered_by_db:
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
            datasets = filtered_datasets

        extra = [
            dataset for dataset in datasets if dataset.get("id") not in dataset_ids
        ]
        if extra:
            extra_ids = ", ".join(str(dataset.get("id")) for dataset in extra)
            raise click.ClickException(
                "Aborting deletion: databases have datasets not in cascade set. "
                f"Extra dataset IDs: {extra_ids}",
            )

    _echo_summary(
        dashboards,
        chart_ids,
        dataset_ids,
        database_ids,
        cascade_names,
        chart_dashboard_context,
        shared_uuids,
        cascade_flags,
        dry_run=False,
    )

    # Pre-delete backup — always-on, near-zero cost (reuses cascade export)
    if cascade_buf is not None:
        cascade_buf.seek(0)
        backup_data = cascade_buf.read()
    else:
        backup_buf = client.export_zip("dashboard", list(dashboard_ids))
        backup_data = backup_buf.read()
    backup_path = _write_backup(backup_data)
    click.echo(f"\nBackup saved to: {backup_path}")
    click.echo(
        f"To restore, run: {_format_restore_command(backup_path, 'dashboard')}\n",
    )

    failures: List[str] = []
    deleted_any = False

    def _delete(resource_name: str, resource_ids: Iterable[int]) -> None:
        nonlocal deleted_any
        for resource_id in sorted(resource_ids):
            try:
                client.delete_resource(resource_name, resource_id)
                deleted_any = True
            except Exception as exc:  # pylint: disable=broad-except
                failures.append(f"{resource_name}:{resource_id} ({exc})")

    # Delete dependencies first, then dashboards.
    _delete("chart", chart_ids)
    _delete("dataset", dataset_ids)
    _delete("database", database_ids)
    _delete("dashboard", dashboard_ids)

    if failures:
        click.echo("Some deletions failed:")
        for failure in failures:
            click.echo(f"  {failure}")

        if rollback and deleted_any:
            click.echo("\nBest-effort rollback attempted...")
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

            expected_types = {"dashboard"}
            if cascade_charts:
                expected_types.add("chart")
            if cascade_datasets:
                expected_types.add("dataset")
            if cascade_databases:
                expected_types.add("database")

            unresolved, missing_types = _verify_rollback_restoration(
                client,
                backup_data,
                expected_types,
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
        raise click.ClickException(
            "Deletion completed with failures. See errors above.",
        )
