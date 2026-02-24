"""
Display and formatting helpers for delete assets.
"""

from __future__ import annotations

import shlex
from typing import Any, Dict, Iterable, List, Set

import click

from preset_cli.cli.superset import dependency_utils as dep_utils
from preset_cli.cli.superset.asset_utils import (
    RESOURCE_CHARTS,
    RESOURCE_DATABASES,
    RESOURCE_DATASETS,
)
from preset_cli.cli.superset.delete_types import _DeleteSummaryData


def _format_restore_command(backup_path: str, asset_type: str) -> str:
    quoted_path = shlex.quote(backup_path)
    return (
        "preset-cli superset import-assets "
        f"{quoted_path} --overwrite --asset-type {asset_type}"
    )


def _format_dashboard_summary(dashboards: Iterable[Dict[str, Any]]) -> List[str]:
    lines = []
    for dashboard in dashboards:
        title = dashboard.get("dashboard_title") or dashboard.get("title") or "Unknown"
        slug = dashboard.get("slug", "n/a")
        lines.append(f"- [ID: {dashboard['id']}] {title} (slug: {slug})")
    return lines


def _format_resource_summary(
    resource_name: str,
    resources: Iterable[Dict[str, Any]],
) -> List[str]:
    lines = []
    keys = dep_utils.RESOURCE_NAME_KEYS.get(resource_name, ("name",))
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
    if shared[RESOURCE_CHARTS]:
        charts = ", ".join(sorted(shared[RESOURCE_CHARTS]))
        click.echo(
            f"  Charts ({len(shared[RESOURCE_CHARTS])}): {charts}",
        )
    if shared[RESOURCE_DATASETS]:
        datasets = ", ".join(sorted(shared[RESOURCE_DATASETS]))
        click.echo(
            f"  Datasets ({len(shared[RESOURCE_DATASETS])}): {datasets}",
        )
    if shared[RESOURCE_DATABASES]:
        databases = ", ".join(sorted(shared[RESOURCE_DATABASES]))
        click.echo(
            f"  Databases ({len(shared[RESOURCE_DATABASES])}): {databases}",
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
        RESOURCE_CHARTS,
        summary.cascade_ids[RESOURCE_CHARTS],
        summary.cascade_names.get(RESOURCE_CHARTS, {}),
        summary.cascade_flags[RESOURCE_CHARTS],
        chart_dashboard_context=summary.chart_dashboard_context,
    )
    _echo_cascade_section(
        RESOURCE_DATASETS,
        summary.cascade_ids[RESOURCE_DATASETS],
        summary.cascade_names.get(RESOURCE_DATASETS, {}),
        summary.cascade_flags[RESOURCE_DATASETS],
    )
    _echo_cascade_section(
        RESOURCE_DATABASES,
        summary.cascade_ids[RESOURCE_DATABASES],
        summary.cascade_names.get(RESOURCE_DATABASES, {}),
        summary.cascade_flags[RESOURCE_DATABASES],
    )

    _echo_shared_summary(summary.shared)
    _echo_dry_run_hint(dry_run)


def _echo_backup_restore_details(backup_path: str, resource_name: str) -> None:
    click.echo(f"\nBackup saved to: {backup_path}")
    click.echo(
        f"To restore, run: {_format_restore_command(backup_path, resource_name)}\n",
    )
