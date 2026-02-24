"""
Internal data models for the delete-assets command.
"""

from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, List, Set, Tuple, TypedDict


@dataclass(frozen=True)
class _NonDashboardDeleteOptions:
    """Execution options for non-dashboard deletes."""

    resource_name: str
    dry_run: bool
    confirm: str | None
    rollback: bool
    db_passwords: Dict[str, str]


@dataclass(frozen=True)
class _DashboardCascadeOptions:
    """Cascade flags for dashboard delete operations."""

    charts: bool
    datasets: bool
    databases: bool
    skip_shared_check: bool


@dataclass(frozen=True)
class _DashboardExecutionOptions:
    """Runtime execution options for dashboard deletes."""

    dry_run: bool
    confirm: str | None
    rollback: bool


@dataclass
class _DashboardSelection:
    """Selected dashboards and optional exported backup buffer."""

    dashboards: List[Dict[str, Any]]
    dashboard_ids: Set[int]
    cascade_buf: BytesIO | None = None


@dataclass
class _CascadeDependencies:
    """UUID-level dependency graph extracted from dashboard exports."""

    chart_uuids: Set[str]
    dataset_uuids: Set[str]
    database_uuids: Set[str]
    chart_dataset_map: Dict[str, str]
    dataset_database_map: Dict[str, str]
    chart_dashboard_titles_by_uuid: Dict[str, Set[str]]


@dataclass(frozen=True)
class _CascadeResolution:
    """Resolved IDs/names/context for cascade targets."""

    ids: Dict[str, Set[int]]
    names: Dict[str, Dict[int, str]]
    chart_dashboard_context: Dict[int, List[str]]
    flags: Dict[str, bool]


@dataclass(frozen=True)
class _DeleteSummaryData:
    """Summary payload used to render delete previews/output."""

    dashboards: List[Dict[str, Any]]
    cascade_ids: Dict[str, Set[int]]
    cascade_names: Dict[str, Dict[int, str]]
    chart_dashboard_context: Dict[int, List[str]]
    shared: Dict[str, Set[str]]
    cascade_flags: Dict[str, bool]


@dataclass(frozen=True)
class _DeleteAssetsCommandOptions:
    """Raw CLI options for the delete-assets command."""

    asset_type: str
    filters: Tuple[str, ...]
    cascade_options: _DashboardCascadeOptions
    execution_options: _DashboardExecutionOptions
    db_password: Tuple[str, ...]


class _DeleteAssetsRawOptions(TypedDict):
    asset_type: str
    filters: Tuple[str, ...]
    cascade_charts: bool
    cascade_datasets: bool
    cascade_databases: bool
    dry_run: bool | str | None
    skip_shared_check: bool
    confirm: str | None
    rollback: bool
    db_password: Tuple[str, ...]


@dataclass(frozen=True)
class _DashboardDeletePlan:
    """Prepared dashboard delete plan used by dry-run and execution paths."""

    selection: _DashboardSelection
    resolution: _CascadeResolution
    summary: _DeleteSummaryData
