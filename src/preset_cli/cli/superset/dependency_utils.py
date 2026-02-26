"""
Reusable dependency and UUID resolution helpers for Superset CLI commands.
"""

from io import BytesIO
from typing import Any, Dict, List, Set, Tuple
from zipfile import ZipFile

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.asset_utils import (
    RESOURCE_CHART,
    RESOURCE_CHARTS,
    RESOURCE_DATABASE,
    RESOURCE_DATABASES,
    RESOURCE_DASHBOARD,
    RESOURCE_DATASET,
    RESOURCE_DATASETS,
    iter_yaml_asset_configs,
)
from preset_cli.cli.superset.delete_types import CascadeDependencies

RESOURCE_NAME_KEYS = {
    RESOURCE_CHART: ("slice_name", "name"),
    RESOURCE_DATASET: ("table_name", "name"),
    RESOURCE_DATABASE: ("database_name", "name"),
}


def extract_backup_uuids_by_type(backup_data: bytes) -> Dict[str, Set[str]]:
    uuids: Dict[str, Set[str]] = {
        RESOURCE_DASHBOARD: set(),
        RESOURCE_CHART: set(),
        RESOURCE_DATASET: set(),
        RESOURCE_DATABASE: set(),
    }

    with ZipFile(BytesIO(backup_data)) as bundle:
        for resource_name, config in iter_yaml_asset_configs(bundle):
            if uuid := config.get("uuid"):
                uuids[resource_name].add(str(uuid))

    return uuids


def extract_dependency_maps(
    buf: BytesIO,
) -> CascadeDependencies:
    chart_uuids: Set[str] = set()
    dataset_uuids: Set[str] = set()
    database_uuids: Set[str] = set()
    chart_dataset_map: Dict[str, str] = {}
    dataset_database_map: Dict[str, str] = {}
    chart_dashboard_titles: Dict[str, Set[str]] = {}

    with ZipFile(buf) as bundle:
        for resource_name, config in iter_yaml_asset_configs(bundle):
            if resource_name == RESOURCE_CHART:
                _collect_chart_dependencies(
                    config,
                    chart_uuids,
                    dataset_uuids,
                    chart_dataset_map,
                )
                continue
            if resource_name == RESOURCE_DATASET:
                _collect_dataset_dependencies(
                    config,
                    dataset_uuids,
                    database_uuids,
                    dataset_database_map,
                )
                continue
            if resource_name == RESOURCE_DATABASE:
                _collect_database_dependencies(config, database_uuids)
                continue
            if resource_name == RESOURCE_DASHBOARD:
                _collect_dashboard_chart_context(
                    config,
                    chart_dashboard_titles,
                )

    return CascadeDependencies(
        chart_uuids=chart_uuids,
        dataset_uuids=dataset_uuids,
        database_uuids=database_uuids,
        chart_dataset_map=chart_dataset_map,
        dataset_database_map=dataset_database_map,
        chart_dashboard_titles_by_uuid=chart_dashboard_titles,
    )


def _collect_chart_dependencies(
    config: Dict[str, Any],
    chart_uuids: Set[str],
    dataset_uuids: Set[str],
    chart_dataset_map: Dict[str, str],
) -> None:
    if uuid := config.get("uuid"):
        chart_uuids.add(uuid)
        if dataset_uuid := config.get("dataset_uuid"):
            chart_dataset_map[uuid] = dataset_uuid
            dataset_uuids.add(dataset_uuid)


def _collect_dataset_dependencies(
    config: Dict[str, Any],
    dataset_uuids: Set[str],
    database_uuids: Set[str],
    dataset_database_map: Dict[str, str],
) -> None:
    if uuid := config.get("uuid"):
        dataset_uuids.add(uuid)
        if database_uuid := config.get("database_uuid"):
            dataset_database_map[uuid] = database_uuid
            database_uuids.add(database_uuid)


def _collect_database_dependencies(
    config: Dict[str, Any],
    database_uuids: Set[str],
) -> None:
    if uuid := config.get("uuid"):
        database_uuids.add(uuid)


def _collect_dashboard_chart_context(
    config: Dict[str, Any],
    chart_dashboard_titles: Dict[str, Set[str]],
) -> None:
    dashboard_title = config.get("dashboard_title") or config.get("title") or "Unknown"
    position = config.get("position")
    if not isinstance(position, dict):
        return

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


def build_uuid_map(
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
            (resource.get(key) for key in name_keys if resource.get(key)),
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


def resolve_ids(
    client: SupersetClient,
    resource_name: str,
    uuids: Set[str],
) -> Tuple[Set[int], Dict[int, str], List[str], bool]:
    if not uuids:
        return set(), {}, [], True

    uuid_map, name_map, resolved = build_uuid_map(client, resource_name)
    if not resolved:
        return set(), {}, list(uuids), False

    ids = {uuid_map[uuid] for uuid in uuids if uuid in uuid_map}
    missing = [uuid for uuid in uuids if uuid not in uuid_map]
    return ids, name_map, missing, True


def compute_shared_uuids(
    dependencies: CascadeDependencies,
    protected: Dict[str, Set[str]],
) -> Dict[str, Set[str]]:
    shared_charts = dependencies.chart_uuids & protected[RESOURCE_CHARTS]
    protected_datasets = dependencies.dataset_uuids & protected[RESOURCE_DATASETS]
    protected_databases = dependencies.database_uuids & protected[RESOURCE_DATABASES]

    for chart_uuid in shared_charts:
        if dataset_uuid := dependencies.chart_dataset_map.get(chart_uuid):
            protected_datasets.add(dataset_uuid)
            if database_uuid := dependencies.dataset_database_map.get(dataset_uuid):
                protected_databases.add(database_uuid)

    for dataset_uuid in protected_datasets:
        if database_uuid := dependencies.dataset_database_map.get(dataset_uuid):
            protected_databases.add(database_uuid)

    return {
        RESOURCE_CHARTS: shared_charts,
        RESOURCE_DATASETS: protected_datasets,
        RESOURCE_DATABASES: protected_databases,
    }
