"""
Helper functions for the Superset export command.
"""

from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Set, cast

import yaml


def extract_uuid_from_asset(
    file_path: Optional[Path] = None,
    file_content: Optional[str] = None,
) -> Optional[str]:
    """
    Load YAML file and extract its UUID.
    """
    if file_path:
        with open(file_path, "r", encoding="utf-8") as content:
            file_content = content.read()

    if not file_content:
        return None

    data = yaml.load(file_content, Loader=yaml.SafeLoader)
    return data.get("uuid")


def _load_yaml(path: Path) -> Dict[str, object]:
    with open(path, "r", encoding="utf-8") as input_:
        return cast(Dict[str, object], yaml.load(input_, Loader=yaml.SafeLoader) or {})


def _get_dashboard_chart_uuids(config: Mapping[str, object]) -> Iterable[str]:
    position = config.get("position")
    if not isinstance(position, dict):
        return
    for child in position.values():
        if not isinstance(child, dict) or child.get("type") != "CHART":
            continue
        meta = child.get("meta")
        if isinstance(meta, dict) and isinstance(meta.get("uuid"), str):
            yield meta["uuid"]


def _get_dashboard_dataset_filter_uuids(config: Mapping[str, object]) -> Set[str]:
    dataset_uuids: Set[str] = set()
    metadata = config.get("metadata")
    if not isinstance(metadata, dict):
        return dataset_uuids
    native_filter_config = metadata.get("native_filter_configuration", [])
    if not isinstance(native_filter_config, list):
        return dataset_uuids
    for filter_config in native_filter_config:
        if not isinstance(filter_config, dict):
            continue
        targets = filter_config.get("targets", [])
        if not isinstance(targets, list):
            continue
        for target in targets:
            if not isinstance(target, dict):
                continue
            uuid = target.get("datasetUuid")
            if isinstance(uuid, str):
                dataset_uuids.add(uuid)
    return dataset_uuids


def _build_resource_uuid_map(
    resource_dir: Path,
    pattern: str,
) -> Dict[str, Path]:
    """
    Build UUID -> file map for resources under a directory.
    """
    resource_map: Dict[str, Path] = {}
    if not resource_dir.exists():
        return resource_map
    for resource_file in resource_dir.glob(pattern):
        if uuid := extract_uuid_from_asset(file_path=resource_file):
            resource_map[uuid] = resource_file
    return resource_map


def _copy_asset_relative_to_root(
    source: Path,
    root: Path,
    dashboard_dir: Path,
) -> None:
    """
    Copy an asset to the dashboard folder preserving export-relative path.
    """
    target = dashboard_dir / source.relative_to(root)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")


def _copy_chart_dependencies(
    root: Path,
    dashboard_dir: Path,
    chart_uuids: Set[str],
    chart_map: Dict[str, Path],
) -> Set[str]:
    """
    Copy chart files and return dataset UUIDs referenced by those charts.
    """
    dataset_uuids: Set[str] = set()
    for chart_uuid in chart_uuids:
        chart_path = chart_map.get(chart_uuid)
        if not chart_path:
            continue
        chart_config = _load_yaml(chart_path)
        dataset_uuid = chart_config.get("dataset_uuid")
        if isinstance(dataset_uuid, str):
            dataset_uuids.add(dataset_uuid)
        _copy_asset_relative_to_root(chart_path, root, dashboard_dir)
    return dataset_uuids


def _copy_dataset_dependencies(
    root: Path,
    dashboard_dir: Path,
    dataset_uuids: Set[str],
    dataset_map: Dict[str, Path],
) -> Set[str]:
    """
    Copy dataset files and return database UUIDs referenced by those datasets.
    """
    database_uuids: Set[str] = set()
    for dataset_uuid in dataset_uuids:
        dataset_path = dataset_map.get(dataset_uuid)
        if not dataset_path:
            continue
        dataset_config = _load_yaml(dataset_path)
        database_uuid = dataset_config.get("database_uuid")
        if isinstance(database_uuid, str):
            database_uuids.add(database_uuid)
        _copy_asset_relative_to_root(dataset_path, root, dashboard_dir)
    return database_uuids


def _copy_database_dependencies(
    root: Path,
    dashboard_dir: Path,
    database_uuids: Set[str],
    database_map: Dict[str, Path],
) -> None:
    """
    Copy database files to the dashboard folder.
    """
    for database_uuid in database_uuids:
        database_path = database_map.get(database_uuid)
        if not database_path:
            continue
        _copy_asset_relative_to_root(database_path, root, dashboard_dir)


def _cleanup_resource_directory(resource_dir: Path) -> None:
    """
    Remove all files/subdirs under a resource directory and then the directory itself.
    """
    if not resource_dir.exists():
        return
    for path in sorted(resource_dir.rglob("*"), reverse=True):
        if path.is_file():
            path.unlink()
            continue
        path.rmdir()
    resource_dir.rmdir()


def _restructure_single_dashboard(
    root: Path,
    dashboard_file: Path,
    chart_map: Dict[str, Path],
    dataset_map: Dict[str, Path],
    database_map: Dict[str, Path],
) -> None:
    """
    Move one dashboard and copy its dependencies into a dedicated folder.
    """
    config = _load_yaml(dashboard_file)
    dashboard_dir = dashboard_file.parent / dashboard_file.stem
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    dashboard_file.rename(dashboard_dir / "dashboard.yaml")

    chart_uuids = set(_get_dashboard_chart_uuids(config))
    dataset_uuids = _get_dashboard_dataset_filter_uuids(config)
    dataset_uuids.update(
        _copy_chart_dependencies(root, dashboard_dir, chart_uuids, chart_map),
    )
    database_uuids = _copy_dataset_dependencies(
        root,
        dashboard_dir,
        dataset_uuids,
        dataset_map,
    )
    _copy_database_dependencies(root, dashboard_dir, database_uuids, database_map)


def restructure_per_asset_folder(root: Path) -> None:
    """
    Reorganize flat export into per-dashboard subfolders.
    """
    dashboards_dir = root / "dashboards"
    if not dashboards_dir.exists():
        return

    charts_dir = root / "charts"
    datasets_dir = root / "datasets"
    databases_dir = root / "databases"
    chart_map = _build_resource_uuid_map(charts_dir, "*.yaml")
    dataset_map = _build_resource_uuid_map(datasets_dir, "**/*.yaml")
    database_map = _build_resource_uuid_map(databases_dir, "*.yaml")

    for dashboard_file in dashboards_dir.glob("*.yaml"):
        _restructure_single_dashboard(
            root,
            dashboard_file,
            chart_map,
            dataset_map,
            database_map,
        )

    for resource_dir in [charts_dir, datasets_dir, databases_dir]:
        _cleanup_resource_directory(resource_dir)
