"""
Shared helpers for Superset asset paths and ZIP YAML traversal.
"""

from typing import Any, Dict, Iterator, Optional, Tuple
from zipfile import ZipFile

import yaml

from preset_cli.lib import remove_root

RESOURCE_DASHBOARD = "dashboard"
RESOURCE_CHART = "chart"
RESOURCE_DATASET = "dataset"
RESOURCE_DATABASE = "database"

RESOURCE_DASHBOARDS = "dashboards"
RESOURCE_CHARTS = "charts"
RESOURCE_DATASETS = "datasets"
RESOURCE_DATABASES = "databases"

YAML_EXTENSIONS = (".yaml", ".yml")
_ASSET_PATH_PREFIXES = (
    (f"{RESOURCE_DASHBOARDS}/", RESOURCE_DASHBOARD, RESOURCE_DASHBOARDS),
    (f"{RESOURCE_CHARTS}/", RESOURCE_CHART, RESOURCE_CHARTS),
    (f"{RESOURCE_DATASETS}/", RESOURCE_DATASET, RESOURCE_DATASETS),
    (f"{RESOURCE_DATABASES}/", RESOURCE_DATABASE, RESOURCE_DATABASES),
)


def classify_asset_path(relative_path: str, plural: bool = False) -> Optional[str]:
    """
    Return the asset type for a relative export path.
    """
    for prefix, singular_name, plural_name in _ASSET_PATH_PREFIXES:
        if relative_path.startswith(prefix):
            return plural_name if plural else singular_name
    return None


def iter_yaml_asset_configs(bundle: ZipFile) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """
    Yield (singular_asset_type, yaml_config) from supported YAML asset entries.
    """
    for file_name in bundle.namelist():
        relative = remove_root(file_name)
        if not relative.endswith(YAML_EXTENSIONS):
            continue
        asset_type = classify_asset_path(relative)
        if not asset_type:
            continue
        config = yaml.load(bundle.read(file_name), Loader=yaml.SafeLoader) or {}
        yield asset_type, config
