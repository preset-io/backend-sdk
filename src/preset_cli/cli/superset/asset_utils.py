"""
Shared helpers for Superset asset paths and ZIP YAML traversal.
"""

from typing import Dict, Iterator, Literal, Optional, Tuple, cast, overload
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

AssetSingularName = Literal["dashboard", "chart", "dataset", "database"]
AssetPluralName = Literal["dashboards", "charts", "datasets", "databases"]
YamlAssetConfig = Dict[str, object]

YAML_EXTENSIONS = (".yaml", ".yml")
_ASSET_PATH_PREFIXES: Tuple[Tuple[str, AssetSingularName, AssetPluralName], ...] = (
    ("dashboards/", "dashboard", "dashboards"),
    ("charts/", "chart", "charts"),
    ("datasets/", "dataset", "datasets"),
    ("databases/", "database", "databases"),
)


@overload
def classify_asset_path(
    relative_path: str,
    plural: Literal[False] = False,
) -> Optional[AssetSingularName]: ...


@overload
def classify_asset_path(
    relative_path: str,
    plural: Literal[True],
) -> Optional[AssetPluralName]: ...


def classify_asset_path(relative_path: str, plural: bool = False) -> Optional[str]:
    """
    Return the asset type for a relative export path.
    """
    for prefix, singular_name, plural_name in _ASSET_PATH_PREFIXES:
        if relative_path.startswith(prefix):
            return plural_name if plural else singular_name
    return None


def iter_yaml_asset_configs(
    bundle: ZipFile,
) -> Iterator[Tuple[AssetSingularName, YamlAssetConfig]]:
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
        if not isinstance(config, dict):
            continue
        yield cast(AssetSingularName, asset_type), cast(YamlAssetConfig, config)
