from io import BytesIO
from zipfile import ZipFile

import yaml

from preset_cli.cli.superset.asset_utils import (
    classify_asset_path,
    iter_yaml_asset_configs,
)


def test_classify_asset_path_returns_singular_and_plural_types() -> None:
    assert classify_asset_path("charts/chart.yaml") == "chart"
    assert classify_asset_path("charts/chart.yaml", plural=True) == "charts"
    assert classify_asset_path("datasets/db/schema.yaml") == "dataset"
    assert classify_asset_path("datasets/db/schema.yaml", plural=True) == "datasets"
    assert classify_asset_path("unknown/resource.yaml") is None


def test_iter_yaml_asset_configs_reads_supported_asset_yaml_entries() -> None:
    buf = BytesIO()
    with ZipFile(buf, "w") as bundle:
        bundle.writestr("bundle/charts/chart.yaml", yaml.dump({"uuid": "chart-uuid"}))
        bundle.writestr(
            "bundle/datasets/db/dataset.yaml",
            yaml.dump({"uuid": "dataset-uuid"}),
        )
        bundle.writestr("bundle/notes.txt", "not-yaml")
        bundle.writestr("bundle/custom/custom.yaml", yaml.dump({"uuid": "ignored"}))

    buf.seek(0)
    with ZipFile(buf) as bundle:
        entries = list(iter_yaml_asset_configs(bundle))

    assert entries == [
        ("chart", {"uuid": "chart-uuid"}),
        ("dataset", {"uuid": "dataset-uuid"}),
    ]
