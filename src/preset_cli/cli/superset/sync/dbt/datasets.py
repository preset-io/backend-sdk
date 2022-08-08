"""
Sync dbt datasets/etrics to Superset.
"""

# pylint: disable=consider-using-f-string

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, cast

import yaml
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.api.operators import OneToMany
from preset_cli.cli.superset.sync.dbt.metrics import MetricType, get_metric_expression

_logger = logging.getLogger(__name__)


def sync_datasets(  # pylint: disable=too-many-locals, too-many-branches
    client: SupersetClient,
    manifest_path: Path,
    database: Any,
    disallow_edits: bool,
    external_url_prefix: str,
) -> List[Any]:
    """
    Read the dbt manifest and import models as datasets with metrics.
    """
    base_url = URL(external_url_prefix) if external_url_prefix else None

    with open(manifest_path, encoding="utf-8") as input_:
        manifest = yaml.load(input_, Loader=yaml.SafeLoader)

    # extract metrics
    metrics: Dict[str, List[MetricType]] = defaultdict(list)
    for metric in manifest["metrics"].values():
        for unique_id in metric["depends_on"]["nodes"]:
            metrics[unique_id].append(cast(MetricType, metric))

    # add datasets
    datasets = []
    configs = list(manifest["sources"].values()) + list(manifest["nodes"].values())
    for config in configs:
        filters = {
            "database": OneToMany(database["id"]),
            # "schema": config["schema"],
            "table_name": config["name"],
        }
        existing = client.get_datasets(**filters)
        if len(existing) > 1:
            raise Exception("More than one dataset found")

        if existing:
            dataset = existing[0]
            _logger.info("Updating dataset %s", config["unique_id"])
        else:
            _logger.info("Creating dataset %s", config["unique_id"])
            try:
                dataset = client.create_dataset(
                    database=database["id"],
                    schema=config["schema"],
                    table_name=config["name"],
                )
            except Exception:  # pylint: disable=broad-except
                # Superset can't add tables from different BigQuery projects
                continue

        extra = {k: config[k] for k in ["resource_type", "unique_id"]}
        if config["resource_type"] == "source":
            extra["depends_on"] = "source('{schema}', '{name}')".format(**config)
        else:  # config["resource_type"] == "model"
            extra["depends_on"] = "ref('{name}')".format(**config)

        # certification info
        extra["certification"] = {
            "details": "This table is produced by dbt",
        }

        dataset_metrics = []
        if config["resource_type"] == "model":
            model_metrics = {
                metric["name"]: metric for metric in metrics[config["unique_id"]]
            }
            for name, metric in model_metrics.items():
                dataset_metrics.append(
                    {
                        "expression": get_metric_expression(name, model_metrics),
                        "metric_name": name,
                        "metric_type": metric["type"],
                        "verbose_name": name,
                        "description": metric["description"],
                        **metric["meta"],
                    },
                )

        # update dataset clearing metrics...
        update = {
            "description": config["description"],
            "extra": json.dumps(extra),
            "is_managed_externally": disallow_edits,
            "metrics": [],
        }
        update.update(config.get("meta", {}).get("superset", {}))
        if base_url:
            fragment = "!/{resource_type}/{unique_id}".format(**config)
            update["external_url"] = str(base_url.with_fragment(fragment))
        client.update_dataset(dataset["id"], **update)

        # ...then update metrics
        if dataset_metrics:
            update = {
                "metrics": dataset_metrics,
            }
            client.update_dataset(dataset["id"], **update)

        datasets.append(dataset)

    return datasets
