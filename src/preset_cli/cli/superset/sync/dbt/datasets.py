"""
Sync dbt datasets/etrics to Superset.
"""

# pylint: disable=consider-using-f-string

import json
import logging
from typing import Any, List

from yarl import URL

from preset_cli.api.clients.dbt import MetricSchema, ModelSchema
from preset_cli.api.clients.superset import SupersetClient
from preset_cli.api.operators import OneToMany
from preset_cli.cli.superset.sync.dbt.metrics import get_metric_expression

_logger = logging.getLogger(__name__)


def sync_datasets(  # pylint: disable=too-many-locals, too-many-branches, too-many-arguments
    client: SupersetClient,
    models: List[ModelSchema],
    metrics: List[MetricSchema],
    database: Any,
    disallow_edits: bool,
    external_url_prefix: str,
) -> List[Any]:
    """
    Read the dbt manifest and import models as datasets with metrics.
    """
    base_url = URL(external_url_prefix) if external_url_prefix else None

    # add datasets
    datasets = []
    for model in models:
        filters = {
            "database": OneToMany(database["id"]),
            "schema": model["schema"],
            "table_name": model["name"],
        }
        existing = client.get_datasets(**filters)
        if len(existing) > 1:
            raise Exception("More than one dataset found")

        if existing:
            dataset = existing[0]
            _logger.info("Updating dataset %s", model["unique_id"])
        else:
            _logger.info("Creating dataset %s", model["unique_id"])
            try:
                dataset = client.create_dataset(
                    database=database["id"],
                    schema=model["schema"],
                    table_name=model["name"],
                )
            except Exception:  # pylint: disable=broad-except
                # Superset can't add tables from different BigQuery projects
                continue

        extra = {
            "unique_id": model["unique_id"],
            "depends_on": "ref('{name}')".format(**model),
            "certification": {
                "details": "This table is produced by dbt",
            },
        }

        dataset_metrics = []
        model_metrics = {
            metric["name"]: metric
            for metric in metrics
            if model["unique_id"] in metric["depends_on"]
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
            "description": model["description"],
            "extra": json.dumps(extra),
            "is_managed_externally": disallow_edits,
            "metrics": [],
        }
        update.update(model.get("meta", {}).get("superset", {}))
        if base_url:
            fragment = "!/model/{unique_id}".format(**model)
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
