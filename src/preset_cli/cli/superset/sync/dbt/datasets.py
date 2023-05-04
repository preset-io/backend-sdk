"""
Sync dbt datasets/metrics to Superset.
"""

# pylint: disable=consider-using-f-string

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy.engine import create_engine
from sqlalchemy.engine.url import URL as SQLAlchemyURL
from sqlalchemy.engine.url import make_url
from yarl import URL

from preset_cli.api.clients.dbt import MetricSchema, ModelSchema
from preset_cli.api.clients.superset import SupersetClient
from preset_cli.api.operators import OneToMany
from preset_cli.cli.superset.sync.dbt.metrics import (
    get_metric_expression,
    get_metrics_for_model,
)

_logger = logging.getLogger(__name__)


def model_in_database(model: ModelSchema, url: SQLAlchemyURL) -> bool:
    """
    Return if a model is in the same database as a SQLAlchemy URI.
    """
    if url.drivername == "bigquery":
        return model["database"] == url.host

    return model["database"] == url.database


def create_dataset(
    client: SupersetClient,
    database: Dict[str, Any],
    model: ModelSchema,
) -> Dict[str, Any]:
    """
    Create a physical or virtual dataset.

    Virtual datasets are created when the table database is different from the main
    database, for systems that support cross-database queries (Trino, BigQuery, etc.)
    """
    url = make_url(database["sqlalchemy_uri"])
    if model_in_database(model, url):
        kwargs = {
            "database": database["id"],
            "schema": model["schema"],
            "table_name": model.get("alias") or model["name"],
        }
    else:
        engine = create_engine(url)
        quote = engine.dialect.identifier_preparer.quote
        source = ".".join(quote(model[key]) for key in ("database", "schema", "name"))
        kwargs = {
            "database": database["id"],
            "schema": model["schema"],
            "table_name": model.get("alias") or model["name"],
            "sql": f"SELECT * FROM {source}",
        }

    return client.create_dataset(**kwargs)


def sync_datasets(  # pylint: disable=too-many-locals, too-many-branches, too-many-arguments
    client: SupersetClient,
    models: List[ModelSchema],
    metrics: List[MetricSchema],
    database: Any,
    disallow_edits: bool,
    external_url_prefix: str,
    certification: Optional[Dict[str, Any]] = None,
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
            "table_name": model.get("alias") or model["name"],
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
                dataset = create_dataset(client, database, model)
            except Exception:  # pylint: disable=broad-except
                _logger.exception("Unable to create dataset")
                continue

        extra = {
            "unique_id": model["unique_id"],
            "depends_on": "ref('{name}')".format(**model),
            "certification": certification
            or {"details": "This table is produced by dbt"},
        }

        dataset_metrics = []
        model_metrics = {
            metric["name"]: metric for metric in get_metrics_for_model(model, metrics)
        }
        for name, metric in model_metrics.items():
            meta = metric.get("meta", {})
            kwargs = meta.pop("superset", {})
            dataset_metrics.append(
                {
                    "expression": get_metric_expression(name, model_metrics),
                    "metric_name": name,
                    "metric_type": (
                        metric.get("type")  # dbt < 1.3
                        or metric.get("calculation_method")  # dbt >= 1.3
                    ),
                    "verbose_name": metric.get("label", name),
                    "description": metric.get("description", ""),
                    "extra": json.dumps(meta),
                    **kwargs,
                },
            )

        # update dataset clearing metrics...
        update = {
            "description": model.get("description", ""),
            "extra": json.dumps(extra),
            "is_managed_externally": disallow_edits,
            "metrics": [],
        }
        update.update(model.get("meta", {}).get("superset", {}))
        if base_url:
            fragment = "!/model/{unique_id}".format(**model)
            update["external_url"] = str(base_url.with_fragment(fragment))
        client.update_dataset(dataset["id"], override_columns=True, **update)

        # ...then update metrics
        if dataset_metrics:
            update = {
                "metrics": dataset_metrics,
            }
            client.update_dataset(dataset["id"], override_columns=False, **update)

        # update column descriptions
        if columns := model.get("columns"):
            column_metadata = {column["name"]: column for column in columns}
            current_columns = client.get_dataset(dataset["id"])["columns"]
            for column in current_columns:
                name = column["column_name"]
                if name in column_metadata:
                    column["description"] = column_metadata[name].get("description", "")
                    column["verbose_name"] = column_metadata[name].get("name", "")

                # remove columns that are not part of the update payload
                for key in ("changed_on", "created_on", "type_generic"):
                    if key in column:
                        del column[key]

                # for some reason this is being sent as null sometimes
                # https://github.com/preset-io/backend-sdk/issues/163
                if "is_active" in column and column["is_active"] is None:
                    del column["is_active"]

            client.update_dataset(
                dataset["id"],
                override_columns=True,
                columns=current_columns,
            )

        datasets.append(dataset)

    return datasets
