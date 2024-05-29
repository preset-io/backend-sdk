"""
Sync dbt datasets/metrics to Superset.
"""

# pylint: disable=consider-using-f-string

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.engine.url import URL as SQLAlchemyURL
from sqlalchemy.engine.url import make_url
from yarl import URL

from preset_cli.api.clients.dbt import ModelSchema
from preset_cli.api.clients.superset import SupersetClient, SupersetMetricDefinition
from preset_cli.api.operators import OneToMany
from preset_cli.cli.superset.sync.dbt.lib import create_engine_with_check
from preset_cli.exceptions import CLIError, SupersetError
from preset_cli.lib import raise_cli_errors

DEFAULT_CERTIFICATION = {"details": "This table is produced by dbt"}

_logger = logging.getLogger(__name__)


def model_in_database(model: ModelSchema, url: SQLAlchemyURL) -> bool:
    """
    Return if a model is in the same database as a SQLAlchemy URI.
    """
    if url.drivername == "bigquery":
        return model["database"] == url.host

    return model["database"] == url.database


def clean_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove incompatbile columns from metatada to create/update a column/metric.
    """
    for key in (
        "autoincrement",
        "changed_on",
        "comment",
        "created_on",
        "default",
        "name",
        "nullable",
        "type_generic",
        "precision",
        "scale",
        "max_length",
        "info",
    ):
        if key in metadata:
            del metadata[key]

    return metadata


@raise_cli_errors
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
    kwargs = {
        "database": database["id"],
        "catalog": model["database"],
        "schema": model["schema"],
        "table_name": model.get("alias") or model["name"],
    }
    try:
        # try to create dataset with catalog
        return client.create_dataset(**kwargs)
    except SupersetError as ex:
        if not no_catalog_support(ex):
            raise ex
        del kwargs["catalog"]

    url = make_url(database["sqlalchemy_uri"])
    if not model_in_database(model, url):
        engine = create_engine_with_check(url)
        quote = engine.dialect.identifier_preparer.quote
        source = ".".join(quote(model[key]) for key in ("database", "schema", "name"))
        kwargs["sql"] = f"SELECT * FROM {source}"

    return client.create_dataset(**kwargs)


def no_catalog_support(ex: SupersetError) -> bool:
    """
    Return if the error is due to a lack of catalog support.

    The errors payload looks like this:

        [
            {
                "message": json.dumps({"message": {"catalog": ["Unknown field."]}}),
                "error_type": "UNKNOWN_ERROR",
                "level": ErrorLevel.ERROR,
            },
        ]

    """
    for error in ex.errors:
        try:
            message = json.loads(error["message"])
            if "Unknown field." in message["message"]["catalog"]:
                return True
        except Exception:  # pylint: disable=broad-except
            pass

    return False


def get_or_create_dataset(
    client: SupersetClient,
    model: ModelSchema,
    database: Any,
) -> Dict[str, Any]:
    """
    Returns the existing dataset or creates a new one.
    """
    filters = {
        "database": OneToMany(database["id"]),
        "schema": model["schema"],
        "table_name": model.get("alias") or model["name"],
    }
    existing = client.get_datasets(**filters)

    if len(existing) > 1:
        raise CLIError("More than one dataset found", 1)

    if existing:
        dataset = existing[0]
        _logger.info("Updating dataset %s", model["unique_id"])
        return client.get_dataset(dataset["id"])

    _logger.info("Creating dataset %s", model["unique_id"])
    try:
        dataset = create_dataset(client, database, model)
        return client.get_dataset(dataset["id"])
    except Exception as excinfo:
        _logger.exception("Unable to create dataset")
        raise CLIError("Unable to create dataset", 1) from excinfo


def get_certification_info(
    model_kwargs: Dict[str, Any],
    certification: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Returns the certification information for a dataset.
    """
    try:
        certification_details = model_kwargs["extra"].pop("certification")
    except KeyError:
        certification_details = certification or DEFAULT_CERTIFICATION
    return certification_details


def compute_metrics(
    dataset_metrics: List[Any],
    dbt_metrics: List[Any],
    reload_columns: bool,
    merge_metadata: bool,
) -> List[Any]:
    """
    Compute the final list of metrics that should be used to update the dataset

    reload_columns (default): dbt data synced & Superset-only metadata deleted
    merge_metadata: dbt data synced & Superset-only metadata preserved
    if both are false: Superset metadata preserved & dbt-only metadata synced
    """
    current_dataset_metrics = {
        metric["metric_name"]: metric for metric in dataset_metrics
    }
    model_metrics = {metric["metric_name"]: metric for metric in dbt_metrics}
    final_dataset_metrics = []

    for name, metric_definition in model_metrics.items():
        if reload_columns or merge_metadata or name not in current_dataset_metrics:
            if name in current_dataset_metrics:
                metric_definition["id"] = current_dataset_metrics[name]["id"]
            final_dataset_metrics.append(metric_definition)

    # Preserving Superset metadata
    if not reload_columns:
        for name, metric in current_dataset_metrics.items():
            if not merge_metadata or name not in model_metrics:
                # remove data that is not part of the update payload
                metric = clean_metadata(metric)
                final_dataset_metrics.append(metric)

    return final_dataset_metrics


def compute_columns(
    dataset_columns: List[Any],
    refreshed_columns_list: List[Any],
) -> List[Any]:
    """
    Refresh the list of columns preserving existing configurations
    """
    final_dataset_columns = []

    current_dataset_columns = {
        column["column_name"]: column for column in dataset_columns
    }
    refreshed_columns = {
        column["column_name"]: column for column in refreshed_columns_list
    }
    for name, column in refreshed_columns.items():
        if name in current_dataset_columns:
            cleaned_column = clean_metadata(current_dataset_columns[name])
            final_dataset_columns.append(cleaned_column)
        else:
            cleaned_column = clean_metadata(column)
            final_dataset_columns.append(cleaned_column)

    return final_dataset_columns


def compute_columns_metadata(
    dbt_columns: List[Any],
    dataset_columns: List[Any],
    reload_columns: bool,
    merge_metadata: bool,
) -> List[Any]:
    """
    Adds dbt metadata to dataset columns.

    reload_columns (default): dbt data synced & Superset-only metadata deleted
    merge_metadata: dbt data synced & Superset-only metadata preserved
    if both are false: Superset metadata preserved & dbt-only metadata synced
    """
    dbt_metadata = {
        column["name"]: {
            key: column[key] for key in ("description", "meta") if key in column
        }
        for column in dbt_columns
    }
    for column, definition in dbt_metadata.items():
        dbt_metadata[column]["verbose_name"] = column
        for key, value in definition.pop("meta", {}).get("superset", {}).items():
            dbt_metadata[column][key] = value

    for column in dataset_columns:
        name = column["column_name"]
        if name in dbt_metadata:
            for key, value in dbt_metadata[name].items():
                if reload_columns or merge_metadata or not column.get(key):
                    column[key] = value

        # remove data that is not part of the update payload
        column = clean_metadata(column)

        # for some reason this is being sent as null sometimes
        # https://github.com/preset-io/backend-sdk/issues/163
        if "is_active" in column and column["is_active"] is None:
            del column["is_active"]

    return dataset_columns


def compute_dataset_metadata(  # pylint: disable=too-many-arguments
    model: Dict[str, Any],
    certification: Optional[Dict[str, Any]],
    disallow_edits: bool,
    final_dataset_metrics: List[Any],
    base_url: Optional[URL],
    final_dataset_columns: List[Any],
) -> Dict[str, Any]:
    """
    Returns the dataset metadata based on the model information
    """
    # load Superset-specific metadata from dbt model definition (model.meta.superset)
    model_kwargs = model.get("meta", {}).pop("superset", {})
    certification_details = get_certification_info(model_kwargs, certification)
    extra = {
        "unique_id": model["unique_id"],
        "depends_on": "ref('{name}')".format(**model),
        **model_kwargs.pop(
            "extra",
            {},
        ),
    }
    if certification_details:
        extra["certification"] = certification_details

    # update dataset metadata
    update = {
        "description": model.get("description", ""),
        "extra": json.dumps(extra),
        "is_managed_externally": disallow_edits,
        "metrics": final_dataset_metrics,
        **model_kwargs,  # include additional model metadata defined in model.meta.superset
    }
    if base_url:
        fragment = "!/model/{unique_id}".format(**model)
        update["external_url"] = str(base_url.with_fragment(fragment))
    if final_dataset_columns:
        update["columns"] = final_dataset_columns

    return update


def sync_datasets(  # pylint: disable=too-many-locals, too-many-arguments
    client: SupersetClient,
    models: List[ModelSchema],
    metrics: Dict[str, List[SupersetMetricDefinition]],
    database: Any,
    disallow_edits: bool,
    external_url_prefix: str,
    certification: Optional[Dict[str, Any]] = None,
    reload_columns: bool = True,
    merge_metadata: bool = False,
) -> Tuple[List[Any], List[str]]:
    """
    Read the dbt manifest and import models as datasets with metrics.
    """
    base_url = URL(external_url_prefix) if external_url_prefix else None
    datasets = []
    failed_datasets = []

    for model in models:
        # get corresponding dataset
        try:
            dataset = get_or_create_dataset(client, model, database)
        except CLIError:
            failed_datasets.append(model["unique_id"])
            continue

        # compute metrics
        final_dataset_metrics = compute_metrics(
            dataset["metrics"],
            metrics.get(model["unique_id"], []),
            reload_columns,
            merge_metadata,
        )

        # compute columns
        final_dataset_columns = []
        if not reload_columns:
            refreshed_columns_list = client.get_refreshed_dataset_columns(dataset["id"])
            final_dataset_columns = compute_columns(
                dataset["columns"],
                refreshed_columns_list,
            )

        # compute update payload
        update = compute_dataset_metadata(
            model,
            certification,
            disallow_edits,
            final_dataset_metrics,
            base_url,
            final_dataset_columns,
        )

        try:
            client.update_dataset(
                dataset["id"], override_columns=reload_columns, **update
            )
        except SupersetError:
            failed_datasets.append(model["unique_id"])
            continue

        # update column metadata
        if dbt_columns := model.get("columns"):
            current_dataset_columns = client.get_dataset(dataset["id"])["columns"]
            dataset_columns = compute_columns_metadata(
                dbt_columns,
                current_dataset_columns,
                reload_columns,
                merge_metadata,
            )
            try:
                client.update_dataset(
                    dataset["id"],
                    override_columns=reload_columns,
                    columns=dataset_columns,
                )
            except SupersetError:
                failed_datasets.append(model["unique_id"])
                continue

        datasets.append(dataset)

    return datasets, failed_datasets
