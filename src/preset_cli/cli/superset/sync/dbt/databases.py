"""
Sync DBT database to Superset.
"""

import logging
from pathlib import Path
from typing import Any

import yaml
from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.sync.dbt.lib import build_sqlalchemy_params
from preset_cli.exceptions import DatabaseNotFoundError

_logger = logging.getLogger(__name__)


def sync_database(  # pylint: disable=too-many-locals, too-many-arguments
    client: SupersetClient,
    profiles_path: Path,
    project_name: str,
    target_name: str,
    import_db: bool,
    disallow_edits: bool,  # pylint: disable=unused-argument
    external_url_prefix: str,
) -> Any:
    """
    Read target database from a DBT profiles.yml and sync to Superset.
    """
    base_url = URL(external_url_prefix) if external_url_prefix else None

    with open(profiles_path, encoding="utf-8") as input_:
        profiles = yaml.load(input_, Loader=yaml.SafeLoader)

    if project_name not in profiles:
        raise Exception(f"Project {project_name} not found in {profiles_path}")

    project = profiles[project_name]
    outputs = project["outputs"]

    if target_name not in outputs:
        raise Exception(
            f"Target {target_name} not found in the outputs of {profiles_path}",
        )

    target = outputs[target_name]

    # read additional metadata that should be applied to the DB
    meta = target.get("meta", {}).get("superset", {})

    if "connection_params" in meta:
        connection_params = meta.pop("connection_params")
    else:
        connection_params = build_sqlalchemy_params(target)

    database_name = meta.pop("database_name", f"{project_name}_{target_name}")
    databases = client.get_databases(
        sqlalchemy_uri=connection_params["sqlalchemy_uri"],
        database_name=database_name,
    )
    if len(databases) > 1:
        raise Exception(
            "More than one database with the same SQLAlchemy URI and name found",
        )

    if base_url and "external_url" not in meta:
        meta["external_url"] = str(base_url.with_fragment("!/overview"))

    if databases:
        _logger.info("Found an existing database, updating it")
        database = databases[0]
        database = client.update_database(
            database_id=database["id"],
            database_name=database_name,
            is_managed_externally=disallow_edits,
            **meta,
        )
    elif not import_db:
        raise DatabaseNotFoundError()
    else:
        _logger.info("No database found, creating it")
        database = client.create_database(
            database_name=database_name,
            is_managed_externally=disallow_edits,
            **connection_params,
            **meta,
        )

    return database
