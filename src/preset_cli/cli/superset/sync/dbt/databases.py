"""
Sync dbt database to Superset.
"""

import logging
from pathlib import Path
from typing import Any

from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.sync.dbt.lib import build_sqlalchemy_params, load_profiles
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
    Read target database from a dbt profiles.yml and sync to Superset.
    """
    base_url = URL(external_url_prefix) if external_url_prefix else None

    profiles = load_profiles(profiles_path, project_name, target_name)
    project = profiles[project_name]
    outputs = project["outputs"]
    target = outputs[target_name]

    # read additional metadata that should be applied to the DB
    meta = target.get("meta", {}).get("superset", {})

    if "connection_params" in meta:
        connection_params = meta.pop("connection_params")
    else:
        connection_params = build_sqlalchemy_params(target)

    database_name = meta.pop("database_name", f"{project_name}_{target_name}")
    databases = client.get_databases(database_name=database_name)
    if len(databases) > 1:
        raise Exception("More than one database with the same name found")

    if base_url and "external_url" not in meta:
        meta["external_url"] = str(base_url.with_fragment("!/overview"))

    if databases:
        _logger.info("Found an existing database, updating it")
        database = databases[0]

        database = client.update_database(
            database_id=database["id"],
            database_name=database_name,
            is_managed_externally=disallow_edits,
            masked_encrypted_extra=connection_params.get("encrypted_extra"),
            sqlalchemy_uri=connection_params["sqlalchemy_uri"],
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
