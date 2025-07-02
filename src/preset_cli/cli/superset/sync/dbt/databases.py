"""
Sync dbt database to Superset.
"""

import logging
from pathlib import Path
from typing import Any, Optional

from yarl import URL

from preset_cli.api.clients.superset import SupersetClient
from preset_cli.cli.superset.sync.dbt.lib import build_sqlalchemy_params, load_profiles
from preset_cli.cli.superset.sync.dbt.schemas import parse_meta_properties
from preset_cli.exceptions import DatabaseNotFoundError

_logger = logging.getLogger(__name__)


def sync_database(  # pylint: disable=too-many-locals, too-many-arguments
    client: SupersetClient,
    profiles_path: Path,
    project_name: str,
    profile_name: str,
    target_name: Optional[str],
    import_db: bool,
    disallow_edits: bool,  # pylint: disable=unused-argument
    external_url_prefix: str,
) -> Any:
    """
    Read target database from a dbt profiles.yml and sync to Superset.
    """
    base_url = URL(external_url_prefix) if external_url_prefix else None

    profiles = load_profiles(profiles_path, project_name, profile_name, target_name)
    project = profiles[profile_name]
    outputs = project["outputs"]
    if target_name is None:
        target_name = project["target"]
    target = outputs[target_name]

    # read additional metadata that should be applied to the DB
    parse_meta_properties(target)

    database_name = target["superset_meta"].pop(
        "database_name",
        f"{project_name}_{target_name}",
    )
    databases = client.get_databases(database_name=database_name)
    if len(databases) > 1:
        raise Exception("More than one database with the same name found")

    if base_url and "external_url" not in target["superset_meta"]:
        target["superset_meta"]["external_url"] = str(
            base_url.with_fragment("!/overview"),
        )

    if import_db:
        connection_params = target["superset_meta"].pop(
            "connection_params",
            build_sqlalchemy_params(target),
        )

        if databases:
            _logger.info("Found an existing database connection, updating it")
            database = databases[0]
            target["superset_meta"].pop("uuid", None)

            database = client.update_database(
                database_id=database["id"],
                database_name=database_name,
                is_managed_externally=disallow_edits,
                masked_encrypted_extra=connection_params.get("encrypted_extra"),
                sqlalchemy_uri=connection_params["sqlalchemy_uri"],
                **target["superset_meta"],
            )

        else:
            _logger.info("No database connection found, creating it")

            database = client.create_database(
                database_name=database_name,
                is_managed_externally=disallow_edits,
                masked_encrypted_extra=connection_params.get("encrypted_extra"),
                **connection_params,
                **target["superset_meta"],
            )

        database["sqlalchemy_uri"] = connection_params["sqlalchemy_uri"]

    elif databases:
        _logger.info("Found an existing database connection, using it")
        database = databases[0]
        database["sqlalchemy_uri"] = client.get_database(database["id"])[
            "sqlalchemy_uri"
        ]

    else:
        raise DatabaseNotFoundError()

    return database
