"""
Helper functions.
"""

import json
from typing import Any, Dict

from sqlalchemy.engine.url import URL


def build_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the SQLAlchemy URI for a given target.
    """
    type_ = target.get("type")

    if type_ == "postgres":
        return build_postgres_sqlalchemy_params(target)
    if type_ == "bigquery":
        return build_bigquery_sqlalchemy_params(target)

    raise Exception(
        f"Unable to build a SQLAlchemy URI for a target of type {type_}. Please file an "
        "issue at https://github.com/preset-io/backend-sdk/issues/new?labels=enhancement&"
        f"title=Backend+for+{type_}.",
    )


def build_postgres_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the SQLAlchemy URI for a Postgres target.
    """
    username = target["user"]
    password = target["pass"] or None
    host = target["host"]
    port = target["port"]
    dbname = target["dbname"]

    return {
        "sqlalchemy_uri": str(
            URL(
                drivername="postgresql+psycopg2",
                username=username,
                password=password,
                host=host,
                port=port,
                database=dbname,
            ),
        ),
    }


def build_bigquery_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the SQLAlchemy URI for a BigQuery target.

    Currently supports only configuration via ``keyfile``.
    """
    parameters: Dict[str, Any] = {}

    parameter_map = {
        "priority": "priority",
        "location": "location",
        "maximum_bytes_billed": "maximum_bytes_billed",
    }
    query = {
        kwarg: str(target[key]) for kwarg, key in parameter_map.items() if key in target
    }
    if "priority" in query:
        query["priority"] = query["priority"].upper()
    parameters["sqlalchemy_uri"] = str(
        URL(
            drivername="bigquery",
            host=target["project"],
            database="",
            query=query,
        ),
    )

    if "keyfile" not in target:
        raise Exception(
            "Only service account auth is supported, you MUST pass `keyfile`.",
        )

    with open(target["keyfile"], encoding="utf-8") as input_:
        credentials_info = json.load(input_)
        parameters["encrypted_extra"] = json.dumps(
            {"credentials_info": credentials_info},
        )

    return parameters
