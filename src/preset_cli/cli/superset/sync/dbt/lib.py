"""
Helper functions.
"""

from typing import Any, Dict

from sqlalchemy.engine.url import URL


def build_sqlalchemy_uri(target: Dict[str, Any]) -> URL:
    """
    Build the SQLAlchemy URI for a given target.
    """
    type_ = target.get("type")

    if type_ == "postgres":
        return build_postgres_sqlalchemy_uri(target)
    if type_ == "bigquery":
        return build_bigquery_sqlalchemy_uri(target)

    raise Exception(
        f"Unable to build a SQLAlchemy URI for a target of type {type_}. Please file an "
        "issue at https://github.com/preset-io/backend-sdk/issues/new.",
    )


def build_postgres_sqlalchemy_uri(target: Dict[str, Any]) -> URL:
    """
    Build the SQLAlchemy URI for a Postgres target.
    """
    username = target["user"]
    password = target["pass"] or None
    host = target["host"]
    port = target["port"]
    dbname = target["dbname"]

    return URL(
        drivername="postgresql+psycopg2",
        username=username,
        password=password,
        host=host,
        port=port,
        database=dbname,
    )


def build_bigquery_sqlalchemy_uri(target: Dict[str, Any]) -> URL:
    """
    Build the SQLAlchemy URI for a BigQuery target.

    Currently supports only configuration via ``keyfile``.
    """
    parameter_map = {
        "credentials_path": "keyfile",
        "priority": "priority",
        "location": "location",
        "maximum_bytes_billed": "maximum_bytes_billed",
    }
    query = {
        kwarg: str(target[key]) for kwarg, key in parameter_map.items() if key in target
    }
    return URL(
        drivername="bigquery",
        host=target["project"],
        query=query,
    )
