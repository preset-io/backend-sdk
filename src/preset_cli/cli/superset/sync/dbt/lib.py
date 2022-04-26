"""
Helper functions.
"""

import ast
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, TypedDict, Union

import yaml
from jinja2 import Environment
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


def env_var(var: str, default: Optional[str] = None) -> str:
    """
    Simplified version of dbt's ``env_var``.

    We need this to load the profile with secrets.
    """
    if var not in os.environ and not default:
        raise Exception(f"Env var required but not provided: '{var}'")
    return os.environ.get(var, default or "")


def as_number(value: str) -> Union[int, float]:
    """
    Simplified version of dbt's ``as_number``.
    """
    try:
        return int(value)
    except ValueError:
        return float(value)


class Target(TypedDict):
    """
    Information about the warehouse connection.
    """

    profile_name: str
    name: str
    schema: str
    type: str
    threads: int


def load_profiles(path: Path, project_name: str, target_name: str) -> Dict[str, Any]:
    """
    Load the file and apply Jinja2 templating.
    """
    with open(path, encoding="utf-8") as input_:
        profiles = yaml.load(input_, Loader=yaml.SafeLoader)

    if project_name not in profiles:
        raise Exception(f"Project {project_name} not found in {path}")
    project = profiles[project_name]
    outputs = project["outputs"]
    if target_name not in outputs:
        raise Exception(f"Target {target_name} not found in the outputs of {path}")
    target = outputs[target_name]

    env = Environment()
    env.filters["as_bool"] = bool
    env.filters["as_native"] = ast.literal_eval
    env.filters["as_number"] = as_number
    env.filters["as_text"] = str

    context = {
        "env_var": env_var,
        "project_name": project_name,
        "target": target,
    }

    def apply_templating(config: Any) -> Any:
        """
        Apply Jinja2 templating to dictionary values recursively.
        """
        if isinstance(config, dict):
            for key, value in config.items():
                config[key] = apply_templating(value)
        elif isinstance(config, list):
            config = [apply_templating(el) for el in config]
        elif isinstance(config, str):
            template = env.from_string(config)
            config = yaml.safe_load(template.render(**context))

        return config

    return apply_templating(profiles)
