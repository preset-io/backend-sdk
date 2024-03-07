"""
Helper functions.
"""

import ast
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypedDict, Union

import yaml
from jinja2 import Environment
from sqlalchemy.engine.url import URL

from preset_cli.api.clients.dbt import ModelSchema

_logger = logging.getLogger(__name__)


def build_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the SQLAlchemy URI for a given target.
    """
    type_ = target.get("type")

    if type_ == "postgres":
        return build_postgres_sqlalchemy_params(target)
    if type_ == "redshift":
        return build_redshift_sqlalchemy_params(target)
    if type_ == "bigquery":
        return build_bigquery_sqlalchemy_params(target)
    if type_ == "snowflake":
        return build_snowflake_sqlalchemy_params(target)

    raise NotImplementedError(
        f"Unable to build a SQLAlchemy URI for a target of type {type_}. Please file an "
        "issue at https://github.com/preset-io/backend-sdk/issues/new?labels=enhancement&"
        f"title=Backend+for+{type_}.",
    )


def build_postgres_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the SQLAlchemy URI for a Postgres target.
    """
    if "search_path" in target:
        _logger.warning("Specifying a search path is not supported in Apache Superset")

    username = target["user"]
    password = target.get("password") or target.get("pass")
    host = target["host"]
    port = target["port"]
    dbname = target["dbname"]

    query = {"sslmode": target["sslmode"]} if "sslmode" in target else None

    return {
        "sqlalchemy_uri": str(
            URL(
                drivername="postgresql+psycopg2",
                username=username,
                password=password,
                host=host,
                port=port,
                database=dbname,
                query=query,
            ),
        ),
    }


def build_redshift_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the SQLAlchemy URI for a Redshift target.
    """
    if "search_path" in target:
        _logger.warning("Specifying a search path is not supported in Apache Superset")

    username = target["user"]
    password = target.get("password") or target.get("pass")
    host = target["host"]
    port = target["port"]
    dbname = target["dbname"]

    query = {"sslmode": target["sslmode"]} if "sslmode" in target else None

    return {
        "sqlalchemy_uri": str(
            URL(
                drivername="redshift+psycopg2",
                username=username,
                password=password,
                host=host,
                port=port,
                database=dbname,
                query=query,
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


def build_snowflake_sqlalchemy_params(target: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the SQLAlchemy URI for a Snowflake target.
    """
    username = target["user"]
    password = target.get("password", "") or None
    database = target["database"]
    host = target["account"]
    query = {"role": target["role"], "warehouse": target["warehouse"]}

    parameters = {
        "sqlalchemy_uri": str(
            URL(
                drivername="snowflake",
                username=username,
                password=password,
                host=host,
                database=database,
                query=query,
            ),
        ),
    }

    authenticator = target.get("authenticator")
    if authenticator:
        if authenticator == "externalbrowser":
            raise NotImplementedError("SSO not supported")
        if authenticator.startswith("http"):
            raise NotImplementedError("SSO not supported")
        parameters["extra"] = json.dumps(
            {
                "engine_params": {
                    "connect_args": {
                        "passcode": authenticator,
                    },
                },
            },
        )

    if "private_key_path" in target:
        with open(target["private_key_path"], encoding="utf-8") as input_:
            pk_body = input_.read()

        parameters["encrypted_extra"] = json.dumps(
            {
                "auth_method": "keypair",
                "auth_params": {
                    "privatekey_body": pk_body,
                    "privatekey_pass": target.get("private_key_passphrase", ""),
                },
            },
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


def load_profiles(
    path: Path,
    project_name: str,
    profile_name: str,
    target_name: Optional[str],
) -> Dict[str, Any]:
    """
    Load the file and apply Jinja2 templating.
    """
    with open(path, encoding="utf-8") as input_:
        profiles = yaml.load(input_, Loader=yaml.SafeLoader)

    if profile_name not in profiles:
        raise Exception(f"Project {profile_name} not found in {path}")
    project = profiles[profile_name]
    outputs = project["outputs"]
    if target_name is None:
        target_name = project["target"]
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
        "profile_name": profile_name,
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
            config = yaml.load(template.render(**context), Loader=yaml.SafeLoader)

        return config

    return apply_templating(profiles)


# pylint: disable=R0911
def filter_models(models: List[ModelSchema], condition: str) -> List[ModelSchema]:
    """
    Filter a list of dbt models given a select condition.

    Currently only a subset of the syntax is supported.

    See https://docs.getdbt.com/reference/node-selection/syntax.
    """
    # match by tag
    if condition.startswith("tag:"):
        tag = condition.split(":", 1)[1]
        return [model for model in models if tag in model["tags"]]

    if condition.startswith("config"):
        filtered_models = []
        config_key, config_value = re.split(r"[.:]", condition)[1:]
        for model in models:
            if model.get("config", {}).get(config_key) == config_value:
                filtered_models.append(model)
        return filtered_models

    # simple match by name
    model_names = {model["name"]: model for model in models}
    if condition in model_names:
        return [model_names[condition]]

    # file
    file_path = Path(condition)
    if file_path.is_file() and file_path.stem in model_names:
        return [model_names[file_path.stem]]

    # path/directory
    if file_path.is_dir() or (
        str(file_path).endswith("/*") and (file_path := file_path.parent)
    ):
        sql_files = [file for file in file_path.rglob("*.sql") if file.is_file()]
        return [
            model_names[file.stem] for file in sql_files if file.stem in model_names
        ]

    # plus and n-plus operators
    if "+" in condition:
        return filter_plus_operator(models, condition)

    # at operator -- from the docs it seems that it can only be used before the model name
    # (https://docs.getdbt.com/reference/node-selection/graph-operators#the-at-operator)
    if condition.startswith("@"):
        return filter_at_operator(models, condition)

    raise NotImplementedError(
        f"Unable to parse the selection {condition}. Please file an issue at "
        "https://github.com/preset-io/backend-sdk/issues/new?labels=enhancement&"
        f"title=dbt+select+{condition}.",
    )


def filter_plus_operator(
    models: List[ModelSchema],
    condition: str,
) -> List[ModelSchema]:
    """
    Filter a list of models using the plus or n-plus operators.
    """
    model_ids = {model["unique_id"]: model for model in models}
    model_names = {model["name"]: model for model in models}

    match = re.match(r"^(\d*\+)?(.*?)(\+\d*)?$", condition)
    # pylint: disable=invalid-name
    up, name, down = match.groups()  # type: ignore
    base_model = model_names[name]
    selected_models: Dict[str, ModelSchema] = {}

    if up:
        degrees = None if len(up) == 1 else int(up[:-1])
        queue = [(base_model, 0)]
        while queue:
            model, degree = queue.pop(0)
            id_ = model["unique_id"]
            if id_ not in selected_models:
                selected_models[id_] = model
                if degrees is None or degree < degrees:
                    queue.extend(
                        (model_ids[parent_id], degree + 1)
                        for parent_id in model.get("depends_on", [])
                        if parent_id in model_ids
                    )

    if down:
        degrees = None if len(down) == 1 else int(down[1:])
        queue = [(base_model, 0)]
        while queue:
            model, degree = queue.pop(0)
            id_ = model["unique_id"]
            if id_ not in selected_models:
                selected_models[id_] = model
                if degrees is None or degree < degrees:
                    queue.extend(
                        (model_ids[child_id], degree + 1)
                        for child_id in model.get("children", [])
                        if child_id in model_ids
                    )

    return list(selected_models.values())


def filter_at_operator(models: List[ModelSchema], condition: str) -> List[ModelSchema]:
    """
    filter a list of models using the at operator.
    """
    model_ids = {model["unique_id"]: model for model in models}
    model_names = {model["name"]: model for model in models}

    base_model = model_names[condition[1:]]
    selected_models: Dict[str, ModelSchema] = {}

    queue = [base_model]
    while queue:
        model = queue.pop(0)
        id_ = model["unique_id"]
        if id_ not in selected_models:
            selected_models[id_] = model

            # add children
            queue.extend(
                model_ids[child_id]
                for child_id in model.get("children", [])
                if child_id in model_ids
            )

            # add parents of the children of the selected model
            if model != base_model:
                queue.extend(
                    model_ids[parent_id]
                    for parent_id in model.get("depends_on", [])
                    if parent_id in model_ids
                )

    return list(selected_models.values())


def apply_select(
    models: List[ModelSchema],
    select: Tuple[str, ...],
    exclude: Tuple[str, ...],
) -> List[ModelSchema]:
    """
    Apply dbt node selection (https://docs.getdbt.com/reference/node-selection/syntax).
    """
    model_ids = {model["unique_id"]: model for model in models}
    selected: Dict[str, ModelSchema]
    if not select:
        selected = {model["unique_id"]: model for model in models}
    else:
        selected = {}
        for selection in select:
            ids = set.intersection(
                *[
                    {model["unique_id"] for model in filter_models(models, condition)}
                    for condition in selection.split(",")
                ]
            )
            selected.update({id_: model_ids[id_] for id_ in ids})

    for selection in exclude:
        for id_ in set.intersection(
            *[
                {model["unique_id"] for model in filter_models(models, condition)}
                for condition in selection.split(",")
            ]
        ):
            if id_ in selected:
                del selected[id_]

    return list(selected.values())


def list_failed_models(failed_models: List[str]) -> str:
    """
    List models that failed to sync.
    """
    error_message = "Below model(s) failed to sync:"
    for failed_model in failed_models:
        error_message += f"\n - {failed_model}"

    return error_message
