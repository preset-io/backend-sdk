"""
Helper functions for the Superset commands
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import IO, Any, Dict, Tuple

import click
import yaml

from preset_cli.lib import dict_merge

LOG_FILE_PATH = Path("progress.log")
DASHBOARD_FILTER_KEYS: Dict[str, type] = {
    "id": int,
    "slug": str,
    "dashboard_title": str,
    "certified_by": str,
    "is_managed_externally": bool,
}


class LogType(str, Enum):
    """
    Roles for users.
    """

    ASSETS = "assets"
    OWNERSHIP = "ownership"


def get_logs(log_type: LogType) -> Tuple[Path, Dict[LogType, Any]]:
    """
    Returns the path and content of the progress log file.

    Creates the file if it does not exist yet. Filters out FAILED
    entries for the particular log type. Defaults to an empty list.
    """
    base_logs: Dict[LogType, Any] = {log_type_: [] for log_type_ in LogType}

    if not LOG_FILE_PATH.exists():
        LOG_FILE_PATH.touch()
        return LOG_FILE_PATH, base_logs

    with open(LOG_FILE_PATH, "r", encoding="utf-8") as log_file:
        logs = yaml.load(log_file, Loader=yaml.SafeLoader) or {}

    logs = {LogType(log_type): log_entries for log_type, log_entries in logs.items()}
    dict_merge(base_logs, logs)
    base_logs[log_type] = [
        log for log in base_logs[log_type] if log["status"] != "FAILED"
    ]
    return LOG_FILE_PATH, base_logs


def serialize_enum_logs_to_string(logs: Dict[LogType, Any]) -> Dict[str, Any]:
    """
    Helper method to serialize the enum keys in the logs dict to str.
    """
    return {log_type.value: log_entries for log_type, log_entries in logs.items()}


def write_logs_to_file(log_file: IO[str], logs: Dict[LogType, Any]) -> None:
    """
    Writes logs list to .log file.
    """
    logs_ = serialize_enum_logs_to_string(logs)
    log_file.seek(0)
    yaml.dump(logs_, log_file)
    log_file.truncate()


def clean_logs(log_type: LogType, logs: Dict[LogType, Any]) -> None:
    """
    Cleans the progress log file for the specific log type.

    If there are no other log types, the file is deleted.
    """
    logs.pop(log_type, None)
    if any(logs.values()):
        with open(LOG_FILE_PATH, "w", encoding="utf-8") as log_file:
            logs_ = serialize_enum_logs_to_string(logs)
            yaml.dump(logs_, log_file)
    else:
        LOG_FILE_PATH.unlink()


def _coerce_filter_value(
    value: str,
    value_type: type,
    key: str,
) -> Any:
    """
    Coerce a filter value to the desired type.
    """
    if value_type is bool:
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
        raise click.BadParameter(
            f"Invalid value for {key}. Expected true or false.",
        )

    try:
        return value_type(value)
    except (TypeError, ValueError) as exc:
        raise click.BadParameter(
            f"Invalid value for {key}. Expected {value_type.__name__}.",
        ) from exc


def parse_filters(
    filters: Tuple[str, ...],
    allowed_keys: Dict[str, type],
) -> Dict[str, Any]:
    """
    Parse repeatable key=value filter strings into kwargs for get_resources().
    """
    parsed: Dict[str, Any] = {}
    for item in filters:
        if "=" not in item:
            raise click.BadParameter(
                f"Invalid filter '{item}'. Expected key=value.",
            )

        key, value = item.split("=", 1)
        if key == "managed_externally":
            key = "is_managed_externally"

        if key not in allowed_keys:
            allowed = ", ".join(sorted(allowed_keys))
            raise click.BadParameter(
                f"Invalid filter key '{key}'. Allowed keys: {allowed}.",
            )

        value_type = allowed_keys[key]
        parsed[key] = _coerce_filter_value(value, value_type, key)

    return parsed
