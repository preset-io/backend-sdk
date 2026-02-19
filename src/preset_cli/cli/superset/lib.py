"""
Helper functions for the Superset commands
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import IO, Any, Callable, Dict, List, Tuple

import click
import yaml

from preset_cli.api.operators import Contains
from preset_cli.exceptions import SupersetError
from preset_cli.lib import dict_merge

LOG_FILE_PATH = Path("progress.log")
CONTAINS_FILTER_KEYS = {"dashboard_title"}
LOCAL_FILTER_KEYS = {"certified_by", "is_managed_externally"}
FILTER_ALIASES = {"managed_externally": "is_managed_externally"}
DASHBOARD_FILTER_KEYS: Dict[str, type] = {
    "id": int,
    "slug": str,
    "dashboard_title": str,
    "certified_by": str,
    "is_managed_externally": bool,
}
DELETE_FILTER_KEYS: Dict[str, Dict[str, type]] = {
    "dashboard": DASHBOARD_FILTER_KEYS,
    "chart": {"id": int},
    "dataset": {"id": int},
    "database": {"id": int},
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
        LOG_FILE_PATH.unlink(missing_ok=True)


def _normalize_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    if isinstance(value, int):
        return bool(value)
    return None


def _coerce_filter_value(
    value: str,
    value_type: type,
    key: str,
) -> Any:
    """
    Coerce a filter value to the desired type.
    """
    if value_type is bool:
        result = _normalize_bool(value)
        if result is None:
            raise click.BadParameter(
                f"Invalid value for {key}. Expected true or false.",
            )
        return result

    try:
        return value_type(value)
    except (TypeError, ValueError) as exc:
        raise click.BadParameter(
            f"Invalid value for {key}. Expected {value_type.__name__}.",
        ) from exc


def coerce_bool_option(
    value: Any,
    key: str,
) -> bool:
    """
    Coerce an option value to bool.
    """
    if isinstance(value, (bool, str)):
        result = _normalize_bool(value)
        if result is not None:
            return result

    raise click.BadParameter(
        f"Invalid value for {key}. Expected true or false.",
    )


def is_filter_not_allowed_error(exc: Exception) -> bool:
    """
    Return True if a Superset error indicates filters are not supported.
    """
    if not isinstance(exc, SupersetError):
        return False

    for error in exc.errors:
        message = str(error.get("message", "")).lower()
        if "filter column" in message and "not allowed to filter" in message:
            return True

    return False


def _matches_contains(actual: Any, expected: Contains) -> bool:
    actual_text = "" if actual is None else str(actual)
    return str(expected.value).lower() in actual_text.lower()


def _matches_bool(actual: Any, expected: bool) -> bool:
    actual_bool = _normalize_bool(actual)
    return actual_bool is not None and actual_bool == expected


def _matches_empty_string(actual: Any, expected: Any) -> bool:
    return expected == "" and (actual is None or actual == "")


def _matches_int(actual: Any, expected: int) -> bool:
    try:
        if actual is None:
            return False
        return int(str(actual)) == expected
    except (TypeError, ValueError):
        return False


def _matches_exact(actual: Any, expected: Any) -> bool:
    return str(actual) == str(expected)


def filter_resources_locally(  # pylint: disable=too-many-return-statements
    resources: list[dict[str, Any]],
    filters: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Apply parsed filters to a list of resources locally.

    Empty-string filter values match both missing and empty values.
    """

    def matches(resource: dict[str, Any]) -> bool:
        for key, expected in filters.items():
            actual = resource.get(key)

            if isinstance(expected, Contains) and _matches_contains(actual, expected):
                continue
            if isinstance(expected, bool) and _matches_bool(actual, expected):
                continue
            if _matches_empty_string(actual, expected):
                continue
            if isinstance(expected, int) and _matches_int(actual, expected):
                continue
            if _matches_exact(actual, expected):
                continue
            if isinstance(expected, Contains):
                return False
            if isinstance(expected, bool):
                return False
            if isinstance(expected, int):
                return False
            return False

        return True

    return [resource for resource in resources if matches(resource)]


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
        key = key.strip()
        value = value.strip()
        if not key:
            raise click.BadParameter(
                f"Invalid filter '{item}'. Filter key cannot be empty.",
            )
        # Normalize user-facing aliases to canonical Superset API keys.
        key = FILTER_ALIASES.get(key, key)

        if key not in allowed_keys:
            allowed = ", ".join(sorted(allowed_keys))
            raise click.BadParameter(
                f"Invalid filter key '{key}'. Allowed keys: {allowed}.",
            )
        if key in parsed:
            raise click.BadParameter(
                f"Duplicate filter key '{key}'. " "Pass each filter key at most once.",
            )

        value_type = allowed_keys[key]
        coerced = _coerce_filter_value(value, value_type, key)
        if key in CONTAINS_FILTER_KEYS:
            parsed[key] = Contains(coerced)
        else:
            parsed[key] = coerced

    return parsed


def fetch_with_filter_fallback(
    fetch_filtered: Callable[..., List[Dict[str, Any]]],
    fetch_all: Callable[[], List[Dict[str, Any]]],
    parsed_filters: Dict[str, Any],
    resource_label: str,
) -> List[Dict[str, Any]]:
    """
    Try fetching with server-side filters; fall back to local filtering.

    If any filter key is in LOCAL_FILTER_KEYS the local path is used immediately.
    On a ``filter not allowed`` API error, results are fetched unfiltered and
    filtered locally.  Any other exception is wrapped in a click.ClickException.
    """
    if set(parsed_filters) & LOCAL_FILTER_KEYS:
        return filter_resources_locally(fetch_all(), parsed_filters)

    try:
        resources = fetch_filtered(**parsed_filters)
    except Exception as exc:  # pylint: disable=broad-except
        if is_filter_not_allowed_error(exc):
            return filter_resources_locally(fetch_all(), parsed_filters)
        filter_keys = ", ".join(parsed_filters.keys())
        raise click.ClickException(
            f"Failed to fetch {resource_label} ({exc}). "
            f"This may indicate that filter key(s) {filter_keys} "
            "may not be supported by this Superset version.",
        ) from exc

    if not resources:
        return resources  # pragma: no cover

    # Verify filtered responses locally to avoid broad results when an API silently
    # ignores one or more predicates.
    if all(all(key in resource for key in parsed_filters) for resource in resources):
        return filter_resources_locally(resources, parsed_filters)

    # Some endpoints return slim payloads that omit filter keys. For contains
    # predicates, re-fetch without filters and apply predicates locally.
    if any(isinstance(value, Contains) for value in parsed_filters.values()):
        return filter_resources_locally(fetch_all(), parsed_filters)
    return resources  # pragma: no cover
