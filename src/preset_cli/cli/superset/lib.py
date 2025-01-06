"""
Helper functions for the Superset commands
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Set

import yaml

LOG_FILE_PATH = Path("progress.log")


def get_logs() -> dict[str, Any]:
    """
    Returns the content of the progress log file.

    If the log file does not exist, an empty one is created.
    """
    if not LOG_FILE_PATH.exists():
        LOG_FILE_PATH.touch()

    with open(LOG_FILE_PATH, "r", encoding="utf-8") as log_file:
        logs = yaml.load(log_file, Loader=yaml.SafeLoader) or {}

    return logs


def add_asset_to_log_dict(  # pylint: disable=too-many-arguments
    log_type: str,
    logs: Dict[str, Any],
    status: str,
    asset_uuid: str,
    asset_path: Path | None = None,
    set_: Set[Path] | None = None,
) -> None:
    """
    Adds an asset log entry to the logs dictionary and optionally to a set of skipped items.

    The `logs` dictionary will is written to a file.
    """
    log_entry = {
        "status": status,
        "uuid": asset_uuid,
    }
    if asset_path is not None:
        log_entry["path"] = str(asset_path)

    if log_type in logs:
        logs[log_type].append(log_entry)
    else:
        logs[log_type] = [log_entry]

    if set_ is not None and asset_path:
        set_.add(asset_path)


def write_logs_to_file(logs: Dict[str, Any]) -> None:
    """
    Writes logs list to .log file.
    """
    with open(LOG_FILE_PATH, "w", encoding="utf-8") as log_file:
        yaml.dump(logs, log_file)


def clean_logs(log_type: str, logs: Dict[str, Any]) -> None:
    """
    Cleans the progress log file for the specific log type.

    If there are no other log types, the file is deleted.
    """
    logs.pop(log_type, None)
    if logs:
        write_logs_to_file(logs)
    else:
        LOG_FILE_PATH.unlink()
