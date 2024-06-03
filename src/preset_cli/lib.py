"""
Basic helper functions.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, cast

import click
from requests import Response
from rich.logging import RichHandler

from preset_cli.exceptions import CLIError, ErrorLevel, ErrorPayload, SupersetError

_logger = logging.getLogger(__name__)


def remove_root(file_path: str) -> str:
    """
    Remove the first directory of a path.
    """
    full_path = Path(file_path)
    return str(Path(*full_path.parts[1:]))


def setup_logging(loglevel: str) -> None:
    """
    Setup basic logging.
    """
    level = getattr(logging, loglevel.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Invalid log level: {loglevel}")

    logformat = "[%(asctime)s] %(levelname)s: %(name)s: %(message)s"
    logging.basicConfig(
        level=level,
        format=logformat,
        datefmt="[%X]",
        handlers=[RichHandler()],
        force=True,
    )
    logging.captureWarnings(True)


def deserialize_error_level(errors: List[Dict[str, Any]]) -> List[ErrorPayload]:
    """
    Convert error level from string to enum.
    """
    for error in errors:
        if isinstance(error, dict) and isinstance(error.get("level"), str):
            error["level"] = ErrorLevel(error["level"])
    return cast(List[ErrorPayload], errors)


def is_sip_40_payload(errors: List[Dict[str, Any]]) -> bool:
    """
    Return if a given error payload comforms with SIP-40.
    """
    return isinstance(errors, list) and all(
        isinstance(error, dict)
        and set(error.keys()) <= {"message", "error_type", "level", "extra"}
        for error in errors
    )


def validate_response(response: Response) -> None:
    """
    Check for errors in a response.
    """
    if response.ok:
        return

    if response.headers.get("content-type") == "application/json":
        payload = response.json()
        message = json.dumps(payload, indent=4)

        if "errors" in payload and is_sip_40_payload(payload["errors"]):
            errors = deserialize_error_level(payload["errors"])
        else:
            errors = [
                {
                    "message": "Unknown error",
                    "error_type": "UNKNOWN_ERROR",
                    "level": ErrorLevel.ERROR,
                    "extra": payload,
                },
            ]
    else:
        message = response.text
        errors = [
            {
                "message": message,
                "error_type": "UNKNOWN_ERROR",
                "level": ErrorLevel.ERROR,
            },
        ]

    _logger.error(message)
    raise SupersetError(errors=errors)


def split_comma(  # pylint: disable=unused-argument
    ctx: click.core.Context,
    param: str,
    value: Optional[str],
) -> List[str]:
    """
    Split CLI option into multiple values.
    """
    if value is None:
        return []

    return [option.strip() for option in value.split(",")]


def dict_merge(base: Dict[Any, Any], overrides: Dict[Any, Any]) -> None:
    """
    Recursive dict merge.
    """
    for k in overrides:  # pylint: disable=invalid-name
        if k in base and isinstance(base[k], dict) and isinstance(overrides[k], dict):
            dict_merge(base[k], overrides[k])
        else:
            base[k] = overrides[k]


def raise_cli_errors(function: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to catch any CLIError raised and exits the execution with an error code.
    """

    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except CLIError as excinfo:
            click.echo(
                click.style(
                    str(excinfo),
                    fg="bright_red",
                ),
            )
            sys.exit(excinfo.exit_code)

    return wrapper
