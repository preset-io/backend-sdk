"""
Custom exceptions.
"""

from enum import Enum
from typing import Any, Dict, List

from typing_extensions import TypedDict


class ErrorLevel(str, Enum):
    """
    Levels of errors that can exist within Superset.
    """

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class ErrorPayload(TypedDict, total=False):
    """
    A SIP-40 error payload.
    """

    message: str
    error_type: str  # import SupersetErrorType from Superset?
    level: ErrorLevel
    extra: Dict[str, Any]


class SupersetError(Exception):
    """
    A SIP-40 compliant exception.
    """

    def __init__(self, errors: List[ErrorPayload]):
        super().__init__()
        self.errors = errors


class DatabaseNotFoundError(SupersetError):
    """
    Exception when no database is found.
    """

    def __init__(self):
        super().__init__(
            [
                {
                    "message": "Database not found",
                    "error_type": "DATABASE_NOT_FOUND_ERROR",
                    "level": ErrorLevel.ERROR,
                },
            ],
        )


class CLIError(Exception):
    """
    Exception raised for errors that occur during the CLI execution that should
    stop the execution with an exit code.
    """

    def __init__(self, message: str, exit_code: int):
        super().__init__(message)
        self.exit_code = exit_code
