"""
Tests for ``preset_cli.lib``.
"""

import logging
import warnings

import pytest
from pytest_mock import MockerFixture

from preset_cli.exceptions import ErrorLevel, SupersetError
from preset_cli.lib import (
    dict_merge,
    log_warning,
    raise_error,
    remove_root,
    setup_logging,
    validate_response,
)


def test_remove_root() -> None:
    """
    Test ``remove_root``.
    """
    assert remove_root("bundle/database/examples.yaml") == "database/examples.yaml"


def test_setup_logging() -> None:
    """
    Test ``setup_logging``.
    """
    setup_logging("debug")
    assert logging.root.level == logging.DEBUG

    with pytest.raises(ValueError) as excinfo:
        setup_logging("invalid")
    assert str(excinfo.value) == "Invalid log level: invalid"


def test_validate_response(mocker: MockerFixture) -> None:
    """
    Test ``validate_response``.
    """
    response = mocker.MagicMock()

    response.ok = True
    validate_response(response)

    # SIP-40 payload
    response.ok = False
    response.headers.get.return_value = "application/json"
    response.json.return_value = {
        "errors": [{"message": "Some message", "level": "error"}],
    }
    with pytest.raises(SupersetError) as excinfo:
        validate_response(response)
    assert excinfo.value.errors == [
        {"message": "Some message", "level": ErrorLevel.ERROR},
    ]

    # SIP-40-ish payload
    response.ok = False
    response.headers.get.return_value = "application/json"
    response.json.return_value = {"errors": {"message": "Some message"}}
    with pytest.raises(SupersetError) as excinfo:
        validate_response(response)
    assert excinfo.value.errors == [
        {
            "message": "Unknown error",
            "error_type": "UNKNOWN_ERROR",
            "level": ErrorLevel.ERROR,
            "extra": {"errors": {"message": "Some message"}},
        },
    ]

    # Old error
    response.ok = False
    response.headers.get.return_value = "application/json"
    response.json.return_value = {"message": "Some message"}
    with pytest.raises(SupersetError) as excinfo:
        validate_response(response)
    assert excinfo.value.errors == [
        {
            "message": "Unknown error",
            "error_type": "UNKNOWN_ERROR",
            "level": ErrorLevel.ERROR,
            "extra": {"message": "Some message"},
        },
    ]

    # Non JSON error
    response.ok = False
    response.headers.get.return_value = "text/plain"
    response.text = "An error occurred"
    with pytest.raises(SupersetError) as excinfo:
        validate_response(response)
    assert excinfo.value.errors == [
        {
            "message": "An error occurred",
            "error_type": "UNKNOWN_ERROR",
            "level": ErrorLevel.ERROR,
        },
    ]


def test_dict_merge() -> None:
    """
    Test ``dict_merge``.
    """
    base = {"a": {"b": 42, "c": 43}, "d": 1, "e": 3}
    overrides = {"a": {"c": 44}, "d": 2, "f": 3}
    dict_merge(base, overrides)
    assert base == {"a": {"b": 42, "c": 44}, "d": 2, "e": 3, "f": 3}


def test_log_warning(mocker: MockerFixture) -> None:
    """
    Test ``log_warning``.
    """
    mock_warn = mocker.patch.object(warnings, "warn")
    test_warning = "Test warning message"
    test_warning_type = UserWarning
    log_warning(test_warning, test_warning_type)
    mock_warn.assert_called_once_with(
        test_warning,
        category=test_warning_type,
        stacklevel=2,
    )


def test_raise_error(capsys: pytest.CaptureFixture[str]) -> None:
    """
    Test ``raise_error``.
    """
    with pytest.raises(SystemExit) as excinfo:
        raise_error(1, "error message")

    captured = capsys.readouterr()
    expected_output = "error message\n"

    assert captured.out == expected_output
    assert excinfo.type == SystemExit
    assert excinfo.value.code == 1


def test_raise_error_no_message() -> None:
    """
    Test ``raise_error``.
    """
    with pytest.raises(SystemExit) as excinfo:
        raise_error(1)

    assert excinfo.type == SystemExit
    assert excinfo.value.code == 1
