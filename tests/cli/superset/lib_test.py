"""
Tests for ``preset_cli.cli.superset.lib``.
"""

# pylint: disable=unused-argument, invalid-name

from pathlib import Path

import yaml
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

import click
import pytest

from preset_cli.api.operators import Contains
from preset_cli.cli.superset.lib import (
    DASHBOARD_FILTER_KEYS,
    LogType,
    clean_logs,
    get_logs,
    parse_filters,
    write_logs_to_file,
)


def test_get_logs_new_file(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``get_logs`` helper when the log file does not exist.
    """
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    assert get_logs(LogType.ASSETS) == (
        Path("progress.log"),
        {"assets": [], "ownership": []},
    )


def test_get_logs_existing_file(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``get_logs`` helper when the log file does not exist.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)

    logs_content = {
        "assets": [
            {
                "path": "/path/to/root/first_path",
                "status": "SUCCESS",
                "uuid": "uuid1",
            },
            {
                "path": "/path/to/root/first_path",
                "status": "FAILED",
                "uuid": "uuid3",
            },
        ],
        "ownership": [
            {
                "status": "SUCCESS",
                "uuid": "uuid2",
            },
        ],
    }
    fs.create_file(
        root / "progress.log",
        contents=yaml.dump(logs_content),
    )
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", root / "progress.log")

    assert get_logs(LogType.ASSETS) == (
        root / "progress.log",
        {
            "assets": [
                {
                    "path": "/path/to/root/first_path",
                    "status": "SUCCESS",
                    "uuid": "uuid1",
                },
            ],
            "ownership": [
                {
                    "status": "SUCCESS",
                    "uuid": "uuid2",
                },
            ],
        },
    )

    assert get_logs(LogType.OWNERSHIP) == (root / "progress.log", logs_content)


def test_write_logs_to_file(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``write_logs_to_file`` helper.
    """
    root = Path("/path/to/root")
    fs.create_dir(root)
    fs.create_file(
        root / "progress.log",
        contents=yaml.dump(
            {
                "assets": [
                    {
                        "path": "/path/to/root/first_path",
                        "status": "FAILED",
                        "uuid": "uuid1",
                    },
                ],
            },
        ),
    )
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", root / "progress.log")

    new_logs = {
        LogType.ASSETS: [
            {
                "path": "/path/to/root/second_path",
                "status": "SUCCESS",
                "uuid": "uuid2",
            },
            {
                "path": "/path/to/root/third_path",
                "status": "SUCCESS",
                "uuid": "uuid3",
            },
        ],
        LogType.OWNERSHIP: [
            {
                "status": "SUCCESS",
                "uuid": "uuid4",
            },
        ],
    }

    with open(root / "progress.log", "r+", encoding="utf-8") as file:
        write_logs_to_file(file, new_logs)

    with open(root / "progress.log", encoding="utf-8") as file:
        content = yaml.load(file, Loader=yaml.SafeLoader)

    assert content == new_logs


def test_clean_logs_delete_file(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``clean_logs`` helper when the log file should be deleted.
    """
    root = Path("/path/to/root")
    logs_path = root / "progress.log"
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", logs_path)
    fs.create_dir(root)
    fs.create_file(
        root / "progress.log",
        contents=yaml.dump(
            {
                "assets": [
                    {
                        "path": "/path/to/root/first_path",
                        "status": "SUCCESS",
                        "uuid": "uuid1",
                    },
                ],
            },
        ),
    )
    assert logs_path.exists()

    current_logs = {
        LogType.ASSETS: [
            {
                "path": "/path/to/root/first_path",
                "status": "SUCCESS",
                "uuid": "uuid1",
            },
        ],
    }

    clean_logs(LogType.ASSETS, current_logs)
    assert not logs_path.exists()


def test_clean_logs_keep_file(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``clean_logs`` helper when the log file should be kept.
    """
    root = Path("/path/to/root")
    logs_path = root / "progress.log"
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", logs_path)
    fs.create_dir(root)
    fs.create_file(
        root / "progress.log",
        contents=yaml.dump(
            {
                "assets": [
                    {
                        "path": "/path/to/root/first_path",
                        "status": "SUCCESS",
                        "uuid": "uuid1",
                    },
                ],
                "ownership": [
                    {
                        "status": "SUCCESS",
                        "uuid": "uuid2",
                    },
                ],
            },
        ),
    )
    assert logs_path.exists()

    current_logs = {
        LogType.ASSETS: [
            {
                "path": "/path/to/root/first_path",
                "status": "SUCCESS",
                "uuid": "uuid1",
            },
        ],
        LogType.OWNERSHIP: [
            {
                "status": "SUCCESS",
                "uuid": "uuid2",
            },
        ],
    }
    clean_logs(LogType.ASSETS, current_logs)

    with open(logs_path, encoding="utf-8") as log:
        content = yaml.load(log, Loader=yaml.SafeLoader)

    assert content == {"ownership": [{"status": "SUCCESS", "uuid": "uuid2"}]}


def test_parse_filters_single() -> None:
    """
    Test ``parse_filters`` with a single filter.
    """
    assert parse_filters(("slug=test",), DASHBOARD_FILTER_KEYS) == {"slug": "test"}


def test_parse_filters_multiple() -> None:
    """
    Test ``parse_filters`` with multiple filters.
    """
    assert parse_filters(("slug=test", "id=123"), DASHBOARD_FILTER_KEYS) == {
        "slug": "test",
        "id": 123,
    }


def test_parse_filters_bool_coercion() -> None:
    """
    Test ``parse_filters`` with boolean coercion.
    """
    assert parse_filters(("is_managed_externally=true",), DASHBOARD_FILTER_KEYS) == {
        "is_managed_externally": True,
    }


def test_parse_filters_bool_false() -> None:
    """
    Test ``parse_filters`` with boolean false.
    """
    assert parse_filters(("is_managed_externally=false",), DASHBOARD_FILTER_KEYS) == {
        "is_managed_externally": False,
    }


def test_parse_filters_managed_externally_alias() -> None:
    """
    Test ``parse_filters`` with managed_externally alias.
    """
    assert parse_filters(("managed_externally=true",), DASHBOARD_FILTER_KEYS) == {
        "is_managed_externally": True,
    }


def test_parse_filters_invalid_key() -> None:
    """
    Test ``parse_filters`` with invalid key.
    """
    with pytest.raises(click.BadParameter):
        parse_filters(("color=red",), DASHBOARD_FILTER_KEYS)


def test_parse_filters_invalid_int() -> None:
    """
    Test ``parse_filters`` with invalid int.
    """
    with pytest.raises(click.BadParameter):
        parse_filters(("id=abc",), DASHBOARD_FILTER_KEYS)


def test_parse_filters_missing_equals() -> None:
    """
    Test ``parse_filters`` with missing equals.
    """
    with pytest.raises(click.BadParameter):
        parse_filters(("slugtest",), DASHBOARD_FILTER_KEYS)


def test_parse_filters_empty_value() -> None:
    """
    Test ``parse_filters`` with empty value.
    """
    assert parse_filters(("certified_by=",), DASHBOARD_FILTER_KEYS) == {
        "certified_by": "",
    }


def test_parse_filters_value_with_equals() -> None:
    """
    Test ``parse_filters`` with value containing equals.
    """
    result = parse_filters(("dashboard_title=A=B",), DASHBOARD_FILTER_KEYS)
    assert isinstance(result["dashboard_title"], Contains)
    assert result["dashboard_title"].value == "A=B"


def test_parse_filters_dashboard_title_uses_contains() -> None:
    """
    Test ``parse_filters`` wraps dashboard_title with Contains operator.
    """
    result = parse_filters(("dashboard_title=Sales",), DASHBOARD_FILTER_KEYS)
    assert isinstance(result["dashboard_title"], Contains)
    assert result["dashboard_title"].value == "Sales"


def test_parse_filters_slug_uses_exact_match() -> None:
    """
    Test ``parse_filters`` passes slug as raw value (exact match).
    """
    result = parse_filters(("slug=test",), DASHBOARD_FILTER_KEYS)
    assert result["slug"] == "test"  # raw value, not an Operator
