"""
Tests for ``preset_cli.cli.superset.lib``.
"""

# pylint: disable=unused-argument, invalid-name

from pathlib import Path
from typing import Any

import click
import pytest
import yaml
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.api.operators import Contains
from preset_cli.cli.superset.lib import (
    DASHBOARD_FILTER_KEYS,
    LogType,
    clean_logs,
    coerce_bool_option,
    fetch_with_filter_fallback,
    filter_resources_locally,
    get_logs,
    is_filter_not_allowed_error,
    parse_filters,
    write_logs_to_file,
)
from preset_cli.exceptions import SupersetError


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


def test_clean_logs_missing_file_no_error(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``clean_logs`` does not fail when the log file was already removed.
    """
    root = Path("/path/to/root")
    logs_path = root / "progress.log"
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", logs_path)
    fs.create_dir(root)
    assert not logs_path.exists()

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


def test_parse_filters_duplicate_key_rejected() -> None:
    """
    Test ``parse_filters`` rejects duplicate keys.
    """
    with pytest.raises(click.BadParameter):
        parse_filters(("slug=test", "slug=other"), DASHBOARD_FILTER_KEYS)


def test_parse_filters_trims_whitespace() -> None:
    """
    Test ``parse_filters`` trims surrounding whitespace for key and value.
    """
    assert parse_filters((" slug = test ",), DASHBOARD_FILTER_KEYS) == {
        "slug": "test",
    }


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


def test_filter_resources_locally() -> None:
    """
    Test ``filter_resources_locally`` with contains and bool filters.
    """
    resources: list[dict[str, Any]] = [
        {
            "id": 1,
            "dashboard_title": "Sales Overview",
            "certified_by": "Alice",
            "is_managed_externally": False,
        },
        {
            "id": 2,
            "dashboard_title": "Marketing Overview",
            "certified_by": "Bob",
            "is_managed_externally": True,
        },
    ]
    parsed = parse_filters(
        ("dashboard_title=sale", "certified_by=Alice", "managed_externally=false"),
        DASHBOARD_FILTER_KEYS,
    )
    result = filter_resources_locally(resources, parsed)
    assert [item["id"] for item in result] == [1]


def test_is_filter_not_allowed_error() -> None:
    """
    Test ``is_filter_not_allowed_error`` detection.
    """
    exc = SupersetError(
        errors=[{"message": "Filter column: certified_by not allowed to filter"}],
    )
    assert is_filter_not_allowed_error(exc) is True
    assert is_filter_not_allowed_error(Exception("other")) is False


def test_parse_filters_slug_uses_exact_match() -> None:
    """
    Test ``parse_filters`` passes slug as raw value (exact match).
    """
    result = parse_filters(("slug=test",), DASHBOARD_FILTER_KEYS)
    assert result["slug"] == "test"  # raw value, not an Operator


def test_parse_filters_invalid_bool() -> None:
    """
    Test ``parse_filters`` with invalid bool value.
    """
    with pytest.raises(click.BadParameter):
        parse_filters(("is_managed_externally=maybe",), DASHBOARD_FILTER_KEYS)


def test_coerce_bool_option() -> None:
    """
    Test ``coerce_bool_option`` conversions.
    """
    assert coerce_bool_option(True, "dry_run") is True
    assert coerce_bool_option("true", "dry_run") is True
    assert coerce_bool_option("false", "dry_run") is False
    with pytest.raises(click.BadParameter):
        coerce_bool_option("invalid", "dry_run")


def test_is_filter_not_allowed_error_false_for_other_message() -> None:
    """
    Test ``is_filter_not_allowed_error`` false path for other Superset errors.
    """
    exc = SupersetError(errors=[{"message": "Some other error"}])
    assert is_filter_not_allowed_error(exc) is False


def test_filter_resources_locally_extra_branches() -> None:
    """
    Test ``filter_resources_locally`` branch behavior for bool/int/empty values.
    """
    resources: list[dict[str, Any]] = [
        {"id": "10", "is_managed_externally": "true", "certified_by": ""},
        {"id": 11, "is_managed_externally": 0, "certified_by": None},
        {"id": "bad", "is_managed_externally": "false", "certified_by": "x"},
    ]

    parsed = parse_filters(
        ("id=10", "is_managed_externally=true", "certified_by="),
        DASHBOARD_FILTER_KEYS,
    )
    result = filter_resources_locally(resources, parsed)
    assert [item["id"] for item in result] == ["10"]

    parsed_false = parse_filters(
        ("is_managed_externally=false",),
        DASHBOARD_FILTER_KEYS,
    )
    result_false = filter_resources_locally(resources, parsed_false)
    assert [item["id"] for item in result_false] == [11, "bad"]

    parsed_id = parse_filters(("id=12",), DASHBOARD_FILTER_KEYS)
    assert filter_resources_locally(resources, parsed_id) == []

    # non-bool string reaches the ``return None`` path in ``_normalize_bool``
    weird_bool = filter_resources_locally(
        [{"id": 1, "is_managed_externally": "maybe"}],
        {"is_managed_externally": True},
    )
    assert weird_bool == []

    # ``actual is None`` branch in int coercion
    none_id = filter_resources_locally([{"id": None}], {"id": 1})
    assert none_id == []

    # non-string/non-bool ``coerce_bool_option`` path
    with pytest.raises(click.BadParameter):
        coerce_bool_option(1, "dry_run")


def test_filter_resources_locally_bool_does_not_match_numeric_strings() -> None:
    """
    Test bool filters do not match numeric-string values.
    """
    resources: list[dict[str, Any]] = [
        {"id": 1, "is_managed_externally": "1"},
        {"id": 2, "is_managed_externally": "0"},
    ]
    parsed_true = parse_filters(("is_managed_externally=true",), DASHBOARD_FILTER_KEYS)
    parsed_false = parse_filters(
        ("is_managed_externally=false",),
        DASHBOARD_FILTER_KEYS,
    )

    assert filter_resources_locally(resources, parsed_true) == []
    assert filter_resources_locally(resources, parsed_false) == []


def test_fetch_with_filter_fallback_local_keys() -> None:
    """
    Test ``fetch_with_filter_fallback`` takes the local path when LOCAL_FILTER_KEYS are present.
    """
    all_resources = [
        {"id": 1, "is_managed_externally": True},
        {"id": 2, "is_managed_externally": False},
    ]

    def fetch_filtered(**_kw: Any):
        raise AssertionError("should not be called")

    def fetch_all() -> list[dict[str, Any]]:
        return all_resources

    result = fetch_with_filter_fallback(
        fetch_filtered,
        fetch_all,
        {"is_managed_externally": True},
        "dashboards",
    )
    assert [r["id"] for r in result] == [1]


def test_fetch_with_filter_fallback_api_success() -> None:
    """
    Test ``fetch_with_filter_fallback`` happy path — API succeeds.
    """
    api_result = [{"id": 5, "slug": "test"}]

    def fetch_filtered(**_kw: Any):
        return api_result

    def fetch_all() -> list[dict[str, Any]]:
        raise AssertionError("should not be called")

    result = fetch_with_filter_fallback(
        fetch_filtered,
        fetch_all,
        {"slug": "test"},
        "dashboards",
    )
    assert result == api_result


def test_fetch_with_filter_fallback_api_success_reverifies_locally() -> None:
    """
    Test ``fetch_with_filter_fallback`` re-verifies API results locally.
    """
    api_result = [
        {"id": 1, "slug": "test"},
        {"id": 2, "slug": "other"},
    ]

    def fetch_filtered(**_kw: Any):
        return api_result

    def fetch_all() -> list[dict[str, Any]]:
        raise AssertionError("should not be called")

    result = fetch_with_filter_fallback(
        fetch_filtered,
        fetch_all,
        {"slug": "test"},
        "dashboards",
    )
    assert [row["id"] for row in result] == [1]


def test_fetch_with_filter_fallback_missing_keys_refetches_for_local_filter() -> None:
    """
    Test ``fetch_with_filter_fallback`` re-fetches when API payload omits filter keys.
    """
    api_result = [
        {"id": 1, "slug": "sales-overview"},
        {"id": 2, "slug": "marketing-overview"},
    ]
    full_result = [
        {"id": 1, "slug": "sales-overview", "dashboard_title": "Sales Overview"},
        {
            "id": 2,
            "slug": "marketing-overview",
            "dashboard_title": "Marketing Overview",
        },
    ]
    called = {"fetch_all": 0}

    def fetch_filtered(**_kw: Any):
        return api_result

    def fetch_all() -> list[dict[str, Any]]:
        called["fetch_all"] += 1
        return full_result

    result = fetch_with_filter_fallback(
        fetch_filtered,
        fetch_all,
        {"dashboard_title": Contains("sales")},
        "dashboards",
    )
    assert called["fetch_all"] == 1
    assert [row["id"] for row in result] == [1]


def test_fetch_with_filter_fallback_not_allowed_fallback() -> None:
    """
    Test ``fetch_with_filter_fallback`` falls back on filter-not-allowed error.
    """
    exc = SupersetError(
        errors=[{"message": "Filter column: slug not allowed to filter"}],
    )

    def fetch_filtered(**kw: Any):
        raise exc

    all_resources = [{"id": 1, "slug": "test"}, {"id": 2, "slug": "other"}]

    def fetch_all() -> list[dict[str, Any]]:
        return all_resources

    result = fetch_with_filter_fallback(
        fetch_filtered,
        fetch_all,
        {"slug": "test"},
        "dashboards",
    )
    assert [r["id"] for r in result] == [1]


def test_fetch_with_filter_fallback_unexpected_error() -> None:
    """
    Test ``fetch_with_filter_fallback`` wraps unexpected errors in ClickException.
    """

    def fetch_filtered(**kw: Any):
        raise RuntimeError("500 Internal Server Error")

    def fetch_all() -> list[dict[str, Any]]:
        return []

    with pytest.raises(click.ClickException, match="may not be supported"):
        fetch_with_filter_fallback(
            fetch_filtered,
            fetch_all,
            {"slug": "test"},
            "dashboards",
        )


def test_parse_filters_empty_key() -> None:
    """
    Test ``parse_filters`` rejects empty filter keys.
    """
    with pytest.raises(click.BadParameter, match="Filter key cannot be empty"):
        parse_filters(("=value",), DASHBOARD_FILTER_KEYS)
