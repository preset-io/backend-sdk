"""
Tests for ``preset_cli.cli.superset.lib``.
"""
# pylint: disable=unused-argument, invalid-name

from pathlib import Path

import yaml
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.cli.superset.lib import (
    add_asset_to_log_dict,
    clean_logs,
    get_logs,
    write_logs_to_file,
)


def test_get_logs_new_file(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``get_logs`` helper when the log file does not exist.
    """
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", Path("progress.log"))
    assert get_logs() == {}


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

    assert get_logs() == logs_content


def test_add_asset_to_log_dict_asset_import() -> None:
    """
    Test the ``add_asset_to_log_dict`` helper when passing asset logs.
    """
    logs = {
        "assets": [
            {
                "path": "/path/to/root/first_path",
                "status": "SUCCESS",
                "uuid": "uuid1",
            },
        ],
    }
    skip = {Path("/path/to/root/first_path")}
    add_asset_to_log_dict(
        "assets",
        logs,
        "FAILED",
        "uuid2",
        asset_path=Path("/path/to/root/second_path"),
        set_=skip,
    )

    assert logs == {
        "assets": [
            {
                "path": "/path/to/root/first_path",
                "status": "SUCCESS",
                "uuid": "uuid1",
            },
            {
                "path": "/path/to/root/second_path",
                "status": "FAILED",
                "uuid": "uuid2",
            },
        ],
    }
    assert skip == {Path("/path/to/root/first_path"), Path("/path/to/root/second_path")}

    logs = {"assets": []}
    skip = set()
    add_asset_to_log_dict(
        "assets",
        logs,
        "SUCCESS",
        "uuid3",
        asset_path=Path("/path/to/root/third_path"),
        set_=skip,
    )

    assert logs == {
        "assets": [
            {
                "path": "/path/to/root/third_path",
                "status": "SUCCESS",
                "uuid": "uuid3",
            },
        ],
    }
    assert skip == {Path("/path/to/root/third_path")}


def test_add_asset_to_log_dict_ownership_import() -> None:
    """
    Test the ``add_asset_to_log_dict`` helper when passing ownership logs.
    """
    logs = {
        "assets": [
            {
                "path": "/path/to/root/first_path",
                "status": "SUCCESS",
                "uuid": "uuid1",
            },
        ],
    }
    add_asset_to_log_dict(
        "ownership",
        logs,
        "FAILED",
        "uuid2",
    )

    assert logs == {
        "assets": [
            {
                "path": "/path/to/root/first_path",
                "status": "SUCCESS",
                "uuid": "uuid1",
            },
        ],
        "ownership": [
            {
                "status": "FAILED",
                "uuid": "uuid2",
            },
        ],
    }


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
                        "status": "SUCCESS",
                        "uuid": "uuid1",
                    },
                ],
            },
        ),
    )
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", root / "progress.log")

    new_logs = {
        "assets": [
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
        "ownership": [
            {
                "status": "SUCCESS",
                "uuid": "uuid4",
            },
        ],
    }

    write_logs_to_file(new_logs)

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
    logs = {
        "assets": [
            {
                "path": "/path/to/root/first_path",
                "status": "SUCCESS",
                "uuid": "uuid1",
            },
        ],
    }
    fs.create_file(
        root / "progress.log",
        contents=yaml.dump(logs),
    )
    assert logs_path.exists()

    clean_logs("assets", logs)
    assert not logs_path.exists()


def test_clean_logs_keep_file(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test the ``clean_logs`` helper when the log file should be kept.
    """
    root = Path("/path/to/root")
    logs_path = root / "progress.log"
    mocker.patch("preset_cli.cli.superset.lib.LOG_FILE_PATH", logs_path)
    fs.create_dir(root)
    logs = {
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
    }
    fs.create_file(
        root / "progress.log",
        contents=yaml.dump(logs),
    )
    assert logs_path.exists()

    clean_logs("assets", logs)

    with open(logs_path, encoding="utf-8") as log:
        content = yaml.load(log, Loader=yaml.SafeLoader)

    assert content == {"ownership": [{"status": "SUCCESS", "uuid": "uuid2"}]}
