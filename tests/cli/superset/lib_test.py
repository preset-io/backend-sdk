"""
Tests for ``preset_cli.cli.superset.lib``.
"""
# pylint: disable=unused-argument, invalid-name

from pathlib import Path

import yaml
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from preset_cli.cli.superset.lib import (
    LogType,
    clean_logs,
    get_logs,
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
