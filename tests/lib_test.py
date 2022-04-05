"""
Tests for ``preset_cli.lib``.
"""

from preset_cli.lib import remove_root


def test_remove_root() -> None:
    """
    Test ``remove_root``.
    """
    assert remove_root("bundle/database/examples.yaml") == "database/examples.yaml"
