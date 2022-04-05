"""
Basic helper functions.
"""

from pathlib import Path


def remove_root(file_path: str) -> str:
    """
    Remove the first directory of a path.
    """
    full_path = Path(file_path)
    return str(Path(*full_path.parts[1:]))
