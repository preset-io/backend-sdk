"""
Shared type aliases for native sync modules.
"""

from __future__ import annotations

from typing import Dict, List, Literal, TypeAlias
from uuid import UUID

AssetConfig = Dict[str, object]
JSONValue: TypeAlias = (
    str | int | float | bool | None | Dict[str, "JSONValue"] | List["JSONValue"]
)
JSONDict: TypeAlias = Dict[str, JSONValue]
ResourceDir: TypeAlias = Literal["databases", "datasets", "charts", "dashboards"]
UUIDLike: TypeAlias = str | UUID
