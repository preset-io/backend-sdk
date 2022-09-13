"""
Custom types.
"""

from typing import List, TypedDict


class UserType(TypedDict):
    """
    Schema for a user.
    """

    id: int
    username: str
    role: List[str]
    first_name: str
    last_name: str
    email: str
