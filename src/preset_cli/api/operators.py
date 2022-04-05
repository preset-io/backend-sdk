"""
Operators for filtering the API.
"""

# pylint: disable=too-few-public-methods

from typing import Any


class Operator:
    """
    A filter operator.
    """

    operator = "invalid"

    def __init__(self, value: Any):
        self.value = value


class Equal(Operator):
    """
    Equality operator.
    """

    operator = "eq"


class OneToMany(Operator):
    """
    Operator for one-to-many relationships.
    """

    operator = "rel_o_m"
