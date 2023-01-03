# -*- coding: UTF-8 -*-

from abc import ABC

from .constants import ON_CONFLICT
from .descriptors import ImmutableStringVar, ImmutableBoolVar
from .exceptions import ConstraintError
from .mixins import Mixin


class Model(ABC):

    name: str = ImmutableStringVar()
    __type_name__: str = ImmutableStringVar()


class Constraint(Model, Mixin):
    """Base SQLite constraint."""


class OnConflict(ABC):
    """SQLite conflict resolution algorithm."""

    on_conflict: str = ImmutableStringVar()

    @staticmethod
    def _check_resolution(value: str) -> str:
        algorithm: str = value.upper()
        if algorithm not in ON_CONFLICT:
            raise ConstraintError(
                f"'ON CONFLICT' resolution algorithm must be one of "
                f"({tuple(ON_CONFLICT)}) not '{value}'!"
            )
        return algorithm


class PrimaryKey(Constraint, OnConflict):
    """SQLite primary key constraint."""

    order: str = ImmutableStringVar()
    autoincrement: bool = ImmutableBoolVar()

    def __init__(self, name: str, **kwargs):
        self.name = name
        self.order = kwargs.pop("order", "ASC")

        self.on_conflict: str = self._check_resolution(
            kwargs.pop("on_conflict", "ABORT")
        )

        self.autoincrement: bool = kwargs.pop("autoincrement", False)
