# -*- coding: UTF-8 -*-

from abc import ABC
from typing import Any

from .constants import SCHEMA_NAME_LIST
from .engines import SQLite
from .exceptions import IllegalOperation, SchemaNameError, SQLiteEngineError


class Utils(ABC):
    """Descriptor utils."""

    @staticmethod
    def _get_name(instance: Any) -> str:
        return instance.__class__.__name__


class Descriptor(ABC):
    """Base descriptor."""

    def __set_name__(self, instance_type, attribute):
        self._type = instance_type
        self._attribute = attribute

    def __get__(self, instance, instance_type):
        if instance is None:
            return self
        return instance.__dict__.get(self._attribute)

    def __set__(self, instance, value):
        instance.__dict__.update({self._attribute: value})

    def __delete__(self, instance):
        if self._attribute in instance.__dict__:
            instance.__dict__.pop(self._attribute)


class Immutable(Descriptor, Utils):
    """Base immutable descriptor"""

    def __set__(self, instance, value):
        if self._attribute in instance.__dict__:
            raise IllegalOperation(
                f"{self._get_name(instance)} object does not support "
                f"'{self._attribute}' attribute update!"
            )
        super(Immutable, self).__set__(instance, value)

    def __delete__(self, instance):
        raise IllegalOperation(
            f"{self._get_name(instance)} object does not support "
            f"'{self._attribute}' attribute deletion!"
        )


class Toggle(Descriptor):
    """Base fallback descriptor."""

    def __init__(self, fallback: str = None):
        self._fallback = fallback

    def __get__(self, instance, instance_type) -> Any:
        if instance is None:
            return self
        return self._get_value(instance, self._attribute, self._fallback)

    @staticmethod
    def _get_value(instance: Any, attribute: str, fallback: str = None) -> Any:
        if fallback is None:
            return instance.__dict__.get(attribute)

        if attribute not in instance.__dict__:
            return instance.__dict__.get(fallback)

        return instance.__dict__.get(attribute)


class AnyVar(Descriptor, Utils):
    """Mutable attribute descriptor."""

    def __set__(self, instance: Any, value: Any):
        super(AnyVar, self).__set__(
            instance,
            self._check_value(instance, value)
        )

    def _check_value(self, instance: Any, value: Any) -> Any:
        if value is None:
            raise ValueError(
                f"{self._get_name(instance)} attribute '{self._attribute}' "
                f"cannot be `None`!"
            )
        return value


class StringVar(AnyVar):
    """Mutable attribute descriptor."""

    def _check_value(self, instance: Any, value: str) -> str:
        if (not isinstance(value, str)) or (not len(value) > 0):
            raise ValueError(
                f"{self._get_name(instance)} attribute '{self._attribute}' "
                f"must be a non-empty string value!"
            )
        return value


class BoolVar(AnyVar):
    """Mutable attribute descriptor."""

    def _check_value(self, instance: Any, value: bool) -> bool:
        if not isinstance(value, bool):
            raise ValueError(
                f"{self._get_name(instance)} attribute '{self._attribute}' "
                f"must be of type 'bool' not '{type(value).__name__}'!"
            )
        return value


class ImmutableVar(AnyVar, Immutable):
    """
    Immutable attribute descriptor.
    Once a value was assigned it can no longer be updated nor deleted.
    """


class ImmutableStringVar(StringVar, Immutable):
    """
    Immutable attribute descriptor.
    Once a value was assigned it can no longer be updated nor deleted.
    """


class ImmutableBoolVar(BoolVar, Immutable):
    """
    Immutable attribute descriptor.
    Once a value was assigned it can no longer be updated nor deleted.
    """


class ToggleVar(ImmutableVar, Toggle):
    """
    Immutable attribute descriptor which can take an
    argument used as fallback key to the original attribute lookup.
    """


class ToggleStringVar(ImmutableStringVar, Toggle):
    """
    Immutable attribute descriptor which can take an
    argument used as fallback key to the original attribute lookup.
    """


class SchemaName(ImmutableStringVar):
    """SQLite schema name attribute descriptor."""

    def _check_value(self, instance: Any, value: str) -> str:
        return self._check_name(
            super(SchemaName, self)._check_value(instance, value)
        )

    @staticmethod
    def _check_name(value: str) -> str:
        name = value.lower()
        if name not in SCHEMA_NAME_LIST:
            raise SchemaNameError(
                f"Schema 'name' attribute must be either "
                f"'main' or 'temp' not '{value}'!"
            )
        return name


class SQLiteEngine(ToggleVar):
    """SQLite schema engine attribute descriptor."""

    def __set__(self, instance, value):
        if self._fallback is not None:
            instance = instance.__dict__.get(self._fallback)
        super(SQLiteEngine, self).__set__(instance, value)

    def _check_value(self, instance: Any, value: SQLite) -> SQLite:
        if not isinstance(value, SQLite):
            raise SQLiteEngineError(
                f"SQLite engine attribute must be of type "
                f"'SQLite' not '{type(value).__name__}'!"
            )
        return value

    @staticmethod
    def _get_value(instance: Any, attribute: str, fallback: str = None) -> SQLite:
        if fallback is None:
            return instance.__dict__.get(attribute)

        if attribute not in instance.__dict__:
            instance = instance.__dict__.get(fallback)

        return instance.__dict__.get(attribute)
