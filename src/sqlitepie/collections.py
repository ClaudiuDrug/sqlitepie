# -*- coding: UTF-8 -*-

from abc import ABC
from itertools import zip_longest
from typing import Union, Dict, Any, Iterator, List, Tuple

from .exceptions import MissingKeyError, DuplicateKeyError, DuplicateItemError, IllegalOperation


class Collection(ABC):
    """Base SQLite item collection."""

    __hash__ = None
    __slots__ = ("_fields",)

    def __init__(self, mapping: Union[Dict, List[Tuple[str, Any]]] = None, **kwargs):
        super(Collection, self).__setattr__("_fields", dict())

        self._update_collection(mapping, **kwargs)

    def __getattr__(self, key: str) -> Any:
        return self._get_by_key(key)

    def __setattr__(self, key: str, obj: Any):
        raise IllegalOperation(
            f"'{self.__type_name__.title()}' collection does not support attribute assignment!"
        )

    def __delattr__(self, item):
        raise IllegalOperation(
            f"'{self.__type_name__.title()}' collection does not support attribute deletion!"
        )

    def __getitem__(self, key: Union[str, int]) -> Any:
        return self._get_item(key)

    def __setitem__(self, key: str, value: Any):
        raise IllegalOperation(
            f"'{self.__type_name__.title()}' collection does not support item assignment!"
        )

    def __delitem__(self, key):
        raise IllegalOperation(
            f"'{self.__type_name__.title()}' collection does not support item deletion!"
        )

    def __iter__(self) -> Iterator:
        return iter(self._fields.values())

    def __len__(self) -> int:
        return len(self._fields)

    def __bool__(self) -> bool:
        return len(self._fields) > 0

    def __contains__(self, item: Any) -> bool:
        return item in self._fields.values()

    def __eq__(self, other) -> bool:
        for left, right in zip_longest(self._fields.values(), other.values()):
            if left is not right:
                return False
        return True

    @property
    def __type_name__(self) -> str:
        return self.__class__.__name__.lower()

    def items(self) -> List[Tuple[str, Any]]:
        return list(self._fields.items())

    def keys(self) -> List[str]:
        return list(self._fields.keys())

    def values(self) -> List[Any]:
        return list(self._fields.values())

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self._get_by_key(key)
        except MissingKeyError:
            return default

    def insert(self, key: str, value: Any):
        raise IllegalOperation(
            f"'{self.__type_name__.title()}' collection does not support the "
            f"method `insert`!"
        )

    def update(self, mapping: Union[Dict, List[Tuple[str, Any]]] = None, **kwargs):
        raise IllegalOperation(
            f"'{self.__type_name__.title()}' collection does not support the "
            f"method `update`!"
        )

    def clear(self):
        raise IllegalOperation(
            f"'{self.__type_name__.title()}' collection does not support the "
            f"method `clear`!"
        )

    def remove(self, key: str):
        raise IllegalOperation(
            f"'{self.__type_name__.title()}' collection does not support the "
            f"method `remove`!"
        )

    def _update_collection(self, mapping: Union[Dict, List[Tuple[str, Any]]] = None, **kwargs):
        if isinstance(mapping, list):
            self._update_fields(mapping)

        elif isinstance(mapping, dict):
            self._update_fields(list(mapping.items()))

        if len(kwargs) > 0:
            self._update_fields(list(kwargs.items()))

    def _update_fields(self, mapping: list):
        for key, value in mapping:
            self._fields.update(
                self._check_item(key, value)
            )

    def _check_item(self, key: str, value: Any) -> Dict[str, Any]:
        return {self._check_key(key): self._check_value(value)}

    def _check_key(self, value: str):
        if value in self._fields:
            raise DuplicateKeyError(
                f"Cannot have duplicate keys ('{value}') in "
                f"'{self.__type_name__.title()}' collection!"
            )
        return value

    def _check_value(self, value: Any):
        if value in self._fields.values():
            raise DuplicateItemError(
                f"Cannot have duplicate items ('{self._item_name(value)}') in "
                f"'{self.__type_name__.title()}' collection!"
            )
        return value

    def _get_item(self, key: Union[str, int]) -> Any:
        if not isinstance(key, (str, int)):
            raise TypeError(
                f"'{self.__type_name__.title()}' keys must be of type "
                f"'str' or 'int' not '{type(key).__name__}'!"
            )

        if isinstance(key, str):
            return self._get_by_key(key)

        return self._get_by_index(key)

    def _get_by_key(self, key: str) -> Any:
        if key not in self._fields:
            raise MissingKeyError(
                f"No such key '{key}' in '{self.__type_name__.title()}' collection!"
            )
        return self._fields.get(key)

    def _get_by_index(self, key: int) -> Any:
        if key >= len(self._fields):
            raise IndexError(
                f"'{self.__type_name__.title()}' collection index '{key}' out of range!"
            )
        return list(self._fields.values())[key]

    @staticmethod
    def _item_name(value: Any) -> str:
        if hasattr(value, "key"):
            return value.key

        if hasattr(value, "name"):
            return value.name

        return value
