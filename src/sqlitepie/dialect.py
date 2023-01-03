# -*- coding: UTF-8 -*-

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Union, Any
from .registry import ClassRegistry


class Command(ABC):
    """Base command factory."""

    # def _dispatch(self, handler: Any, *args, **kwargs) -> Syntax:
    #     syntax = handler(model=self)
    #     return syntax(*args, *kwargs)


class DDL(Command):
    """Base DDL command factory."""

    @abstractmethod
    def create(self, *args, **kwargs) -> Syntax:
        raise NotImplementedError

    @abstractmethod
    def drop(self, *args, **kwargs) -> Syntax:
        raise NotImplementedError


class TableDDL(DDL):
    """`TABLE` DDL commands."""

    def create(self, if_not_exists: bool = False) -> Syntax:
        return ClassRegistry.get("create_table", if_not_exists=if_not_exists)

    def drop(self, if_exists: bool = False) -> Syntax:
        return ClassRegistry.get("drop_table", if_exists=if_exists)


class Syntax(ABC):
    """Base sqlite syntax handler."""

    def __init__(self, model: Any):
        self._model = model

    def __call__(self, *args, **kwargs):
        self._statement = self._make_stmt(self._model)
        return self

    @property
    def statement(self) -> str:
        return self._statement

    @staticmethod
    def _make_stmt(model: Any) -> str:
        return model.typename


class ConflictClause(Syntax):
    """ON CONFLICT"""


@ClassRegistry.register("create_table")
class CreateTable(Syntax):
    """CREATE TABLE"""


@ClassRegistry.register("drop_table")
class DropTable(Syntax):
    """DROP TABLE"""
