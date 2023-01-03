# -*- coding: UTF-8 -*-

from __future__ import annotations

from abc import ABC
from typing import Union, Generator, Dict, List, Tuple

from .collections import Collection
from .constraints import PrimaryKey
from .descriptors import (
    ImmutableVar,
    ImmutableStringVar,
    ImmutableBoolVar,
    ToggleVar,
    ToggleStringVar,
    SchemaName,
    SQLiteEngine,
)
from .engines import SQLite
from .exceptions import ArgumentError
from .mixins import Mixin
from .utils import single_quote, to_list


class Model(ABC):
    """Base SQLite model."""

    name: str = ImmutableStringVar()

    @staticmethod
    def _filter_items(target: str, args: Union[Tuple, List], kwargs: dict) -> Generator:

        if not target.islower():
            target: str = target.lower()

        args: list = to_list(args)

        for arg in args.copy():
            if hasattr(arg, "__type_name__") and (arg.__type_name__ == target):
                idx = args.index(arg)
                yield args.pop(idx)

        for key, value in kwargs.copy().items():
            if hasattr(value, "__type_name__") and (value.__type_name__ == target):
                item = kwargs.pop(key)
                item.key = key
                yield item

    @property
    def __type_name__(self) -> str:
        return self.__class__.__name__.lower()

    def _set_child(self, model: BaseItem):
        model.parent = self

    def _check_params(self, args: Union[Tuple, List], kwargs: Dict):
        args: list = to_list(args)
        params: list = args + list(kwargs)

        if len(params) > 0:
            params = [single_quote(param) for param in params]
            raise ArgumentError(
                f"Failed to resolve parameters ({', '.join(params)}) "
                f"for {self.__type_name__} '{self.name}'!"
            )


class BaseSchema(Model, Mixin):
    """Base SQLite schema."""

    name: str = SchemaName()
    engine: SQLite = SQLiteEngine()

    tables: Tables = ImmutableVar()
    indexes: Indexes = ImmutableVar()
    views: Views = ImmutableVar()
    triggers: Triggers = ImmutableVar()

    def add_table(self, model: Table):
        self._set_child(model)
        self.tables.insert(model.key, model)

    def add_index(self, model: Index):
        self._set_child(model)
        self.indexes.insert(model.key, model)

    def add_view(self, model: View):
        self._set_child(model)
        self.views.insert(model.key, model)

    def add_trigger(self, model: Trigger):
        self._set_child(model)
        self.triggers.insert(model.key, model)

    def _set_child(self, model: SchemaItem):
        model.schema = self
        super(BaseSchema, self)._set_child(model)


class Schema(BaseSchema):
    """SQLite schema model."""

    def __init__(self, name: str = "main", *args, **kwargs):
        args: list = to_list(args)

        self.name: str = name

        self.tables = Tables()
        self.indexes = Indexes()
        self.views = Views()
        self.triggers = Triggers()

        if "engine" in kwargs:
            self.engine: SQLite = kwargs.pop("engine")

        self._check_params(args, kwargs)


class BaseItem(Model, Mixin):
    """Base SQLite schema item."""

    key: str = ToggleStringVar("name")
    parent: SchemaItem = ImmutableVar()

    def _traverse(self, key, value):
        if value is self.parent:
            return super(BaseItem, self)._traverse(key, value.name)
        return super(BaseItem, self)._traverse(key, value)


class SchemaItem(BaseItem):
    """SQLite schema item."""

    parent: Schema = ImmutableVar()
    schema: Schema = ToggleVar("parent")
    engine: SQLite = SQLiteEngine("schema")


class Table(SchemaItem):
    """SQLite table model."""

    row_id: bool = ImmutableBoolVar()
    c = columns = ImmutableVar()

    def __init__(self, name: str, schema: Schema, *args, **kwargs):
        self.name = name

        schema.add_table(self)

        args: list = to_list(args)

        self.columns: Columns = self._rezolve_columns(args, kwargs)

        # TODO: Constraints!!!

        # `False` to include `WITHOUT ROWID` in `CREATE` statement
        self.row_id: bool = kwargs.pop("row_id", True)

        if "engine" in kwargs:
            self.engine: SQLite = kwargs.pop("engine")

        self._check_params(args, kwargs)

    def _rezolve_columns(self, args, kwargs) -> Columns:
        columns = list(
            self._filter_columns(args, kwargs)
        )
        return Columns(columns)

    def _filter_columns(self, args, kwargs) -> Generator:
        for column in self._filter_items("column", args, kwargs):
            self._set_child(column)
            yield column.key, column

    def add_column(self, model: Column):
        self._set_child(model)
        self.columns.insert(model.key, model)

    def _set_child(self, model: BaseItem):
        model.table = self
        super(Table, self)._set_child(model)


class View(SchemaItem):
    """SQLite view model."""


class Index(SchemaItem):
    """SQLite index model."""


class Trigger(SchemaItem):
    """SQLite trigger model."""


class Column(BaseItem):
    """SQLite column model."""

    type: str = ImmutableStringVar()

    null: bool = ImmutableBoolVar()
    primary: PrimaryKey = ImmutableVar()
    # autoincrement: bool = ImmutableBoolVar()
    unique: bool = ImmutableBoolVar()
    index: bool = ImmutableBoolVar()

    table: Table = ImmutableVar()

    def __init__(self, name: str, type: str = "TEXT", *args, **kwargs):
        self.name = name
        self.type = type

        self.null: bool = kwargs.pop("null", True)
        self.primary: Union[PrimaryKey, bool] = kwargs.pop("primary", False)
        # self.autoincrement: bool = kwargs.pop("autoincrement", False)
        self.unique: bool = kwargs.pop("unique", False)

        self.index: bool = kwargs.pop("index", False)

        self._check_params(args, kwargs)


class MixinCollection(Collection, Mixin):
    """Collection handler."""

    def as_dict(self) -> dict:
        return self._traverse_dict(self._fields)

    def insert(self, key: str, value: BaseItem):
        self._fields.update(
            self._check_item(key, value)
        )


class Tables(MixinCollection):
    """Tables collection."""

    def insert(self, key: str, value: Table):
        super(Tables, self).insert(key, value)


class Indexes(MixinCollection):
    """Indexes collection."""


class Views(MixinCollection):
    """Views collection."""


class Triggers(MixinCollection):
    """Triggers collection."""


class Columns(MixinCollection):
    """SQLite columns collection."""

    def insert(self, key: str, value: Column):
        super(Columns, self).insert(key, value)
