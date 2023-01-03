# -*- coding: UTF-8 -*-

from weakref import WeakValueDictionary

# thread lock instances:
LOCKS = WeakValueDictionary()

SCHEMA_NAME_LIST: list = ["main", "temp"]

ON_CONFLICT: list = [
    "ROLLBACK",
    "ABORT",
    "FAIL",
    "IGNORE",
    "REPLACE",
]

ORDER: list = [
    "ASC",
    "DESC",
]
