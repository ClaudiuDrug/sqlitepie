# -*- coding: UTF-8 -*-

from dataclasses import dataclass, make_dataclass
from decimal import Decimal
from os import makedirs
from os.path import dirname, realpath, exists
from typing import Union, Any, Tuple, List


def from_decimal(value: Union[Decimal, bytes], encoding: str = "UTF-8") -> bytes:
    """From decimal to bytes."""
    if isinstance(value, Decimal):
        return encode(str(value), encoding)
    return value


def to_decimal(value: Union[bytes, Decimal], encoding: str = "UTF-8") -> Decimal:
    """From bytes to decimal."""
    if isinstance(value, bytes):
        return Decimal(
            decode(value, encoding)
        )
    return value


def encode(value: Union[str, bytes], encoding: str = "UTF-8") -> bytes:
    if not isinstance(value, (str, bytes)):
        raise TypeError(
            f"'value' must be of type 'str' or 'bytes' not '{type(value).__name__}'!"
        )
    if isinstance(value, str):
        return value.encode(encoding)
    return value


def decode(value: Union[bytes, str], encoding: str = "UTF-8") -> str:
    if not isinstance(value, (bytes, str)):
        raise TypeError(
            f"'value' must be of type 'bytes' or 'str' not '{type(value).__name__}'!"
        )
    if isinstance(value, bytes):
        return value.decode(encoding)
    return value


def ensure_folder(path: str):
    """
    Read the file path and recursively create the folder structure if needed.
    """
    path: str = dirname(realpath(path))

    if not exists(path):
        make_dirs(path)


def make_dirs(path: str):
    """Checks if a folder path exists and create one if not."""
    try:
        makedirs(path)
    except FileExistsError:
        pass


def single_quote(value: Any) -> Any:
    if isinstance(value, str):
        return f"'{value}'"
    return value


# def double_quote(value: Any) -> Any:
#     if isinstance(value, str):
#         return f'"{value}"'
#     return value


def to_list(args: Union[Tuple, List]) -> List:
    if not isinstance(args, list):
        return list(args)
    return args


# def dict_factory(cursor: Cursor, row: tuple) -> dict:
#     return {
#         col[0]: row[idx]
#         for idx, col in enumerate(cursor.description)
#     }


@dataclass
class UniformResourceIdentifier(object):
    """SQLite `URI` base class."""
    file: str

    def __str__(self) -> str:
        return self.as_string()

    def as_string(self) -> str:
        params: list = [
            f"{key}={value}"
            for key, value in self.__dict__.items()
            if key != "file"
        ]
        return f"file:{self.file}?{'&'.join(params)}"


def parse_uri(value: str) -> UniformResourceIdentifier:
    """
    Parse the SQLite URI string `value` and return a
    :class:`UniformResourceIdentifier` dataclass.
    """
    file, params = value.split("?")
    params = dict(tuple(param.split("=")) for param in params.split("&"))
    uri = make_dataclass(
        "URI",
        fields=[
            "file",
            *params.keys()
        ],
        bases=(UniformResourceIdentifier,)
    )
    return uri(file=del_prefix(file, "file:"), **params)


def del_prefix(target: str, prefix: str) -> str:
    """
    If `target` starts with the `prefix` string and `prefix` is not empty,
    return string[len(prefix):].
    Otherwise, return a copy of the original string.
    """
    if (len(prefix) > 0) and target.startswith(prefix):
        try:  # python >= 3.9
            return target.removeprefix(prefix)
        except AttributeError:  # python <= 3.7
            return target[len(prefix):]
    return target
