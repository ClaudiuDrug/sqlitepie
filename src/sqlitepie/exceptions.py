# -*- coding: UTF-8 -*-


class SQLiteError(Exception):
    """Base exception class."""


class IllegalOperation(SQLiteError):
    """Exception raised for calls to restricted methods."""


class SchemaNameError(SQLiteError):
    """Exception raised for schema name errors."""


class SQLiteEngineError(SQLiteError):
    """Exception raised for engine related errors."""


class MissingKeyError(SQLiteError):
    """Exception raised for missing keys."""


class IndexTypeError(SQLiteError):
    """Exception raised for key type errors."""


class DuplicateKeyError(SQLiteError):
    """Exception raised for duplicate keys."""


class DuplicateItemError(SQLiteError):
    """Exception raised for duplicate values."""


class ArgumentError(SQLiteError):
    """Exception raised for argument errors."""


class ConstraintError(SQLiteError):
    """
    Exception raised for constraint conflict resolution algorithm errors.
    """


class RegistryKeyError(SQLiteError):
    """Exception raised for registry key errors."""


# class MissingEngineError(SQLiteError):
#     """Exception raised for missing sqlite engine."""


# class MissingColumnsError(SQLiteError):
#     """Exception raised for missing columns."""
