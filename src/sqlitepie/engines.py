# -*- coding: UTF-8 -*-

from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal
from os.path import realpath
from sqlite3 import (
    Connection,
    Cursor,
    connect,
    PARSE_COLNAMES,
    PARSE_DECLTYPES,
    Error,
    ProgrammingError,
    Row,
    register_adapter,
    register_converter,
)
from threading import RLock
from typing import Iterable, List, Dict, Any, Union

from logpie import get_logger, Logger

from .constants import LOCKS
from .utils import from_decimal, to_decimal, ensure_folder, parse_uri

# Registering adapters:
register_adapter(Decimal, from_decimal)

# Registering converters:
register_converter("DECIMAL", to_decimal)


class SQLiteConnection(Connection):

    def __init__(self, *args, **kwargs):
        super(SQLiteConnection, self).__init__(*args, **kwargs)
        self.row_factory = Row


class SQLiteCursor(Cursor):

    @staticmethod
    def _to_dict(row: Row) -> dict:
        if row is not None:
            return dict(zip(row.keys(), tuple(row)))

    def __init__(self, *args, **kwargs):
        super(SQLiteCursor, self).__init__(*args, **kwargs)
        self._is_closed: bool = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def fetchall(self, **kwargs) -> List[Union[Dict, Any]]:
        """
        Fetch all (remaining) rows of a query result.

        **Keyword arguments:**
            ``row_factory``: Any
                A callable that accepts the row as parameter and implements
                more advanced ways of returning results.

        :param kwargs: Additional keyword arguments.
        :return: Search results as a list of dictionaries. An empty list is
            returned when no rows are available.
        :raise ProgrammingError: If operating on a closed cursor.
        """
        factory = kwargs.pop("row_factory", self._to_dict)

        try:
            return [
                factory(row)
                for row in super(SQLiteCursor, self).fetchall()
            ]
        except ProgrammingError:
            raise
        finally:
            self.close()

    def fetchmany(self, **kwargs) -> List[Union[Dict, Any]]:
        """
        Fetches the next set of rows of a query result, returning a list.
        An empty list is returned when no more rows are available.

        **Keyword arguments:**
            ``size``: int
                The number of rows to be fetched.
            ``row_factory``: Any
                A callable that accepts the row as parameter and implements
                more advanced ways of returning results.

        :param kwargs: Optional keyword arguments.
        :return: A list of rows.
        :raise ProgrammingError: If operating on a closed cursor.
        """
        factory = kwargs.pop("row_factory", self._to_dict)
        rows: list = []

        try:
            rows.extend(
                factory(row)
                for row in super(SQLiteCursor, self).fetchmany(**kwargs)
            )
        except ProgrammingError:
            raise
        else:
            return rows
        finally:
            if len(rows) == 0:
                self.close()

    def fetchone(self, **kwargs) -> Union[Dict, Any]:
        """
        Fetches the next row of a query result set, returning a single
        sequence, or None when no more data is available.

        **Keyword arguments:**
            ``row_factory``: Any
                A callable that accepts the row as parameter and implements
                more advanced ways of returning results.

        :param kwargs: Additional keyword arguments.
        :return: The next row as a dictionary.
        :raise ProgrammingError: If operating on a closed cursor.
        """

        factory = kwargs.pop("row_factory", self._to_dict)

        try:
            return factory(
                super(SQLiteCursor, self).fetchone()
            )
        except ProgrammingError:
            raise
        finally:
            self.close()

    def close(self):
        """Close this cursor and set `_closed` as `True`."""
        super(SQLiteCursor, self).close()
        self._is_closed = True

    def is_closed(self) -> bool:
        return self._is_closed


class Engine(ABC):
    """Base `SQLite` database engine."""

    @staticmethod
    def _get_lock(key: Union[str, bytes, int]) -> RLock:
        if key not in LOCKS:
            instance = RLock()
            LOCKS.update({key: instance})
        return LOCKS.get(key)

    @staticmethod
    def _get_file_path(database: str, uri: bool) -> str:
        if (
            uri and
            isinstance(uri, bool) and
            database.startswith("file:")
        ):
            return realpath(parse_uri(database).file)
        return realpath(database)

    def __init__(
            self,
            database: str,
            timeout: float = 5.0,
            detect_types: int = 0,
            isolation_level: str = "DEFERRED",
            check_same_thread: bool = True,
            factory: Connection = Connection,
            cached_statements: int = 100,
            uri: bool = False,
    ):
        self._database = database
        self._timeout = timeout
        self._detect_types = detect_types
        self._isolation_level = isolation_level
        self._check_same_thread = check_same_thread
        self._factory = factory
        self._cached_statements = cached_statements
        self._uri = uri

        self._file_path = self._get_file_path(self._database, self._uri)
        self._thread_lock: RLock = self._get_lock(self._file_path)

        self._connection: Connection = self.acquire(
            self._database,
            self._timeout,
            self._detect_types,
            self._isolation_level,
            self._check_same_thread,
            self._factory,
            self._cached_statements,
            self._uri
        )

    def __enter__(self):
        self._thread_lock.acquire()
        try:
            if not hasattr(self, "_connection"):
                self._connection: Connection = self.acquire(
                    self._database,
                    self._timeout,
                    self._detect_types,
                    self._isolation_level,
                    self._check_same_thread,
                    self._factory,
                    self._cached_statements,
                    self._uri
                )
        except Error:
            self._thread_lock.release()
            raise
        else:
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, "_connection"):
            self.release(self._connection)
            del self._connection
        self._thread_lock.release()

    @abstractmethod
    def acquire(self, *args, **kwargs) -> Connection:
        raise NotImplementedError

    @abstractmethod
    def release(self, *args, **kwargs):
        raise NotImplementedError

    def close(self):
        """Close the connection with the sqlite database file and release the resources."""
        with self._thread_lock:
            if hasattr(self, "_connection"):
                self.release(self._connection)
                del self._connection


class SQLite(Engine):

    def __init__(
            self,
            database: str,
            timeout: float = 5.0,
            detect_types: int = PARSE_COLNAMES | PARSE_DECLTYPES,
            isolation_level: str = "DEFERRED",
            check_same_thread: bool = True,
            factory: Connection = SQLiteConnection,
            cached_statements: int = 100,
            uri: bool = False,
            **kwargs
    ):

        self._ensure_folder: bool = kwargs.pop("ensure_folder", False)
        self._log: Logger = kwargs.pop(
            "logger",
            get_logger("sqlitepie", state="off")
        )

        super(SQLite, self).__init__(
            database,
            timeout,
            detect_types,
            isolation_level,
            check_same_thread,
            factory,
            cached_statements,
            uri
        )

    def acquire(self, *args, **kwargs) -> Connection:
        """Acquire and return a new SQLite connection."""

        self._thread_lock.acquire()
        self._log.debug(f"Connecting with the SQLite database '{self._file_path}'...")

        self._check_path(self._file_path)

        try:
            connection: Connection = connect(*args, **kwargs)
        except Error as sql_error:
            self._log.error(
                f"Failed to connect with the SQLite database '{self._file_path}'!",
                exc_info=sql_error
            )
            raise
        else:
            self._log.debug(f"Successfully connected with the SQLite database '{self._file_path}'.")
            return connection
        finally:
            self._thread_lock.release()

    def release(self, connection: Connection):
        """Close the connection with the sqlite database file and release the resources."""

        self._thread_lock.acquire()
        self._log.debug(f"Closing the connection with the SQLite database '{self._file_path}'...")
        try:
            connection.close()
        except Error as sql_error:
            self._log.warning(
                f"Failed to close connection with the SQLite database '{self._file_path}'!",
                exc_info=sql_error
            )
        else:
            self._log.debug(f"Terminated connection with the SQLite database '{self._file_path}'.")
        finally:
            self._thread_lock.release()

    def _check_path(self, file_path: str):
        """
        Check if the folder tree exists and creates it
        if database is not ':memory:'.
        """
        if self._ensure_folder and (file_path != ":memory:"):
            ensure_folder(file_path)

    def query(self, sql: str, *args, **kwargs) -> Cursor:
        """
        Execute `sql` query statement.
        Bind values to the statement using placeholders that map to the
        sequence `args`.

        **Keyword arguments:**
            ``arraysize``:
                The cursor’s `arraysize` which determines the number
                of rows to be fetched.

        :param sql: SQL command.
        :param args: Parameter substitution to avoid using Python’s string
            operations.
        :param kwargs: Additional keyword arguments.
        :return: The cursor instance.
        """

        self._thread_lock.acquire()
        self._log.debug(sql)

        cursor: Cursor = self._get_cursor(**kwargs)

        try:
            return cursor.execute(sql, args)
        except Error as sql_error:
            self._log.error("Failed to execute the last SQLite query!", exc_info=sql_error)
            cursor.close()
            raise
        finally:
            self._thread_lock.release()

    def _get_cursor(self, **kwargs) -> Union[Cursor, SQLiteCursor]:
        self._thread_lock.acquire()
        self._log.debug("Acquiring a new SQLite cursor...")
        try:
            cursor = self._connection.cursor(SQLiteCursor)
        except Error as sqlite_error:
            self._log.error("Failed to acquire a SQLite cursor!", exc_info=sqlite_error)
            raise
        else:
            self._log.debug(f"Successfully acquired SQLite cursor.")
            if "arraysize" in kwargs:
                cursor.arraysize = kwargs.pop("arraysize")
            return cursor
        finally:
            self._thread_lock.release()

    def execute(self, sql: str, *args):
        """
        Execute SQL statement `sql`.
        Bind values to the statement using placeholders that map to the
        sequence `args`. If error occurs database will roll back the
        last transaction(s) else it will commit the changes.

        :param sql: SQL command.
        :param args: Parameter substitution to avoid using Python’s string
            operations.
        """
        self._thread_lock.acquire()
        self._log.debug(sql)
        try:
            with self._connection:
                self._connection.execute(sql, args)
        except Error as sql_error:
            self._log.error("Last sqlite transaction(s) failed!", exc_info=sql_error)
            raise
        finally:
            self._thread_lock.release()

    def execute_many(self, sql: str, data: Iterable[Iterable]):
        """
        Execute parameterized SQL statement `sql` against all parameter sequences
        or mappings found in the sequence `args`. It is also possible to
        use an iterator yielding parameters instead of a sequence. Uses the
        same implicit transaction handling as :func:`execute()`. If error occurs
        database will :func:`rollback()` the last transaction(s) else it will commit the changes.

        :param sql: SQL command.
        :param data: Parameter substitution to avoid using Python’s string operations.
        """
        self._thread_lock.acquire()
        self._log.debug(sql)
        try:
            with self._connection:
                self._connection.executemany(sql, data)
        except Error as sql_error:
            self._log.error("Last sqlite transaction(s) failed!", exc_info=sql_error)
            raise
        finally:
            self._thread_lock.release()

    def execute_script(self, sql_script: str):
        """
        Execute the SQL statements in `sql_script`.
        If there is a pending transaction, an implicit `COMMIT` statement is
        executed first. No other implicit transaction control is performed;
        any transaction control must be added to sql_script.

        If error occurs database will roll back the last transaction(s) else it
        will commit the changes.

        :param sql_script: SQL script.
        """
        self._thread_lock.acquire()
        self._log.debug(sql_script)
        try:
            with self._connection:
                self._connection.executescript(sql_script)
        except Error as sql_error:
            self._log.error("Last sqlite transaction(s) failed!", exc_info=sql_error)
            raise
        finally:
            self._thread_lock.release()
