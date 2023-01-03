# sqlitepie

`SQLite` made easy.

This module it's an `ORM` which aims to automate the work with `SQLite` databases to some extent.

---

### Installation:

```commandline
python -m pip install [--upgrade] sqlitepie
```

---

### How to:

<details>
<summary>SQLite</summary>
<p>

This is a [sqlite3](https://docs.python.org/3.7/library/sqlite3.html) handler used by the `ORM` for all SQL operations.

To use this module we must first connect to a database:

```python
from src.sqlitepie.engines import SQLite

sql = SQLite(database="test_db.sqlite")
```

As with the `sqlite3` module we can also pass the special name `:memory:` to create a database in RAM.

Besides our database path `SQLite` accepts all the other params that `sqlite3` is taking and also:

```python
from src.sqlitepie.engines import SQLite
from logpie import Logger, get_logger

log: Logger = get_logger(
    name="SQLitePie",
    basename="sqlitepie",
    handler="file",
    debug=True
)

sql = SQLite(
    database=r"\path\to\test_db.sqlite",
    ensure_folder=True,
    logger=log  # incompatible with all other logging modules
)
```

* `ensure_folder`:
    Will check if the folder exists, and it will create any intermediate path segment also (not just the rightmost)
    if it does not exist.
* `logger`:
    Will make use of that logger instance to emit debug and error messages.
    By default, it uses the `nostream` handler which as its name suggests it does not stream any messages
    (see [logpie](https://github.com/ClaudiuDrug/logpie)).

Executing SQL commands is simple:

```python
from src.sqlitepie.engines import SQLite

sql = SQLite(database="test_db.sqlite")

sql.execute_script(
    """
    -- some DDL script
    CREATE TABLE IF NOT EXISTS "accounts" (
        "user_id"   INTEGER NOT NULL UNIQUE,
        "firstname" TEXT NOT NULL,
        "lastname"  TEXT NOT NULL,
        "email"     TEXT NOT NULL UNIQUE,
        PRIMARY KEY ("user_id" AUTOINCREMENT)
    );
    """
)

sql.execute(
    'INSERT INTO "accounts" ("firstname", "lastname", "email") VALUES (?, ?, ?);',
    "Fred", "Flintstone", "fred.flintstone@stone-age.com"
)

sql.execute_many(
    'INSERT INTO "accounts" ("firstname", "lastname", "email") VALUES (?, ?, ?);',
    [
        ("Wilma", "Flintstone", "wilma.flintstone@stone-age.com"),
        ("Pebbles", "Flintstone", "pebbles.flintstone@stone-age.com"),
    ]
)

sql.close()
```

In the event of an exception, the transactions are rolled back (`ROLLBACK`).
Changes are also committed (`COMMIT`) automatically.

For SQL queries:

```python
from src.sqlitepie.engines import SQLite

sql = SQLite(database="test_db.sqlite")

cursor = sql.query('SELECT * FROM "accounts";')

rows = sql.fetchall(cursor)
for row in rows:
    print(row)

cursor.close()

sql.close()
```

By default, the returned rows are dictionaries.
To implement more advanced ways of returning rows we can use the param `row_factory` to pass a callable. 

```python
from src.sqlitepie.engines import SQLite
from collections import namedtuple

sql = SQLite(database="test_db.sqlite")

cursor = sql.query('SELECT * FROM "accounts";')
Row = namedtuple("Row", [item[0] for item in cursor.description])

def row_factory(item):
    if item is not None:
        return Row(*tuple(item))

row = sql.fetchone(cursor, row_factory=row_factory)
print(row)

cursor.close()
sql.close()
```

We can specify the number of returned rows as well either when calling the `query` method:

```python
cursor = sql.query('SELECT * FROM "accounts";', arraysize=1000)
```

Or when fetching the results:
```python
rows = sql.fetchmany(cursor, size=1000)
```

The method should try to fetch as many rows as indicated by the size parameter.
If this is not possible due to the specified number of rows not being available, fewer rows may be returned.

</p>
</details>

---
