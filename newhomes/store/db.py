"""SQLite connection + migration runner."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "sql" / "schema.sql"


def connect(db_path: str | Path) -> sqlite3.Connection:
    """Open a connection with sensible defaults."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), isolation_level=None)  # autocommit
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def init_db(db_path: str | Path) -> None:
    """Create the schema if it doesn't already exist."""
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    conn = connect(db_path)
    try:
        conn.executescript(schema_sql)
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Explicit transaction wrapper since we use autocommit."""
    conn.execute("BEGIN")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def source_id(conn: sqlite3.Connection, code: str) -> int:
    row = conn.execute("SELECT id FROM sources WHERE code = ?", (code,)).fetchone()
    if not row:
        raise ValueError(f"Unknown source code: {code!r}. Add it to sql/schema.sql sources seed.")
    return int(row["id"])
