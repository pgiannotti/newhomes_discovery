"""Write SourceRecords to the audit log. This is the only path through which
sources insert facts — they never touch developers/projects directly. The
entity_resolution stage promotes audit rows to canonical rows.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Iterable

from ..store.db import source_id, transaction
from ..store.models import SourceRecord


def write_records(
    conn: sqlite3.Connection,
    records: Iterable[SourceRecord],
    discovery_run_id: int | None = None,
) -> int:
    """Bulk-write SourceRecords. Returns count inserted."""
    rows = list(records)
    if not rows:
        return 0
    src_cache: dict[str, int] = {}
    inserted = 0
    with transaction(conn):
        for r in rows:
            sid = src_cache.get(r.source_code)
            if sid is None:
                sid = source_id(conn, r.source_code)
                src_cache[r.source_code] = sid
            conn.execute(
                """
                INSERT INTO source_records (
                    source_id, discovery_run_id, raw_url, raw_html_path,
                    parsed_developer_name, parsed_project_name,
                    parsed_project_domain, parsed_fb_url,
                    parsed_state, parsed_suburb, parsed_status,
                    confidence, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    sid, discovery_run_id, r.raw_url, r.raw_html_path,
                    r.parsed_developer_name, r.parsed_project_name,
                    r.parsed_project_domain, r.parsed_fb_url,
                    r.parsed_state, r.parsed_suburb, r.parsed_status,
                    r.confidence, json.dumps(r.extra) if r.extra else None,
                ),
            )
            inserted += 1
    return inserted


def start_run(conn: sqlite3.Connection, source_code: str, args: dict) -> int:
    """Create a discovery_runs row and return its id."""
    cur = conn.execute(
        "INSERT INTO discovery_runs (source_code, args_json) VALUES (?, ?)",
        (source_code, json.dumps(args)),
    )
    return int(cur.lastrowid)


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    rows_inserted: int,
    status: str = "ok",
    error: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE discovery_runs
           SET finished_at = CURRENT_TIMESTAMP,
               status = ?, rows_inserted = ?, error = ?
         WHERE id = ?
        """,
        (status, rows_inserted, error, run_id),
    )
