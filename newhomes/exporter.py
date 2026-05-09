"""CSV exports for downstream consumers (the dashboard ingests these)."""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path


def export_developers(conn: sqlite3.Connection, out_path: str | Path) -> int:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(
        """
        SELECT  d.id AS developer_id, d.name, d.normalised_name, d.abn, d.type,
                d.primary_domain, d.fb_url, d.hq_state,
                p.name AS parent_name,
                (SELECT GROUP_CONCAT(DISTINCT s.code)
                   FROM source_records sr JOIN sources s ON s.id = sr.source_id
                  WHERE sr.developer_id = d.id) AS sources,
                d.last_verified_at
          FROM developers d
     LEFT JOIN developers p ON p.id = d.parent_developer_id
         ORDER BY d.id
        """
    ).fetchall()
    cols = [
        "developer_id","name","normalised_name","abn","type","primary_domain",
        "fb_url","hq_state","parent_name","sources","last_verified_at",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r[c] for c in cols])
    return len(rows)


def export_projects(conn: sqlite3.Connection, out_path: str | Path) -> int:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rows = conn.execute(
        """
        SELECT  p.id AS project_id, p.developer_id, d.name AS developer_name,
                p.name AS project_name, p.project_domain, p.fb_url,
                p.state, p.suburb, p.status,
                (SELECT GROUP_CONCAT(DISTINCT s.code)
                   FROM source_records sr JOIN sources s ON s.id = sr.source_id
                  WHERE sr.project_id = p.id) AS sources,
                p.last_verified_at
          FROM projects p
          JOIN developers d ON d.id = p.developer_id
         ORDER BY d.name, p.name
        """
    ).fetchall()
    cols = [
        "project_id","developer_id","developer_name","project_name",
        "project_domain","fb_url","state","suburb","status","sources",
        "last_verified_at",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r[c] for c in cols])
    return len(rows)
