"""End-to-end smoke test using mock source records.

No network. No Playwright. No API keys. Demonstrates that:

  * schema initialises
  * SourceRecords insert
  * entity_resolution promotes them into developers + projects
  * CSV exports work and contain the expected rows
"""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from newhomes import entity_resolution, exporter
from newhomes.core.audit import write_records
from newhomes.store.db import connect, init_db
from newhomes.store.models import SourceRecord


def make_db(tmpdir: Path) -> sqlite3.Connection:
    db_path = tmpdir / "smoke.db"
    init_db(db_path)
    return connect(db_path)


def test_smoke_end_to_end(tmp_path: Path):
    conn = make_db(tmp_path)

    # 1. Stage a handful of records as if multiple sources had run.
    records = [
        # UDIA-style: developer rows
        SourceRecord(
            source_code="udia", raw_url="https://udiansw.com.au/about/membership",
            parsed_developer_name="Stockland", parsed_state="NSW",
            confidence=0.85, extra={"developer_domain": "stockland.com.au"},
        ),
        SourceRecord(
            source_code="udia", raw_url="https://udiavic.com.au/Membership/UDIA-Members",
            parsed_developer_name="Mirvac Group", parsed_state="VIC",
            confidence=0.85, extra={"developer_domain": "mirvac.com"},
        ),
        # Realestate-style: developer + project + suburb + state
        SourceRecord(
            source_code="realestate_com_au",
            raw_url="https://www.realestate.com.au/new-homes/in-vic/list-1",
            parsed_developer_name="Mirvac", parsed_project_name="Smiths Lane",
            parsed_state="VIC", parsed_suburb="Clyde North",
            parsed_status="selling", confidence=0.8,
            extra={"listing_url": "https://www.realestate.com.au/project/smiths-lane-clyde-north"},
        ),
        SourceRecord(
            source_code="realestate_com_au",
            raw_url="https://www.realestate.com.au/new-homes/in-nsw/list-1",
            parsed_developer_name="Stockland",  parsed_project_name="Aura",
            parsed_state="QLD", parsed_suburb="Caloundra South",
            parsed_status="selling", confidence=0.8,
        ),
        # Same project, second source — should de-dupe under same developer.
        SourceRecord(
            source_code="planning_nsw",
            raw_url="https://api.planningportal.nsw.gov.au/major-projects/api/v1/projects?id=42",
            parsed_developer_name="Stockland Development Pty Ltd",
            parsed_project_name="Aura",
            parsed_state="NSW", confidence=0.75,
        ),
        # SERP-style: domain only, weak signal — should still collapse onto Stockland
        SourceRecord(
            source_code="google_serp", raw_url="https://stockland.com.au/aura",
            parsed_project_name="Aura — Stockland",
            parsed_project_domain="stockland.com.au",
            confidence=0.5,
            extra={"developer_domain": "stockland.com.au"},
        ),
    ]
    inserted = write_records(conn, records)
    assert inserted == len(records)

    # 2. Run entity resolution.
    counts = entity_resolution.resolve(conn)
    assert counts["developers_inserted"] >= 2, counts
    assert counts["projects_inserted"] >= 2, counts

    # 3. Validate developers table has both Stockland & Mirvac and they were
    #    de-duped despite "Stockland Development Pty Ltd" / "Stockland".
    devs = {r["normalised_name"]: dict(r) for r in conn.execute(
        "SELECT name, normalised_name, primary_domain, hq_state FROM developers"
    ).fetchall()}
    assert "stockland" in devs, devs
    assert "mirvac" in devs, devs
    assert devs["stockland"]["primary_domain"] == "stockland.com.au"
    assert devs["mirvac"]["primary_domain"] == "mirvac.com"

    # 4. Projects: Aura should belong to Stockland, Smiths Lane to Mirvac.
    rows = conn.execute(
        """SELECT p.name AS project, d.name AS developer
             FROM projects p JOIN developers d ON d.id = p.developer_id
            ORDER BY p.name"""
    ).fetchall()
    project_to_dev = {r["project"]: r["developer"] for r in rows}
    assert project_to_dev["Aura"] == "Stockland", project_to_dev
    assert project_to_dev["Smiths Lane"] == "Mirvac", project_to_dev

    # 5. CSV exports.
    out = tmp_path / "out"
    n_dev = exporter.export_developers(conn, out / "developers.csv")
    n_proj = exporter.export_projects(conn, out / "projects.csv")
    assert n_dev >= 2
    assert n_proj >= 2

    with (out / "projects.csv").open() as f:
        reader = csv.DictReader(f)
        proj_rows = list(reader)
    assert any(r["project_name"] == "Aura" and r["developer_name"] == "Stockland" for r in proj_rows)
    assert any(r["sources"] and "planning_nsw" in r["sources"] and "realestate_com_au" in r["sources"]
               for r in proj_rows if r["project_name"] == "Aura")
