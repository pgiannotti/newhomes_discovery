"""Promote source_records → developers + projects (deduped, clustered).

This is run AFTER one or more sources have written into source_records and
BEFORE LLM enrichment. Idempotent: running twice produces the same canonical
rows.

Algorithm
---------
1. Cluster developer mentions by:
     a. exact normalised_name match
     b. exact registrable-domain match (when source contributed a domain)
     c. ABN match (rarely available at this stage)
   Within a cluster, choose canonical name = mode of source names, weighted
   by source confidence and source kind (industry > planning > portal > serp).

2. For each cluster, upsert into `developers`. Set `primary_domain` if any
   source contributed a domain that DOES NOT match a portal.

3. Cluster project mentions by (developer_id, normalised_name). Upsert into
   `projects`. Project domain is set when the resolver stage runs separately;
   here we only consolidate names + suburb + state.

4. Backfill `developer_id` and `project_id` on the source_records rows that
   contributed, so provenance views work.
"""
from __future__ import annotations

import logging
import sqlite3
from collections import Counter, defaultdict
from typing import Iterable

from .core.normalise import canonical_domain, normalise_name
from .store.db import transaction

log = logging.getLogger("newhomes.entity_resolution")

# Source kind weights — high-trust sources break ties on canonical names.
_KIND_WEIGHT = {
    "industry": 1.0,
    "planning": 0.9,
    "portal":   0.7,
    "serp":     0.4,
    "llm":      0.6,
    "manual":   1.0,
}


def _kind_for(conn: sqlite3.Connection, source_id: int) -> str:
    row = conn.execute("SELECT kind FROM sources WHERE id = ?", (source_id,)).fetchone()
    return row["kind"] if row else "portal"


def _pick_canonical_name(weights: Counter, cluster_norm: str | None = None) -> str:
    """Pick the best display name from a Counter of {name: weight}.

    Rule: among names whose normalised form equals the cluster norm (i.e. all
    plausible spellings of the same brand), prefer the SHORTEST. Ties broken
    by highest weight. This biases toward clean brand forms — "Mirvac" wins
    over "Mirvac Group" and "Stockland" beats "Stockland Development Pty Ltd"
    even when the longer legal form has higher source weight.
    """
    if not weights:
        return ""
    candidates: list[tuple[str, int]] = list(weights.items())
    if cluster_norm:
        same_norm = [(n, w) for n, w in candidates if normalise_name(n) == cluster_norm]
        if same_norm:
            candidates = same_norm
    candidates.sort(key=lambda nw: (len(nw[0]), -nw[1]))
    return candidates[0][0]


def resolve(conn: sqlite3.Connection) -> dict[str, int]:
    """Run the full entity-resolution pass. Returns counts dict."""
    out = {"developers_inserted": 0, "developers_updated": 0,
           "projects_inserted": 0, "projects_updated": 0,
           "source_records_linked": 0}

    # ── 1. Build developer clusters from source_records ────────────────────
    rows = conn.execute(
        """
        SELECT  sr.id AS sr_id, sr.source_id, sr.parsed_developer_name AS name,
                sr.parsed_state AS state, sr.confidence,
                sr.extra_json
          FROM source_records sr
         WHERE sr.parsed_developer_name IS NOT NULL
           AND sr.developer_id IS NULL
        """
    ).fetchall()

    # Cluster key: normalised_name OR developer_domain
    clusters: dict[str, dict] = {}
    for r in rows:
        name = r["name"]
        norm = normalise_name(name)
        if not norm:
            continue
        # Try to recover a domain from extra_json (source-specific field name)
        domain = None
        if r["extra_json"]:
            import json
            try:
                extra = json.loads(r["extra_json"])
                domain = extra.get("developer_domain") or extra.get("domain")
                if domain:
                    domain = canonical_domain(domain)
            except json.JSONDecodeError:
                pass

        key = f"d:{domain}" if domain else f"n:{norm}"
        c = clusters.setdefault(key, {
            "norm": norm,
            "domain": domain,
            "names": Counter(),
            "states": Counter(),
            "src_ids": [],
            "kinds": [],
            "confidence_sum": 0.0,
        })
        kind = _kind_for(conn, r["source_id"])
        weight = _KIND_WEIGHT.get(kind, 0.5) * float(r["confidence"] or 0.5)
        c["names"][name] += int(weight * 10) or 1
        if r["state"]:
            c["states"][r["state"]] += 1
        c["src_ids"].append(r["sr_id"])
        c["kinds"].append(kind)
        c["confidence_sum"] += weight

    # ── 1b. Merge clusters that share a normalised_name ─────────────────────
    # Without this, "Mirvac" (from a portal, no domain) and "Mirvac Group"
    # (from UDIA, with domain mirvac.com) live in two clusters and the
    # canonical-name picker only sees evidence within one. Merge them so the
    # shorter brand form wins across all sources.
    by_norm: dict[str, list[str]] = defaultdict(list)
    for key, c in clusters.items():
        by_norm[c["norm"]].append(key)
    for norm, keys in list(by_norm.items()):
        if len(keys) <= 1:
            continue
        # Primary cluster: the one with a domain wins (more identity signal).
        # Tie-break on highest confidence_sum.
        keys.sort(
            key=lambda k: (clusters[k]["domain"] is not None, clusters[k]["confidence_sum"]),
            reverse=True,
        )
        primary_key, *to_merge = keys
        primary = clusters[primary_key]
        for k in to_merge:
            other = clusters[k]
            primary["names"].update(other["names"])
            primary["states"].update(other["states"])
            primary["src_ids"].extend(other["src_ids"])
            primary["kinds"].extend(other["kinds"])
            primary["confidence_sum"] += other["confidence_sum"]
            del clusters[k]

    # ── 2. Upsert developers ───────────────────────────────────────────────
    sr_to_dev: dict[int, int] = {}
    with transaction(conn):
        for c in clusters.values():
            canonical_name = _pick_canonical_name(c["names"], c["norm"])
            norm = c["norm"]
            domain = c["domain"]

            # Find existing by domain or normalised_name
            existing = None
            if domain:
                existing = conn.execute(
                    "SELECT id FROM developers WHERE primary_domain = ?", (domain,)
                ).fetchone()
            if not existing:
                existing = conn.execute(
                    "SELECT id FROM developers WHERE normalised_name = ?", (norm,)
                ).fetchone()

            if existing:
                dev_id = existing["id"]
                # Don't overwrite the canonical name unless we have stronger evidence
                if domain:
                    conn.execute(
                        "UPDATE developers SET primary_domain = COALESCE(primary_domain, ?) WHERE id = ?",
                        (domain, dev_id),
                    )
                out["developers_updated"] += 1
            else:
                hq_state = c["states"].most_common(1)[0][0] if c["states"] else None
                cur = conn.execute(
                    """
                    INSERT INTO developers (name, normalised_name, primary_domain, hq_state)
                    VALUES (?, ?, ?, ?)
                    """,
                    (canonical_name, norm, domain, hq_state),
                )
                dev_id = cur.lastrowid
                out["developers_inserted"] += 1

            for sr_id in c["src_ids"]:
                sr_to_dev[sr_id] = dev_id

        # Apply developer_id back to source_records
        for sr_id, dev_id in sr_to_dev.items():
            conn.execute(
                "UPDATE source_records SET developer_id = ? WHERE id = ?",
                (dev_id, sr_id),
            )
            out["source_records_linked"] += 1

    # ── 3. Project clusters ────────────────────────────────────────────────
    rows = conn.execute(
        """
        SELECT  sr.id AS sr_id, sr.developer_id,
                sr.parsed_project_name AS name,
                sr.parsed_state AS state,
                sr.parsed_suburb AS suburb,
                sr.parsed_status AS status
          FROM source_records sr
         WHERE sr.parsed_project_name IS NOT NULL
           AND sr.developer_id IS NOT NULL
           AND sr.project_id IS NULL
        """
    ).fetchall()

    proj_clusters: dict[tuple[int, str], dict] = {}
    for r in rows:
        norm = normalise_name(r["name"])
        if not norm:
            continue
        key = (r["developer_id"], norm)
        c = proj_clusters.setdefault(key, {
            "names": Counter(), "states": Counter(),
            "suburbs": Counter(), "statuses": Counter(),
            "src_ids": [],
        })
        c["names"][r["name"]] += 1
        if r["state"]: c["states"][r["state"]] += 1
        if r["suburb"]: c["suburbs"][r["suburb"]] += 1
        if r["status"]: c["statuses"][r["status"]] += 1
        c["src_ids"].append(r["sr_id"])

    sr_to_proj: dict[int, int] = {}
    with transaction(conn):
        for (dev_id, norm), c in proj_clusters.items():
            canonical_name = _pick_canonical_name(c["names"], norm)
            existing = conn.execute(
                "SELECT id FROM projects WHERE developer_id = ? AND normalised_name = ?",
                (dev_id, norm),
            ).fetchone()
            if existing:
                proj_id = existing["id"]
                out["projects_updated"] += 1
            else:
                cur = conn.execute(
                    """
                    INSERT INTO projects (developer_id, name, normalised_name, state, suburb, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        dev_id, canonical_name, norm,
                        c["states"].most_common(1)[0][0] if c["states"] else None,
                        c["suburbs"].most_common(1)[0][0] if c["suburbs"] else None,
                        c["statuses"].most_common(1)[0][0] if c["statuses"] else "unknown",
                    ),
                )
                proj_id = cur.lastrowid
                out["projects_inserted"] += 1
            for sr_id in c["src_ids"]:
                sr_to_proj[sr_id] = proj_id

        for sr_id, proj_id in sr_to_proj.items():
            conn.execute(
                "UPDATE source_records SET project_id = ? WHERE id = ?",
                (proj_id, sr_id),
            )

    return out
