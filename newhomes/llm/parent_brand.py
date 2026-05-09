"""LLM-driven parent-brand resolution.

Given a candidate developer/builder + their About-page text and SERP snippets,
ask Claude to identify:
  - The parent brand name (or confirm the candidate IS the parent)
  - Whether the candidate is a developer / builder / hybrid
  - Any obvious child projects mentioned in the page
  - Confidence and a short justification that cites which evidence it used

Why an LLM here
---------------
The "is X a subsidiary/sub-brand of Y" question is exactly the kind of
ambiguous, context-heavy classification where regexes and heuristics fall
over. Examples:

  * "Aspect by AVJennings" — Aspect is a project, AVJennings is the parent.
    A regex would never know.
  * "Mirvac Group" → "Mirvac" → and Mirvac runs many sub-brands ("Mirvac
    Living", "Mirvac Communities") — the parent is still "Mirvac".
  * "Burbank Australia" vs "Burbank Homes" — same parent, two trading names.

Output contract
---------------
The model is asked to return a single JSON object. Schema in the prompt below.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

import httpx

from ..core.normalise import normalise_name
from .client import ClaudeClient

log = logging.getLogger("newhomes.llm.parent_brand")

SYSTEM = """You are a specialist analyst on the Australian residential
property industry. Given information about a company that builds or sells
homes in Australia, you classify its role and identify any parent brand it
trades under.

Return ONLY a JSON object. No prose.

Schema:
{
  "parent_brand": string | null,         // if the input IS the parent, repeat the name; otherwise the parent's name
  "is_parent": boolean,                  // true if the input company is itself the top-level brand
  "type": "developer" | "builder" | "hybrid" | "unknown",
                                         // hybrid = builder selling H&L across multiple branded estates
  "child_projects": [string],            // any project/estate names this entity markets
  "confidence": number,                  // 0..1
  "evidence": string                     // one sentence citing which input sentence(s) you relied on
}

Definitions:
- "developer": acquires land, plans estates/apartments, sells lots/units (often does not build).
- "builder": constructs homes for end buyers; usually has display homes.
- "hybrid": both — typically a national project home builder with branded house-and-land estates."""


USER_TEMPLATE = """Company: {name}
Website: {site}
About-page extract (may be empty):
\"\"\"
{about}
\"\"\"

Search snippets:
{snippets}

Classify."""


def _format_snippets(snippets: list[dict[str, str]]) -> str:
    if not snippets:
        return "(none)"
    return "\n".join(f"- {s.get('title','')}: {s.get('snippet','')}" for s in snippets[:8])


async def _fetch_about(client: httpx.AsyncClient, site_url: str) -> str:
    """Best-effort About page text extraction. Returns at most ~6k chars."""
    if not site_url.startswith(("http://", "https://")):
        site_url = "https://" + site_url
    candidates = [site_url, site_url.rstrip("/") + "/about", site_url.rstrip("/") + "/about-us"]
    for url in candidates:
        try:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                continue
            from selectolax.parser import HTMLParser
            tree = HTMLParser(resp.text)
            for sel in ("main", "article", "#content", ".content", "body"):
                node = tree.css_first(sel)
                if node:
                    text = node.text(separator=" ", strip=True)
                    if len(text) > 200:
                        return text[:6000]
        except Exception:
            continue
    return ""


async def resolve_parent_brand(
    claude: ClaudeClient,
    name: str,
    site_url: str | None,
    snippets: list[dict[str, str]] | None = None,
    model: str = "claude-sonnet-4-6",
) -> dict[str, Any]:
    """Resolve one developer/builder. Returns the JSON dict from Claude."""
    about = ""
    if site_url:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as c:
            about = await _fetch_about(c, site_url)
    user = USER_TEMPLATE.format(
        name=name,
        site=site_url or "(unknown)",
        about=about[:6000] if about else "(empty)",
        snippets=_format_snippets(snippets or []),
    )
    return claude.call(
        purpose="parent_brand",
        model=model,
        system=SYSTEM,
        user=user,
        max_tokens=600,
        json_mode=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Batch driver: walk developers with no parent_developer_id resolved yet.
# ─────────────────────────────────────────────────────────────────────────────
async def resolve_all(
    conn: sqlite3.Connection,
    claude: ClaudeClient,
    *,
    limit: int | None = None,
    model: str = "claude-sonnet-4-6",
) -> int:
    """For every developer with type='unknown' or no parent resolved, run LLM.

    Two outcomes per call:
      * If is_parent=True we update the developer's `type`.
      * If is_parent=False we ensure a parent row exists, link it, and set
        `parent_developer_id` on the candidate. The dashboard's "Parent Brand"
        filter walks the parent chain.
    """
    rows = conn.execute(
        """
        SELECT id, name, primary_domain, type, parent_developer_id
          FROM developers
         WHERE type = 'unknown' OR parent_developer_id IS NULL
         ORDER BY id
         LIMIT ?
        """,
        (limit if limit is not None else -1,),
    ).fetchall()
    updated = 0
    for r in rows:
        try:
            result = await resolve_parent_brand(
                claude, r["name"], r["primary_domain"], snippets=None, model=model,
            )
        except Exception as e:
            log.warning("parent_brand resolve failed for %s: %s", r["name"], e)
            continue

        new_type = result.get("type") or "unknown"
        if new_type not in ("developer", "builder", "hybrid", "unknown"):
            new_type = "unknown"

        parent_name = (result.get("parent_brand") or "").strip()
        is_parent = bool(result.get("is_parent"))

        if is_parent or not parent_name or normalise_name(parent_name) == normalise_name(r["name"]):
            conn.execute(
                "UPDATE developers SET type = ?, last_verified_at = CURRENT_TIMESTAMP WHERE id = ?",
                (new_type, r["id"]),
            )
        else:
            # Find or create the parent
            norm = normalise_name(parent_name)
            parent_row = conn.execute(
                "SELECT id FROM developers WHERE normalised_name = ?", (norm,)
            ).fetchone()
            if parent_row:
                parent_id = parent_row["id"]
            else:
                cur = conn.execute(
                    """
                    INSERT INTO developers (name, normalised_name, type, notes)
                    VALUES (?, ?, ?, ?)
                    """,
                    (parent_name, norm, "unknown", "auto-created from parent_brand resolver"),
                )
                parent_id = cur.lastrowid
            conn.execute(
                """
                UPDATE developers
                   SET type = ?, parent_developer_id = ?, last_verified_at = CURRENT_TIMESTAMP
                 WHERE id = ?
                """,
                (new_type, parent_id, r["id"]),
            )
        updated += 1
    return updated
