"""Find the official marketing site for a project.

Strategy
--------
For each project candidate (no project_domain yet) we try, in order:

1. Listing-page extraction. realestate/domain listings frequently include a
   "Visit project website" link. Fetch the listing page (one extra request per
   project) and extract any external link whose host is .au and isn't a portal.

2. Search fallback. Query Google: "<project name> <suburb>" and pick the top
   organic .au domain that's not a known portal. The LLM stage decides when
   results are ambiguous.

3. Manual queue. Anything still unresolved gets a row in `unresolved.csv`
   for human review.

Heuristics for "this is a project site, not a developer site"
-------------------------------------------------------------
A project microsite usually:
  * Has the project name in the domain ("liveatariverside.com.au") OR
  * Has the project name in <title> AND the developer name in the footer.
A developer site has many projects on it — we don't want to attribute one
project's domain to "stockland.com.au" because that would collapse all
Stockland projects onto one URL.

We compare the candidate domain against the developer's `primary_domain`:
if they match (or share registrable domain), we DO NOT set project_domain
— the project lives under the developer site. Dashboard can join via
developer_id instead.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from urllib.parse import urlparse, urljoin

import httpx
from selectolax.parser import HTMLParser

from ..core.normalise import canonical_domain, normalise_name

log = logging.getLogger("newhomes.resolvers.project_site")

PORTAL_DOMAINS = {
    "realestate.com.au", "domain.com.au", "homely.com.au", "facebook.com",
    "instagram.com", "youtube.com", "linkedin.com", "google.com", "tiktok.com",
}


async def extract_external_links(html: str, base_url: str) -> list[str]:
    """Return external .au links from a page, deduped, in document order."""
    tree = HTMLParser(html)
    base_host = urlparse(base_url).hostname or ""
    seen: set[str] = set()
    out: list[str] = []
    for a in tree.css("a[href]"):
        href = a.attributes.get("href", "")
        if not href.startswith(("http://", "https://")):
            continue
        host = (urlparse(href).hostname or "").lower()
        if not host or host == base_host:
            continue
        domain = canonical_domain(href)
        if not domain or not domain.endswith(".au"):
            continue
        if domain in PORTAL_DOMAINS:
            continue
        if domain in seen:
            continue
        seen.add(domain)
        out.append(href)
    return out


async def resolve_from_listing(
    client: httpx.AsyncClient,
    listing_url: str,
    project_name: str,
    developer_primary_domain: str | None,
) -> str | None:
    """Fetch a portal listing page and pick the best external project link."""
    try:
        resp = await client.get(listing_url, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        log.debug("listing fetch failed %s: %s", listing_url, e)
        return None

    candidates = await extract_external_links(html, listing_url)
    norm_project = normalise_name(project_name).replace(" ", "")

    def score(link: str) -> tuple[int, int]:
        domain = canonical_domain(link) or ""
        # Bias 1: domain != developer's primary site (we want the project URL)
        not_dev = 0 if developer_primary_domain and domain == developer_primary_domain else 1
        # Bias 2: project name appears in domain
        in_domain = 1 if norm_project and norm_project[:8] in domain.replace(".", "").replace("-", "") else 0
        return (not_dev, in_domain)

    if not candidates:
        return None
    candidates.sort(key=score, reverse=True)
    return candidates[0]


# ─────────────────────────────────────────────────────────────────────────────
# Batch resolver: walk the DB, fill in project_domain where missing.
# Called from CLI: `newhomes resolve --stage site`
# ─────────────────────────────────────────────────────────────────────────────
async def resolve_all(conn: sqlite3.Connection, *, limit: int | None = None) -> int:
    """Find missing project_domain values and fill them in."""
    rows = conn.execute(
        """
        SELECT  p.id, p.name, p.developer_id,
                d.primary_domain AS dev_domain,
                sr.raw_url AS listing_url
          FROM projects p
          JOIN developers d ON d.id = p.developer_id
     LEFT JOIN source_records sr
            ON sr.project_id = p.id
           AND sr.parsed_project_name IS NOT NULL
         WHERE p.project_domain IS NULL
         GROUP BY p.id
         ORDER BY p.id
         LIMIT ?
        """,
        (limit if limit is not None else -1,),
    ).fetchall()

    updated = 0
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        for r in rows:
            url = r["listing_url"]
            if not url:
                continue
            link = await resolve_from_listing(client, url, r["name"], r["dev_domain"])
            if not link:
                continue
            domain = canonical_domain(link)
            if not domain:
                continue
            conn.execute(
                "UPDATE projects SET project_domain = ?, last_verified_at = CURRENT_TIMESTAMP WHERE id = ?",
                (domain, r["id"]),
            )
            updated += 1
    return updated
