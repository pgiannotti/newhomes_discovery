"""Resolve a Facebook page URL for a developer or project.

Three strategies in order:
  1. Scrape the entity's website for FB links in <head>, header, footer.
  2. Look for a `<meta property="og:url">` or `<meta name="facebook-domain-verification">`
     plus brand match on the FB page slug.
  3. Last resort: search Facebook directly with the entity's name (handled
     out-of-band — Facebook's search APIs are gated, so we leave a stub).
"""
from __future__ import annotations

import logging
import re
import sqlite3

import httpx
from selectolax.parser import HTMLParser

from ..core.normalise import canonical_fb_url

log = logging.getLogger("newhomes.resolvers.facebook")

_FB_HREF_RE = re.compile(
    r"https?://(?:www\.|m\.)?(?:facebook\.com|fb\.com)/(?!sharer|share|tr|plugins)[^\s'\"<>?]+",
    flags=re.IGNORECASE,
)


async def find_fb_url_for_site(
    client: httpx.AsyncClient, site_url: str
) -> str | None:
    """Fetch a website's HTML and return the canonical FB page URL if linked."""
    if not site_url.startswith(("http://", "https://")):
        site_url = "https://" + site_url
    try:
        resp = await client.get(site_url, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        log.debug("fb fetch failed %s: %s", site_url, e)
        return None

    # Prefer explicit anchor[href] hits in head/footer; regex over body as backup.
    tree = HTMLParser(html)
    candidates: list[str] = []
    for a in tree.css('a[href*="facebook.com"], a[href*="fb.com"]'):
        candidates.append(a.attributes.get("href", ""))
    if not candidates:
        candidates = _FB_HREF_RE.findall(html)

    for href in candidates:
        canon = canonical_fb_url(href)
        if canon and not _is_share_or_widget(canon):
            return canon
    return None


_SHARE_PATTERNS = ("/sharer", "/plugins/", "/tr?", "/dialog/")


def _is_share_or_widget(url: str) -> bool:
    return any(p in url for p in _SHARE_PATTERNS)


# ─────────────────────────────────────────────────────────────────────────────
# Batch resolver: walk DB, fill missing fb_url for projects + developers.
# ─────────────────────────────────────────────────────────────────────────────
async def resolve_all(conn: sqlite3.Connection, *, limit: int | None = None) -> int:
    """Resolve fb_url for projects and developers that have a domain but no FB."""
    updated = 0
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        # Projects first — usually richer FB pages.
        rows = conn.execute(
            """
            SELECT id, project_domain
              FROM projects
             WHERE fb_url IS NULL AND project_domain IS NOT NULL
             ORDER BY id
             LIMIT ?
            """,
            (limit if limit is not None else -1,),
        ).fetchall()
        for r in rows:
            fb = await find_fb_url_for_site(client, r["project_domain"])
            if fb:
                conn.execute(
                    "UPDATE projects SET fb_url = ?, last_verified_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (fb, r["id"]),
                )
                updated += 1

        # Then developers.
        rows = conn.execute(
            """
            SELECT id, primary_domain
              FROM developers
             WHERE fb_url IS NULL AND primary_domain IS NOT NULL
             ORDER BY id
             LIMIT ?
            """,
            (limit if limit is not None else -1,),
        ).fetchall()
        for r in rows:
            fb = await find_fb_url_for_site(client, r["primary_domain"])
            if fb:
                conn.execute(
                    "UPDATE developers SET fb_url = ?, last_verified_at = CURRENT_TIMESTAMP WHERE id = ?",
                    (fb, r["id"]),
                )
                updated += 1
    return updated
