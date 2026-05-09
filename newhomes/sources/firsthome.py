"""firsthome.com.au — first home buyer focused portal.

Covers a long tail of house-and-land packages and entry-level estates that
the major portals under-represent. Particularly valuable for finding
estate-level branded H&L offerings under hybrid builders (Metricon, Burbank,
Henley, etc.) where the same builder runs many small estate brands.

QA disclaimer
-------------
The DOM selectors below are best-effort. firsthome.com.au has redesigned
several times and is the lowest-confidence parser in this set — the fallback
heuristic (any link to a project/estate page with a plausible title) carries
the load if the structured selectors miss. Records emitted from this source
get confidence=0.5 by default, deliberately low so entity_resolution prefers
matching evidence from urban/realestate/UDIA when those are present.

Run a quick QA pass after enabling: spot-check 20 random records, check
that the developer attribution looks right ~80% of the time, and update
the selectors below if not. Or, more practical: leave it as a coverage
booster and let the LLM parent-brand stage clean up the messy ones.
"""
from __future__ import annotations

import logging
import re
from typing import AsyncIterator
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from ..core.http import Fetcher
from ..core.normalise import canonical_state
from ..store.models import SourceRecord
from .base import Source

log = logging.getLogger("newhomes.sources.firsthome")

BASE = "https://www.firsthome.com.au"
# Plausible index URLs as of last review. If the site has moved, the parser
# returns 0 records and the source bails — it won't crash the orchestrator.
INDEX_CANDIDATES = [
    BASE + "/house-and-land",
    BASE + "/new-home-builders",
    BASE + "/estates",
]

_BY_DEVELOPER_RE = re.compile(
    r"\b(?:by|developer:?|builder:?|marketed by|developed by|built by)\s+"
    r"([A-Z][\w&'\-\. ]{2,80})",
    flags=re.IGNORECASE,
)


class FirsthomeSource(Source):
    code = "firsthome"
    name = "firsthome.com.au"

    def __init__(
        self,
        fetcher: Fetcher,
        max_pages_per_index: int = 20,
        states: list[str] | None = None,
    ):
        self.fetcher = fetcher
        self.max_pages = max_pages_per_index
        self.states = states or []  # filter post-parse if provided

    async def iter_records(self, **_) -> AsyncIterator[SourceRecord]:
        for index in INDEX_CANDIDATES:
            async for r in self._crawl_index(index):
                if self.states and r.parsed_state and r.parsed_state not in self.states:
                    continue
                yield r

    async def _crawl_index(self, index_url: str) -> AsyncIterator[SourceRecord]:
        for page in range(1, self.max_pages + 1):
            url = index_url + (f"?page={page}" if page > 1 else "")
            try:
                html = await self.fetcher.get_text(url)
            except Exception as e:
                log.debug("firsthome fetch failed %s: %s", url, e)
                return
            records = list(_parse(html, url))
            if not records:
                return
            log.info("firsthome %s p%d: %d records", index_url.rsplit("/", 1)[-1], page, len(records))
            for r in records:
                yield r


def _parse(html: str, page_url: str) -> list[SourceRecord]:
    tree = HTMLParser(html)
    out: list[SourceRecord] = []
    cards = tree.css("article, .estate-card, .builder-card, .listing-card, [data-listing]")
    if not cards:
        # Last-resort fallback — any heading-anchor pair that looks like a project.
        cards = []
        for h in tree.css("h2, h3"):
            anchor = h.css_first("a[href]") or (h.parent.css_first("a[href]") if h.parent else None)
            if anchor:
                cards.append(h.parent or h)

    for c in cards:
        name_el = c.css_first("h2, h3, .title")
        if not name_el:
            continue
        name = name_el.text(strip=True)
        if not name or len(name) < 3:
            continue

        link_el = c.css_first("a[href]")
        href = link_el.attributes.get("href", "") if link_el else ""
        listing_url = urljoin(BASE, href) if href else page_url

        full_text = c.text(separator=" ")
        developer = None
        m = _BY_DEVELOPER_RE.search(full_text)
        if m:
            developer = m.group(1).strip()

        # Suburb + state hint from card text
        suburb = None; state_code = None
        sm = re.search(
            r"\b([A-Z][a-zA-Z\s\-]{2,40}?),\s*(NSW|VIC|QLD|WA|SA|TAS|ACT|NT)\b",
            full_text,
        )
        if sm:
            suburb = sm.group(1).strip()
            state_code = canonical_state(sm.group(2))

        out.append(SourceRecord(
            source_code="firsthome",
            raw_url=listing_url,
            parsed_developer_name=developer,
            parsed_project_name=name,
            parsed_state=state_code,
            parsed_suburb=suburb,
            confidence=0.5,           # low — fragile parser, see module docstring
            extra={"index_url": page_url, "fragile_parser": True},
        ))
    return out
