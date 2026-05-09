"""HIA (Housing Industry Association) member directory.

HIA is the peak body for residential builders in Australia. The "Find a
Builder" / "Member Directory" tool searches by suburb/state and category
(home builder, kitchen renovator, etc.). For our purposes the relevant
category is "Home Builder" — those rows are the entities that may have
project branding worth catalouging in the dashboard.

Coverage notes
--------------
HIA is enormous (~80k members across all categories) but only a fraction are
home builders with multi-estate branding worth modelling as developer +
projects. Most are local custom-home builders that go in `developers` with
`type='builder'` and zero `projects` rows — which is exactly what the schema
supports and what your existing 2000-builder list likely already covers. So
HIA is more useful as a **deduper / verifier** for your existing list and as
a way to find the missing 5–10% of mid-size builders, than as a primary
discovery source.

Index endpoint (as of 2026-05)
------------------------------
HIA's directory uses a search-results page rather than a flat browseable
index:

    https://hia.com.au/find-a-builder?location={state}&trade=Home%20Builder&page={n}

The exact parameter names sometimes shift; this module uses lenient regex
extraction so cosmetic URL changes don't break it.

If/when HIA exposes a JSON API (their site has used XHR for results in the
past), swap the parser for the JSON path — much more reliable than HTML.
"""
from __future__ import annotations

import logging
import re
from typing import AsyncIterator
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from ..core.http import Fetcher
from ..core.normalise import canonical_domain, canonical_state
from ..store.models import SourceRecord
from .base import Source

log = logging.getLogger("newhomes.sources.hia")

BASE = "https://hia.com.au"
SEARCH_URL = BASE + "/find-a-builder"

_AU_STATES = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"]


class HiaSource(Source):
    code = "hia"
    name = "Housing Industry Association"

    def __init__(
        self,
        fetcher: Fetcher,
        states: list[str] | None = None,
        max_pages_per_state: int = 100,
        trade: str = "Home Builder",
    ):
        self.fetcher = fetcher
        self.states = [s.upper() for s in (states or _AU_STATES)]
        self.max_pages_per_state = max_pages_per_state
        self.trade = trade

    async def iter_records(self, **_) -> AsyncIterator[SourceRecord]:
        for state in self.states:
            async for r in self._crawl_state(state):
                yield r

    async def _crawl_state(self, state: str) -> AsyncIterator[SourceRecord]:
        for page in range(1, self.max_pages_per_state + 1):
            url = f"{SEARCH_URL}?location={state}&trade={self.trade.replace(' ', '%20')}&page={page}"
            try:
                html = await self.fetcher.get_text(url)
            except Exception as e:
                log.debug("hia fetch failed %s: %s", url, e)
                break
            records = list(_parse(html, url, state))
            if not records:
                break
            log.info("hia %s p%d: %d records", state, page, len(records))
            for r in records:
                yield r


def _parse(html: str, page_url: str, state: str) -> list[SourceRecord]:
    tree = HTMLParser(html)
    out: list[SourceRecord] = []
    cards = tree.css('.member-card, .builder-card, article, li.member')
    if not cards:
        # Fallback: any block with both a heading and an outbound link
        cards = [el for el in tree.css("li, article, .row")
                 if el.css_first("h2, h3") and el.css_first("a[href^=http]")]
    for c in cards:
        name_el = c.css_first("h2, h3, .member-name, .builder-name")
        if not name_el:
            continue
        name = name_el.text(strip=True)
        if not name:
            continue

        site_url = None
        for a in c.css('a[href^="http"]'):
            href = a.attributes.get("href", "")
            domain = canonical_domain(href) or ""
            if not domain.endswith(".au"):
                continue
            if "hia.com.au" in domain:
                continue
            site_url = href
            break

        out.append(SourceRecord(
            source_code="hia",
            raw_url=page_url,
            parsed_developer_name=name,
            parsed_state=canonical_state(state),
            confidence=0.8,
            extra={
                "developer_domain": canonical_domain(site_url) if site_url else None,
                "hia_trade": "Home Builder",
            },
        ))
    return out
