"""homely.com.au — community-focused property portal with a new-homes section.

Lighter coverage than realestate/domain but adds long-tail boutique
developments that don't pay for portal listings. Lighter bot protection too,
so plain httpx works.

Index URL pattern (as of 2026-05):
    https://www.homely.com.au/new-apartments/{state}
    https://www.homely.com.au/new-houses/{state}
    https://www.homely.com.au/land/{state}

Each state index paginates. Project cards include developer attribution
text but it's not a structured field, so we extract via regex. The LLM
parent-brand stage cleans it up later.
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

log = logging.getLogger("newhomes.sources.homely")

BASE = "https://www.homely.com.au"
INDEX_TEMPLATES = [
    BASE + "/new-apartments/{state}",
    BASE + "/new-houses/{state}",
    BASE + "/land/{state}",
]
_AU_STATES = ["nsw", "vic", "qld", "wa", "sa", "tas", "act", "nt"]


_BY_DEVELOPER_RE = re.compile(
    r"\b(?:by|developer:?|builder:?|marketed by)\s+([A-Z][\w&'\-\. ]{2,80})",
    flags=re.IGNORECASE,
)


class HomelyComAuSource(Source):
    code = "homely_com_au"
    name = "homely.com.au"

    def __init__(
        self,
        fetcher: Fetcher,
        states: list[str] | None = None,
        max_pages_per_state: int = 30,
    ):
        self.fetcher = fetcher
        self.states = [s.lower() for s in (states or _AU_STATES)]
        self.max_pages_per_state = max_pages_per_state

    async def iter_records(self, **_) -> AsyncIterator[SourceRecord]:
        for state in self.states:
            for tmpl in INDEX_TEMPLATES:
                async for r in self._crawl(tmpl, state):
                    yield r

    async def _crawl(self, tmpl: str, state: str) -> AsyncIterator[SourceRecord]:
        for page in range(1, self.max_pages_per_state + 1):
            url = tmpl.format(state=state) + (f"?page={page}" if page > 1 else "")
            try:
                html = await self.fetcher.get_text(url)
            except Exception as e:
                log.debug("homely fetch failed %s: %s", url, e)
                break
            records = list(_parse(html, url, state))
            if not records:
                break
            log.info("homely %s p%d (%s): %d", state, page, tmpl.split("/")[-2], len(records))
            for r in records:
                yield r


def _parse(html: str, page_url: str, state: str) -> list[SourceRecord]:
    tree = HTMLParser(html)
    out: list[SourceRecord] = []
    cards = tree.css('article, .listing-card, [data-testid*="listing"], [data-listing]')
    state_code = canonical_state(state)
    for c in cards:
        name_el = c.css_first("h2, h3, .listing-title")
        link_el = c.css_first("a[href]")
        if not name_el or not link_el:
            continue
        name = name_el.text(strip=True)
        href = link_el.attributes.get("href", "")
        listing_url = urljoin(BASE, href) if href else page_url

        # Developer attribution — homely doesn't have a structured field, so
        # we regex over the card's full text.
        txt = c.text(separator=" ")
        developer = None
        m = _BY_DEVELOPER_RE.search(txt)
        if m:
            developer = m.group(1).strip()

        # Suburb extraction
        suburb = None
        addr_el = c.css_first("address, .address, .listing-address")
        if addr_el:
            atxt = addr_el.text(strip=True)
            sm = re.match(r"([^,]+),", atxt)
            if sm:
                suburb = sm.group(1).strip()

        out.append(SourceRecord(
            source_code="homely_com_au",
            raw_url=listing_url,
            parsed_developer_name=developer,
            parsed_project_name=name,
            parsed_state=state_code,
            parsed_suburb=suburb,
            confidence=0.7 if developer else 0.55,
            extra={"index_url": page_url},
        ))
    return out
