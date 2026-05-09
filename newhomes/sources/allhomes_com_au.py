"""allhomes.com.au — Domain Group portal with strong ACT + regional NSW coverage.

allhomes.com.au is the dominant property portal for the ACT and surrounding
regional NSW (Queanbeyan, Yass, etc.) — areas where domain.com.au and
realestate.com.au have weaker coverage. Including it closes the geographic
gap.

Index URL pattern (as of 2026-05):
    https://www.allhomes.com.au/ah/act/buy/new-homes/list-{n}
    https://www.allhomes.com.au/ah/nsw/buy/new-homes/list-{n}

The site is part of Domain Group, so its anti-bot posture is similar to
domain.com.au — needs Playwright at scale, but lighter than realestate.com.au.
This module uses httpx by default and falls back gracefully if blocked
(yields nothing rather than crashing).
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

log = logging.getLogger("newhomes.sources.allhomes")

BASE = "https://www.allhomes.com.au"
INDEX_TMPL = BASE + "/ah/{state}/buy/new-homes/list-{page}"

_BY_DEVELOPER_RE = re.compile(
    r"\b(?:by|developer:?|builder:?|marketed by)\s+([A-Z][\w&'\-\. ]{2,80})",
    flags=re.IGNORECASE,
)


class AllhomesSource(Source):
    code = "allhomes_com_au"
    name = "allhomes.com.au"

    def __init__(
        self,
        fetcher: Fetcher,
        states: list[str] | None = None,
        max_pages_per_state: int = 30,
    ):
        self.fetcher = fetcher
        # ACT is the priority — that's where allhomes wins.
        self.states = [s.lower() for s in (states or ["act", "nsw"])]
        self.max_pages_per_state = max_pages_per_state

    async def iter_records(self, **_) -> AsyncIterator[SourceRecord]:
        for state in self.states:
            for n in range(1, self.max_pages_per_state + 1):
                url = INDEX_TMPL.format(state=state, page=n)
                try:
                    html = await self.fetcher.get_text(url)
                except Exception as e:
                    log.debug("allhomes fetch failed %s: %s", url, e)
                    break
                if "Pardon Our Interruption" in html or "captcha" in html.lower():
                    log.warning("allhomes %s p%d: bot challenge — bail", state, n)
                    break
                records = list(_parse(html, url, state))
                if not records:
                    break
                log.info("allhomes %s p%d: %d records", state, n, len(records))
                for r in records:
                    yield r


def _parse(html: str, page_url: str, state: str) -> list[SourceRecord]:
    tree = HTMLParser(html)
    out: list[SourceRecord] = []
    cards = tree.css('article, [data-testid*="listing"], .listing-card, .new-homes-card')
    state_code = canonical_state(state)
    for c in cards:
        name_el = c.css_first("h2, h3, .listing-title")
        link_el = c.css_first("a[href]")
        if not name_el or not link_el:
            continue
        name = name_el.text(strip=True)
        href = link_el.attributes.get("href", "")
        listing_url = urljoin(BASE, href) if href else page_url

        txt = c.text(separator=" ")
        developer = None
        m = _BY_DEVELOPER_RE.search(txt)
        if m:
            developer = m.group(1).strip()

        suburb = None
        addr_el = c.css_first("address, .address")
        if addr_el:
            atxt = addr_el.text(strip=True)
            sm = re.match(r"([^,]+),", atxt)
            if sm:
                suburb = sm.group(1).strip()

        out.append(SourceRecord(
            source_code="allhomes_com_au",
            raw_url=listing_url,
            parsed_developer_name=developer,
            parsed_project_name=name,
            parsed_state=state_code,
            parsed_suburb=suburb,
            confidence=0.7 if developer else 0.55,
            extra={"index_url": page_url},
        ))
    return out
