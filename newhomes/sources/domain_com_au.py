"""domain.com.au /new-homes scraper — sister source to realestate.com.au.

Coverage overlaps but isn't identical: domain.com.au tends to over-index on
projects in its core markets (Sydney, Melbourne) where it competes hardest
with realestate.com.au, and under-index on smaller markets. Running both
gives noticeable coverage uplift after dedup.

Operational reality
-------------------
domain.com.au is also bot-protected (Cloudflare + custom heuristics) but
slightly less aggressive than realestate.com.au's Akamai. Same Playwright
strategy applies; same proxy escape hatch.

Index URLs
----------
https://www.domain.com.au/new-homes/{state}/?page={n}

domain.com.au sometimes redirects to a Sydney/Melbourne micro-site for the
new-homes vertical (https://www.newhomes.domain.com.au) — the parser handles
both.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import AsyncIterator

from selectolax.parser import HTMLParser

from ..core.normalise import canonical_state
from ..store.models import SourceRecord
from .base import Source

log = logging.getLogger("newhomes.sources.domain")

INDEX_URL = "https://www.domain.com.au/new-homes/{state}/?page={page}"

_BY_DEVELOPER_RE = re.compile(
    r"\b(?:by|developer:?|builder:?|marketed by|developed by)\s+([A-Z][\w&'\-\. ]{2,80})",
    flags=re.IGNORECASE,
)


class DomainSource(Source):
    code = "domain_com_au"
    name = "domain.com.au new homes"

    def __init__(
        self,
        states: list[str] | None = None,
        max_pages_per_state: int = 50,
        playwright_proxy: str | None = None,
        headless: bool = True,
    ):
        self.states = [s.lower() for s in (states or ["nsw", "vic", "qld", "wa", "sa"])]
        self.max_pages_per_state = max_pages_per_state
        self.proxy = playwright_proxy
        self.headless = headless

    async def iter_records(self, **_) -> AsyncIterator[SourceRecord]:
        try:
            from playwright.async_api import async_playwright
        except ImportError as e:
            raise RuntimeError(
                "playwright is required for domain_com_au source. "
                "pip install playwright && playwright install chromium"
            ) from e

        async with async_playwright() as p:
            launch_kwargs = {"headless": self.headless}
            if self.proxy:
                launch_kwargs["proxy"] = {"server": self.proxy}
            browser = await p.chromium.launch(**launch_kwargs)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 900},
                locale="en-AU",
            )
            try:
                for state in self.states:
                    async for r in self._iter_state(context, state):
                        yield r
            finally:
                await context.close()
                await browser.close()

    async def _iter_state(self, context, state: str) -> AsyncIterator[SourceRecord]:
        page = await context.new_page()
        try:
            for n in range(1, self.max_pages_per_state + 1):
                url = INDEX_URL.format(state=state, page=n)
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
                except Exception as e:
                    log.warning("domain %s p%d goto failed: %s", state, n, e)
                    break
                html = await page.content()
                if "Verifying you are human" in html or "Just a moment" in html:
                    log.warning("domain %s p%d: bot challenge — bail", state, n)
                    break
                records = list(_parse(html, url, state))
                if not records:
                    log.info("domain %s p%d: 0 records — end of pagination", state, n)
                    break
                log.info("domain %s p%d: %d records", state, n, len(records))
                for r in records:
                    yield r
                await asyncio.sleep(2.0)
        finally:
            await page.close()


def _parse(html: str, page_url: str, state: str) -> list[SourceRecord]:
    """Parse a domain.com.au /new-homes/<state> page."""
    tree = HTMLParser(html)
    cards = tree.css('[data-testid*="project"], [data-testid*="listing"], article')
    out: list[SourceRecord] = []
    state_code = canonical_state(state)
    for c in cards:
        name_el = (
            c.css_first('[data-testid="project-card-title"]')
            or c.css_first("h2") or c.css_first("h3")
        )
        link_el = c.css_first('a[href*="/project/"], a[href*="/new-homes/"]') or c.css_first("a[href]")
        if not name_el or not link_el:
            continue
        project_name = name_el.text(strip=True)
        listing_href = link_el.attributes.get("href", "")
        if listing_href.startswith("/"):
            listing_href = "https://www.domain.com.au" + listing_href

        suburb_el = c.css_first('[data-testid*="address"], address')
        suburb = None
        if suburb_el:
            txt = suburb_el.text(strip=True)
            txt = txt.split(",")[-1].strip() if "," in txt else txt
            txt = re.sub(r"\b(NSW|VIC|QLD|WA|SA|TAS|ACT|NT)\b.*$", "", txt).strip()
            suburb = txt or None

        # Developer attribution
        dev_el = c.css_first('[data-testid*="developer"], .developer, [class*="developer"]')
        developer = dev_el.text(strip=True) if dev_el else None
        if not developer:
            txt = c.text()
            m = _BY_DEVELOPER_RE.search(txt)
            if m:
                developer = m.group(1).strip()

        status = None
        for badge in c.css('[data-testid*="badge"], [class*="badge"], [class*="status"]'):
            t = badge.text(strip=True).lower()
            if "selling" in t or "now selling" in t:
                status = "selling"; break
            if "coming soon" in t or "register" in t:
                status = "planning"; break
            if "sold out" in t:
                status = "sold_out"; break

        out.append(SourceRecord(
            source_code="domain_com_au",
            raw_url=page_url,
            parsed_developer_name=developer,
            parsed_project_name=project_name,
            parsed_state=state_code,
            parsed_suburb=suburb,
            parsed_status=status,
            confidence=0.8 if developer else 0.6,
            extra={"listing_url": listing_href},
        ))
    return out
