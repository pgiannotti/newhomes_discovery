"""realestate.com.au /new-homes scraper.

This is the highest-coverage source for **active selling** projects in
Australia. Every project has a developer attribution badge ("By Mirvac",
"Marketed by Stockland Communities", etc.) and a project landing page that
typically links to the developer's project microsite.

Operational reality
-------------------
realestate.com.au is protected by Akamai. Direct httpx requests will get
either a 403 challenge page or PerimeterX/Akamai cookie injection. This
module therefore uses Playwright with stealth tweaks. Even so, expect:

  * Rate limit yourself to ~6–10 pages/min.
  * Hard caps from Akamai if you exceed ~200 pages from one IP/day.
  * For volume crawls, plug a residential proxy via config.playwright.proxy.

Index URLs by state (current as of 2026-05):
  https://www.realestate.com.au/new-homes/in-{state}/list-1
where {state} ∈ {nsw, vic, qld, wa, sa, tas, act, nt}.

Parsing strategy
----------------
1. Hit list-N for each state, paginate until empty/limit.
2. For each project card, capture:
     - project name (h2/aria-label)
     - listing URL (canonical realestate.com.au URL)
     - developer name (from "by ..." attribution)
     - suburb + state
     - status hint (selling / coming soon)
3. Yield one SourceRecord per card. Don't follow into project pages here —
   `resolvers.project_site` does that as a separate stage with its own
   politeness budget.
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

log = logging.getLogger("newhomes.sources.realestate")

INDEX_URL = "https://www.realestate.com.au/new-homes/in-{state}/list-{page}"

_BY_DEVELOPER_RE = re.compile(
    r"\b(?:by|marketed by|developer:?|builder:?)\s+([A-Z][\w&'\-\. ]{2,80})",
    flags=re.IGNORECASE,
)


class RealestateSource(Source):
    code = "realestate_com_au"
    name = "realestate.com.au new homes"

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
        # Lazy import so the package works without playwright installed
        # (e.g. in CI for entity-resolution-only tests).
        try:
            from playwright.async_api import async_playwright
        except ImportError as e:
            raise RuntimeError(
                "playwright is required for realestate_com_au source. "
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
                    log.warning("realestate %s p%d goto failed: %s", state, n, e)
                    break
                # Akamai sometimes returns a soft challenge — the body has no
                # listings but the page is technically 200. Detect and bail.
                html = await page.content()
                if "Pardon Our Interruption" in html or "Verifying you are human" in html:
                    log.warning("realestate %s p%d: bot challenge — bail", state, n)
                    break
                records = list(_parse_listing_page(html, url, state))
                if not records:
                    log.info("realestate %s p%d: 0 records — end of pagination", state, n)
                    break
                log.info("realestate %s p%d: %d records", state, n, len(records))
                for r in records:
                    yield r
                await asyncio.sleep(2.0)   # politeness
        finally:
            await page.close()


def _parse_listing_page(html: str, page_url: str, state: str) -> list[SourceRecord]:
    """Parse a /new-homes/in-{state}/list-N page into SourceRecords.

    Selectors are robust-ish: target [data-testid] hooks the site has used
    consistently. Falls back to broader heuristics if those change.
    """
    tree = HTMLParser(html)
    cards = tree.css('[data-testid^="project-card"], [data-testid="project-tile"], article')
    out: list[SourceRecord] = []
    for c in cards:
        name_el = (
            c.css_first('[data-testid="project-card-title"]')
            or c.css_first("h2")
            or c.css_first("h3")
        )
        link_el = c.css_first('a[href*="/new-homes/"]') or c.css_first("a[href]")
        if not name_el or not link_el:
            continue
        project_name = name_el.text(strip=True)
        listing_href = link_el.attributes.get("href", "")
        if listing_href.startswith("/"):
            listing_href = "https://www.realestate.com.au" + listing_href

        # Suburb is often in <p data-testid="project-card-address"> or similar
        suburb_el = c.css_first('[data-testid="project-card-address"]') or c.css_first("address")
        suburb = suburb_el.text(strip=True) if suburb_el else None
        if suburb:
            # "Some Estate, Truganina VIC 3029" → "Truganina"
            suburb = suburb.split(",")[-1].strip()
            suburb = re.sub(r"\b(NSW|VIC|QLD|WA|SA|TAS|ACT|NT)\b.*$", "", suburb).strip() or None

        # Developer attribution: "By Mirvac" badge, or fallback regex on card text
        dev_el = c.css_first('[data-testid="project-card-developer"], [data-testid="developer-name"]')
        developer = dev_el.text(strip=True) if dev_el else None
        if not developer:
            txt = c.text()
            m = _BY_DEVELOPER_RE.search(txt)
            if m:
                developer = m.group(1).strip()

        # Status — selling/coming-soon if explicitly badged
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
            source_code="realestate_com_au",
            raw_url=page_url,
            parsed_developer_name=developer,
            parsed_project_name=project_name,
            parsed_state=canonical_state(state),
            parsed_suburb=suburb,
            parsed_status=status,
            confidence=0.8 if developer else 0.6,
            extra={"listing_url": listing_href},
        ))
    return out
