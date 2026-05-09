"""UDIA (Urban Development Institute of Australia) member directory.

UDIA is the peak industry body for Australian residential property developers.
Their member directories are *the* authoritative starting list for medium-to-
large developers — far better signal than Google for parent brands.

Coverage notes
--------------
UDIA is federated by state. Each state has its own directory:
  - NSW:  https://udiansw.com.au/about/membership/our-members
  - VIC:  https://udiavic.com.au/Membership/UDIA-Members
  - QLD:  https://udiaqld.com.au/membership/our-members/
  - WA:   https://udiawa.com.au/membership/our-members/
  - SA:   https://udiasa.com.au/membership/members/
  - ACT:  https://udiaact.com.au/membership/

Each state site is a different CMS so the parsers vary. This module starts
with NSW (largest membership) and provides a `STATE_PARSERS` registry the
orchestrator iterates. New states slot in by adding to the registry.

Robustness notes
----------------
The DOM selectors below are best-effort against the public listings as of
early 2026. If a state site is redesigned, the parser falls back to extracting
*any* outbound link with an .au TLD whose anchor text looks like a company
name — that yields a noisier but non-empty result, which `entity_resolution`
can clean up.
"""
from __future__ import annotations

import logging
from typing import AsyncIterator, Callable
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from ..core.http import Fetcher
from ..core.normalise import canonical_domain
from ..store.models import SourceRecord
from .base import Source

log = logging.getLogger("newhomes.sources.udia")


# Each parser receives (html_text, page_url) and yields SourceRecord
StateParser = Callable[[str, str], list[SourceRecord]]


def _parse_nsw(html: str, page_url: str) -> list[SourceRecord]:
    """UDIA NSW members page. Cards under .member-card with name + website."""
    out: list[SourceRecord] = []
    tree = HTMLParser(html)
    cards = tree.css(".member-card, .member-listing, article.member") or tree.css(".member")
    for c in cards:
        name_el = c.css_first(".member-name, h3, h2") or c.css_first("strong")
        link_el = c.css_first('a[href^="http"]')
        if not name_el or not link_el:
            continue
        name = name_el.text(strip=True)
        site = link_el.attributes.get("href", "")
        domain = canonical_domain(site)
        if not name or not domain:
            continue
        out.append(SourceRecord(
            source_code="udia",
            raw_url=page_url,
            parsed_developer_name=name,
            parsed_project_domain=None,        # UDIA gives developer site
            parsed_state="NSW",
            confidence=0.85,
            extra={"developer_domain": domain},
        ))
    if not out:
        out.extend(_fallback_link_extract(html, page_url, "NSW"))
    return out


def _parse_vic(html: str, page_url: str) -> list[SourceRecord]:
    out: list[SourceRecord] = []
    tree = HTMLParser(html)
    for row in tree.css("tr, .member-row, li.member"):
        name_el = row.css_first("td:nth-child(1), .name, strong, a")
        link_el = row.css_first('a[href*="http"]')
        if not name_el:
            continue
        name = name_el.text(strip=True)
        site = link_el.attributes.get("href", "") if link_el else ""
        domain = canonical_domain(site)
        if not name or not domain:
            continue
        out.append(SourceRecord(
            source_code="udia", raw_url=page_url,
            parsed_developer_name=name, parsed_state="VIC",
            confidence=0.8, extra={"developer_domain": domain},
        ))
    if not out:
        out.extend(_fallback_link_extract(html, page_url, "VIC"))
    return out


def _fallback_link_extract(html: str, page_url: str, state: str) -> list[SourceRecord]:
    """Last resort: any outbound .au link with non-trivial anchor text."""
    tree = HTMLParser(html)
    seen: set[str] = set()
    out: list[SourceRecord] = []
    for a in tree.css("a[href]"):
        href = a.attributes.get("href", "")
        if not href.startswith(("http://", "https://")):
            continue
        domain = canonical_domain(href)
        if not domain or not domain.endswith(".au"):
            continue
        # skip the directory's own domain
        host_of_page = canonical_domain(page_url) or ""
        if domain == host_of_page:
            continue
        if domain in seen:
            continue
        seen.add(domain)
        text = a.text(strip=True)
        if len(text) < 3 or len(text) > 80:
            continue
        out.append(SourceRecord(
            source_code="udia", raw_url=page_url,
            parsed_developer_name=text, parsed_state=state,
            confidence=0.4, extra={"developer_domain": domain, "fallback": True},
        ))
    return out


STATE_PARSERS: dict[str, tuple[str, StateParser]] = {
    "NSW": ("https://udiansw.com.au/about/membership/our-members",   _parse_nsw),
    "VIC": ("https://udiavic.com.au/Membership/UDIA-Members",        _parse_vic),
    # QLD/WA/SA/ACT: TODO — confirm DOM and add parsers. Until then, the
    # generic fallback runs. Add real parsers when QC'ing each state.
    "QLD": ("https://udiaqld.com.au/membership/our-members/",
            lambda h, u: _fallback_link_extract(h, u, "QLD")),
    "WA":  ("https://udiawa.com.au/membership/our-members/",
            lambda h, u: _fallback_link_extract(h, u, "WA")),
    "SA":  ("https://udiasa.com.au/membership/members/",
            lambda h, u: _fallback_link_extract(h, u, "SA")),
    "ACT": ("https://udiaact.com.au/membership/",
            lambda h, u: _fallback_link_extract(h, u, "ACT")),
}


class UdiaSource(Source):
    code = "udia"
    name = "UDIA member directory"

    def __init__(self, fetcher: Fetcher, states: list[str] | None = None):
        self.fetcher = fetcher
        self.states = states or list(STATE_PARSERS.keys())

    async def iter_records(self, **_) -> AsyncIterator[SourceRecord]:
        for state in self.states:
            spec = STATE_PARSERS.get(state)
            if not spec:
                log.warning("No UDIA parser for state %s", state)
                continue
            url, parser = spec
            try:
                html = await self.fetcher.get_text(url)
            except Exception as e:
                log.warning("UDIA fetch failed for %s: %s", state, e)
                continue
            records = parser(html, url)
            log.info("UDIA %s: %d records", state, len(records))
            for r in records:
                yield r

            # Also expand any per-member detail pages if the listing has them.
            # Many UDIA state sites link to /members/<slug> — fetching those
            # often surfaces the developer's primary website explicitly when
            # the listing only shows a placeholder. Disabled by default to
            # keep first-run cost low; flip via env or extend here.
            # TODO: opt-in detail crawl
