"""urban.com.au — the highest-value source in the pipeline.

urban.com.au is purpose-built around new apartments and master-planned
communities in Australia. Critically, it explicitly models the
**developer → project** relationship on its own site: every project carries
developer attribution and every developer has a profile page listing their
portfolio. That's exactly the graph this pipeline produces, so urban.com.au
contributes high-confidence edges with structured attribution rather than
text-extracted attribution.

Two complementary phases
------------------------
1. **Developer index crawl** (priority — gives us the parent brands):
   /developers/<state> → list of developer profile pages.
   Each profile page yields:
     * developer name + canonical website
     * social links (FB included)
     * the developer's full project list with internal urban URLs

2. **Project index crawl** (fills gaps):
   /new-apartments-projects/{state} and /master-planned-communities/{state}
   → project cards with developer attribution. Useful for projects that
   are missing from a developer profile or where the developer isn't yet
   on urban.com.au with a full profile.

Operational notes
-----------------
urban.com.au is much friendlier to scrape than realestate/domain — plain
httpx works, no Playwright required. Be polite (1 rps) and the site is
reliable.

Confidence
----------
Records emitted from this source carry confidence=0.9 (highest among portals)
because the developer attribution is structured, not extracted from card text.
"""
from __future__ import annotations

import logging
import re
from typing import AsyncIterator
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from ..core.http import Fetcher
from ..core.normalise import canonical_domain, canonical_fb_url, canonical_state
from ..store.models import SourceRecord
from .base import Source

log = logging.getLogger("newhomes.sources.urban")

BASE = "https://www.urban.com.au"

# Developer profile index URLs are state-scoped on urban.com.au.
DEVELOPER_INDEX = BASE + "/developers"

# Project index URLs by category. urban.com.au splits new-apartments from
# master-planned-communities (large land estates) — both are in scope.
PROJECT_INDEX_TEMPLATES = [
    BASE + "/new-apartments-projects",
    BASE + "/master-planned-communities",
]

_AU_STATES = ["nsw", "vic", "qld", "wa", "sa", "tas", "act", "nt"]


class UrbanComAuSource(Source):
    code = "urban_com_au"
    name = "urban.com.au"

    def __init__(
        self,
        fetcher: Fetcher,
        states: list[str] | None = None,
        crawl_developer_profiles: bool = True,
        max_developer_profiles: int = 2000,
        max_project_pages_per_state: int = 30,
    ):
        self.fetcher = fetcher
        self.states = [s.lower() for s in (states or _AU_STATES)]
        self.crawl_dev_profiles = crawl_developer_profiles
        self.max_dev_profiles = max_developer_profiles
        self.max_proj_pages = max_project_pages_per_state

    async def iter_records(self, **_) -> AsyncIterator[SourceRecord]:
        # Phase 1: developers index → profile pages
        if self.crawl_dev_profiles:
            async for r in self._crawl_developers():
                yield r

        # Phase 2: project indexes per state per category
        for state in self.states:
            for tmpl in PROJECT_INDEX_TEMPLATES:
                async for r in self._crawl_project_index(tmpl, state):
                    yield r

    # ── Phase 1 ────────────────────────────────────────────────────────────
    async def _crawl_developers(self) -> AsyncIterator[SourceRecord]:
        try:
            html = await self.fetcher.get_text(DEVELOPER_INDEX)
        except Exception as e:
            log.warning("urban developers index fetch failed: %s", e)
            return
        profile_urls = list(_extract_developer_profile_urls(html))[: self.max_dev_profiles]
        log.info("urban developers: %d profile pages to crawl", len(profile_urls))
        for prof_url in profile_urls:
            try:
                prof_html = await self.fetcher.get_text(prof_url)
            except Exception as e:
                log.debug("urban dev profile fetch failed %s: %s", prof_url, e)
                continue
            for r in _parse_developer_profile(prof_html, prof_url):
                yield r

    # ── Phase 2 ────────────────────────────────────────────────────────────
    async def _crawl_project_index(
        self, base_tmpl: str, state: str,
    ) -> AsyncIterator[SourceRecord]:
        # urban.com.au paginates with ?page=N
        for n in range(1, self.max_proj_pages + 1):
            url = f"{base_tmpl}/{state}?page={n}" if state else f"{base_tmpl}?page={n}"
            try:
                html = await self.fetcher.get_text(url)
            except Exception as e:
                log.debug("urban project index fetch failed %s: %s", url, e)
                break
            records = list(_parse_project_index(html, url, state))
            if not records:
                break
            log.info("urban %s p%d: %d project records (%s)",
                     state, n, len(records),
                     "apartments" if "apartments" in base_tmpl else "masterplan")
            for r in records:
                yield r


# ── Parsers ────────────────────────────────────────────────────────────────

_DEV_PROFILE_PATTERN = re.compile(r"^/developer/[A-Za-z0-9\-]+/?$")


def _extract_developer_profile_urls(html: str) -> list[str]:
    """From the /developers index, collect every /developer/<slug> link."""
    tree = HTMLParser(html)
    seen: set[str] = set()
    out: list[str] = []
    for a in tree.css("a[href]"):
        href = a.attributes.get("href", "")
        # Tolerate absolute too
        if href.startswith(BASE):
            path = href[len(BASE):]
        else:
            path = href
        if _DEV_PROFILE_PATTERN.match(path):
            full = urljoin(BASE, path)
            if full not in seen:
                seen.add(full)
                out.append(full)
    return out


def _parse_developer_profile(html: str, prof_url: str) -> list[SourceRecord]:
    """Yield one developer record + one project record per project listed.

    Selector strategy: look for the obvious anchors — h1 for name, an
    'official website' link for primary domain, social link container for FB,
    and a project list section. Falls back to looser heuristics (any
    outbound .au link, any project-card-like block) if the DOM has changed.
    """
    tree = HTMLParser(html)
    out: list[SourceRecord] = []

    name_el = tree.css_first("h1") or tree.css_first(".developer-name")
    developer_name = name_el.text(strip=True) if name_el else None

    # Primary website: prefer explicit "Visit website" / "Official site" anchor
    site_url = None
    for a in tree.css("a[href]"):
        text = (a.text(strip=True) or "").lower()
        href = a.attributes.get("href", "")
        if not href.startswith(("http://", "https://")):
            continue
        if "facebook.com" in href or "instagram.com" in href or "linkedin.com" in href:
            continue
        domain = canonical_domain(href) or ""
        if domain.endswith(".urban.com.au") or domain == "urban.com.au":
            continue
        if any(kw in text for kw in ("visit website", "official site", "view website", "developer website")):
            site_url = href
            break
    # If no explicit anchor, fall back to first .au outbound link
    if not site_url:
        for a in tree.css("a[href^=http]"):
            href = a.attributes["href"]
            domain = canonical_domain(href) or ""
            if domain.endswith(".urban.com.au") or domain == "urban.com.au":
                continue
            if domain.endswith(".au") and "facebook.com" not in href:
                site_url = href
                break

    fb_url = None
    for a in tree.css('a[href*="facebook.com"]'):
        candidate = canonical_fb_url(a.attributes.get("href", ""))
        if candidate:
            fb_url = candidate
            break

    if developer_name:
        out.append(SourceRecord(
            source_code="urban_com_au",
            raw_url=prof_url,
            parsed_developer_name=developer_name,
            parsed_fb_url=fb_url,
            confidence=0.9,
            extra={
                "developer_domain": canonical_domain(site_url) if site_url else None,
                "profile_url": prof_url,
            },
        ))

    # Project list on the profile page. Common markup: cards with project name
    # + suburb/state. Selectors are loose; the fallback covers most rebrands.
    cards = tree.css("[data-project], .project-card, article.project, li.project") \
        or tree.css('a[href*="/project/"]')
    seen_projects: set[str] = set()
    for c in cards:
        if c.tag == "a":
            link_el = c
            name_el = c
        else:
            link_el = c.css_first('a[href*="/project/"]')
            name_el = c.css_first("h2, h3, .project-name") or link_el
        if not name_el:
            continue
        proj_name = name_el.text(strip=True)
        if not proj_name or proj_name in seen_projects:
            continue
        seen_projects.add(proj_name)

        href = link_el.attributes.get("href", "") if link_el else ""
        proj_internal = urljoin(BASE, href) if href else None

        # Try to pull suburb/state from sibling text
        suburb = None; state = None
        if c.tag != "a":
            sub_el = c.css_first(".suburb, .location, address")
            if sub_el:
                txt = sub_el.text(strip=True)
                # "Suburb, NSW 2000" → suburb="Suburb", state="NSW"
                m = re.match(r"([^,]+),\s*(NSW|VIC|QLD|WA|SA|TAS|ACT|NT)", txt)
                if m:
                    suburb, state = m.group(1).strip(), m.group(2)

        out.append(SourceRecord(
            source_code="urban_com_au",
            raw_url=proj_internal or prof_url,
            parsed_developer_name=developer_name,
            parsed_project_name=proj_name,
            parsed_state=state,
            parsed_suburb=suburb,
            confidence=0.9,
            extra={
                "urban_project_url": proj_internal,
                "developer_profile_url": prof_url,
            },
        ))
    return out


def _parse_project_index(html: str, page_url: str, state: str) -> list[SourceRecord]:
    """Parse a /new-apartments-projects/<state> or /master-planned-communities/<state> page."""
    tree = HTMLParser(html)
    out: list[SourceRecord] = []
    cards = tree.css('[data-project], article.project, .project-tile, .project-card')
    if not cards:
        cards = tree.css('a[href*="/project/"]')
    for c in cards:
        if c.tag == "a":
            link_el = c
            name_el = c
        else:
            link_el = c.css_first('a[href*="/project/"]')
            name_el = c.css_first("h2, h3, .project-name") or link_el
        if not name_el:
            continue
        project_name = name_el.text(strip=True)
        if not project_name:
            continue

        # Developer attribution — urban renders this as "by <Developer>"
        # in a dedicated element, often .developer-name or a link to
        # /developer/<slug>.
        dev_el = c.css_first('.developer-name, a[href^="/developer/"]')
        developer_name = dev_el.text(strip=True) if dev_el else None

        # Suburb/state
        suburb = None
        state_code = canonical_state(state)
        loc_el = c.css_first(".suburb, .location, address")
        if loc_el:
            txt = loc_el.text(strip=True)
            m = re.match(r"([^,]+),", txt)
            if m:
                suburb = m.group(1).strip()

        listing_href = link_el.attributes.get("href", "") if link_el else ""
        listing_url = urljoin(BASE, listing_href) if listing_href else page_url

        out.append(SourceRecord(
            source_code="urban_com_au",
            raw_url=listing_url,
            parsed_developer_name=developer_name,
            parsed_project_name=project_name,
            parsed_state=state_code,
            parsed_suburb=suburb,
            confidence=0.85 if developer_name else 0.65,
            extra={"index_url": page_url, "category":
                   "apartments" if "apartments" in page_url else "masterplan"},
        ))
    return out
