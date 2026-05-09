"""Property Council of Australia (PCA) member directory.

PCA represents larger commercial and residential developers — its
membership skews toward listed and institutional players (Mirvac, Stockland,
Lendlease, Frasers, Charter Hall residential arm, etc.). Lower volume than
UDIA but higher trust on parent-brand attribution since PCA members are
typically the top-of-corporate-tree entity.

Endpoint
--------
PCA's member directory is part of the main site:
    https://www.propertycouncil.com.au/membership/our-members

Pagination is via ?page=N or load-more (we treat as ?page=N first; the
fallback grabs everything visible on the first page). The DOM has shifted
several times in recent years, so this parser is deliberately loose:
extract any anchor whose href is an external .au domain and whose anchor
text looks like a company name.
"""
from __future__ import annotations

import logging
from typing import AsyncIterator

from selectolax.parser import HTMLParser

from ..core.http import Fetcher
from ..core.normalise import canonical_domain
from ..store.models import SourceRecord
from .base import Source

log = logging.getLogger("newhomes.sources.pca")

BASE = "https://www.propertycouncil.com.au"
INDEX_URL = BASE + "/membership/our-members"


class PropertyCouncilSource(Source):
    code = "property_council"
    name = "Property Council of Australia"

    def __init__(self, fetcher: Fetcher, max_pages: int = 50):
        self.fetcher = fetcher
        self.max_pages = max_pages

    async def iter_records(self, **_) -> AsyncIterator[SourceRecord]:
        seen_domains: set[str] = set()
        for page in range(1, self.max_pages + 1):
            url = INDEX_URL + (f"?page={page}" if page > 1 else "")
            try:
                html = await self.fetcher.get_text(url)
            except Exception as e:
                log.debug("pca fetch failed %s: %s", url, e)
                break
            records = list(_parse(html, url, seen_domains))
            if not records and page > 1:
                break
            log.info("pca p%d: %d records", page, len(records))
            for r in records:
                yield r
            if not records and page == 1:
                # Page 1 was empty — no point paginating
                break


def _parse(html: str, page_url: str, seen_domains: set[str]) -> list[SourceRecord]:
    """Loose extractor: anchor[href] with external .au domain + plausible name."""
    tree = HTMLParser(html)
    out: list[SourceRecord] = []
    page_host = canonical_domain(page_url) or ""
    for a in tree.css("a[href]"):
        href = a.attributes.get("href", "")
        if not href.startswith(("http://", "https://")):
            continue
        domain = canonical_domain(href)
        if not domain or not domain.endswith(".au"):
            continue
        if domain == page_host or domain == "propertycouncil.com.au":
            continue
        if domain in seen_domains:
            continue
        text = a.text(strip=True)
        if not text or len(text) < 3 or len(text) > 80:
            continue
        # Filter obvious nav noise
        lower = text.lower()
        if lower in {"home", "members", "about", "contact", "join", "media", "events"}:
            continue
        seen_domains.add(domain)
        out.append(SourceRecord(
            source_code="property_council",
            raw_url=page_url,
            parsed_developer_name=text,
            confidence=0.75,
            extra={"developer_domain": domain},
        ))
    return out
