"""Google SERP discovery via Serper.dev.

Used two ways:

1. **Long-tail project discovery** — query the long tail of Australian suburbs
   for "new apartments in {suburb}", "land for sale {suburb}", "house and land
   {region}". The top organic results frequently include vanity project
   microsites (e.g. liveatariverside.com.au) that no portal lists.

2. **Developer ↔ project linking** — given a project name, query
   "<project name> developer" and let the LLM enrichment stage parse the
   organic snippets to identify the parent brand.

Why Serper
----------
~$0.30 per 1k queries, returns clean JSON with title/link/snippet for organic
results plus People-Also-Ask and knowledge panels. SerpAPI works equivalently
if you'd rather use it — swap the request URL.
"""
from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator, Iterable

import httpx

from ..core.normalise import canonical_domain
from ..store.models import SourceRecord
from .base import Source

log = logging.getLogger("newhomes.sources.serp")

SERPER_URL = "https://google.serper.dev/search"

# Domains that are NEVER project sites — exclude from candidate set.
_PORTAL_DOMAINS = {
    "realestate.com.au", "domain.com.au", "homely.com.au", "ratemyagent.com.au",
    "onthehouse.com.au", "openagent.com.au", "facebook.com", "instagram.com",
    "youtube.com", "linkedin.com", "tiktok.com", "google.com",
    "abs.gov.au", "wikipedia.org",
}


class SerperSource(Source):
    code = "google_serp"
    name = "Google SERP via Serper"

    def __init__(
        self,
        api_key: str,
        max_queries: int = 1000,
        results_per_query: int = 10,
        rate_limit_rps: float = 5.0,
    ):
        if not api_key:
            raise ValueError("Serper API key required")
        self.api_key = api_key
        self.max_queries = max_queries
        self.results_per_query = results_per_query
        self.rate_limit_rps = rate_limit_rps

    async def iter_records(
        self,
        queries: Iterable[str] | None = None,
        **_,
    ) -> AsyncIterator[SourceRecord]:
        queries = list(queries) if queries else list(default_query_seeds())
        queries = queries[: self.max_queries]
        log.info("Serper: %d queries", len(queries))
        sleep_s = 1.0 / max(self.rate_limit_rps, 0.1)

        async with httpx.AsyncClient(timeout=20.0) as client:
            for q in queries:
                try:
                    payload = {"q": q, "gl": "au", "hl": "en", "num": self.results_per_query}
                    resp = await client.post(
                        SERPER_URL,
                        headers={"X-API-KEY": self.api_key, "Content-Type": "application/json"},
                        json=payload,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    log.warning("serper query failed %r: %s", q, e)
                    continue

                for item in (data.get("organic") or []):
                    link = item.get("link", "")
                    domain = canonical_domain(link)
                    if not domain or not domain.endswith(".au"):
                        continue
                    if domain in _PORTAL_DOMAINS:
                        continue
                    title = item.get("title", "").strip()
                    snippet = item.get("snippet", "").strip()
                    yield SourceRecord(
                        source_code="google_serp",
                        raw_url=link,
                        parsed_project_name=title or None,
                        parsed_project_domain=domain,
                        confidence=0.5,    # raw signal; resolution decides
                        extra={"query": q, "snippet": snippet},
                    )
                await asyncio.sleep(sleep_s)


def default_query_seeds() -> Iterable[str]:
    """Yield a default set of discovery queries.

    For real use, expand this with a suburb list (≈15k Australian suburbs).
    Even a coarse pass — capital city + region — surfaces most project sites
    that are actively SEO-ing.
    """
    base_queries = [
        "house and land package {region}",
        "new apartments for sale {region}",
        "new estate {region}",
        "land for sale {region} new release",
        "off the plan apartments {region}",
        "masterplanned community {region}",
    ]
    regions = [
        # Capital cities + major growth corridors. Replace with full suburb
        # list at run time for full coverage.
        "Sydney", "Western Sydney", "South West Sydney", "North West Sydney",
        "Melbourne", "Western Melbourne", "South East Melbourne",
        "Brisbane", "Gold Coast", "Sunshine Coast",
        "Perth", "Adelaide", "Hobart", "Canberra", "Darwin",
        "Newcastle", "Wollongong", "Geelong", "Ballarat", "Bendigo",
        "Cairns", "Townsville",
    ]
    for q in base_queries:
        for r in regions:
            yield q.format(region=r)
