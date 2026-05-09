"""NSW Planning Portal — Major Projects open data feed.

The NSW Planning Portal exposes a public API for major projects (residential,
mixed-use, etc.). Each record carries a "Proponent" / "Applicant" name which
is typically the developer entity (often a project-specific SPV like
"Mirvac (Waterloo) Pty Ltd" — useful for ABN-graph linking).

API
---
Base: https://www.planningportal.nsw.gov.au/major-projects
Public OData feed and JSON endpoints exist; the simplest is the search API
used by the public website. Schema as of 2026-05:

    GET https://api.planningportal.nsw.gov.au/major-projects/api/v1/projects
        ?searchText=&type=&status=&pageNumber=1&pageSize=100

Response (abridged):
    {
      "items": [
        {
          "id": 12345,
          "displayName": "...",
          "proponent": "Stockland Development Pty Ltd",
          "applicant": "...",
          "status": "Under Assessment",
          "lga": "Camden",
          "addressString": "...",
          "developmentTypes": ["Residential"],
          ...
        },
      ],
      "totalCount": 1234
    }

Coverage
--------
Major projects only (>$30m or "state significant"). Catches large apartment
towers and masterplanned estates, misses small suburban subdivisions — that
end gets covered by realestate/domain instead.
"""
from __future__ import annotations

import logging
from typing import AsyncIterator

import httpx

from ..store.models import SourceRecord
from .base import Source

log = logging.getLogger("newhomes.sources.planning_nsw")

API_URL = "https://api.planningportal.nsw.gov.au/major-projects/api/v1/projects"


class PlanningNswSource(Source):
    code = "planning_nsw"
    name = "NSW Planning Portal — Major Projects"

    def __init__(self, page_size: int = 100, max_pages: int = 1000):
        self.page_size = page_size
        self.max_pages = max_pages

    async def iter_records(self, **_) -> AsyncIterator[SourceRecord]:
        async with httpx.AsyncClient(timeout=30.0) as client:
            for page in range(1, self.max_pages + 1):
                try:
                    resp = await client.get(API_URL, params={
                        "pageNumber": page, "pageSize": self.page_size,
                        "developmentType": "Residential",
                    })
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    log.warning("planning_nsw page %d failed: %s", page, e)
                    break
                items = data.get("items") or []
                if not items:
                    break
                for it in items:
                    proponent = (it.get("proponent") or it.get("applicant") or "").strip()
                    name = (it.get("displayName") or it.get("name") or "").strip()
                    if not proponent and not name:
                        continue
                    status = _map_status(it.get("status") or "")
                    suburb = (it.get("lga") or "").strip() or None
                    yield SourceRecord(
                        source_code="planning_nsw",
                        raw_url=f"{API_URL}?id={it.get('id')}",
                        parsed_developer_name=proponent or None,
                        parsed_project_name=name or None,
                        parsed_state="NSW",
                        parsed_suburb=suburb,
                        parsed_status=status,
                        confidence=0.75,
                        extra={
                            "planning_status_raw": it.get("status"),
                            "lga": it.get("lga"),
                            "address": it.get("addressString"),
                            "id": it.get("id"),
                        },
                    )
                if len(items) < self.page_size:
                    break


def _map_status(s: str) -> str:
    s = s.lower()
    if "approved" in s or "determined" in s:
        return "planning"   # approved-but-not-yet-marketing
    if "construction" in s or "active" in s or "selling" in s:
        return "selling"
    if "withdrawn" in s or "refused" in s:
        return "completed"
    return "planning"


# ─────────────────────────────────────────────────────────────────────────────
# Stubs for other states — populate as you pipe each one through.
# Each state has its own data feed; documenting the entry point is half the
# work, so the URLs are listed here even when the parser is unimplemented.
# ─────────────────────────────────────────────────────────────────────────────

# VIC: https://www.planning.vic.gov.au/permits-and-applications  (no public JSON;
#      bulk DA data is via DELWP open-data CKAN portal. Search "planning permit
#      activity reporting" datasets — quarterly CSVs by LGA.)

# QLD: https://developmenti.statedevelopment.qld.gov.au/  (Development.i has a
#      JSON search endpoint; check network tab on a /search to grab it.)

# WA: https://dplh.wa.gov.au/  (open data via data.wa.gov.au)

# SA: https://plan.sa.gov.au/  (PlanSA has a portal; Open Data SA has DA datasets)

# TAS: https://www.planning.tas.gov.au/  (very low volume; manual review may suffice)

# ACT: https://app.actplanning.act.gov.au/  (DA register accessible)

# NT: https://nt.gov.au/property/land-planning  (small; manual review)
