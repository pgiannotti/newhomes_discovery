"""Typed row dataclasses. Light wrappers around dicts; only what's useful."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional

DeveloperType = Literal["developer", "builder", "hybrid", "unknown"]
ProjectStatus = Literal["planning", "selling", "sold_out", "completed", "unknown"]
StateCode = Literal["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT", "NATIONAL", "UNKNOWN"]


@dataclass
class SourceRecord:
    """One observed fact from one source. Written to source_records.

    All `parsed_*` fields are optional — sources contribute whatever they see.
    """
    source_code: str
    raw_url: str
    parsed_developer_name: Optional[str] = None
    parsed_project_name: Optional[str] = None
    parsed_project_domain: Optional[str] = None
    parsed_fb_url: Optional[str] = None
    parsed_state: Optional[str] = None
    parsed_suburb: Optional[str] = None
    parsed_status: Optional[str] = None
    confidence: float = 0.5
    raw_html_path: Optional[str] = None
    extra: dict = field(default_factory=dict)


@dataclass
class Developer:
    name: str
    normalised_name: str
    type: DeveloperType = "unknown"
    primary_domain: Optional[str] = None
    fb_url: Optional[str] = None
    abn: Optional[str] = None
    hq_state: Optional[StateCode] = None
    parent_developer_id: Optional[int] = None
    notes: Optional[str] = None


@dataclass
class Project:
    developer_id: int
    name: str
    normalised_name: str
    project_domain: Optional[str] = None
    fb_url: Optional[str] = None
    state: Optional[str] = None
    suburb: Optional[str] = None
    postcode: Optional[str] = None
    status: ProjectStatus = "unknown"
    project_type: Optional[str] = None
