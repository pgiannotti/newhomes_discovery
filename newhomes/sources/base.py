"""Abstract base for sources.

A `Source` produces an async iterable of SourceRecords. The orchestrator
handles persistence and run tracking — sources just yield facts.
"""
from __future__ import annotations

import abc
from typing import AsyncIterator

from ..store.models import SourceRecord


class Source(abc.ABC):
    """One discovery source.

    Subclasses set `code` (matching sources.code in the DB) and implement
    `iter_records()` as an async generator.
    """
    code: str = ""
    name: str = ""

    @abc.abstractmethod
    def iter_records(self, **kwargs) -> AsyncIterator[SourceRecord]:
        """Yield SourceRecord instances. Implementation MUST be an async generator."""
        ...
