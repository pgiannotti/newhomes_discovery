"""Async HTTP client with retries, per-host rate limiting, and HTML caching.

Use `Fetcher` as a context manager:

    async with Fetcher(cfg) as f:
        html = await f.get_text("https://example.com")
"""
from __future__ import annotations

import asyncio
import gzip
import hashlib
import logging
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger("newhomes.http")


class Fetcher:
    def __init__(
        self,
        user_agent: str,
        rate_limit_rps: float = 1.0,
        timeout_seconds: int = 30,
        cache_dir: Path | None = None,
    ):
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.cache_dir = cache_dir
        self._client: httpx.AsyncClient | None = None
        self._host_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._host_last: dict[str, float] = defaultdict(float)
        self._min_interval = 1.0 / rate_limit_rps if rate_limit_rps > 0 else 0.0

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            headers={"User-Agent": self.user_agent},
            timeout=self.timeout_seconds,
            follow_redirects=True,
            http2=True,
        )
        return self

    async def __aexit__(self, *exc):
        if self._client:
            await self._client.aclose()

    async def _wait_turn(self, host: str) -> None:
        async with self._host_locks[host]:
            now = time.monotonic()
            elapsed = now - self._host_last[host]
            if elapsed < self._min_interval:
                await asyncio.sleep(self._min_interval - elapsed)
            self._host_last[host] = time.monotonic()

    async def get(self, url: str, **kwargs) -> httpx.Response:
        host = urlparse(url).hostname or ""
        await self._wait_turn(host)
        assert self._client is not None
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
            reraise=True,
        ):
            with attempt:
                resp = await self._client.get(url, **kwargs)
                # Retry on 5xx and 429
                if resp.status_code >= 500 or resp.status_code == 429:
                    raise httpx.HTTPStatusError(
                        f"transient {resp.status_code}", request=resp.request, response=resp
                    )
                return resp
        raise RuntimeError("unreachable")

    async def get_text(self, url: str, **kwargs) -> str:
        resp = await self.get(url, **kwargs)
        resp.raise_for_status()
        text = resp.text
        if self.cache_dir:
            self._cache_write(url, text)
        return text

    def _cache_write(self, url: str, text: str) -> None:
        assert self.cache_dir is not None
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        key = hashlib.sha256(url.encode()).hexdigest()[:32]
        path = self.cache_dir / f"{key}.html.gz"
        try:
            with gzip.open(path, "wb") as f:
                f.write(text.encode("utf-8"))
        except OSError as e:
            log.warning("cache write failed for %s: %s", url, e)
