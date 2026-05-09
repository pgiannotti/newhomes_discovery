"""Thin Anthropic client wrapper with on-disk caching keyed on (purpose, input).

The LLM stage is the most expensive part of the pipeline — caching by content
hash means re-running is essentially free, and you can iterate on prompts
without re-spending. Cache lives in `llm_calls` table.
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from typing import Any

log = logging.getLogger("newhomes.llm")


class ClaudeClient:
    def __init__(self, api_key: str, conn: sqlite3.Connection):
        if not api_key:
            raise ValueError("Anthropic API key required")
        # Lazy import — anthropic SDK is optional for entity-resolution-only flows.
        from anthropic import Anthropic
        self._client = Anthropic(api_key=api_key)
        self.conn = conn

    def call(
        self,
        purpose: str,
        model: str,
        system: str,
        user: str,
        max_tokens: int = 1024,
        json_mode: bool = True,
    ) -> dict[str, Any]:
        """Call Claude with caching. Returns parsed JSON if json_mode else {"text": ...}."""
        cache_key = hashlib.sha256(
            f"{purpose}|{model}|{system}|{user}".encode()
        ).hexdigest()

        cached = self.conn.execute(
            "SELECT response_json FROM llm_calls WHERE cache_key = ?", (cache_key,)
        ).fetchone()
        if cached:
            return json.loads(cached["response_json"])

        log.info("Claude call: purpose=%s tokens~=%d", purpose, len(user) // 4)
        resp = self._client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        text = resp.content[0].text if resp.content else ""
        if json_mode:
            try:
                # Strip ```json fences if Claude added them
                t = text.strip()
                if t.startswith("```"):
                    t = t.split("\n", 1)[1].rsplit("```", 1)[0]
                parsed = json.loads(t)
            except json.JSONDecodeError:
                log.warning("non-JSON response for purpose=%s; returning raw", purpose)
                parsed = {"_raw": text}
        else:
            parsed = {"text": text}

        self.conn.execute(
            """
            INSERT INTO llm_calls (purpose, model, cache_key, input_tokens, output_tokens, response_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                purpose, model, cache_key,
                getattr(resp.usage, "input_tokens", None),
                getattr(resp.usage, "output_tokens", None),
                json.dumps(parsed),
            ),
        )
        return parsed
