"""Configuration loader. Reads config.toml, falls back to env vars."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:
    import tomllib                 # py 3.11+
except ModuleNotFoundError:        # py 3.10 fallback
    import tomli as tomllib  # type: ignore


@dataclass
class CrawlCfg:
    user_agent: str = "newhomes-discovery/0.1"
    rate_limit_rps: float = 1.0
    timeout_seconds: int = 30
    respect_robots: bool = True


@dataclass
class PlaywrightCfg:
    headless: bool = True
    proxy: str | None = None


@dataclass
class AnthropicCfg:
    api_key: str | None = None
    model_strong: str = "claude-sonnet-4-6"
    model_cheap: str = "claude-haiku-4-5"


@dataclass
class SerperCfg:
    api_key: str | None = None


@dataclass
class PathsCfg:
    db: Path = Path("./data/newhomes.db")
    html_cache: Path = Path("./data/html_cache")
    log_dir: Path = Path("./data/logs")


@dataclass
class DiscoveryCfg:
    realestate_max_pages: int = 100
    domain_max_pages: int = 100
    serp_max_queries: int = 1000
    states: str = "all"


@dataclass
class Config:
    paths: PathsCfg = field(default_factory=PathsCfg)
    crawl: CrawlCfg = field(default_factory=CrawlCfg)
    playwright: PlaywrightCfg = field(default_factory=PlaywrightCfg)
    anthropic: AnthropicCfg = field(default_factory=AnthropicCfg)
    serper: SerperCfg = field(default_factory=SerperCfg)
    discovery: DiscoveryCfg = field(default_factory=DiscoveryCfg)


def load(config_path: str | Path | None = None) -> Config:
    """Load config from TOML file (default ./config.toml). Env vars override."""
    cfg = Config()
    path = Path(config_path) if config_path else Path("config.toml")
    if path.exists():
        with path.open("rb") as f:
            raw = tomllib.load(f)
        if "paths" in raw:
            for k, v in raw["paths"].items():
                setattr(cfg.paths, k, Path(v))
        if "crawl" in raw:
            for k, v in raw["crawl"].items():
                setattr(cfg.crawl, k, v)
        if "playwright" in raw:
            for k, v in raw["playwright"].items():
                setattr(cfg.playwright, k, v)
        if "anthropic" in raw:
            for k, v in raw["anthropic"].items():
                setattr(cfg.anthropic, k, v)
        if "serper" in raw:
            for k, v in raw["serper"].items():
                setattr(cfg.serper, k, v)
        if "discovery" in raw:
            for k, v in raw["discovery"].items():
                setattr(cfg.discovery, k, v)

    # Env var fallbacks
    cfg.anthropic.api_key = cfg.anthropic.api_key or os.getenv("ANTHROPIC_API_KEY")
    cfg.serper.api_key    = cfg.serper.api_key    or os.getenv("SERPER_API_KEY")
    return cfg
