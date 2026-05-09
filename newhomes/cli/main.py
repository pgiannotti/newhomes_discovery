"""CLI entrypoint.

    python -m newhomes init-db
    python -m newhomes crawl --source udia
    python -m newhomes crawl --source realestate --states nsw,vic --max-pages 30
    python -m newhomes crawl --source serp --max-queries 200
    python -m newhomes crawl --source planning_nsw

    python -m newhomes entity-resolve

    python -m newhomes resolve --stage site
    python -m newhomes resolve --stage facebook
    python -m newhomes resolve --stage parent-brand

    python -m newhomes export --out ./out/

    python -m newhomes run-all --states nsw,vic,qld
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import typer
from rich.logging import RichHandler

from .. import config as cfg_mod
from .. import entity_resolution, exporter
from ..core.audit import finish_run, start_run, write_records
from ..core.http import Fetcher
from ..store.db import connect, init_db

app = typer.Typer(add_completion=False, no_args_is_help=True)
log = logging.getLogger("newhomes")


def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(rich_tracebacks=True, markup=True)],
    )


@app.command("init-db")
def cmd_init_db(config: str = typer.Option("config.toml")):
    """Create the SQLite schema."""
    _setup_logging()
    cfg = cfg_mod.load(config)
    init_db(cfg.paths.db)
    log.info("Initialised DB at %s", cfg.paths.db)


@app.command("crawl")
def cmd_crawl(
    source: str = typer.Option(
        ...,
        help=("udia|realestate|domain|urban|homely|allhomes|"
              "hia|property_council|firsthome|serp|planning_nsw"),
    ),
    states: str = typer.Option("", help="comma-separated AU state codes"),
    max_pages: int = typer.Option(0, help="cap pagination"),
    max_queries: int = typer.Option(0, help="cap SERP queries"),
    config: str = typer.Option("config.toml"),
):
    """Run one source and write SourceRecords."""
    _setup_logging()
    cfg = cfg_mod.load(config)
    init_db(cfg.paths.db)
    asyncio.run(_run_crawl(cfg, source, states, max_pages, max_queries))


async def _run_crawl(cfg, source: str, states: str, max_pages: int, max_queries: int):
    state_list = [s.strip().upper() for s in states.split(",") if s.strip()] or None
    conn = connect(cfg.paths.db)
    args = {"source": source, "states": state_list, "max_pages": max_pages, "max_queries": max_queries}
    run_id = start_run(conn, source, args)

    try:
        async with Fetcher(
            user_agent=cfg.crawl.user_agent,
            rate_limit_rps=cfg.crawl.rate_limit_rps,
            timeout_seconds=cfg.crawl.timeout_seconds,
            cache_dir=cfg.paths.html_cache,
        ) as fetcher:
            src = _build_source(source, cfg, fetcher, state_list, max_pages, max_queries)
            buf, total = [], 0
            async for rec in src.iter_records():
                buf.append(rec)
                if len(buf) >= 200:
                    total += write_records(conn, buf, run_id)
                    buf.clear()
            if buf:
                total += write_records(conn, buf, run_id)
        finish_run(conn, run_id, total, "ok")
        log.info("[green]done[/]: %d records", total)
    except Exception as e:
        finish_run(conn, run_id, 0, "failed", str(e))
        raise


def _build_source(source: str, cfg, fetcher, states, max_pages, max_queries):
    if source == "udia":
        from ..sources.udia import UdiaSource
        return UdiaSource(fetcher, states=states)
    if source == "realestate":
        from ..sources.realestate_com_au import RealestateSource
        return RealestateSource(
            states=states or ["NSW", "VIC", "QLD", "WA", "SA"],
            max_pages_per_state=max_pages or cfg.discovery.realestate_max_pages,
            playwright_proxy=cfg.playwright.proxy,
            headless=cfg.playwright.headless,
        )
    if source == "domain":
        from ..sources.domain_com_au import DomainSource
        return DomainSource(
            states=states or ["NSW", "VIC", "QLD", "WA", "SA"],
            max_pages_per_state=max_pages or cfg.discovery.domain_max_pages,
            playwright_proxy=cfg.playwright.proxy,
            headless=cfg.playwright.headless,
        )
    if source == "urban":
        from ..sources.urban_com_au import UrbanComAuSource
        return UrbanComAuSource(
            fetcher,
            states=states,
            max_project_pages_per_state=max_pages or 30,
        )
    if source == "homely":
        from ..sources.homely_com_au import HomelyComAuSource
        return HomelyComAuSource(
            fetcher, states=states, max_pages_per_state=max_pages or 30,
        )
    if source == "allhomes":
        from ..sources.allhomes_com_au import AllhomesSource
        return AllhomesSource(
            fetcher, states=states, max_pages_per_state=max_pages or 30,
        )
    if source == "hia":
        from ..sources.hia import HiaSource
        return HiaSource(fetcher, states=states, max_pages_per_state=max_pages or 100)
    if source == "property_council":
        from ..sources.property_council import PropertyCouncilSource
        return PropertyCouncilSource(fetcher, max_pages=max_pages or 50)
    if source == "firsthome":
        from ..sources.firsthome import FirsthomeSource
        return FirsthomeSource(fetcher, states=states, max_pages_per_index=max_pages or 20)
    if source == "serp":
        from ..sources.google_serp import SerperSource
        return SerperSource(
            api_key=cfg.serper.api_key or "",
            max_queries=max_queries or cfg.discovery.serp_max_queries,
        )
    if source == "planning_nsw":
        from ..sources.planning_nsw import PlanningNswSource
        return PlanningNswSource(max_pages=max_pages or 1000)
    raise typer.BadParameter(f"unknown source {source!r}")


@app.command("entity-resolve")
def cmd_entity_resolve(config: str = typer.Option("config.toml")):
    """Promote source_records → developers + projects (deduped)."""
    _setup_logging()
    cfg = cfg_mod.load(config)
    conn = connect(cfg.paths.db)
    counts = entity_resolution.resolve(conn)
    log.info("entity resolution: %s", counts)


@app.command("resolve")
def cmd_resolve(
    stage: str = typer.Option(..., help="site|facebook|parent-brand"),
    limit: int = typer.Option(0),
    config: str = typer.Option("config.toml"),
):
    """Run a single resolution stage."""
    _setup_logging()
    cfg = cfg_mod.load(config)
    conn = connect(cfg.paths.db)
    n_limit = limit or None
    if stage == "site":
        from ..resolvers.project_site import resolve_all
        n = asyncio.run(resolve_all(conn, limit=n_limit))
        log.info("site resolution: %d projects updated", n)
    elif stage == "facebook":
        from ..resolvers.facebook import resolve_all
        n = asyncio.run(resolve_all(conn, limit=n_limit))
        log.info("facebook resolution: %d entities updated", n)
    elif stage == "parent-brand":
        from ..llm.client import ClaudeClient
        from ..llm.parent_brand import resolve_all
        if not cfg.anthropic.api_key:
            raise typer.BadParameter("anthropic.api_key not set")
        claude = ClaudeClient(cfg.anthropic.api_key, conn)
        n = asyncio.run(resolve_all(conn, claude, limit=n_limit, model=cfg.anthropic.model_strong))
        log.info("parent-brand resolution: %d developers updated", n)
    else:
        raise typer.BadParameter(f"unknown stage {stage!r}")


@app.command("export")
def cmd_export(
    out: Path = typer.Option(Path("./out"), exists=False),
    config: str = typer.Option("config.toml"),
):
    """Write developers.csv and projects.csv to <out>/."""
    _setup_logging()
    cfg = cfg_mod.load(config)
    conn = connect(cfg.paths.db)
    out.mkdir(parents=True, exist_ok=True)
    nd = exporter.export_developers(conn, out / "developers.csv")
    np = exporter.export_projects(conn, out / "projects.csv")
    log.info("exported %d developers, %d projects to %s", nd, np, out)


@app.command("run-all")
def cmd_run_all(
    states: str = typer.Option("nsw,vic,qld,wa,sa"),
    config: str = typer.Option("config.toml"),
):
    """Convenience: crawl every source, resolve, export."""
    _setup_logging()
    cfg = cfg_mod.load(config)
    init_db(cfg.paths.db)
    # Order matters: high-trust + light-protection sources first so the
    # canonical-name picker sees clean evidence before the noisier sources.
    for source in [
        "urban",            # highest-value: structured developer→project edges
        "udia",
        "property_council",
        "hia",
        "planning_nsw",
        "homely",
        "allhomes",
        "realestate",       # Akamai — slow, may need proxy
        "domain",           # Cloudflare — same caveat
        "firsthome",        # fragile parser — coverage booster
        "serp",
    ]:
        try:
            asyncio.run(_run_crawl(cfg, source, states, 0, 0))
        except Exception as e:
            log.warning("source %s failed: %s — continuing", source, e)
    cmd_entity_resolve(config)
    cmd_resolve("site", 0, config)
    cmd_resolve("facebook", 0, config)
    cmd_resolve("parent-brand", 0, config)
    cmd_export(Path("./out"), config)


if __name__ == "__main__":
    app()
