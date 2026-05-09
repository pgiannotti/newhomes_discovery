# newhomes_discovery

A repeatable Python pipeline for discovering and cataloguing **new home developers and their projects** across Australia. Output is a graph of `Developer (Parent Brand) → Project`, with each project having a website domain, Facebook page URL, and provenance for every fact.

This pipeline is the data-acquisition layer that feeds your ad-creative dashboard. It does not pull ads itself — it produces the `domain` list your existing ad-pull system consumes.

---

## What it produces

Two normalised CSV exports (and the same data in SQLite):

**developers.csv**
```
developer_id, name, normalised_name, abn, type, primary_domain, fb_url, hq_state, sources, last_verified
```
`type` ∈ `{developer, builder, hybrid}`. A "hybrid" is a builder who also markets house-and-land packages across multiple branded estates — those need the same parent → project treatment as developers.

**projects.csv**
```
project_id, developer_id, name, project_domain, fb_url, state, suburb, status, sources, last_verified
```
`status` ∈ `{planning, selling, sold_out, completed}`.

A `provenance` view in SQLite shows which source(s) contributed each row, so you can audit and re-run.

---

## Architecture

Six stages, each idempotent and re-runnable:

```
┌──────────────────┐   ┌──────────────────┐   ┌─────────────────┐
│ 1. SOURCE CRAWL  │──▶│ 2. PARSE & STAGE │──▶│ 3. RESOLVE SITE │
│ realestate, UDIA,│   │ Raw rows into    │   │ Find official   │
│ planning portals,│   │ source_records   │   │ marketing site  │
│ Google SERP      │   │ (audit log)      │   │ for each project│
└──────────────────┘   └──────────────────┘   └────────┬────────┘
                                                       │
┌──────────────────┐   ┌──────────────────┐   ┌────────▼────────┐
│ 6. EXPORT        │◀──│ 5. ENTITY RESOLVE│◀──│ 4. RESOLVE FB + │
│ CSV + dashboard  │   │ Dedupe, link     │   │ PARENT BRAND    │
│ feed             │   │ projects→parents │   │ (LLM + site)    │
└──────────────────┘   └──────────────────┘   └─────────────────┘
```

Every stage writes to SQLite. Re-running a stage replays from the previous stage's table — you do not need to re-crawl to re-run resolution.

---

## Sources (in priority order)

| Source | What it gives | Cost | Reliability |
|---|---|---|---|
| **urban.com.au** | **Highest-value source.** Structured developer→project graph: `/developers` profile pages list every project, every project lists its developer. Apartments + masterplans focus. | Free, light protection — plain httpx | High |
| **UDIA** member directory (per-state) | Authoritative developer list (esp. apartments / masterplans) | Free | High |
| **Property Council of Australia** member list | Larger commercial + listed developers (Mirvac, Stockland, Lendlease) | Free | High |
| **HIA** member directory | Builders by state (you may already have most) | Free | High |
| **NSW Planning Portal** open data | DA-stage and major projects with proponent name | Free API | High |
| **homely.com.au** /new-homes | Long-tail boutique developments, lighter bot protection | Free | Medium |
| **allhomes.com.au** /new-homes | ACT + regional NSW coverage gap | Free, light Cloudflare | Medium |
| **realestate.com.au** /new-homes | Active **selling** projects with developer attribution | Free but Akamai-protected (Playwright + stealth) | Medium |
| **domain.com.au** /new-homes | Sister source to realestate.com.au, complementary coverage | Free, Cloudflare-protected (Playwright) | Medium |
| **firsthome.com.au** | First-home buyer focused; long-tail H&L estates under hybrid builders | Free | Low (fragile parser — coverage booster only) |
| **VIC / QLD / WA / SA / TAS / ACT / NT** planning data | Stub modules with documented entry points | Free | Mixed |
| **Google SERP** (Serper.dev) | Long-tail discovery: vanity project domains, microsites | ~$0.30 / 1k queries | High signal, needs filtering |
| **Claude API** | Parent-brand resolution, About-page parsing, ambiguous-name disambiguation | ~$20–100 for full run on 2k seeds | High |

### Operational reality

The portal scrapers (realestate, domain) are gated by Akamai/Cloudflare. Direct `requests` will get 403'd. The pipeline handles this two ways:

1. **Playwright with stealth** (default) — works for low-volume re-crawls but flaky at scale.
2. **Bring-your-own-proxy** hook in `config.py` for residential proxy rotation if you hit volume.

If portals get too painful, **Google SERP + LLM extraction** is the pragmatic fallback: query for `"new apartments in {suburb} site:realestate.com.au"`, extract project URLs from results, hit individual project pages (lighter protection than the index).

---

## Schema rationale

A single `developers` table holds **parent brands** (whether they call themselves developer, builder, or hybrid). `projects` is a child table FK'd to `developers`. The `type` column on `developers` tells the dashboard whether to expect projects underneath:

- A pure builder with no project branding (e.g. a local custom-home builder) gets one `developers` row and **zero** `projects` rows. The dashboard shows them flat.
- A developer with a portfolio (e.g. Stockland) gets one `developers` row and many `projects` rows.
- A hybrid (e.g. a national builder selling H&L packages across 12 estates) gets one `developers` row and one `projects` row per estate.

This means **your "Parent Brand" filter works the same for all three cases** — every ad creative is attached to a project, and every project is attached to a developer, even if the developer has only one nominal "project" that's just the parent brand itself.

---

## Layout

```
newhomes_discovery/
├── README.md                 (this file)
├── pyproject.toml
├── config.example.toml
├── sql/
│   └── schema.sql            (canonical SQLite schema)
├── newhomes/
│   ├── store/
│   │   ├── db.py             (sqlite connection, migrations)
│   │   └── models.py         (typed dataclasses for rows)
│   ├── core/
│   │   ├── http.py           (httpx + retries, robots.txt aware)
│   │   ├── playwright_pool.py
│   │   ├── normalise.py      (name/domain/FB URL canonicalisation)
│   │   └── audit.py          (source_records writer)
│   ├── sources/
│   │   ├── base.py           (Source abstract class)
│   │   ├── urban_com_au.py   (highest-value — developer profile crawl)
│   │   ├── realestate_com_au.py
│   │   ├── domain_com_au.py
│   │   ├── homely_com_au.py
│   │   ├── allhomes_com_au.py
│   │   ├── firsthome.py      (fragile — see module docstring)
│   │   ├── udia.py
│   │   ├── property_council.py
│   │   ├── hia.py
│   │   ├── planning_nsw.py
│   │   └── google_serp.py
│   ├── resolvers/
│   │   ├── project_site.py   (find official marketing URL)
│   │   └── facebook.py       (find canonical FB page)
│   ├── llm/
│   │   ├── client.py         (Anthropic SDK wrapper)
│   │   └── parent_brand.py   (resolve parent → child relationships)
│   ├── entity_resolution.py  (dedupe + cluster)
│   └── cli/
│       ├── __init__.py
│       └── main.py           (Typer CLI: crawl, resolve, export, run-all)
├── data/                     (gitignored; holds db + raw HTML cache)
└── tests/
```

---

## CLI usage

```bash
# Initialise db
python -m newhomes init-db

# Crawl one source
python -m newhomes crawl --source udia
python -m newhomes crawl --source realestate --state nsw --max-pages 50

# Resolve project sites + FB pages for every project missing them
python -m newhomes resolve --stage site
python -m newhomes resolve --stage facebook

# LLM-resolve parent brands for unresolved developers
python -m newhomes resolve --stage parent-brand

# Dedupe and cluster
python -m newhomes entity-resolve

# Export
python -m newhomes export --out ./out/

# Or end-to-end
python -m newhomes run-all --states nsw,vic,qld
```

Every command is idempotent: re-running with `--since 7d` only re-processes records older than 7 days.

---

## Configuration

Copy `config.example.toml` → `config.toml` and fill in:

- `anthropic.api_key` — for LLM enrichment
- `serper.api_key` — for Google SERP (optional but strongly recommended)
- `playwright.proxy` — optional residential proxy URL
- `crawl.user_agent`, `crawl.rate_limit_rps`

---

## Cost estimate (first full run, ~2k builders → ~5–10k projects expected)

| Stage | Estimate |
|---|---|
| Serper SERP | ~10k queries × $0.0003 = **$3** |
| Claude (Sonnet) parent-brand resolution | ~3k developers × ~5k tokens each ≈ **$30** |
| Claude (Haiku) About-page extraction | ~5k pages × ~3k tokens ≈ **$8** |
| Playwright runtime | self-hosted, $0 |
| **Total** | **~$40–50** |

---

## What's not in scope (yet)

- **Ad creative ingestion** — that's your existing system; this pipeline only feeds it `domain` and `fb_url`.
- **ABR (Australian Business Register) lookups** for ABN verification — easy to add as a resolver stage; left as a TODO with clear hook.
- **Subdomain discovery** for vanity project sites — added as TODO in `resolvers/project_site.py`.
