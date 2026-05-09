-- newhomes_discovery canonical schema
-- SQLite. Run via newhomes.store.db.init_db().
--
-- Design notes:
--   * `developers` is the parent-brand table. Every catalogued entity (developer,
--     builder, hybrid) gets a row here. `type` discriminates so the dashboard
--     knows whether to expect children.
--   * `projects` is the child table. Every ad creative ultimately attaches to
--     a project, even if the project is just "the parent brand itself" for a
--     plain builder with no estate branding.
--   * `source_records` is the audit log: every fact has a row pointing at the
--     source URL it came from, with confidence. We never overwrite a fact —
--     we add a new source_record and let resolution pick a winner.
--   * `discovery_runs` lets you pin a result to a run for reproducibility.

PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

-- ─────────────────────────────────────────────────────────────────────────────
-- developers (parent brands)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS developers (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT    NOT NULL,
    normalised_name     TEXT    NOT NULL,         -- lowercase, no Pty Ltd, no &/and
    abn                 TEXT,                     -- 11-digit, no spaces
    type                TEXT    NOT NULL DEFAULT 'unknown'
                        CHECK (type IN ('developer','builder','hybrid','unknown')),
    primary_domain      TEXT,                     -- e.g. stockland.com.au
    fb_url              TEXT,                     -- canonical https://www.facebook.com/<page>
    hq_state            TEXT    CHECK (hq_state IN
                            ('NSW','VIC','QLD','WA','SA','TAS','ACT','NT','NATIONAL','UNKNOWN')
                            OR hq_state IS NULL),
    parent_developer_id INTEGER REFERENCES developers(id) ON DELETE SET NULL,
    -- ↑ for the rare case of M&A / sub-brands where two "parent brands" are
    --   themselves nested. Null for the vast majority.
    notes               TEXT,
    first_seen_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_verified_at    TIMESTAMP,
    UNIQUE (normalised_name)
);

CREATE INDEX IF NOT EXISTS idx_developers_domain ON developers(primary_domain);
CREATE INDEX IF NOT EXISTS idx_developers_abn    ON developers(abn);
CREATE INDEX IF NOT EXISTS idx_developers_type   ON developers(type);

-- ─────────────────────────────────────────────────────────────────────────────
-- projects
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS projects (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    developer_id      INTEGER NOT NULL REFERENCES developers(id) ON DELETE CASCADE,
    name              TEXT    NOT NULL,
    normalised_name   TEXT    NOT NULL,
    project_domain    TEXT,                       -- e.g. risehomesvic.com.au
    fb_url            TEXT,
    state             TEXT    CHECK (state IN
                            ('NSW','VIC','QLD','WA','SA','TAS','ACT','NT')
                            OR state IS NULL),
    suburb            TEXT,
    postcode          TEXT,
    lat               REAL,
    lng               REAL,
    status            TEXT    NOT NULL DEFAULT 'unknown'
                        CHECK (status IN ('planning','selling','sold_out','completed','unknown')),
    project_type      TEXT,                       -- 'apartments','townhomes','land','h&l','masterplan'
    first_seen_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_verified_at  TIMESTAMP,
    UNIQUE (developer_id, normalised_name)
);

CREATE INDEX IF NOT EXISTS idx_projects_developer ON projects(developer_id);
CREATE INDEX IF NOT EXISTS idx_projects_domain    ON projects(project_domain);
CREATE INDEX IF NOT EXISTS idx_projects_state     ON projects(state);
CREATE INDEX IF NOT EXISTS idx_projects_status    ON projects(status);

-- ─────────────────────────────────────────────────────────────────────────────
-- sources (registry of where data comes from)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sources (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    code      TEXT NOT NULL UNIQUE,               -- 'realestate_com_au', 'udia', etc.
    name      TEXT NOT NULL,                      -- human-friendly
    kind      TEXT NOT NULL CHECK (kind IN
                ('portal','industry','planning','serp','llm','manual')),
    base_url  TEXT
);

-- Seed the registry. Re-running ON CONFLICT DO NOTHING keeps it idempotent.
INSERT INTO sources (code, name, kind, base_url) VALUES
    ('realestate_com_au', 'realestate.com.au new homes', 'portal', 'https://www.realestate.com.au/new-homes'),
    ('domain_com_au',     'domain.com.au new homes',     'portal', 'https://www.domain.com.au/new-homes'),
    ('udia',              'UDIA member directory',        'industry', 'https://udia.com.au'),
    ('property_council',  'Property Council of Australia','industry', 'https://www.propertycouncil.com.au'),
    ('hia',               'Housing Industry Association', 'industry', 'https://hia.com.au'),
    ('planning_nsw',      'NSW Planning Portal',          'planning', 'https://www.planningportal.nsw.gov.au'),
    ('planning_vic',      'VIC Planning',                 'planning', 'https://www.planning.vic.gov.au'),
    ('planning_qld',      'QLD Development.i',            'planning', 'https://developmenti.statedevelopment.qld.gov.au'),
    ('google_serp',       'Google SERP via Serper',       'serp',     'https://google.com'),
    ('claude',            'Claude (Anthropic)',           'llm',      'https://api.anthropic.com'),
    ('urban_com_au',      'urban.com.au',                 'portal',   'https://www.urban.com.au'),
    ('homely_com_au',     'homely.com.au',                'portal',   'https://www.homely.com.au'),
    ('allhomes_com_au',   'allhomes.com.au',              'portal',   'https://www.allhomes.com.au'),
    ('firsthome',         'firsthome.com.au',             'portal',   'https://www.firsthome.com.au')
ON CONFLICT(code) DO NOTHING;

-- ─────────────────────────────────────────────────────────────────────────────
-- source_records (audit / provenance log)
-- ─────────────────────────────────────────────────────────────────────────────
-- Each row = one observed fact from one source. Multiple rows per developer
-- or project are normal (and desirable). Resolution picks a winner using
-- confidence + recency.
CREATE TABLE IF NOT EXISTS source_records (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id               INTEGER NOT NULL REFERENCES sources(id),
    discovery_run_id        INTEGER REFERENCES discovery_runs(id) ON DELETE SET NULL,
    fetched_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_url                 TEXT NOT NULL,
    raw_html_path           TEXT,                 -- gzipped path on disk for replay
    -- the entities/facts this record contributes (any may be NULL)
    parsed_developer_name   TEXT,
    parsed_project_name     TEXT,
    parsed_project_domain   TEXT,
    parsed_fb_url           TEXT,
    parsed_state            TEXT,
    parsed_suburb           TEXT,
    parsed_status           TEXT,
    -- bookkeeping for resolution
    developer_id            INTEGER REFERENCES developers(id) ON DELETE SET NULL,
    project_id              INTEGER REFERENCES projects(id)   ON DELETE SET NULL,
    confidence              REAL NOT NULL DEFAULT 0.5,        -- 0..1
    extra_json              TEXT                              -- raw extra fields
);

CREATE INDEX IF NOT EXISTS idx_sr_source       ON source_records(source_id);
CREATE INDEX IF NOT EXISTS idx_sr_dev_name     ON source_records(parsed_developer_name);
CREATE INDEX IF NOT EXISTS idx_sr_project_name ON source_records(parsed_project_name);
CREATE INDEX IF NOT EXISTS idx_sr_developer    ON source_records(developer_id);
CREATE INDEX IF NOT EXISTS idx_sr_project      ON source_records(project_id);
CREATE INDEX IF NOT EXISTS idx_sr_fetched      ON source_records(fetched_at);

-- ─────────────────────────────────────────────────────────────────────────────
-- discovery_runs (so each `python -m newhomes crawl ...` is auditable)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS discovery_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at     TIMESTAMP,
    source_code     TEXT NOT NULL,                -- which source was crawled
    args_json       TEXT,                         -- CLI args for replay
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running','ok','failed','partial')),
    rows_inserted   INTEGER NOT NULL DEFAULT 0,
    error           TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_source ON discovery_runs(source_code);
CREATE INDEX IF NOT EXISTS idx_runs_status ON discovery_runs(status);

-- ─────────────────────────────────────────────────────────────────────────────
-- llm_calls (cost + cache log for Claude calls)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS llm_calls (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    called_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    purpose          TEXT NOT NULL,               -- 'parent_brand', 'about_page', 'name_match'
    model            TEXT NOT NULL,
    cache_key        TEXT NOT NULL UNIQUE,        -- sha256 of (purpose, input)
    input_tokens     INTEGER,
    output_tokens    INTEGER,
    response_json    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_purpose ON llm_calls(purpose);

-- ─────────────────────────────────────────────────────────────────────────────
-- Convenience views
-- ─────────────────────────────────────────────────────────────────────────────
CREATE VIEW IF NOT EXISTS v_developer_provenance AS
SELECT  d.id           AS developer_id,
        d.name,
        d.type,
        GROUP_CONCAT(DISTINCT s.code) AS sources,
        COUNT(DISTINCT sr.id)         AS source_record_count,
        MAX(sr.fetched_at)            AS last_seen
FROM developers d
LEFT JOIN source_records sr ON sr.developer_id = d.id
LEFT JOIN sources        s  ON s.id = sr.source_id
GROUP BY d.id;

CREATE VIEW IF NOT EXISTS v_project_full AS
SELECT  p.id              AS project_id,
        p.name            AS project_name,
        d.name            AS developer_name,
        d.type            AS developer_type,
        p.project_domain,
        p.fb_url          AS project_fb_url,
        d.fb_url          AS developer_fb_url,
        p.state, p.suburb, p.status
FROM projects p
JOIN developers d ON d.id = p.developer_id;
