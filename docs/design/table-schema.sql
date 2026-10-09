-- Proposed schema for the RCA results table.
-- Columns used in WHERE/JOIN/grouping stay real columns; everything that is
-- only read and rendered lives in the root_cause JSONB blob.
-- See root-cause-example.json for the blob's contents.

CREATE TABLE aap2_job_results (
    id                             SERIAL PRIMARY KEY,

    batch_id                       TEXT NOT NULL,
    job_id                         BIGINT NOT NULL,
    status                         TEXT NOT NULL,

    root_cause_category            TEXT,
    confidence                     TEXT,
    catalog_item                   TEXT,
    job_duration_seconds           INTEGER,

    -- { summary, platform, failing_role, failing_github_path, analysis_path,
    --   evidence[], causal_chain[], misidentifications[], recommendations[] }
    root_cause                     JSONB,

    -- Same-batch grouping signal, written by store_cross_patterns().
    -- Unconfirmed: rendered as "possibly related", never as a verified match.
    cross_job_pattern              TEXT,
    cross_job_pattern_description  TEXT,

    -- ticket_key is the short Jira key (RHDP-1234); the MCP tools take a key,
    -- not a URL. ticket_resolve_datetime_gmt drives the known-issue
    -- suppression window in known_issue_active_sql().
    ticket_key                     TEXT,
    ticket_link                    TEXT,
    ticket_resolve_datetime_gmt    TIMESTAMPTZ,

    created_at                     TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Required by store_report.py's ON CONFLICT (batch_id, job_id) clause.
    UNIQUE (batch_id, job_id)
);


-- Changes from what the code uses today:
--
--   root_cause_summary             -> root_cause->>'summary'
--   root_cause                     NEW  JSONB, all render-only detail
--   ticket_key                     NEW  short Jira key alongside the URL
--   created_at                     NEW  removes parsing timestamps out of batch_id
--   UNIQUE (batch_id, job_id)      NEW  constraint the ON CONFLICT assumes exists
--
--   cross_job_pattern              unchanged
--   cross_job_pattern_description  unchanged
--
-- Not added: cross_job_pattern_github_path. A shared offending file is just a
-- 'related_job' evidence entry's github_path.
--
-- Not columns: platform, failing_role, failing_github_path, analysis_path.
-- These live inside root_cause.
