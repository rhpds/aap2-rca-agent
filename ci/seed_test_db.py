#!/usr/bin/env python3
"""Seed the CI test database with the synthetic job events.

Creates the source/results tables and inserts one event row per job defined
in synthetic-logs/generate.py, so the database and JOB_LOGS_DIR tell the same
story. Timestamps are relative to now so --since windows work on every run.

Database settings come from the same places rca-batch reads them:
RCA_SETTINGS_FILE (settings.json env block) and the process environment,
falling back to the local docker-compose defaults.

Splunk, GitHub, and Jira are intentionally left unconfigured: the batch's
steps 2 and 4 record a skip and continue.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg2


def _settings() -> dict[str, str]:
    try:
        from rca.config import load_environment

        return load_environment()
    except ImportError:
        import os

        return dict(os.environ)


def _database_settings(env: dict[str, str]) -> dict[str, Any]:
    return {
        "host": env.get("SOURCE_DB_HOST") or "localhost",
        "port": int(env.get("SOURCE_DB_PORT") or "5433"),
        "dbname": env.get("SOURCE_DB_NAME") or "rca_test",
        "user": env.get("SOURCE_DB_USER") or "rca_test",
        "password": env.get("SOURCE_DB_PASSWORD") or "rca_test",
    }


def _load_job_specs(logs_dir: str | None) -> list[dict]:
    candidates = [
        Path(logs_dir) / "generate.py" if logs_dir else None,
        Path(__file__).resolve().parent / "synthetic-logs" / "generate.py",
    ]
    generator = next((path for path in candidates if path and path.is_file()), None)
    if generator is None:
        raise FileNotFoundError(
            "Could not find synthetic-logs/generate.py (looked in "
            f"{logs_dir!r} and next to this script)"
        )
    spec = importlib.util.spec_from_file_location("synthetic_generate", generator)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module.JOBS


def main() -> int:
    env = _settings()
    database = _database_settings(env)
    source_table = env.get("SOURCE_DB_TABLE") or "aap2_events"
    results_table = env.get("SOURCE_DB_RESULT_TABLE") or "aap2_job_results"
    logs_dir = env.get("JOB_LOGS_DIR")
    jobs = _load_job_specs(logs_dir)
    now = datetime.now(timezone.utc)

    # The batch reads job logs from JOB_LOGS_DIR and only falls back to a
    # bastion fetch when a job id has no local log file. Refuse to seed ids
    # without logs so the test never exercises (and never depends on) SSH.
    if logs_dir:
        missing = [
            str(job["job_id"])
            for job in jobs
            if not any(Path(logs_dir).glob(f"job_{job['job_id']}.*"))
        ]
        if missing:
            print(
                f"ERROR: no job log files under {logs_dir} for job ids "
                f"{', '.join(missing)}; the batch would try to fetch them "
                "from a bastion host",
                file=sys.stderr,
            )
            return 1

    schema_sql = f"""
    DROP TABLE IF EXISTS {results_table};
    DROP TABLE IF EXISTS {source_table};

    CREATE TABLE {source_table} (
        job_id BIGINT PRIMARY KEY,
        job_name TEXT,
        error_message TEXT,
        job_started TIMESTAMPTZ,
        job_finished TIMESTAMPTZ,
        ai_processed BOOLEAN DEFAULT FALSE,
        cluster_name TEXT,
        aap2_job_results_fk_id INTEGER
    );

    CREATE TABLE {results_table} (
        id SERIAL PRIMARY KEY,
        batch_id TEXT NOT NULL,
        job_id BIGINT NOT NULL,
        status TEXT,
        root_cause_category TEXT,
        root_cause_summary TEXT,
        confidence TEXT,
        catalog_item TEXT,
        job_duration_seconds INTEGER,
        cross_job_pattern TEXT,
        cross_job_pattern_description TEXT,
        ticket_link TEXT,
        ticket_resolve_datetime_gmt TIMESTAMPTZ,
        CONSTRAINT results_batch_job_unique UNIQUE (batch_id, job_id)
    );
    """

    conn = psycopg2.connect(**database)
    try:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
            for index, job in enumerate(jobs):
                started = now - timedelta(minutes=30 + index * 7, seconds=index * 13)
                finished = started + timedelta(seconds=job["duration"])
                cur.execute(
                    f"""
                    INSERT INTO {source_table}
                        (job_id, job_name, error_message, job_started, job_finished,
                         ai_processed, cluster_name)
                    VALUES (%s, %s, %s, %s, %s, FALSE, %s)
                    """,
                    (
                        job["job_id"],
                        job["job_name"],
                        job["failed_task"]["msg"],
                        started,
                        finished,
                        job["cluster"],
                    ),
                )
        conn.commit()
    finally:
        conn.close()

    print(f"Seeded {len(jobs)} synthetic job event(s) into {database['dbname']}.{source_table}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
