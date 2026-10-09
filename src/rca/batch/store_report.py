"""Store batch RCA JSON reports into a results table on the source database."""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from collections.abc import Mapping
from typing import Any

import psycopg2
import psycopg2.sql

from rca.config import load_database_config
from rca.database import connect_db
from rca.batch.match import Confidence, MatchSignals, score_similarity, weakest_confidence



def store_report(
    conn: Any, config: dict[str, Any], report: dict[str, Any], filename: str | None = None
) -> bool:
    batch_id = report.get("batch_id")
    if not batch_id and filename:
        base = os.path.splitext(os.path.basename(filename))[0]
        batch_id = base
    if not batch_id:
        print("[ERROR] Cannot determine batch_id", file=sys.stderr)
        return False

    results_table = config["results_table"]
    source_table = config["source_table"]

    jobs = report.get("job_results") or report.get("job_summaries") or report.get("jobs", [])
    result_ids_by_job: dict[str, int] = {}
    with conn.cursor() as cur:
        for job in jobs:
            jid = str(job.get("job_id", ""))
            status = job.get("status")
            cur.execute(
                psycopg2.sql.SQL(
                    """INSERT INTO {}
                       (batch_id, job_id, status, root_cause_category, root_cause_summary,
                        confidence, catalog_item, job_duration_seconds)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (batch_id, job_id) DO UPDATE SET
                           status = EXCLUDED.status,
                           root_cause_category = EXCLUDED.root_cause_category,
                           root_cause_summary = EXCLUDED.root_cause_summary,
                           confidence = EXCLUDED.confidence,
                           catalog_item = EXCLUDED.catalog_item,
                           job_duration_seconds = EXCLUDED.job_duration_seconds
                       RETURNING id"""
                ).format(psycopg2.sql.Identifier(results_table)),
                (
                    batch_id,
                    jid,
                    status,
                    job.get("root_cause_category"),
                    job.get("root_cause_summary"),
                    job.get("confidence"),
                    job.get("catalog_item"),
                    job.get("job_duration_seconds"),
                ),
            )
            row = cur.fetchone()
            new_id = (row["id"] if isinstance(row, Mapping) else row[0]) if row else None
            if new_id is None:
                print(f"[ERROR] Result insert for job {jid} did not return an ID", file=sys.stderr)
                conn.rollback()
                return False

            result_ids_by_job[jid] = new_id
            job["result_id"] = new_id
            if status in ("analyzed", "matched_known_issue"):
                cur.execute(
                    psycopg2.sql.SQL(
                        """UPDATE {} SET aap2_job_results_fk_id = %s, ai_processed = TRUE
                           WHERE job_id = %s"""
                    ).format(psycopg2.sql.Identifier(source_table)),
                    (new_id, jid),
                )

    for pattern in report.get("cross_job_patterns", []):
        if pattern.get("pattern_id"):
            continue
        result_ids = [
            result_ids_by_job[str(job_id)]
            for job_id in pattern.get("jobs", [])
            if str(job_id) in result_ids_by_job
        ]
        if result_ids:
            pattern["pattern_id"] = str(min(result_ids))

    conn.commit()
    return True


def store_pre_matched(conn: Any, config: dict[str, Any], pre_matched: list[dict[str, Any]]) -> int:
    """Store pre-filtered jobs that were matched before Claude invocation."""
    source_table = config["source_table"]
    count = 0
    with conn.cursor() as cur:
        for job in pre_matched:
            jid = str(job["job_id"])
            matched_id = job["matched_result_id"]
            cur.execute(
                psycopg2.sql.SQL(
                    """UPDATE {} SET aap2_job_results_fk_id = %s, ai_processed = TRUE
                       WHERE job_id = %s"""
                ).format(psycopg2.sql.Identifier(source_table)),
                (matched_id, jid),
            )
            print(
                f"[PRE-MATCH] job {jid} -> result {matched_id}"
                f" (reason: {job.get('match_reason', 'catalog_item')})"
            )
            count += 1
    conn.commit()
    return count


def link_intra_batch_dupes(
    conn: Any, config: dict[str, Any], intra_batch_dupes: list[dict[str, Any]]
) -> int:
    """Copy aap2_job_results_fk_id from representative to each duplicate in the same batch."""
    source_table = config["source_table"]
    count = 0
    with conn.cursor() as cur:
        for entry in intra_batch_dupes:
            dupe_id = str(entry["job_id"])
            rep_id = str(entry["representative_job_id"])

            cur.execute(
                psycopg2.sql.SQL("SELECT aap2_job_results_fk_id FROM {} WHERE job_id = %s").format(
                    psycopg2.sql.Identifier(source_table)
                ),
                (rep_id,),
            )
            row = cur.fetchone()
            fk_id = (
                row.get("aap2_job_results_fk_id") if isinstance(row, Mapping) else row[0]
            ) if row else None
            if fk_id is None:
                print(
                    f"[WARN] intra-batch dupe {dupe_id}: representative {rep_id} has no FK yet",
                    file=sys.stderr,
                )
                continue

            cur.execute(
                psycopg2.sql.SQL(
                    """UPDATE {} SET aap2_job_results_fk_id = %s, ai_processed = TRUE
                       WHERE job_id = %s"""
                ).format(psycopg2.sql.Identifier(source_table)),
                (fk_id, dupe_id),
            )
            print(f"[DUPE-LINK] job {dupe_id} -> result {fk_id} (via representative {rep_id})")
            count += 1

    conn.commit()
    return count


def _job_summary_signals(job: dict[str, Any]) -> MatchSignals:
    return MatchSignals(
        catalog_item=job.get("catalog_item"),
        text=job.get("root_cause_summary"),
        category=job.get("root_cause_category"),
    )


def _fetch_result(
    cur: Any, results_table: str, result_id: int
) -> dict[str, Any] | None:
    cur.execute(
        psycopg2.sql.SQL(
            """SELECT id, catalog_item, root_cause_category, root_cause_summary,
                      cross_job_pattern
               FROM {} WHERE id = %s
               FOR UPDATE"""
        ).format(psycopg2.sql.Identifier(results_table)),
        (result_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    if isinstance(row, Mapping):
        return dict(row)
    return {
        "id": row[0],
        "catalog_item": row[1],
        "root_cause_category": row[2],
        "root_cause_summary": row[3],
        "cross_job_pattern": row[4],
    }


def _canonical_anchor(row: dict[str, Any]) -> int:
    candidate = row.get("cross_job_pattern")
    try:
        return int(candidate) if candidate is not None else int(row["id"])
    except (TypeError, ValueError):
        return int(row["id"])


def _resolved_anchor(cur: Any, results_table: str, row: dict[str, Any]) -> int:
    candidate = _canonical_anchor(row)
    if candidate == int(row["id"]):
        return candidate
    anchor_row = _fetch_result(cur, results_table, candidate)
    return candidate if anchor_row is not None else int(row["id"])


def _assign_cluster(
    cur: Any,
    results_table: str,
    result_ids: list[int],
    target_anchor: int,
    confidence: Confidence,
    description: str | None,
) -> int:
    existing_rows = [_fetch_result(cur, results_table, result_id) for result_id in result_ids]
    if any(row is None for row in existing_rows):
        return target_anchor

    existing_anchors = {
        _resolved_anchor(cur, results_table, row)
        for row in existing_rows
        if row is not None
    }
    existing_anchors.add(target_anchor)
    canonical_anchor = min(existing_anchors)

    anchor_values = [str(anchor) for anchor in existing_anchors]
    current_ids = [row["id"] for row in existing_rows if row is not None]
    cur.execute(
        psycopg2.sql.SQL(
            """UPDATE {}
               SET cross_job_pattern = %s,
                   cross_job_pattern_description = CASE
                       WHEN id = ANY(%s) THEN %s
                       ELSE cross_job_pattern_description
                   END,
                   cross_job_pattern_confidence = CASE
                       WHEN id = ANY(%s) THEN %s
                       ELSE cross_job_pattern_confidence
                   END
               WHERE cross_job_pattern = ANY(%s) OR id = ANY(%s)"""
        ).format(psycopg2.sql.Identifier(results_table)),
        (
            str(canonical_anchor),
            current_ids,
            description,
            current_ids,
            confidence,
            anchor_values,
            current_ids,
        ),
    )
    return canonical_anchor


def post_analysis_link(conn: Any, config: dict[str, Any], report: dict[str, Any]) -> None:
    """Apply validated semantic and deterministic links as canonical clusters."""

    results_table = config["results_table"]
    jobs = report.get("job_results") or report.get("job_summaries") or report.get("jobs", [])
    jobs_by_id = {str(job.get("job_id")): job for job in jobs}
    result_ids_by_job = {
        job_id: job.get("result_id")
        for job_id, job in jobs_by_id.items()
        if job.get("result_id") is not None
    }

    with conn.cursor() as cur:
        for pattern in report.get("cross_job_patterns", []) or []:
            pattern_jobs = [
                jobs_by_id[str(job_id)]
                for job_id in pattern.get("jobs", [])
                if str(job_id) in jobs_by_id
            ]
            if len(pattern_jobs) < 2:
                continue
            pairwise_confidences: list[Confidence | None] = []
            for left_index, left_job in enumerate(pattern_jobs):
                for right_job in pattern_jobs[left_index + 1 :]:
                    score = score_similarity(
                        _job_summary_signals(left_job),
                        _job_summary_signals(right_job),
                        "post_analysis",
                        semantic_confidence=pattern.get("confidence"),
                        semantic_reasoning=pattern.get("description"),
                    )
                    pairwise_confidences.append(score.confidence)
            confidence = weakest_confidence(pairwise_confidences)
            if confidence is None:
                continue

            pattern_result_ids = [
                int(result_ids_by_job[str(job["job_id"])])
                for job in pattern_jobs
                if str(job["job_id"]) in result_ids_by_job
            ]
            if len(pattern_result_ids) != len(pattern_jobs):
                continue
            anchor = min(pattern_result_ids)
            canonical_anchor = _assign_cluster(
                cur,
                results_table,
                pattern_result_ids,
                anchor,
                confidence,
                pattern.get("description"),
            )
            pattern["pattern_id"] = str(canonical_anchor)

        for job in jobs:
            for match in job.get("historical_matches", []) or []:
                matched_result_id = match.get("matched_result_id")
                if matched_result_id is None:
                    continue
                target_row = _fetch_result(cur, results_table, int(matched_result_id))
                if target_row is None:
                    continue
                score = score_similarity(
                    _job_summary_signals(job),
                    MatchSignals(
                        catalog_item=target_row.get("catalog_item"),
                        text=target_row.get("root_cause_summary"),
                        category=target_row.get("root_cause_category"),
                    ),
                    "post_analysis",
                    semantic_confidence=match.get("confidence"),
                    semantic_reasoning=match.get("similarity_reasoning"),
                )
                if score.confidence is None:
                    continue
                result_id = job.get("result_id")
                if result_id is None:
                    continue
                target_anchor = _resolved_anchor(cur, results_table, target_row)
                canonical_anchor = _assign_cluster(
                    cur,
                    results_table,
                    [int(result_id)],
                    target_anchor,
                    score.confidence,
                    match.get("similarity_reasoning"),
                )
                match["pattern_id"] = str(canonical_anchor)

    conn.commit()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Store batch RCA reports in PostgreSQL")
    parser.add_argument("report", nargs="?", help="Path to a batch report JSON file")
    parser.add_argument("--backfill", metavar="DIR", help="Load all batch_*.json files from DIR")
    parser.add_argument(
        "--pre-matched",
        metavar="JSON",
        help="JSON array of pre-matched jobs from pre_filter_jobs.py (stdin if '-')",
    )
    parser.add_argument(
        "--link-dupes",
        metavar="JSON",
        help="JSON array of intra-batch dupes [{job_id, representative_job_id}] to link",
    )
    args = parser.parse_args(argv)

    if not args.report and not args.backfill and not args.pre_matched and not args.link_dupes:
        parser.error(
            "Provide a report path, --backfill DIR, --pre-matched JSON, or --link-dupes JSON"
        )

    try:
        config = load_database_config(required=("name", "user", "password", "results_table", "source_table"))
    except SystemExit:
        return 1

    try:
        conn = connect_db(config)
    except psycopg2.OperationalError as e:
        print(f"Cannot connect to database: {e}", file=sys.stderr)
        return 1

    print(f"[INFO] Connected as user: {config['user']} on {config['results_table']}")

    if args.pre_matched:
        try:
            if args.pre_matched == "-":
                pre_matched = json.load(sys.stdin)
            else:
                pre_matched = json.loads(args.pre_matched)
            count = store_pre_matched(conn, config, pre_matched)
            print(f"[DONE] {count} pre-matched job(s) stored")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[ERROR] Invalid pre-matched JSON: {e}", file=sys.stderr)
            return 1
        finally:
            conn.close()
        return 0

    if args.link_dupes:
        try:
            dupes = json.loads(args.link_dupes)
            count = link_intra_batch_dupes(conn, config, dupes)
            print(f"[DONE] {count} intra-batch dupe(s) linked")
        except (json.JSONDecodeError, KeyError) as e:
            print(f"[ERROR] Invalid link-dupes JSON: {e}", file=sys.stderr)
            return 1
        finally:
            conn.close()
        return 0

    files: list[str] = []
    if args.backfill:
        files = sorted(glob.glob(os.path.join(args.backfill, "batch_*.json")))
        if not files:
            print(f"[WARN] No batch_*.json files found in {args.backfill}", file=sys.stderr)
            conn.close()
            return 0
    elif args.report:
        files = [args.report]

    inserted = 0
    for path in files:
        try:
            with open(path) as f:
                report = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"[ERROR] Failed to read {path}: {e}", file=sys.stderr)
            continue

        if store_report(conn, config, report, filename=path):
            bid = report.get("batch_id") or os.path.splitext(os.path.basename(path))[0]
            jobs = (
                report.get("job_results") or report.get("job_summaries") or report.get("jobs", [])
            )
            inserted += 1
            print(f"[OK] Stored {bid} ({len(jobs)} jobs)")
            try:
                post_analysis_link(conn, config, report)
            except Exception as e:
                print(f"[WARN] Failed to apply post-analysis links: {e}", file=sys.stderr)

    conn.close()
    print(f"[DONE] {inserted}/{len(files)} report(s) stored in {config['results_table']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
