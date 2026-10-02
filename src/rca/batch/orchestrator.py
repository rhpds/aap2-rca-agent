"""Python Agent SDK coordinator for batch root-cause analysis."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from typing import Any, Mapping

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

from rca.analysis.jumpbox_io import upload_to_jumpbox
from rca.analysis.pipeline import AnalysisArtifacts, run_analysis
from rca.batch.fetch_known_issues import fetch_known_issues
from rca.batch.pre_filter_jobs import (
    dedup_batch,
    fetch_job_metadata,
    filter_against_known_issues,
)
from rca.batch.query_source_db import query_job_ids
from rca.batch.store_report import (
    link_intra_batch_dupes,
    store_cross_patterns,
    store_pre_matched,
    store_report,
)
from rca.config import Config
from rca.database import pooled_connection

logger = logging.getLogger("rca.batch")

DEFAULT_LOOKBACK_HOURS = 4
DEFAULT_KNOWN_ISSUE_LIMIT = 50
_PRIORITY_ORDER = {"high": 0, "medium": 1, "low": 2}
_CATEGORY_NAMES = {
    "application_bug": "Application bug",
    "infrastructure": "Infrastructure",
    "configuration": "Configuration",
    "dependency": "Dependency",
    "network": "Network",
    "resource": "Resource",
    "cloud_api": "Cloud API",
    "secrets": "Secrets",
    "unknown": "Unknown",
}
_VALID_CATEGORIES = set(_CATEGORY_NAMES)
_CATEGORY_ALIASES = {"workload_bug": "application_bug", "credential": "secrets"}
_VALID_CONFIDENCE = {"high", "medium", "low"}

_SEMANTIC_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["historical_matches", "cross_job_patterns"],
    "properties": {
        "historical_matches": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["job_id", "matches"],
                "properties": {
                    "job_id": {"type": "string"},
                    "matches": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["matched_result_id", "similarity_reasoning"],
                            "properties": {
                                "matched_result_id": {"type": "integer"},
                                "similarity_reasoning": {"type": "string"},
                            },
                        },
                    },
                },
            },
        },
        "cross_job_patterns": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["pattern", "jobs", "description"],
                "properties": {
                    "pattern": {"type": "string"},
                    "jobs": {"type": "array", "items": {"type": "string"}},
                    "description": {"type": "string"},
                    "shared_github_path": {"type": "string"},
                },
            },
        },
    },
}


@dataclass(frozen=True)
class SDKQueryResult:
    """Terminal result and token accounting from one Agent SDK query."""

    is_error: bool
    cost_usd: float | None
    usage: dict[str, Any]
    text: str
    model_usage: dict[str, dict[str, Any]] = field(default_factory=dict)
    structured_output: Any = None
    errors: tuple[str, ...] = ()
    session_id: str | None = None


@dataclass(frozen=True)
class JobExecution:
    """One job's result, including failures isolated from other jobs."""

    job_id: str
    status: str
    duration_ms: int
    artifacts: AnalysisArtifacts | None = None
    summary: dict[str, Any] | None = None
    error: str | None = None
    stage: str | None = None
    cost_usd: float | None = None
    usage: dict[str, Any] | None = None
    model_usage: dict[str, dict[str, Any]] | None = None


def _positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a batch of root-cause analyses")
    parser.add_argument(
        "--since",
        help="Only include events after this timestamp (defaults to 30 minutes ago, UTC)",
    )
    parser.add_argument("--limit", type=_positive_int, help="Maximum number of job IDs to query")
    parser.add_argument(
        "--no-pre-filter",
        action="store_true",
        help="Skip matching jobs against recent known issues before full analysis",
    )
    return parser


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _default_since(now: datetime | None = None) -> str:
    current = now or _utc_now()
    return (current - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")


def _batch_database_config(config: Config) -> dict[str, Any]:
    database = dict(config.database)
    required = ("name", "user", "password", "source_table", "results_table")
    missing = [key for key in required if not database.get(key)]
    if missing:
        env_names = {
            "name": "SOURCE_DB_NAME",
            "user": "SOURCE_DB_USER",
            "password": "SOURCE_DB_PASSWORD",
            "source_table": "SOURCE_DB_TABLE",
            "results_table": "SOURCE_DB_RESULT_TABLE",
        }
        names = ", ".join(env_names[key] for key in missing)
        raise ValueError(f"Missing required batch database configuration: {names}")

    # Match the previous helper CLIs' localhost default while Config keeps an
    # empty host distinct for its optional bastion-lookup behavior.
    database["host"] = database.get("host") or "localhost"
    database["port"] = int(database.get("port") or 5432)
    return database


def _create_connection_pool(config: Config) -> ThreadedConnectionPool:
    database = _batch_database_config(config)
    return ThreadedConnectionPool(
        minconn=1,
        maxconn=max(1, config.max_parallel_jobs),
        host=database["host"],
        port=database["port"],
        dbname=database["name"],
        user=database["user"],
        password=database["password"],
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def _load_agent_sdk() -> Any:
    try:
        import claude_agent_sdk
    except ImportError as exc:  # pragma: no cover - exercised in installed images
        raise RuntimeError(
            "claude-agent-sdk is required to run batch analysis; install the project runtime dependencies"
        ) from exc
    return claude_agent_sdk


def _agent_environment(config: Config) -> dict[str, str]:
    environment = dict(config.environment)
    environment["RCA_STATE_DIR"] = str(config.state_dir)
    return environment


def _is_result_message(message: Any) -> bool:
    return hasattr(message, "is_error") and hasattr(message, "total_cost_usd")


def _as_usage_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_model_usage_mapping(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        return {}
    return {
        str(model): dict(metrics)
        for model, metrics in value.items()
        if isinstance(metrics, Mapping)
    }


def _result_error_text(message: Any) -> str:
    errors = getattr(message, "errors", None)
    if isinstance(errors, (list, tuple)):
        details = "; ".join(str(error) for error in errors if error)
        if details:
            return details[:1000]
    text = getattr(message, "result", None)
    if isinstance(text, str) and text.strip():
        return text.strip()[:1000]
    subtype = getattr(message, "subtype", None)
    return f"Agent SDK query failed{f' ({subtype})' if subtype else ''}"


def _make_sdk_options(
    sdk: Any,
    config: Config,
    *,
    cwd: Path,
    include_skill: bool,
    output_format: dict[str, Any] | None = None,
) -> Any:
    kwargs: dict[str, Any] = {
        "model": config.environment.get("CLAUDE_MODEL") or None,
        "cwd": str(cwd),
        "permission_mode": "dontAsk",
        "setting_sources": ["user", "project"],
        "env": _agent_environment(config),
    }
    if include_skill:
        kwargs.update(
            skills=["root-cause-analysis"],
            allowed_tools=[
                "Bash",
                "Read",
                "Write",
                "mcp__github__search_code",
                "mcp__github__get_file_contents",
            ],
        )
    else:
        kwargs["tools"] = []
    if output_format is not None:
        kwargs["output_format"] = output_format
    return sdk.ClaudeAgentOptions(**kwargs)


async def _run_sdk_query(
    prompt: str,
    config: Config,
    *,
    cwd: Path,
    semaphore: asyncio.Semaphore,
    include_skill: bool = False,
    output_format: dict[str, Any] | None = None,
) -> SDKQueryResult:
    sdk = _load_agent_sdk()
    options = _make_sdk_options(
        sdk,
        config,
        cwd=cwd,
        include_skill=include_skill,
        output_format=output_format,
    )

    terminal_message = None
    async with semaphore:
        async for message in sdk.query(prompt=prompt, options=options):
            if _is_result_message(message):
                terminal_message = message

    if terminal_message is None:
        raise RuntimeError("Agent SDK query ended without a terminal result")

    raw_cost = getattr(terminal_message, "total_cost_usd", None)
    try:
        cost = float(raw_cost) if raw_cost is not None else None
    except (TypeError, ValueError):
        cost = None
    if cost is not None and not math.isfinite(cost):
        cost = None

    text = getattr(terminal_message, "result", None)
    return SDKQueryResult(
        is_error=bool(getattr(terminal_message, "is_error", False)),
        cost_usd=cost,
        usage=_as_usage_mapping(getattr(terminal_message, "usage", None)),
        text=text if isinstance(text, str) else "",
        model_usage=_as_model_usage_mapping(getattr(terminal_message, "model_usage", None)),
        structured_output=getattr(terminal_message, "structured_output", None),
        errors=tuple(
            str(error)
            for error in (getattr(terminal_message, "errors", None) or [])
            if error
        ),
        session_id=getattr(terminal_message, "session_id", None),
    )


def _skill_prompt(job_id: str, artifacts: AnalysisArtifacts) -> str:
    return f"""Analyze AAP job {job_id} using the `root-cause-analysis` Skill.

Invoke the Skill tool for this job:
Skill({{skill: 'root-cause-analysis', args: '{job_id}'}})

The Python batch orchestrator has already completed deterministic Steps 1–4
and saved their artifacts in `{artifacts.analysis_dir}`. Do not rerun those
steps, fetch the job log again, or overwrite their files. Follow the Skill's
Step 5 analysis guidance using the existing artifacts, and write the summary to
`{artifacts.analysis_dir / 'step5_analysis_summary.json'}`. After writing the
summary, stop and briefly report whether it was written. Do not run the Skill's
upload command: the Python orchestrator will perform and verify the required
upload after validating your summary."""


def _read_step5_summary(artifacts: AnalysisArtifacts) -> dict[str, Any]:
    path = artifacts.analysis_dir / "step5_analysis_summary.json"
    if not path.is_file():
        raise FileNotFoundError(f"Step 5 summary was not created: {path}")
    try:
        with path.open(encoding="utf-8") as stream:
            data = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not read Step 5 summary {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"Step 5 summary must contain a JSON object: {path}")
    root_cause = data.get("root_cause")
    if not isinstance(root_cause, dict) or not isinstance(root_cause.get("summary"), str):
        raise ValueError(f"Step 5 summary has no root_cause.summary: {path}")
    return data


async def _execute_one_job(
    job_id: int | str,
    config: Config,
    db_pool: Any,
    executor: ThreadPoolExecutor,
    sdk_semaphore: asyncio.Semaphore,
    cwd: Path,
) -> JobExecution:
    rendered_id = str(job_id)
    started = time.monotonic()
    loop = asyncio.get_running_loop()
    artifacts: AnalysisArtifacts | None = None
    result: SDKQueryResult | None = None

    try:
        artifacts = await loop.run_in_executor(
            executor,
            partial(run_analysis, config=config, job_id=rendered_id, fetch=True, db_pool=db_pool),
        )
    except Exception as exc:
        return JobExecution(
            job_id=rendered_id,
            status="failed",
            duration_ms=max(0, int((time.monotonic() - started) * 1000)),
            error=f"Analysis pipeline failed: {exc}"[:1000],
            stage="analysis_pipeline",
        )

    try:
        # A retry reuses the job directory; only a summary written by this
        # query may qualify the current execution as successful.
        (artifacts.analysis_dir / "step5_analysis_summary.json").unlink(missing_ok=True)
        result = await _run_sdk_query(
            _skill_prompt(rendered_id, artifacts),
            config,
            cwd=cwd,
            semaphore=sdk_semaphore,
            include_skill=True,
        )
        if result.is_error:
            detail = "; ".join(result.errors) or result.text or "agent reported an error"
            return JobExecution(
                job_id=rendered_id,
                status="failed",
                duration_ms=max(0, int((time.monotonic() - started) * 1000)),
                artifacts=artifacts,
                error=f"Skill execution failed: {detail}"[:1000],
                stage="skill_execution",
                cost_usd=result.cost_usd,
                usage=result.usage,
                model_usage=result.model_usage,
            )
        summary = _read_step5_summary(artifacts)
    except FileNotFoundError as exc:
        return JobExecution(
            job_id=rendered_id,
            status="failed",
            duration_ms=max(0, int((time.monotonic() - started) * 1000)),
            artifacts=artifacts,
            error=str(exc)[:1000],
            stage="step5_missing",
            cost_usd=result.cost_usd if result else None,
            usage=result.usage if result else None,
            model_usage=result.model_usage if result else None,
        )
    except Exception as exc:
        return JobExecution(
            job_id=rendered_id,
            status="failed",
            duration_ms=max(0, int((time.monotonic() - started) * 1000)),
            artifacts=artifacts,
            error=f"Skill execution failed: {exc}"[:1000],
            stage="skill_execution",
            cost_usd=result.cost_usd if result else None,
            usage=result.usage if result else None,
            model_usage=result.model_usage if result else None,
        )

    try:
        uploaded = await loop.run_in_executor(
            executor,
            partial(
                upload_to_jumpbox,
                rendered_id,
                artifacts.analysis_dir,
                jumpbox_uri=config.jumpbox_uri,
                session_id=result.session_id,
            ),
        )
        if not uploaded:
            raise RuntimeError("Upload helper reported failure")
    except Exception as exc:
        return JobExecution(
            job_id=rendered_id,
            status="failed",
            duration_ms=max(0, int((time.monotonic() - started) * 1000)),
            artifacts=artifacts,
            summary=summary,
            error=f"Analysis upload failed: {exc}"[:1000],
            stage="upload",
            cost_usd=result.cost_usd,
            usage=result.usage,
            model_usage=result.model_usage,
        )

    return JobExecution(
        job_id=rendered_id,
        status="completed",
        duration_ms=max(0, int((time.monotonic() - started) * 1000)),
        artifacts=artifacts,
        summary=summary,
        cost_usd=result.cost_usd if result else None,
        usage=result.usage if result else None,
        model_usage=result.model_usage if result else None,
    )


async def _analyze_one_job(
    job_id: int | str,
    config: Config,
    db_pool: Any,
    executor: ThreadPoolExecutor,
    job_semaphore: asyncio.Semaphore,
    sdk_semaphore: asyncio.Semaphore,
    cwd: Path,
) -> JobExecution:
    """Keep deterministic analysis and its Skill query under one job limit."""
    async with job_semaphore:
        return await _execute_one_job(
            job_id,
            config,
            db_pool,
            executor,
            sdk_semaphore,
            cwd,
        )


async def _analyze_jobs(
    job_ids: list[int],
    config: Config,
    db_pool: Any,
    *,
    cwd: Path,
) -> list[JobExecution]:
    job_semaphore = asyncio.Semaphore(config.max_parallel_jobs)
    sdk_semaphore = asyncio.Semaphore(config.max_parallel_jobs)
    with ThreadPoolExecutor(
        max_workers=config.max_parallel_jobs, thread_name_prefix="rca-analysis"
    ) as executor:
        tasks = [
            _analyze_one_job(
                job_id,
                config,
                db_pool,
                executor,
                job_semaphore,
                sdk_semaphore,
                cwd,
            )
            for job_id in job_ids
        ]
        return list(await asyncio.gather(*tasks))


def _normalize_json_output(result: SDKQueryResult) -> dict[str, Any]:
    output = result.structured_output
    if isinstance(output, dict):
        return output
    text = result.text.strip()
    if not text:
        raise ValueError("Agent SDK did not return structured output")
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        # Some CLI versions may include a short preamble despite JSON mode.
        start, end = text.find("{"), text.rfind("}")
        if start < 0 or end <= start:
            raise ValueError("Agent SDK response did not contain a JSON object")
        value = json.loads(text[start : end + 1])
    if not isinstance(value, dict):
        raise ValueError("Agent SDK response must be a JSON object")
    return value


def _semantic_prompt(
    job_summaries: list[dict[str, Any]], known_issues: list[dict[str, Any]]
) -> str:
    return f"""Compare these newly analyzed jobs against recent known issues and
against one another. Return only the requested structured JSON.

Historical matching rules:
- Only consider new jobs with high or medium confidence.
- Match only when the error type, failing component, and failure mode are
  genuinely the same. A shared category or catalog item alone is insufficient.
- Use only matched_result_id values present in the supplied known-issue list.
- Give one concise sentence of evidence-based similarity reasoning per match.

Cross-job pattern rules:
- Report only clear overlaps across at least two new jobs: same failing
  component and substantially the same error/failure mode.
- Do not infer a pattern from a shared category alone. Return an empty array
  when no clear pattern exists.
- Include only job IDs from the supplied newly analyzed jobs.

Newly analyzed jobs:
{json.dumps(job_summaries, ensure_ascii=False, default=str)}

Known recent issues:
{json.dumps(known_issues, ensure_ascii=False, default=str)}"""


def _normalize_semantics(
    output: dict[str, Any],
    job_summaries: list[dict[str, Any]],
    known_issues: list[dict[str, Any]],
) -> dict[str, Any]:
    eligible_jobs = {
        str(job["job_id"])
        for job in job_summaries
        if job.get("status") == "analyzed" and job.get("confidence") in ("high", "medium")
    }
    current_job_ids = {
        str(job["job_id"]) for job in job_summaries if job.get("status") == "analyzed"
    }
    known_by_id = {str(issue.get("result_id")): issue for issue in known_issues}
    matches_by_job: dict[str, list[dict[str, Any]]] = defaultdict(list)

    raw_matches = output.get("historical_matches", [])
    if isinstance(raw_matches, list):
        for item in raw_matches:
            if not isinstance(item, dict):
                continue
            job_id = str(item.get("job_id", ""))
            if job_id not in eligible_jobs:
                continue
            raw_job_matches = item.get("matches", [])
            if not isinstance(raw_job_matches, list):
                continue
            seen_result_ids: set[str] = set()
            for match in raw_job_matches:
                if not isinstance(match, dict):
                    continue
                result_id = str(match.get("matched_result_id", ""))
                issue = known_by_id.get(result_id)
                reasoning = match.get("similarity_reasoning")
                if issue is None or not isinstance(reasoning, str) or not reasoning.strip():
                    continue
                if result_id in seen_result_ids:
                    continue
                seen_result_ids.add(result_id)
                try:
                    numeric_id = int(issue["result_id"])
                    recurrence = max(1, int(issue.get("recurrence_count", 1)))
                except (KeyError, TypeError, ValueError):
                    continue
                matches_by_job[job_id].append(
                    {
                        "matched_result_id": numeric_id,
                        "recurrence_count": recurrence,
                        "similarity_reasoning": reasoning.strip(),
                    }
                )

    cross_patterns: list[dict[str, Any]] = []
    raw_patterns = output.get("cross_job_patterns", [])
    if isinstance(raw_patterns, list):
        for pattern in raw_patterns:
            if not isinstance(pattern, dict):
                continue
            name = pattern.get("pattern")
            description = pattern.get("description")
            jobs = pattern.get("jobs")
            if not isinstance(name, str) or not name.strip():
                continue
            if not isinstance(description, str) or not description.strip():
                continue
            if not isinstance(jobs, list):
                continue
            unique_jobs = list(dict.fromkeys(str(job_id) for job_id in jobs))
            unique_jobs = [job_id for job_id in unique_jobs if job_id in current_job_ids]
            if len(unique_jobs) < 2:
                continue
            item: dict[str, Any] = {
                "pattern": name.strip(),
                "jobs": unique_jobs,
                "description": description.strip(),
                "source": "current_batch",
            }
            shared_path = pattern.get("shared_github_path")
            if isinstance(shared_path, str) and shared_path.strip():
                item["shared_github_path"] = shared_path.strip()
            cross_patterns.append(item)

    for summary in job_summaries:
        summary["historical_matches"] = matches_by_job.get(str(summary["job_id"]), [])
    return {"cross_job_patterns": cross_patterns}


async def _aggregate_semantics(
    job_summaries: list[dict[str, Any]],
    known_issues: list[dict[str, Any]],
    config: Config,
    *,
    cwd: Path,
    semaphore: asyncio.Semaphore,
) -> tuple[list[dict[str, Any]], SDKQueryResult | None]:
    if not job_summaries or (len(job_summaries) < 2 and not known_issues):
        for summary in job_summaries:
            summary["historical_matches"] = []
        return [], None

    result = await _run_sdk_query(
        _semantic_prompt(job_summaries, known_issues),
        config,
        cwd=cwd,
        semaphore=semaphore,
        output_format={"type": "json_schema", "schema": _SEMANTIC_OUTPUT_SCHEMA},
    )
    if result.is_error:
        detail = "; ".join(result.errors) or result.text or "agent reported an error"
        raise RuntimeError(f"Semantic batch aggregation failed: {detail[:1000]}")
    output = _normalize_json_output(result)
    normalized = _normalize_semantics(output, job_summaries, known_issues)
    return normalized["cross_job_patterns"], result


def _confidence(value: Any) -> str:
    candidate = str(value or "").lower()
    return candidate if candidate in _VALID_CONFIDENCE else "low"


def _category(value: Any) -> str:
    candidate = str(value or "").lower()
    candidate = _CATEGORY_ALIASES.get(candidate, candidate)
    return candidate if candidate in _VALID_CATEGORIES else "unknown"


def _job_summary(execution: JobExecution) -> dict[str, Any]:
    if execution.status != "completed" or execution.summary is None:
        error = execution.error or "Analysis failed"
        return {
            "job_id": execution.job_id,
            "status": "failed",
            "root_cause_category": "unknown",
            "root_cause_summary": error,
            "confidence": "low",
            "analysis_duration_ms": execution.duration_ms,
        }

    data = execution.summary
    root_cause = data.get("root_cause") or {}
    artifacts = execution.artifacts
    context = artifacts.job_context if artifacts else {}
    window = context.get("time_window") or {}
    summary: dict[str, Any] = {
        "job_id": execution.job_id,
        "status": "analyzed",
        "root_cause_category": _category(root_cause.get("category")),
        "root_cause_summary": str(root_cause.get("summary") or ""),
        "confidence": _confidence(root_cause.get("confidence")),
        "analysis_duration_ms": execution.duration_ms,
        "analysis_path": str(artifacts.analysis_dir) if artifacts else "",
        "historical_matches": [],
    }
    job_name = context.get("job_name") or ""
    # The legacy batch report includes catalog_item where it can be parsed from
    # the AAP job name; keep it optional for logs with other naming schemes.
    from rca.batch.pre_filter_jobs import extract_catalog_item

    catalog_item = data.get("catalog_item") or extract_catalog_item(job_name)
    if catalog_item:
        summary["catalog_item"] = str(catalog_item)
    platform = data.get("platform") or context.get("env_type")
    if platform:
        summary["platform"] = str(platform)
    duration = window.get("duration_seconds")
    try:
        if duration is not None:
            summary["job_duration_seconds"] = max(0, int(duration))
    except (TypeError, ValueError):
        pass

    recommendations = data.get("recommendations")
    if isinstance(recommendations, list):
        summary["recommendations"] = [item for item in recommendations if isinstance(item, dict)]
    return summary


def _recommendations(job_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}
    order = 0
    for job in job_summaries:
        if job.get("status") != "analyzed":
            continue
        for recommendation in job.get("recommendations", []):
            action = recommendation.get("action")
            if not isinstance(action, str) or not action.strip():
                continue
            normalized_action = " ".join(action.lower().split())
            priority = str(recommendation.get("priority", "low")).lower()
            if priority not in _PRIORITY_ORDER:
                priority = "low"
            existing = candidates.get(normalized_action)
            if existing is None:
                order += 1
                existing = {
                    "priority": priority,
                    "action": action.strip(),
                    "affects_jobs": [],
                    "category": _category(job.get("root_cause_category")),
                    "details": str(recommendation.get("details") or recommendation.get("change") or ""),
                    "github_path": str(recommendation.get("github_path") or ""),
                    "_order": order,
                }
                candidates[normalized_action] = existing
            elif _PRIORITY_ORDER[priority] < _PRIORITY_ORDER[existing["priority"]]:
                existing["priority"] = priority
                existing["details"] = str(
                    recommendation.get("details") or recommendation.get("change") or existing["details"]
                )
                existing["category"] = _category(job.get("root_cause_category"))
                existing["github_path"] = str(
                    recommendation.get("github_path") or existing["github_path"]
                )
            if job["job_id"] not in existing["affects_jobs"]:
                existing["affects_jobs"].append(job["job_id"])

    ranked = sorted(
        candidates.values(),
        key=lambda item: (_PRIORITY_ORDER[item["priority"]], item["_order"]),
    )[:5]
    result: list[dict[str, Any]] = []
    for rank, item in enumerate(ranked, start=1):
        recommendation = {key: value for key, value in item.items() if not key.startswith("_")}
        recommendation["rank"] = rank
        result.append(recommendation)
    return result


def _build_report(
    *,
    batch_id: str,
    requested_count: int,
    executions: list[JobExecution],
    job_summaries: list[dict[str, Any]],
    cross_job_patterns: list[dict[str, Any]],
    agent_spawn: str,
    batch_started: float,
    generated_at: datetime,
) -> dict[str, Any]:
    category_jobs: dict[str, list[str]] = defaultdict(list)
    confidence_counts = {"high": 0, "medium": 0, "low": 0}
    for job in job_summaries:
        if job.get("status") != "analyzed":
            continue
        category = _category(job.get("root_cause_category"))
        category_jobs[category].append(str(job["job_id"]))
        confidence_counts[_confidence(job.get("confidence"))] += 1

    category_breakdown = {
        category: {
            "count": len(job_ids),
            "job_ids": job_ids,
            "description": _CATEGORY_NAMES[category],
        }
        for category, job_ids in category_jobs.items()
    }
    completions = {
        execution.job_id: {
            "duration_ms": execution.duration_ms,
            "status": "completed" if execution.status == "completed" else "failed",
        }
        for execution in executions
    }
    total_analyzed = sum(1 for item in executions if item.status == "completed")
    total_failed = len(executions) - total_analyzed
    report = {
        "batch_id": batch_id,
        "generated_at": generated_at.isoformat().replace("+00:00", "Z"),
        "total_jobs_requested": requested_count,
        "total_jobs_analyzed": total_analyzed,
        "total_jobs_failed": total_failed,
        "timing": {
            "agent_spawn": agent_spawn,
            "agent_completion": completions,
            "wall_clock_total_ms": max(0, int((time.monotonic() - batch_started) * 1000)),
            "aggregation_completed_at": _utc_now().isoformat().replace("+00:00", "Z"),
        },
        "root_cause_category_breakdown": category_breakdown,
        "confidence_breakdown": confidence_counts,
        "high_priority_recommendations": _recommendations(job_summaries),
        "job_summaries": job_summaries,
        "failed_analyses": [
            {
                "job_id": execution.job_id,
                "error": execution.error or "Analysis failed",
                "stage": execution.stage or "skill_execution",
            }
            for execution in executions
            if execution.status != "completed"
        ],
        "cross_job_patterns": cross_job_patterns,
    }
    return report


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _write_report(config: Config, report: dict[str, Any]) -> Path:
    reports_dir = config.state_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / f"{report['batch_id']}.json"
    report_path.write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=_json_default) + "\n",
        encoding="utf-8",
    )
    logger.info("Batch report written to %s", report_path)
    return report_path


def _numeric_usage(usage: Mapping[str, Any]) -> dict[str, float]:
    numeric: dict[str, float] = {}
    for key, value in usage.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        try:
            number = float(value)
        except (OverflowError, ValueError):
            continue
        if math.isfinite(number):
            numeric[str(key)] = number
    return numeric


def _mlflow_metric_component(value: str) -> str:
    """Return a compact, predictable component for a dynamic metric key."""
    component = "".join(
        character if character.isascii() and (character.isalnum() or character in "_-") else "_"
        for character in value
    ).strip("_")
    return (component or "unknown")[:64]


def _log_model_usage_metrics(
    mlflow: Any,
    model_usage: Mapping[str, Mapping[str, Any]],
    *,
    prefix: str,
    totals: dict[tuple[str, str], float],
) -> None:
    for model_name, metrics in sorted(model_usage.items()):
        model_component = _mlflow_metric_component(str(model_name))
        for metric_name, value in sorted(_numeric_usage(metrics).items()):
            metric_component = _mlflow_metric_component(metric_name)
            mlflow.log_metric(
                f"{prefix}_model_{model_component}_{metric_component}",
                value,
            )
            totals[(model_component, metric_component)] += value


@contextmanager
def _mlflow_environment(environment: Mapping[str, str]):
    keys = (
        "MLFLOW_TRACKING_URI",
        "MLFLOW_TRACKING_USERNAME",
        "MLFLOW_TRACKING_PASSWORD",
        "MLFLOW_TRACKING_TOKEN",
        "MLFLOW_EXPERIMENT_NAME",
    )
    previous = {key: os.environ.get(key) for key in keys}
    try:
        for key in keys:
            if environment.get(key):
                os.environ[key] = environment[key]
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _log_mlflow_usage(
    config: Config,
    batch_id: str,
    job_executions: list[JobExecution],
    aggregation_result: SDKQueryResult | None,
) -> None:
    tracking_uri = config.environment.get("MLFLOW_TRACKING_URI", "")
    experiment_name = config.environment.get("MLFLOW_EXPERIMENT_NAME", "")
    if not tracking_uri or not experiment_name:
        logger.info("MLflow usage logging is not configured; skipping")
        return

    try:
        import mlflow

        with _mlflow_environment(config.environment):
            mlflow.set_tracking_uri(tracking_uri)
            experiment = mlflow.get_experiment_by_name(experiment_name)
            if experiment is None:
                logger.warning("MLflow experiment %r was not found; skipping usage logging", experiment_name)
                return
            with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=batch_id):
                mlflow.set_tags(
                    {
                        "batch_id": batch_id,
                        "model": config.environment.get("CLAUDE_MODEL", ""),
                    }
                )
                total_cost = 0.0
                usage_totals: dict[str, float] = defaultdict(float)
                model_usage_totals: dict[tuple[str, str], float] = defaultdict(float)
                for execution in job_executions:
                    usage = _numeric_usage(execution.usage or {})
                    prefix = f"job_{execution.job_id}"
                    if execution.cost_usd is not None:
                        mlflow.log_metric(f"{prefix}_cost_usd", execution.cost_usd)
                        total_cost += execution.cost_usd
                    for key, value in usage.items():
                        mlflow.log_metric(f"{prefix}_{key}", value)
                        usage_totals[key] += value
                    _log_model_usage_metrics(
                        mlflow,
                        execution.model_usage or {},
                        prefix=prefix,
                        totals=model_usage_totals,
                    )

                if aggregation_result is not None:
                    if aggregation_result.cost_usd is not None:
                        mlflow.log_metric("batch_aggregation_cost_usd", aggregation_result.cost_usd)
                        total_cost += aggregation_result.cost_usd
                    for key, value in _numeric_usage(aggregation_result.usage).items():
                        mlflow.log_metric(f"batch_aggregation_{key}", value)
                        usage_totals[key] += value
                    _log_model_usage_metrics(
                        mlflow,
                        aggregation_result.model_usage,
                        prefix="batch_aggregation",
                        totals=model_usage_totals,
                    )

                mlflow.log_metric("batch_cost_usd", total_cost)
                for key, value in usage_totals.items():
                    mlflow.log_metric(f"batch_{key}", value)
                for (model, metric), value in sorted(model_usage_totals.items()):
                    mlflow.log_metric(f"batch_model_{model}_{metric}", value)
    except Exception as exc:  # usage telemetry must not discard a completed report
        logger.warning("Could not log Agent SDK usage to MLflow: %s", exc)


def prepare_jira_tickets(_report: Mapping[str, Any]) -> None:
    """Placeholder for future Jira ticket preparation; deliberately does no work."""
    logger.info("[STEP 6] Preparing Jira tickets... (placeholder; no tickets created)")


def _store_batch_report(
    pool: Any,
    database: dict[str, Any],
    report: dict[str, Any],
    report_path: Path,
    dupes: list[dict[str, Any]],
) -> bool:
    with pooled_connection(pool) as conn:
        stored = store_report(conn, database, report, filename=str(report_path))
        if not stored:
            return False
        store_cross_patterns(conn, database, report)
    if dupes:
        try:
            with pooled_connection(pool) as conn:
                link_intra_batch_dupes(conn, database, dupes)
        except psycopg2.Error as exc:
            logger.warning("Could not link intra-batch duplicate jobs: %s", exc)
    return True


def _run_agent_batch(
    executions: list[JobExecution],
    known_issues: list[dict[str, Any]],
    config: Config,
    *,
    cwd: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], SDKQueryResult | None]:
    successful_summaries = [
        _job_summary(execution) for execution in executions if execution.status == "completed"
    ]
    report_summaries = [_job_summary(execution) for execution in executions]
    semaphore = asyncio.Semaphore(config.max_parallel_jobs)
    try:
        cross_patterns, aggregation_result = asyncio.run(
            _aggregate_semantics(
                successful_summaries,
                known_issues,
                config,
                cwd=cwd,
                semaphore=semaphore,
            )
        )
    except Exception as exc:
        logger.warning("Semantic batch aggregation failed; using deterministic report only: %s", exc)
        cross_patterns = []
        aggregation_result = None
        for summary in successful_summaries:
            summary["historical_matches"] = []

    # Reuse the semantic matches on the report copies. The successful summaries
    # were normalized in place by _aggregate_semantics.
    matches = {item["job_id"]: item.get("historical_matches", []) for item in successful_summaries}
    for summary in report_summaries:
        summary["historical_matches"] = matches.get(summary["job_id"], [])
    return report_summaries, cross_patterns, aggregation_result


def run_batch(
    config: Config,
    *,
    since: str | None = None,
    limit: int | None = None,
    no_pre_filter: bool = False,
    pool: Any | None = None,
    now: datetime | None = None,
    cwd: Path | None = None,
) -> int:
    """Run one batch; ``pool`` and ``now`` are injectable for focused tests."""
    try:
        database = _batch_database_config(config)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1

    owns_pool = pool is None
    try:
        if pool is None:
            pool = _create_connection_pool(config)
    except psycopg2.Error as exc:
        logger.error("Cannot connect to source database: %s", exc)
        return 1

    started_at = now or _utc_now()
    batch_started = time.monotonic()
    batch_id = f"batch_{started_at.strftime('%Y%m%d_%H%M%S')}"
    effective_since = since or _default_since(started_at)
    working_dir = cwd or Path.cwd()

    try:
        logger.info("[STEP 1] Querying source database for unanalyzed jobs since %s", effective_since)
        with pooled_connection(pool) as conn:
            job_ids = query_job_ids(
                conn,
                database["source_table"],
                since=effective_since,
                limit=limit,
            )
        if not job_ids:
            logger.info("No unanalyzed jobs found")
            return 0

        logger.info("Found %d job(s): %s", len(job_ids), ", ".join(map(str, job_ids)))
        logger.info("[STEP 1a] Deduplicating within batch")
        try:
            with pooled_connection(pool) as conn:
                metadata = fetch_job_metadata(conn, database["source_table"], job_ids)
            representative_ids, dupes = dedup_batch(job_ids, metadata)
        except psycopg2.Error as exc:
            logger.warning("Could not deduplicate jobs; analyzing all queried IDs: %s", exc)
            representative_ids, dupes = list(job_ids), []

        known_issues: list[dict[str, Any]] = []
        try:
            with pooled_connection(pool) as conn:
                known_issues = fetch_known_issues(
                    conn,
                    database["results_table"],
                    lookback_hours=DEFAULT_LOOKBACK_HOURS,
                    limit=DEFAULT_KNOWN_ISSUE_LIMIT,
                )
        except psycopg2.Error as exc:
            logger.warning("Could not load recent known issues; continuing without them: %s", exc)

        analyze_ids = list(representative_ids)
        pre_matched: list[dict[str, Any]] = []
        if no_pre_filter:
            logger.info("[STEP 1b] Pre-filter disabled (--no-pre-filter)")
        elif known_issues:
            logger.info(
                "[STEP 1b] Pre-filtering %d job(s) against %d known issue(s)",
                len(representative_ids),
                len(known_issues),
            )
            try:
                with pooled_connection(pool) as conn:
                    filter_result = filter_against_known_issues(
                        conn,
                        database["results_table"],
                        database["source_table"],
                        representative_ids,
                        lookback_hours=DEFAULT_LOOKBACK_HOURS,
                    )
                analyze_ids = filter_result["analyze"]
                pre_matched = filter_result["pre_matched"]
            except psycopg2.Error as exc:
                logger.warning("Pre-filter failed; analyzing unmatched jobs normally: %s", exc)

        if pre_matched:
            logger.info("Pre-filter matched %d job(s)", len(pre_matched))
            try:
                with pooled_connection(pool) as conn:
                    store_pre_matched(conn, database, pre_matched)
            except psycopg2.Error as exc:
                logger.error("Failed to store pre-matched jobs: %s", exc)
                return 1

        if not analyze_ids:
            logger.info("No jobs require full RCA")
            if dupes:
                try:
                    with pooled_connection(pool) as conn:
                        link_intra_batch_dupes(conn, database, dupes)
                except psycopg2.Error as exc:
                    logger.warning("Could not link intra-batch duplicates: %s", exc)
            prepare_jira_tickets({"batch_id": batch_id, "job_summaries": []})
            logger.info("[SUCCESS] Batch RCA completed")
            return 0

        logger.info("[STEP 2] Running deterministic analysis and Skill-based RCA for %d job(s)", len(analyze_ids))
        agent_spawn = _utc_now().isoformat().replace("+00:00", "Z")
        executions = asyncio.run(_analyze_jobs(analyze_ids, config, pool, cwd=working_dir))
        report_summaries, cross_patterns, aggregation_result = _run_agent_batch(
            executions,
            known_issues,
            config,
            cwd=working_dir,
        )
        report = _build_report(
            batch_id=batch_id,
            requested_count=len(analyze_ids),
            executions=executions,
            job_summaries=report_summaries,
            cross_job_patterns=cross_patterns,
            agent_spawn=agent_spawn,
            batch_started=batch_started,
            generated_at=started_at,
        )
        report_path = _write_report(config, report)
        _log_mlflow_usage(config, batch_id, executions, aggregation_result)

        logger.info("[STEP 5] Storing batch report")
        try:
            if not _store_batch_report(pool, database, report, report_path, dupes):
                logger.error("Report storage helper rejected batch %s", batch_id)
                return 1
        except psycopg2.Error as exc:
            logger.error("Failed to store report in database: %s", exc)
            return 1

        prepare_jira_tickets(report)
        failed_count = report["total_jobs_failed"]
        if report["total_jobs_analyzed"] == 0 and not pre_matched:
            logger.error(
                "Batch %s produced no successful analyses: %d failed; report: %s",
                batch_id,
                failed_count,
                report_path,
            )
            return 1
        logger.info(
            "[SUCCESS] Batch %s completed: %d analyzed, %d failed; report: %s",
            batch_id,
            report["total_jobs_analyzed"],
            failed_count,
            report_path,
        )
        return 0
    except (psycopg2.Error, OSError, ValueError) as exc:
        logger.error("Batch RCA failed: %s", exc)
        return 1
    finally:
        if owns_pool and pool is not None:
            pool.closeall()


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _build_parser().parse_args(argv)
    try:
        config = Config.from_env()
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Could not load RCA configuration: %s", exc)
        return 1
    return run_batch(
        config,
        since=args.since,
        limit=args.limit,
        no_pre_filter=args.no_pre_filter,
    )


if __name__ == "__main__":
    raise SystemExit(main())
