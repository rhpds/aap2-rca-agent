"""Unit tests for the Python Agent SDK batch coordinator."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import threading
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from rca.analysis.pipeline import AnalysisArtifacts
from rca.batch import orchestrator
from rca.config import Config


def _config(tmp_path: Path, *, max_parallel_jobs: int = 2) -> Config:
    return Config.from_env(
        environment={
            "RCA_STATE_DIR": str(tmp_path),
            "RCA_MAX_PARALLEL_JOBS": str(max_parallel_jobs),
            "SOURCE_DB_NAME": "rca",
            "SOURCE_DB_USER": "agent",
            "SOURCE_DB_PASSWORD": "not-a-real-secret",
            "SOURCE_DB_TABLE": "aap2_events",
            "SOURCE_DB_RESULT_TABLE": "aap2_job_results",
            "JUMPBOX_URI": "agent@jumpbox.example.test",
        },
        env_file=tmp_path / "missing.env",
    )


def _write_summary(analysis_dir: Path) -> None:
    (analysis_dir / "step5_analysis_summary.json").write_text(
        json.dumps(
            {
                "root_cause": {
                    "summary": "A test failure",
                    "category": "infrastructure",
                    "confidence": "high",
                }
            }
        ),
        encoding="utf-8",
    )


def test_main_parses_batch_options_and_calls_python_orchestrator(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    run_batch = Mock(return_value=0)
    monkeypatch.setattr(orchestrator.Config, "from_env", classmethod(lambda cls: config))
    monkeypatch.setattr(orchestrator, "run_batch", run_batch)

    assert (
        orchestrator.main(
            ["--since", "2026-10-01 10:00:00", "--limit", "7", "--no-pre-filter"]
        )
        == 0
    )

    run_batch.assert_called_once_with(
        config,
        since="2026-10-01 10:00:00",
        limit=7,
        no_pre_filter=True,
    )


def test_cli_rejects_non_positive_limit() -> None:
    with pytest.raises(SystemExit):
        orchestrator._build_parser().parse_args(["--limit", "0"])


def test_sdk_result_usage_and_semaphore_limit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    active = 0
    peak_active = 0
    option_values: list[dict[str, object]] = []

    class FakeSDK:
        @staticmethod
        def ClaudeAgentOptions(**kwargs: object) -> dict[str, object]:
            options = dict(kwargs)
            option_values.append(options)
            return options

        async def query(self, *, prompt: str, options: dict[str, object]):
            nonlocal active, peak_active
            active += 1
            peak_active = max(peak_active, active)
            try:
                await asyncio.sleep(0.01)
                yield SimpleNamespace(
                    is_error=False,
                    total_cost_usd=0.125,
                    usage={"input_tokens": 12, "output_tokens": 4},
                    model_usage={
                        "claude-test-v1": {
                            "inputTokens": 12,
                            "outputTokens": 4,
                            "costUSD": 0.125,
                        }
                    },
                    result=f"completed {prompt}",
                    structured_output={"ok": True},
                    errors=[],
                    session_id="session-test",
                )
            finally:
                active -= 1

    monkeypatch.setattr(orchestrator, "_load_agent_sdk", lambda: FakeSDK())
    config = _config(tmp_path, max_parallel_jobs=2)

    async def run_queries():
        semaphore = asyncio.Semaphore(config.max_parallel_jobs)
        return await asyncio.gather(
            *(
                orchestrator._run_sdk_query(
                    f"job {index}",
                    config,
                    cwd=tmp_path,
                    semaphore=semaphore,
                    include_skill=True,
                )
                for index in range(5)
            )
        )

    results = asyncio.run(run_queries())

    assert peak_active == 2
    assert len(results) == 5
    assert results[0].is_error is False
    assert results[0].cost_usd == 0.125
    assert results[0].usage == {"input_tokens": 12, "output_tokens": 4}
    assert results[0].model_usage == {
        "claude-test-v1": {
            "inputTokens": 12,
            "outputTokens": 4,
            "costUSD": 0.125,
        }
    }
    assert results[0].text == "completed job 0"
    assert results[0].structured_output == {"ok": True}
    assert results[0].session_id == "session-test"
    assert all(item["skills"] == ["root-cause-analysis"] for item in option_values)


def test_job_failures_are_isolated_from_other_jobs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_run_analysis(*, config: Config, job_id: str, fetch: bool, db_pool: object):
        assert fetch is True
        if job_id == "101":
            raise RuntimeError("missing job log")
        analysis_dir = tmp_path / ".analysis" / job_id
        analysis_dir.mkdir(parents=True)
        return AnalysisArtifacts(
            job_id=job_id,
            analysis_dir=analysis_dir,
            job_context={},
            splunk_logs={},
            correlation={},
            github_fetch_history={},
        )

    async def fake_sdk_query(*args: object, **kwargs: object) -> orchestrator.SDKQueryResult:
        _write_summary(tmp_path / ".analysis" / "102")
        return orchestrator.SDKQueryResult(
            is_error=False,
            cost_usd=0.02,
            usage={"input_tokens": 20},
            model_usage={"claude-test-v1": {"inputTokens": 20, "costUSD": 0.02}},
            text="uploaded",
        )

    monkeypatch.setattr(orchestrator, "run_analysis", fake_run_analysis)
    monkeypatch.setattr(orchestrator, "_run_sdk_query", fake_sdk_query)
    monkeypatch.setattr(orchestrator, "upload_to_jumpbox", Mock(return_value=True))
    config = _config(tmp_path, max_parallel_jobs=2)

    executions = asyncio.run(
        orchestrator._analyze_jobs([101, 102], config, object(), cwd=tmp_path)
    )

    assert [execution.status for execution in executions] == ["failed", "completed"]
    assert executions[0].stage == "analysis_pipeline"
    assert "missing job log" in (executions[0].error or "")
    assert executions[1].cost_usd == 0.02
    assert executions[1].usage == {"input_tokens": 20}
    assert executions[1].model_usage == {
        "claude-test-v1": {"inputTokens": 20, "costUSD": 0.02}
    }


def test_job_concurrency_bounds_analysis_and_skill_as_one_unit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    active_jobs: set[str] = set()
    active_lock = threading.Lock()
    peak_active_jobs = 0

    def fake_run_analysis(*, config: Config, job_id: str, fetch: bool, db_pool: object):
        nonlocal peak_active_jobs
        assert fetch is True
        with active_lock:
            active_jobs.add(job_id)
            peak_active_jobs = max(peak_active_jobs, len(active_jobs))

        analysis_dir = tmp_path / ".analysis" / job_id
        analysis_dir.mkdir(parents=True, exist_ok=True)
        time.sleep(0.03)
        return AnalysisArtifacts(
            job_id=job_id,
            analysis_dir=analysis_dir,
            job_context={},
            splunk_logs={},
            correlation={},
            github_fetch_history={},
        )

    async def fake_sdk_query(prompt: str, *args: object, **kwargs: object) -> orchestrator.SDKQueryResult:
        job_id = prompt.split("AAP job ", 1)[1].split(" ", 1)[0]
        await asyncio.sleep(0.04)
        _write_summary(tmp_path / ".analysis" / job_id)
        return orchestrator.SDKQueryResult(
            is_error=False,
            cost_usd=0.01,
            usage={},
            text="summary written",
        )

    def fake_upload(job_id: str, *args: object, **kwargs: object) -> bool:
        time.sleep(0.03)
        with active_lock:
            active_jobs.remove(job_id)
        return True

    monkeypatch.setattr(orchestrator, "run_analysis", fake_run_analysis)
    monkeypatch.setattr(orchestrator, "_run_sdk_query", fake_sdk_query)
    monkeypatch.setattr(orchestrator, "upload_to_jumpbox", fake_upload)
    config = _config(tmp_path, max_parallel_jobs=2)

    executions = asyncio.run(
        orchestrator._analyze_jobs([101, 102, 103, 104, 105], config, object(), cwd=tmp_path)
    )

    assert all(execution.status == "completed" for execution in executions)
    assert peak_active_jobs == config.max_parallel_jobs
    assert active_jobs == set()


def test_retry_rejects_a_previous_step5_summary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config = _config(tmp_path)
    analysis_dir = config.analysis_dir / "101"
    analysis_dir.mkdir(parents=True)
    _write_summary(analysis_dir)
    artifacts = AnalysisArtifacts("101", analysis_dir, {}, {}, {}, {})
    monkeypatch.setattr(orchestrator, "run_analysis", Mock(return_value=artifacts))
    monkeypatch.setattr(
        orchestrator,
        "_run_sdk_query",
        AsyncMock(return_value=orchestrator.SDKQueryResult(False, 0.02, {}, "No summary written")),
    )
    upload = Mock(return_value=True)
    monkeypatch.setattr(orchestrator, "upload_to_jumpbox", upload)

    execution = asyncio.run(
        orchestrator._analyze_jobs([101], config, object(), cwd=tmp_path)
    )[0]

    assert execution.status == "failed"
    assert execution.stage == "step5_missing"
    assert not (analysis_dir / "step5_analysis_summary.json").exists()
    upload.assert_not_called()


@pytest.mark.parametrize("upload_result", [True, False, OSError("SSH unavailable")])
def test_job_success_requires_a_verified_upload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, upload_result: bool | Exception
) -> None:
    config = _config(tmp_path)
    analysis_dir = config.analysis_dir / "101"
    analysis_dir.mkdir(parents=True)
    artifacts = AnalysisArtifacts("101", analysis_dir, {}, {}, {}, {})
    monkeypatch.setattr(orchestrator, "run_analysis", Mock(return_value=artifacts))

    async def fake_sdk_query(*args: object, **kwargs: object) -> orchestrator.SDKQueryResult:
        _write_summary(analysis_dir)
        return orchestrator.SDKQueryResult(
            False, 0.02, {"input_tokens": 20}, "summary written", session_id="job-session"
        )

    monkeypatch.setattr(orchestrator, "_run_sdk_query", fake_sdk_query)
    upload = (
        Mock(side_effect=upload_result)
        if isinstance(upload_result, Exception)
        else Mock(return_value=upload_result)
    )
    monkeypatch.setattr(orchestrator, "upload_to_jumpbox", upload)

    execution = asyncio.run(
        orchestrator._analyze_jobs([101], config, object(), cwd=tmp_path)
    )[0]

    upload.assert_called_once_with(
        "101", analysis_dir, jumpbox_uri=config.jumpbox_uri, session_id="job-session"
    )
    assert execution.cost_usd == 0.02
    assert execution.usage == {"input_tokens": 20}
    if upload_result is True:
        assert execution.status == "completed"
    else:
        assert execution.status == "failed"
        assert execution.stage == "upload"
        assert "Analysis upload failed" in execution.error
        assert orchestrator._job_summary(execution)["status"] == "failed"


@pytest.mark.parametrize(
    ("category", "expected"),
    [("workload_bug", "application_bug"), ("credential", "secrets")],
)
def test_legacy_skill_categories_survive_report_aggregation(category: str, expected: str) -> None:
    execution = orchestrator.JobExecution(
        "101",
        "completed",
        1,
        summary={
            "root_cause": {"summary": "test", "category": category, "confidence": "high"},
            "recommendations": [{"action": "Fix the cause", "priority": "high"}],
        },
    )
    summary = orchestrator._job_summary(execution)
    assert summary["root_cause_category"] == expected
    assert orchestrator._recommendations([summary])[0]["category"] == expected


@pytest.mark.parametrize(
    ("statuses", "has_pre_match", "expected_exit"),
    [
        (["failed", "failed"], False, 1),
        (["completed", "failed"], False, 0),
        (["completed", "completed"], False, 0),
        (["failed", "failed"], True, 0),
        ([], True, 0),
    ],
)
def test_batch_exit_status_preserves_reports_and_partial_success(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    statuses: list[str],
    has_pre_match: bool,
    expected_exit: int,
) -> None:
    config = _config(tmp_path)
    executions = [
        orchestrator.JobExecution(
            str(101 + index),
            status,
            1,
            summary={"root_cause": {"summary": "test"}} if status == "completed" else None,
            error="authentication failed" if status == "failed" else None,
        )
        for index, status in enumerate(statuses)
    ]
    pre_matched = [{"job_id": 103, "matched_result_id": 9}] if has_pre_match else []
    monkeypatch.setattr(orchestrator, "pooled_connection", lambda pool: nullcontext(object()))
    monkeypatch.setattr(orchestrator, "query_job_ids", Mock(return_value=[101, 102, 103]))
    monkeypatch.setattr(orchestrator, "fetch_job_metadata", Mock(return_value={}))
    monkeypatch.setattr(orchestrator, "fetch_known_issues", Mock(return_value=[{"result_id": 9}]))
    monkeypatch.setattr(
        orchestrator,
        "filter_against_known_issues",
        Mock(return_value={"analyze": [int(item.job_id) for item in executions], "pre_matched": pre_matched}),
    )
    monkeypatch.setattr(orchestrator, "store_pre_matched", Mock())
    monkeypatch.setattr(orchestrator, "_analyze_jobs", AsyncMock(return_value=executions))
    monkeypatch.setattr(orchestrator, "_aggregate_semantics", AsyncMock(return_value=([], None)))
    monkeypatch.setattr(orchestrator, "_log_mlflow_usage", Mock())
    store = Mock(return_value=True)
    monkeypatch.setattr(orchestrator, "_store_batch_report", store)

    assert orchestrator.run_batch(config, pool=object(), cwd=tmp_path) == expected_exit
    if executions:
        store.assert_called_once()
        report = store.call_args.args[2]
        assert report["total_jobs_failed"] == statuses.count("failed")
        assert report["total_jobs_analyzed"] == statuses.count("completed")
        assert store.call_args.args[3].is_file()
    else:
        store.assert_not_called()


def test_normalize_semantics_validates_job_and_result_ids() -> None:
    summaries = [
        {"job_id": "job-1", "status": "analyzed", "confidence": "high"},
        {"job_id": "job-2", "status": "analyzed", "confidence": "low"},
        {"job_id": "failed", "status": "failed", "confidence": "low"},
    ]
    known_issues = [{"result_id": 9, "recurrence_count": 3}]
    output = {
        "historical_matches": [
            {
                "job_id": "job-1",
                "matches": [
                    {"matched_result_id": 9, "similarity_reasoning": "Same failure mode."}
                ],
            },
            {
                "job_id": "job-2",
                "matches": [
                    {"matched_result_id": 9, "similarity_reasoning": "Same failure mode."}
                ],
            },
            {
                "job_id": "failed",
                "matches": [
                    {"matched_result_id": 9, "similarity_reasoning": "Same failure mode."}
                ],
            },
        ],
        "cross_job_patterns": [
            {
                "pattern": "shared failure",
                "jobs": ["job-1", "job-2", "failed", "not-in-batch"],
                "description": "Both analyzed jobs show the same failure.",
            }
        ],
    }

    normalized = orchestrator._normalize_semantics(output, summaries, known_issues)

    assert summaries[0]["historical_matches"] == [
        {
            "matched_result_id": 9,
            "recurrence_count": 3,
            "similarity_reasoning": "Same failure mode.",
        }
    ]
    assert summaries[1]["historical_matches"] == []
    assert summaries[2]["historical_matches"] == []
    assert normalized["cross_job_patterns"][0]["jobs"] == ["job-1", "job-2"]


def test_report_aggregation_is_deterministic(tmp_path: Path) -> None:
    first_artifacts = AnalysisArtifacts(
        job_id="201",
        analysis_dir=tmp_path / "201",
        job_context={"job_name": "RHPDS a.widget.dev-201-create"},
        splunk_logs={},
        correlation={},
        github_fetch_history={},
    )
    second_artifacts = AnalysisArtifacts(
        job_id="202",
        analysis_dir=tmp_path / "202",
        job_context={"job_name": "RHPDS a.widget.dev-202-create"},
        splunk_logs={},
        correlation={},
        github_fetch_history={},
    )
    executions = [
        orchestrator.JobExecution(
            job_id="201",
            status="completed",
            duration_ms=100,
            artifacts=first_artifacts,
            summary={
                "root_cause": {
                    "summary": "Worker capacity exhausted",
                    "category": "infrastructure",
                    "confidence": "high",
                },
                "recommendations": [
                    {"action": "Add worker capacity", "priority": "low", "details": "Scale up"}
                ],
            },
        ),
        orchestrator.JobExecution(
            job_id="202",
            status="completed",
            duration_ms=200,
            artifacts=second_artifacts,
            summary={
                "root_cause": {
                    "summary": "Same worker capacity issue",
                    "category": "infrastructure",
                    "confidence": "medium",
                },
                "recommendations": [
                    {
                        "action": "  ADD worker capacity  ",
                        "priority": "high",
                        "details": "Increase worker pool",
                    }
                ],
            },
        ),
        orchestrator.JobExecution(
            job_id="203",
            status="failed",
            duration_ms=50,
            error="Could not fetch job log",
            stage="analysis_pipeline",
        ),
    ]
    job_summaries = [orchestrator._job_summary(execution) for execution in executions]

    report = orchestrator._build_report(
        batch_id="batch_test",
        requested_count=3,
        executions=executions,
        job_summaries=job_summaries,
        cross_job_patterns=[],
        agent_spawn="2026-10-01T10:00:00Z",
        batch_started=time.monotonic(),
        generated_at=datetime(2026, 10, 1, 10, tzinfo=timezone.utc),
    )

    assert report["total_jobs_requested"] == 3
    assert report["total_jobs_analyzed"] == 2
    assert report["total_jobs_failed"] == 1
    assert report["root_cause_category_breakdown"]["infrastructure"]["job_ids"] == [
        "201",
        "202",
    ]
    assert report["confidence_breakdown"] == {"high": 1, "medium": 1, "low": 0}
    assert report["high_priority_recommendations"] == [
        {
            "priority": "high",
            "action": "Add worker capacity",
            "affects_jobs": ["201", "202"],
            "category": "infrastructure",
            "details": "Increase worker pool",
            "github_path": "",
            "rank": 1,
        }
    ]
    assert report["failed_analyses"] == [
        {
            "job_id": "203",
            "error": "Could not fetch job log",
            "stage": "analysis_pipeline",
        }
    ]


def test_jira_step_is_an_explicit_no_op(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="rca.batch")

    orchestrator.prepare_jira_tickets({"batch_id": "batch_test"})

    assert "[STEP 6] Preparing Jira tickets" in caplog.text
    assert "no tickets created" in caplog.text


def test_mlflow_logs_sdk_cost_usage_and_per_model_usage(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    metrics: dict[str, float] = {}
    mlflow = ModuleType("mlflow")
    mlflow.set_tracking_uri = Mock()
    mlflow.get_experiment_by_name = Mock(return_value=SimpleNamespace(experiment_id="exp-1"))
    mlflow.start_run = Mock(return_value=nullcontext())
    mlflow.set_tags = Mock()
    mlflow.log_metric = lambda name, value: metrics.__setitem__(name, value)
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)

    config = _config(tmp_path)
    config.environment.update(
        {
            "MLFLOW_TRACKING_URI": "file:///tmp/mlruns",
            "MLFLOW_EXPERIMENT_NAME": "rca-tests",
        }
    )
    executions = [
        orchestrator.JobExecution(
            job_id="42",
            status="completed",
            duration_ms=100,
            cost_usd=0.1,
            usage={"input_tokens": 10, "output_tokens": 4},
            model_usage={
                "claude-test-v1": {
                    "inputTokens": 8,
                    "outputTokens": 4,
                    "costUSD": 0.1,
                }
            },
        )
    ]
    aggregation_result = orchestrator.SDKQueryResult(
        is_error=False,
        cost_usd=0.02,
        usage={"input_tokens": 3},
        text="aggregated",
        model_usage={"claude-test-v1": {"inputTokens": 2, "costUSD": 0.02}},
    )

    orchestrator._log_mlflow_usage(config, "batch-test", executions, aggregation_result)

    assert metrics["job_42_cost_usd"] == pytest.approx(0.1)
    assert metrics["job_42_input_tokens"] == 10
    assert metrics["job_42_model_claude-test-v1_inputTokens"] == 8
    assert metrics["job_42_model_claude-test-v1_costUSD"] == pytest.approx(0.1)
    assert metrics["batch_aggregation_model_claude-test-v1_inputTokens"] == 2
    assert metrics["batch_model_claude-test-v1_inputTokens"] == 10
    assert metrics["batch_model_claude-test-v1_costUSD"] == pytest.approx(0.12)
    assert metrics["batch_input_tokens"] == 13
    assert metrics["batch_cost_usd"] == pytest.approx(0.12)
