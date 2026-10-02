"""Tests for reusable batch pre-filter functions."""

from typing import Any
from unittest.mock import Mock

from rca.batch import pre_filter_jobs


def test_filter_against_known_issues_preserves_unknown_jobs_and_input_order(
    monkeypatch,
) -> None:
    recent_results = [
        {
            "id": 77,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "root_cause_summary": "Repeated connection reset",
            "error_message": "Connection reset by peer",
        }
    ]
    job_metadata = {
        11: {
            "job_name": "RHPDS a.widget.dev-11-create",
            "error_message": "Connection reset by peer",
        },
        12: {
            "job_name": "RHPDS a.another-widget.dev-12-create",
            "error_message": "A completely unrelated authentication failure",
        },
    }

    def fake_fetch_context(
        conn: Any,
        results_table: str,
        source_table: str,
        job_ids: list[int],
        lookback_hours: int,
    ):
        assert (results_table, source_table, job_ids, lookback_hours) == (
            "results",
            "events",
            [13, 11, 12],
            4,
        )
        return recent_results, job_metadata

    monkeypatch.setattr(pre_filter_jobs, "fetch_filter_context", fake_fetch_context)

    result = pre_filter_jobs.filter_against_known_issues(
        object(), "results", "events", [13, 11, 12], lookback_hours=4
    )

    assert result["analyze"] == [13, 12]
    assert result["pre_matched"] == [
        {
            "job_id": 11,
            "matched_result_id": 77,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "match_reason": "pre_filter_catalog_item+error_message",
            "recent_result_summary": "Repeated connection reset",
        }
    ]


def test_filter_against_known_issues_does_not_query_for_empty_batch(monkeypatch) -> None:
    fetch_context = Mock()
    monkeypatch.setattr(pre_filter_jobs, "fetch_filter_context", fetch_context)

    assert pre_filter_jobs.filter_against_known_issues(object(), "results", "events", []) == {
        "analyze": [],
        "pre_matched": [],
    }
    fetch_context.assert_not_called()
