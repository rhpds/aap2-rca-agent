"""Tests for storing batch reports with psycopg2 mapping-style rows."""

from unittest.mock import MagicMock

from rca.batch import store_report


def _connection_and_cursor():
    connection = MagicMock()
    cursor_context = MagicMock()
    cursor = cursor_context.__enter__.return_value
    cursor.connection = connection
    connection.cursor.return_value = cursor_context
    return connection, cursor


def test_store_report_reads_mapping_insert_result() -> None:
    connection, cursor = _connection_and_cursor()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = {"id": 55}

    assert store_report.store_report(
        connection,
        {"results_table": "results", "source_table": "events"},
        {
            "batch_id": "batch_test",
            "job_summaries": [
                {
                    "job_id": "123",
                    "status": "analyzed",
                    "root_cause_category": "infrastructure",
                    "root_cause_summary": "A worker ran out of capacity",
                    "confidence": "high",
                    "catalog_item": "widget",
                    "historical_matches": [
                        {
                            "matched_result_id": 99,
                            "similarity_reasoning": "Same worker capacity issue.",
                            "confidence": "high",
                        }
                    ],
                }
            ],
        },
    )

    assert (55, "123") in [call.args[1] for call in cursor.execute.call_args_list]
    assert (99, "123") not in [call.args[1] for call in cursor.execute.call_args_list]
    connection.commit.assert_called_once_with()


def test_link_intra_batch_dupes_reads_mapping_result_row() -> None:
    connection, cursor = _connection_and_cursor()
    cursor.fetchone.return_value = {"aap2_job_results_fk_id": 91}

    linked = store_report.link_intra_batch_dupes(
        connection,
        {"source_table": "events"},
        [{"job_id": "222", "representative_job_id": "111"}],
    )

    assert linked == 1
    assert (91, "222") in [call.args[1] for call in cursor.execute.call_args_list]
    connection.commit.assert_called_once_with()


def test_store_report_assigns_pattern_id_from_result_ids() -> None:
    connection, cursor = _connection_and_cursor()
    cursor.fetchall.return_value = []
    cursor.fetchone.side_effect = [{"id": 12}, {"id": 11}]
    report = {
        "batch_id": "batch_test",
        "job_summaries": [
            {"job_id": "123", "status": "analyzed", "confidence": "high"},
            {"job_id": "124", "status": "analyzed", "confidence": "high"},
        ],
        "cross_job_patterns": [
            {
                "pattern": "worker timeout",
                "jobs": ["123", "124"],
                "description": "Both jobs time out in worker 7.",
                "source": "current_batch",
            }
        ],
    }

    assert store_report.store_report(
        connection,
        {"results_table": "results", "source_table": "events"},
        report,
    )

    assert report["job_summaries"][0]["result_id"] == 12
    assert report["job_summaries"][1]["result_id"] == 11
    assert report["cross_job_patterns"][0]["pattern_id"] == "11"
    connection.commit.assert_called_once_with()


def test_post_analysis_link_persists_derived_pattern_id() -> None:
    connection, cursor = _connection_and_cursor()
    result_rows = [
        {
            "id": 42,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "root_cause_summary": "Worker 7 timed out.",
            "cross_job_pattern": None,
        },
        {
            "id": 43,
            "catalog_item": "widget",
            "root_cause_category": "infrastructure",
            "root_cause_summary": "Worker 7 timed out.",
            "cross_job_pattern": None,
        },
    ]
    cursor.fetchone.side_effect = result_rows + result_rows
    report = {
        "batch_id": "batch_test",
        "job_summaries": [
            {
                "job_id": "123",
                "status": "analyzed",
                "result_id": 42,
                "catalog_item": "widget",
                "root_cause_category": "infrastructure",
                "root_cause_summary": "Worker 7 timed out.",
            },
            {
                "job_id": "124",
                "status": "analyzed",
                "result_id": 43,
                "catalog_item": "widget",
                "root_cause_category": "infrastructure",
                "root_cause_summary": "Worker 7 timed out.",
            },
        ],
        "cross_job_patterns": [
            {
                "pattern": "worker timeout",
                "jobs": ["123", "124"],
                "description": "Both jobs time out in worker 7.",
                "source": "current_batch",
                "pattern_id": "42",
                "confidence": "high",
            }
        ],
    }

    store_report.post_analysis_link(connection, {"results_table": "results"}, report)

    update_params = [call.args[1] for call in cursor.execute.call_args_list if len(call.args) > 1]
    assert any(
        params[0] == "42"
        and params[1] == [42, 43]
        and params[3] == [42, 43]
        and params[4] == "high"
        for params in update_params
    )
    connection.commit.assert_called_once_with()
