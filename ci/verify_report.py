#!/usr/bin/env python3
"""Assert the batch report shows a successful run.

Verifies structure and counts, not root-cause text: the LLM's content is
non-deterministic, but which jobs were analyzed is not.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <batch_report.json>", file=sys.stderr)
        return 2
    report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))

    analyzed = report["total_jobs_analyzed"]
    failed = report["total_jobs_failed"]
    requested = report["total_jobs_requested"]
    print(f"batch {report['batch_id']}: {requested} requested, {analyzed} analyzed, {failed} failed")
    for job in report.get("failed_analyses", []):
        print(f"  failed job {job['job_id']} at {job['stage']}: {job['error'][:120]}")

    assert requested >= 1, "no jobs were requested"
    assert analyzed >= 1, "batch analyzed no jobs successfully"
    assert failed == 0, f"{failed} job(s) failed analysis"
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
