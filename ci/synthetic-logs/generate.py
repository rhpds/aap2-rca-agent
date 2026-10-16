#!/usr/bin/env python3
"""Generate synthetic AAP job logs for batch-analysis testing.

Produces job_{id}.json.gz files in this directory, matching the ETL-extracted
document shape that rca.analysis.job_parser.parse_job_log expects.

The five jobs are designed around the batch's similarity thresholds:
  900001/900002 - same catalog item, near-identical errors  -> dedup/pre-filter cluster
  900003        - same catalog item, different error        -> below 0.75 threshold
  900004        - other catalog item, error ~= 900003       -> cross-catalog (>= 0.90)
  900005        - other catalog item, unique error          -> standalone analysis
"""

from __future__ import annotations

import gzip
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent
RUN_DATE = datetime(2026, 10, 16, 14, 0, 0, tzinfo=timezone.utc)

JOBS = [
    {
        "job_id": 900001,
        "job_name": "RHPDS zt-hp.zt-ocp4-get-started-cluster.dev-syn01-1-provision",
        "guid": "syn01-1",
        "cluster": "synthetic-cluster-a",
        "template_id": 203390,
        "duration": 1570,
        "failed_task": {
            "task": "Run security validation",
            "play": "Step 004 Software",
            "role": "ocp_workload_security_validation",
            "task_action": "rhpds.security.validate_tenant_security",
            "msg": (
                "Task failed: Finalization of task args for 'rhpds.security.validate_tenant_security' "
                "failed: 'cluster_admin_agnosticd_sa_token' is undefined"
            ),
        },
    },
    {
        "job_id": 900002,
        "job_name": "RHPDS zt-hp.zt-ocp4-get-started-cluster.dev-syn02-1-provision",
        "guid": "syn02-1",
        "cluster": "synthetic-cluster-a",
        "template_id": 203390,
        "duration": 1521,
        "failed_task": {
            "task": "Run security validation",
            "play": "Step 004 Software",
            "role": "ocp_workload_security_validation",
            "task_action": "rhpds.security.validate_tenant_security",
            "msg": (
                "Task failed: Finalization of task args for 'rhpds.security.validate_tenant_security' "
                "failed: 'cluster_admin_agnosticd_sa_token' is undefined"
            ),
        },
    },
    {
        "job_id": 900003,
        "job_name": "RHPDS zt-hp.zt-ocp4-get-started-cluster.dev-syn03-1-provision",
        "guid": "syn03-1",
        "cluster": "synthetic-cluster-a",
        "template_id": 203390,
        "duration": 2311,
        "failed_task": {
            "task": "Wait for router rollout to complete (IngressController available)",
            "play": "Step 004 Software",
            "role": "ocp4_workload_ingress",
            "task_action": "kubernetes.core.k8s_info",
            "msg": (
                "Platform error: the IngressController operator reported Degraded=True - "
                "ingress controller router-default failed to rollout within the timeout window"
            ),
        },
    },
    {
        "job_id": 900004,
        "job_name": "RHPDS zt-hp.zt-ocp4-ansible-automation.dev-syn04-2-provision",
        "guid": "syn04-2",
        "cluster": "synthetic-cluster-b",
        "template_id": 204001,
        "duration": 2087,
        "failed_task": {
            "task": "Wait for ingress to become available",
            "play": "Step 005 Platform",
            "role": "ocp4_workload_ingress",
            "task_action": "kubernetes.core.k8s_info",
            "msg": (
                "Platform error: the IngressController operator reported Degraded=True - "
                "ingress controller router-default failed to rollout within the timeout window"
            ),
        },
    },
    {
        "job_id": 900005,
        "job_name": "RHPDS zt-aws.zt-ocp4-virtualization-cluster.dev-syn05-3-provision",
        "guid": "syn05-3",
        "cluster": "synthetic-cluster-c",
        "template_id": 203410,
        "duration": 344,
        "failed_task": {
            "task": "Create worker node instance",
            "play": "Step 002 Infrastructure",
            "role": "ocp4_workload_aws_nodes",
            "task_action": "amazon.aws.ec2_instance",
            "msg": (
                "Infrastructure error: AWSServiceError - Instance limit exceeded for instance "
                "type m5.xlarge in availability zone us-east-1a (quota: Running On-Demand m5 instances)"
            ),
        },
    },
]

PLAY_NAMES = [
    "Step 0000 Install Galaxy roles and collections",
    "Step 001 Pre Infrastructure",
    "Step 002 Infrastructure",
    "Step 003 Pre Software",
    "Step 004 Software",
    "Step 005 Platform",
]
ROLE_NAMES = [
    "ocp4_workload_gitea_operator",
    "ocp_workload_security_validation",
    "ocp4_workload_ingress",
    "ocp4_workload_aws_nodes",
    "ocp4_workload_monitoring",
]


def _event(idx, kind, created, **extra):
    event = {
        "event_id": 1000 + idx,
        "event": kind,
        "created": created.isoformat(),
        "counter": idx,
        "stdout": "",
        "verbosity": 0,
    }
    event.update(extra)
    return event


def build_document(spec, job_index):
    job_id = spec["job_id"]
    started = RUN_DATE - timedelta(minutes=30 + job_index * 7, seconds=job_index * 13)
    finished = started + timedelta(seconds=spec["duration"])
    namespace = f"sandbox-{spec['guid']}-ocp4"
    ft = spec["failed_task"]

    events = []
    idx = 0
    events.append(_event(idx, "playbook_on_start", started, event_data={"playbook": "./ansible/site.yml"}))
    idx += 1
    for play in PLAY_NAMES:
        events.append(
            _event(idx, "playbook_on_play_start", started + timedelta(seconds=idx), play=play, event_data={"name": play})
        )
        idx += 1
    # Namespace mention in verbose output
    events.append(
        _event(
            idx,
            "verbose",
            started + timedelta(seconds=20),
            stdout=f"TASK [ocp4_workload_namespace : Create project] namespace: {namespace} created",
        )
    )
    idx += 1
    # A couple of pod references
    events.append(
        _event(
            idx,
            "runner_on_ok",
            started + timedelta(seconds=60),
            task="Wait for pods",
            stdout=f"pod/router-default-7d9c8b6f5-x2k9l is Running",
        )
    )
    idx += 1
    events.append(
        _event(
            idx,
            "runner_on_ok",
            started + timedelta(seconds=90),
            task="Wait for pods",
            stdout=f"pod/coordinator-5f7b8c9d1-mn3pq is Running",
        )
    )
    idx += 1
    # Some ordinary successful tasks
    for n in range(12):
        events.append(
            _event(
                idx,
                "runner_on_ok",
                started + timedelta(seconds=120 + n * 30),
                task=f"Routine setup task {n}",
                play=ft["play"],
                role=ROLE_NAMES[n % len(ROLE_NAMES)],
                event_data={"res": {"changed": True, "msg": "ok"}},
            )
        )
        idx += 1
    # Retries on the failing task
    for n in range(3):
        events.append(
            _event(
                idx,
                "runner_retry",
                started + timedelta(seconds=300 + n * 60),
                task=ft["task"],
                play=ft["play"],
                role=ft["role"],
                event_data={"res": {"msg": "Retrying task, attempt " + str(n + 1), "attempts": n + 1}},
            )
        )
        idx += 1
    # The terminal failure
    events.append(
        _event(
            idx,
            "runner_on_failed",
            finished - timedelta(seconds=5),
            failed=True,
            task=ft["task"],
            play=ft["play"],
            role=ft["role"],
            created_iso=finished.isoformat(),
            event_data={
                "res": {"msg": ft["msg"], "changed": False},
                "task_path": f"/runner/requirements_collections/ansible_collections/rhpds/security/{ft['task_action']}.yml",
                "task_action": ft["task_action"],
                "duration": 0.02,
            },
        )
    )
    idx += 1

    event_types: dict[str, int] = {}
    for e in events:
        event_types[e["event"]] = event_types.get(e["event"], 0) + 1

    document = {
        "job_id": job_id,
        "extraction_timestamp": (finished + timedelta(seconds=92)).isoformat(),
        "extractor_version": "1.0.0",
        "data_sources": {
            "metadata_available": True,
            "events_available": True,
            "retry_analysis_available": True,
        },
        "metadata": {
            "extraction_metadata": {
                "extraction_timestamp": (finished + timedelta(seconds=90)).isoformat(),
                "extractor_version": "1.0",
            },
            "job_metadata": {
                "job_id": job_id,
                "job_name": spec["job_name"],
                "status": "failed",
                "created": started.isoformat(),
                "started": started.isoformat(),
                "finished": finished.isoformat(),
                "duration_seconds": spec["duration"],
                "elapsed": float(spec["duration"]) + 0.2,
                "launch_type": "scheduled",
                "failed": True,
                "job_explanation": "",
                "ansible_version": "",
                "host_status_counts": {"changed": 13, "ok": 15},
                "cloud_provider": "aws",
                "guid": spec["guid"],
                "sandbox_openshift_cluster": spec["cluster"],
                "env_type": "sandbox",
                "action": "provision",
            },
            "template_metadata": {
                "template_id": spec["template_id"],
                "template_name": spec["job_name"],
                "template_description": "",
                "organization_id": 2,
            },
            "git_context": {
                "scm_url": None,
                "scm_branch": None,
                "scm_revision": None,
                "scm_type": None,
                "execution_environment_image": None,
                "scm_ref": None,
            },
        },
        "events_summary": {
            "total_events": len(events),
            "failed_events": 1,
            "changed_events": 13,
            "event_types": event_types,
        },
        "events": events,
        "retry_analysis": {
            "job_analysis": {
                "total_plays": len(PLAY_NAMES),
                "total_tasks": 18,
                "tasks_with_retries": 1,
                "total_retry_attempts": 3,
                "retry_success_rate": 0.0,
                "most_retried_task": {"name": ft["task"], "host": None, "retry_count": 3},
                "analysis_timestamp": (finished + timedelta(seconds=91)).isoformat(),
            },
            "plays": [
                {
                    "play_name": play,
                    "play_uuid": f"syn-{job_id}-{n:012d}",
                    "start_time": started.isoformat(),
                    "duration_seconds": spec["duration"] / len(PLAY_NAMES),
                    "task_count": 3,
                    "host_count": 1,
                    "retry_count": 3 if play == ft["play"] else 0,
                    "failed_task_count": 1 if play == ft["play"] else 0,
                    "changed_task_count": 2,
                }
                for n, play in enumerate(PLAY_NAMES)
            ],
            "task_executions": [
                {
                    "task_name": ft["task"],
                    "host_name": None,
                    "play_name": ft["play"],
                    "retry_count": 3,
                    "total_attempts": 4,
                    "final_outcome": "runner_on_failed",
                    "duration_seconds": spec["duration"] / 4,
                    "error_patterns": [ft["msg"][:60]],
                    "first_attempt": (started + timedelta(seconds=300)).isoformat(),
                    "final_completion": finished.isoformat(),
                }
            ],
            "error_patterns": [{"pattern": ft["msg"].split(":", 1)[0], "frequency": 1}],
        },
        "extraction_metadata": {
            "scratch_directory": f"/var/spool/aap2-etl/synthetic/scratch/extract/job_{job_id}",
            "found_files": ["metadata.json", "events.jsonl", "retries.json"],
            "missing_files": [],
            "cluster_id": spec["cluster"],
            "extraction_mode": "all",
        },
    }
    return document


def main() -> None:
    for index, spec in enumerate(JOBS):
        document = build_document(spec, index)
        path = OUT_DIR / f"job_{spec['job_id']}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            json.dump(document, stream, ensure_ascii=False)
            stream.write("\n")
        print(f"wrote {path.name} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
