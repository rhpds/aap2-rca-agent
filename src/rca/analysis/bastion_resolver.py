"""Resolve per-job bastion SSH targets using the shared DB and SSH helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from rca.database import lookup_job_bastion_row, pooled_connection
from rca.ssh import (
    ensure_bastion_host as ensure_ssh_bastion_host,
    ensure_jumpbox_alias as ensure_ssh_jumpbox_alias,
    parse_jumpbox_uri,
    ssh_host_exists,
)

if TYPE_CHECKING:
    from rca.config import Config


@dataclass(frozen=True)
class BastionTarget:
    """SSH target for fetching a job log from a cluster bastion."""

    remote_host: str
    remote_log_dir: str | None = None
    cluster_name: str | None = None
    bastion_hostname: str | None = None
    bastion_ssh_port: int | None = None


def bastion_alias(cluster_name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9-]", "-", cluster_name.lower()).strip("-")
    return f"bastion-{sanitized}"


def resolve_bastion_user(config: Config) -> str:
    if config.bastion_ssh_user:
        return config.bastion_ssh_user
    if config.jumpbox_uri:
        user, _, _ = parse_jumpbox_uri(config.jumpbox_uri)
        return user
    raise ValueError("BASTION_SSH_USER or JUMPBOX_URI is required for bastion SSH user")


def ensure_jumpbox_alias(config: Config, identity_file: str | None = None) -> None:
    ensure_ssh_jumpbox_alias(
        config.ssh_jumpbox_alias,
        config.jumpbox_uri,
        identity_file=identity_file,
    )


def ensure_bastion_host(
    target: BastionTarget,
    config: Config,
    identity_file: str | None = None,
) -> None:
    if not target.cluster_name or not target.bastion_hostname or target.bastion_ssh_port is None:
        raise ValueError("Bastion target is missing cluster_name, bastion_hostname, or port")

    ensure_ssh_bastion_host(
        target.remote_host,
        target.bastion_hostname,
        target.bastion_ssh_port,
        resolve_bastion_user(config),
        config.ssh_jumpbox_alias,
        identity_file=identity_file,
    )


def lookup_job_bastion(
    config: Config, job_id: str, *, conn: object | None = None
) -> BastionTarget | None:
    """Look up cluster and bastion mapping for a job from the source database."""
    if conn is None and not config.has_source_db():
        return None

    db_config = {
        "host": config.source_db_host,
        "port": config.source_db_port,
        "name": config.source_db_name,
        "user": config.source_db_user,
        "password": config.source_db_password,
        "source_table": config.source_db_table,
        "bastion_table": config.source_db_bastion_table,
    }
    if conn is None:
        row = lookup_job_bastion_row(db_config, job_id)
    else:
        row = lookup_job_bastion_row(db_config, job_id, conn=conn)
    if not row:
        return None

    cluster_name = row.get("cluster_name")
    bastion_hostname = row.get("bastion_hostname")
    bastion_ssh_port = row.get("bastion_ssh_port")
    instance_base_path = (row.get("instance_base_path") or "").strip() or None
    if not cluster_name or not bastion_hostname or bastion_ssh_port is None:
        return None

    return BastionTarget(
        remote_host=bastion_alias(cluster_name),
        remote_log_dir=instance_base_path,
        cluster_name=cluster_name,
        bastion_hostname=bastion_hostname,
        bastion_ssh_port=int(bastion_ssh_port),
    )


def resolve_bastion_for_job(
    config: Config, job_id: str, *, db_pool: object | None = None
) -> BastionTarget:
    """Resolve the SSH host alias to use when fetching a job log."""
    if db_pool is None:
        target = lookup_job_bastion(config, job_id)
    else:
        with pooled_connection(db_pool) as conn:
            target = lookup_job_bastion(config, job_id, conn=conn)
    if target:
        return target

    if config.remote_host and config.remote_log_dir:
        return BastionTarget(
            remote_host=config.remote_host,
            remote_log_dir=config.remote_log_dir,
        )

    raise ValueError(
        "Cannot resolve bastion for job fetch. Configure SOURCE_DB_* + JUMPBOX_URI for "
        "per-cluster bastions, or REMOTE_HOST + REMOTE_DIR for a single log server."
    )


def resolve_remote_log_dir(target: BastionTarget, config: Config) -> str:
    """Resolve the remote directory for fetching job logs."""
    if (target.remote_log_dir or "").strip():
        # instance_base_path is the ETL instance root; job logs live in its extract/ subdir
        return target.remote_log_dir.rstrip("/") + "/extract"
    remote_dir = config.remote_log_dir
    if not remote_dir:
        raise ValueError(
            "No remote log directory resolved. Populate instance_base_path in "
            f"{config.source_db_bastion_table} or set REMOTE_DIR."
        )
    return remote_dir


def prepare_bastion_for_fetch(config: Config, target: BastionTarget) -> None:
    """Ensure SSH config entries exist for the resolved bastion target."""
    if target.bastion_hostname and target.bastion_ssh_port is not None:
        ensure_jumpbox_alias(config)
        ensure_bastion_host(target, config)
        return

    if not ssh_host_exists(target.remote_host):
        raise ValueError(f"REMOTE_HOST alias '{target.remote_host}' not found in SSH config")
