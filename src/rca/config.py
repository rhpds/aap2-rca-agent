"""Application configuration loaded safely from settings.json, .env, and env vars."""

from __future__ import annotations

import json
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

DATABASE_ENV_KEYS = {
    "host": ("SOURCE_DB_HOST", "localhost"),
    "port": ("SOURCE_DB_PORT", "5432"),
    "name": ("SOURCE_DB_NAME", ""),
    "user": ("SOURCE_DB_USER", ""),
    "password": ("SOURCE_DB_PASSWORD", ""),
    "source_table": ("SOURCE_DB_TABLE", ""),
    "results_table": ("SOURCE_DB_RESULT_TABLE", ""),
    "bastion_table": ("SOURCE_DB_BASTION_TABLE", ""),
}

_ENV_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_MAX_PARALLEL_JOBS = 5


def _positive_int_environment_value(env: Mapping[str, str], key: str, default: int) -> int:
    value = env.get(key, str(default))
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be a positive integer") from exc
    if parsed < 1:
        raise ValueError(f"{key} must be a positive integer")
    return parsed


def _settings_path(
    environment: Mapping[str, str],
    settings_file: str | Path | None,
    *,
    discover: bool,
) -> Path | None:
    if settings_file is not None:
        return Path(settings_file).expanduser()

    configured = environment.get("RCA_SETTINGS_FILE")
    if configured:
        return Path(configured).expanduser()
    if not discover:
        return None

    candidates = (
        Path.cwd() / ".claude" / "settings.json",
        Path.cwd() / ".claude" / "settings.local.json",
        Path.home() / ".claude" / "settings.json",
    )
    return next((path for path in candidates if path.is_file()), None)


def _string_environment(values: Mapping[str, Any], *, source: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in values.items():
        if not isinstance(key, str) or not _ENV_KEY.fullmatch(key):
            raise ValueError(f"Invalid environment variable name in {source}: {key!r}")
        if value is None:
            continue
        if not isinstance(value, (str, int, float, bool)):
            raise ValueError(f"Environment value for {key} in {source} must be a scalar")
        if isinstance(value, bool):
            result[key] = "true" if value else "false"
        else:
            result[key] = str(value)
    return result


def load_environment(
    *,
    environment: Mapping[str, str] | None = None,
    settings_file: str | Path | None = None,
    env_file: str | Path | None = None,
) -> dict[str, str]:
    """Merge ``.env`` < settings ``env`` < process environment without shell evaluation.

    The settings file contributes data only. Its values are never evaluated or
    exported into the parent process environment.

    Implicit discovery (``./.env``, ``./.claude/settings.json``,
    ``~/.claude/settings.json``) only applies when reading the real process
    environment. An explicit ``environment`` mapping is hermetic: only an
    explicit ``settings_file``/``env_file`` or ``RCA_SETTINGS_FILE``/``RCA_ENV_FILE``
    inside that mapping is honoured.
    """
    discover = environment is None
    process_environment = dict(os.environ if environment is None else environment)
    dotenv_candidate = env_file or process_environment.get("RCA_ENV_FILE")
    if dotenv_candidate is None and discover:
        dotenv_candidate = Path.cwd() / ".env"
    merged: dict[str, str] = {}
    if dotenv_candidate is not None:
        dotenv_path = Path(dotenv_candidate).expanduser()
        dotenv_data = dotenv_values(dotenv_path) if dotenv_path.is_file() else {}
        merged = _string_environment(
            {key: value for key, value in dotenv_data.items() if value is not None},
            source=str(dotenv_path),
        )

    # The explicit caller path takes precedence over RCA_SETTINGS_FILE.
    settings_path = _settings_path(process_environment, settings_file, discover=discover)
    if settings_path is not None:
        if not settings_path.is_file():
            raise FileNotFoundError(f"RCA settings file not found: {settings_path}")
        try:
            with settings_path.open(encoding="utf-8") as stream:
                settings = json.load(stream)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in settings file {settings_path}: {exc}") from exc
        settings_env = settings.get("env", {})
        if not isinstance(settings_env, dict):
            raise ValueError(f"The 'env' value in {settings_path} must be a JSON object")
        merged.update(_string_environment(settings_env, source=str(settings_path)))

    merged.update(_string_environment(process_environment, source="process environment"))
    return merged


def load_database_config(
    required: tuple[str, ...] = (),
    *,
    defaults: Mapping[str, Any] | None = None,
    env: Mapping[str, str] | None = None,
    env_file: str | Path | None = None,
) -> dict[str, Any]:
    """Read normalized ``SOURCE_DB_*`` settings and validate required fields."""
    environment = (
        dict(env)
        if env is not None
        else load_environment(env_file=env_file)
    )
    effective_defaults = {key: value for key, (_, value) in DATABASE_ENV_KEYS.items()}
    if defaults:
        unknown_keys = defaults.keys() - DATABASE_ENV_KEYS.keys()
        if unknown_keys:
            raise KeyError(f"Unknown database configuration key(s): {', '.join(sorted(unknown_keys))}")
        effective_defaults.update(defaults)

    config: dict[str, Any] = {}
    for key, (env_var, _) in DATABASE_ENV_KEYS.items():
        value = environment.get(env_var, effective_defaults[key])
        try:
            config[key] = int(value) if key == "port" else value
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{env_var} must be an integer") from exc

    unknown_required = set(required) - DATABASE_ENV_KEYS.keys()
    if unknown_required:
        raise KeyError(
            f"Unknown required database configuration key(s): {', '.join(sorted(unknown_required))}"
        )
    errors = [f"{DATABASE_ENV_KEYS[key][0]} is required" for key in required if not config[key]]
    if errors:
        print("\n".join(errors), file=sys.stderr)
        raise SystemExit(1)
    return config


def _none_if_empty(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


@dataclass
class SplunkConfig:
    host: str
    username: str
    password: str
    index: str | None = None
    verify_ssl: bool = True
    token: str | None = None
    ocp_app_index: str | None = None
    ocp_infra_index: str | None = None

    @property
    def auth_method(self) -> str:
        if self.username and self.password:
            return "basic"
        if self.token:
            return "token"
        return "none"


@dataclass(frozen=True)
class Config:
    """Immutable runtime configuration shared by analysis and batch code."""

    environment: dict[str, str]
    database: dict[str, Any]
    splunk: SplunkConfig
    state_dir: Path
    job_logs_dir: Path | None
    github_token: str | None
    remote_host: str
    remote_log_dir: str
    jumpbox_uri: str
    ssh_jumpbox_alias: str
    bastion_ssh_user: str
    max_parallel_jobs: int

    @classmethod
    def from_env(
        cls,
        *,
        environment: Mapping[str, str] | None = None,
        settings_file: str | Path | None = None,
        env_file: str | Path | None = None,
    ) -> Config:
        env = load_environment(
            environment=environment,
            settings_file=settings_file,
            env_file=env_file,
        )
        database = load_database_config(
            defaults={
                # Bastion lookup is opt-in; an omitted host must not enable a
                # connection to localhost when the other DB credentials exist.
                "host": "",
                "source_table": "aap2_events",
                "bastion_table": "aap2_user_url",
            },
            env=env,
        )

        splunk = SplunkConfig(
            host=env.get("SPLUNK_HOST", ""),
            username=env.get("SPLUNK_USERNAME", ""),
            password=env.get("SPLUNK_PASSWORD", ""),
            index=_none_if_empty(env.get("SPLUNK_INDEX")),
            verify_ssl=env.get("SPLUNK_VERIFY_SSL", "false").lower() == "true",
            token=_none_if_empty(env.get("SPLUNK_TOKEN")),
            ocp_app_index=_none_if_empty(env.get("SPLUNK_OCP_APP_INDEX")),
            ocp_infra_index=_none_if_empty(env.get("SPLUNK_OCP_INFRA_INDEX")),
        )

        job_logs_dir = _none_if_empty(env.get("JOB_LOGS_DIR"))
        state_dir = Path(env.get("RCA_STATE_DIR", str(Path.home() / ".rca"))).expanduser()
        max_parallel_jobs = _positive_int_environment_value(
            env, "RCA_MAX_PARALLEL_JOBS", DEFAULT_MAX_PARALLEL_JOBS
        )
        return cls(
            environment=env,
            database=database,
            splunk=splunk,
            state_dir=state_dir,
            job_logs_dir=Path(job_logs_dir).expanduser() if job_logs_dir else None,
            github_token=_none_if_empty(env.get("GITHUB_TOKEN")),
            remote_host=env.get("REMOTE_HOST", ""),
            remote_log_dir=env.get("REMOTE_DIR", ""),
            jumpbox_uri=env.get("JUMPBOX_URI", ""),
            ssh_jumpbox_alias=env.get("SSH_JUMPBOX_ALIAS", "rca-jumpbox"),
            bastion_ssh_user=env.get("BASTION_SSH_USER", ""),
            max_parallel_jobs=max_parallel_jobs,
        )

    @property
    def analysis_dir(self) -> Path:
        return self.state_dir / ".analysis"

    @property
    def source_db_host(self) -> str:
        return str(self.database["host"])

    @property
    def source_db_port(self) -> int:
        return int(self.database["port"])

    @property
    def source_db_name(self) -> str:
        return str(self.database["name"])

    @property
    def source_db_user(self) -> str:
        return str(self.database["user"])

    @property
    def source_db_password(self) -> str:
        return str(self.database["password"])

    @property
    def source_db_table(self) -> str:
        return str(self.database["source_table"])

    @property
    def source_db_bastion_table(self) -> str:
        return str(self.database["bastion_table"])

    def find_job_log(self, job_id: str) -> Path | None:
        if not self.job_logs_dir or not self.job_logs_dir.is_dir():
            return None
        patterns = (
            f"job_{job_id}.json",
            f"job_{job_id}.json.gz",
            f"job_{job_id}.json.gz.transform-processed",
            f"job_{job_id}.json.transform-processed",
        )
        for pattern in patterns:
            path = self.job_logs_dir / pattern
            if path.is_file():
                return path
        return next(iter(sorted(self.job_logs_dir.glob(f"job_{job_id}.*"))), None)

    def validate_splunk(self) -> list[str]:
        errors = []
        if not self.splunk.host:
            errors.append("SPLUNK_HOST is required")
        if self.splunk.auth_method == "none":
            errors.append("SPLUNK_USERNAME/SPLUNK_PASSWORD or SPLUNK_TOKEN is required")
        return errors

    def validate_github(self) -> list[str]:
        if not self.github_token or self.github_token == "your-github-token":
            return ["GITHUB_TOKEN is required"]
        return []

    def has_source_db(self) -> bool:
        return bool(
            self.source_db_host
            and self.source_db_name
            and self.source_db_user
            and self.source_db_password
        )
