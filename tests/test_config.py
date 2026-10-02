"""Tests for settings loading and per-job analysis configuration."""

import json
from pathlib import Path

import pytest

from rca.config import Config, load_environment


def _config(tmp_path: Path, **environment: str) -> Config:
    return Config.from_env(
        environment={"RCA_STATE_DIR": str(tmp_path), **environment},
        env_file=tmp_path / "missing.env",
    )


def test_config_from_env_loads_vars(tmp_path: Path) -> None:
    logs_dir = tmp_path / "logs"
    config = _config(
        tmp_path,
        SPLUNK_HOST="https://splunk.test",
        SPLUNK_USERNAME="testuser",
        SPLUNK_PASSWORD="testpass",
        SPLUNK_INDEX="main",
        SPLUNK_VERIFY_SSL="true",
        SPLUNK_OCP_APP_INDEX="ocp_apps",
        SPLUNK_OCP_INFRA_INDEX="ocp_infra",
        JOB_LOGS_DIR=str(logs_dir),
    )

    assert config.splunk.host == "https://splunk.test"
    assert config.splunk.username == "testuser"
    assert config.splunk.password == "testpass"
    assert config.splunk.index == "main"
    assert config.splunk.verify_ssl is True
    assert config.splunk.ocp_app_index == "ocp_apps"
    assert config.splunk.ocp_infra_index == "ocp_infra"
    assert config.job_logs_dir == logs_dir
    assert config.analysis_dir == tmp_path / ".analysis"


def test_config_defaults(tmp_path: Path) -> None:
    config = _config(tmp_path)

    assert config.splunk.host == ""
    assert config.splunk.username == ""
    assert config.splunk.password == ""
    assert config.splunk.index is None
    assert config.splunk.verify_ssl is False
    assert config.splunk.token is None
    assert config.splunk.ocp_app_index is None
    assert config.splunk.ocp_infra_index is None
    assert config.job_logs_dir is None
    assert config.max_parallel_jobs == 5


def test_config_parses_max_parallel_jobs(tmp_path: Path) -> None:
    config = _config(tmp_path, RCA_MAX_PARALLEL_JOBS="3")

    assert config.max_parallel_jobs == 3


@pytest.mark.parametrize("value", ["0", "-1", "not-an-integer", "1.5"])
def test_config_rejects_invalid_max_parallel_jobs(tmp_path: Path, value: str) -> None:
    with pytest.raises(ValueError, match="RCA_MAX_PARALLEL_JOBS must be a positive integer"):
        _config(tmp_path, RCA_MAX_PARALLEL_JOBS=value)


def test_source_database_is_opt_in_when_host_is_not_configured(tmp_path: Path) -> None:
    config = _config(
        tmp_path,
        SOURCE_DB_NAME="rca",
        SOURCE_DB_USER="agent",
        SOURCE_DB_PASSWORD="secret",
    )

    assert config.source_db_host == ""
    assert config.has_source_db() is False


@pytest.mark.parametrize(
    ("value", "expected"), [("true", True), ("True", True), ("false", False), ("foo", False)]
)
def test_config_verify_ssl_parsing(tmp_path: Path, value: str, expected: bool) -> None:
    assert _config(tmp_path, SPLUNK_VERIFY_SSL=value).splunk.verify_ssl is expected


@pytest.mark.parametrize(
    "filename",
    [
        "job_123.json",
        "job_123.json.gz",
        "job_123.json.gz.transform-processed",
        "job_123.json.transform-processed",
    ],
)
def test_find_job_log_supported_extensions(tmp_path: Path, filename: str) -> None:
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    log_path = logs_dir / filename
    log_path.touch()

    config = _config(tmp_path, JOB_LOGS_DIR=str(logs_dir))
    assert config.find_job_log("123") == log_path


def test_find_job_log_returns_none_when_missing_or_not_a_directory(tmp_path: Path) -> None:
    config = _config(tmp_path, JOB_LOGS_DIR=str(tmp_path / "missing"))
    assert config.find_job_log("123") is None


def test_validate_splunk() -> None:
    assert _config(Path("."), SPLUNK_HOST="host", SPLUNK_USERNAME="user", SPLUNK_PASSWORD="pass").validate_splunk() == []
    assert _config(Path("."), SPLUNK_USERNAME="user", SPLUNK_PASSWORD="pass").validate_splunk() == [
        "SPLUNK_HOST is required"
    ]
    assert _config(Path("."), SPLUNK_HOST="host").validate_splunk() == [
        "SPLUNK_USERNAME/SPLUNK_PASSWORD or SPLUNK_TOKEN is required"
    ]


def test_load_settings_json_as_data_without_shell_evaluation(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(
        json.dumps({"env": {"SOURCE_DB_PASSWORD": 'dollar$quote"backtick`', "SPLUNK_VERIFY_SSL": False}}),
        encoding="utf-8",
    )

    environment = load_environment(
        environment={},
        settings_file=settings_path,
        env_file=tmp_path / "missing.env",
    )

    assert environment["SOURCE_DB_PASSWORD"] == 'dollar$quote"backtick`'
    assert environment["SPLUNK_VERIFY_SSL"] == "false"


def test_process_environment_overrides_settings_json(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(json.dumps({"env": {"SPLUNK_HOST": "https://settings"}}), encoding="utf-8")

    environment = load_environment(
        environment={"SPLUNK_HOST": "https://process"},
        settings_file=settings_path,
        env_file=tmp_path / "missing.env",
    )

    assert environment["SPLUNK_HOST"] == "https://process"
