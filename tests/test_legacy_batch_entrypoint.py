"""The legacy shell entry point forwards to the Python batch command."""

import os
import subprocess
from pathlib import Path


def test_legacy_batch_script_forwards_args_and_exit_status(tmp_path: Path) -> None:
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    args_file = tmp_path / "args.txt"
    fake_runner = binary_dir / "rca-batch"
    fake_runner.write_text(
        "#!/bin/bash\n"
        'printf "%s\\n" "$@" > "$RCA_TEST_ARGS_FILE"\n'
        "exit 7\n",
        encoding="utf-8",
    )
    fake_runner.chmod(0o755)

    script = (
        Path(__file__).parents[1]
        / "deploy"
        / "batch-rca-automation"
        / "batch_rca_headless.sh"
    )
    environment = {
        **os.environ,
        "PATH": f"{binary_dir}{os.pathsep}{os.environ.get('PATH', '')}",
        "RCA_TEST_ARGS_FILE": str(args_file),
    }

    result = subprocess.run(
        ["bash", str(script), "--limit", "5", "--no-pre-filter"],
        check=False,
        env=environment,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 7
    assert args_file.read_text(encoding="utf-8").splitlines() == [
        "--limit",
        "5",
        "--no-pre-filter",
    ]
