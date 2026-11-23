# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from pathlib import Path
from zipfile import ZipFile

import pytest
from click.testing import CliRunner

from airflow_breeze.commands import testing_commands
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH


@pytest.fixture
def strict_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, str, Path]:
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is required for the workspace compatibility test")

    monkeypatch.setenv("UV_OFFLINE", "1")
    monkeypatch.setenv("UV_PYTHON_DOWNLOADS", "never")
    monkeypatch.setenv("UV_CACHE_DIR", str(tmp_path / "cache"))
    for variable in ("UV_NO_SYNC", "UV_LOCKED", "UV_FROZEN", "UV_PYTHON", "VIRTUAL_ENV"):
        monkeypatch.delenv(variable, raising=False)

    interpreters = []
    for request in ("3.14", ">=3.11,<3.14"):
        result = subprocess.run(
            [uv, "python", "find", "--no-project", request], capture_output=True, text=True, check=False
        )
        if result.returncode != 0:
            pytest.skip(f"Install Python {request} to exercise workspace interpreter selection")
        interpreters.append(Path(result.stdout.strip()).resolve())
    target_python, resolver_python = interpreters

    wheel_path = tmp_path / "pytest-1.0-py3-none-any.whl"
    with ZipFile(wheel_path, "w") as wheel:
        wheel.writestr("pytest.py", "import sys\nprint(f'pytest fixture on {sys.version_info[:2]}')\n")
        wheel.writestr("pytest-1.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: pytest\nVersion: 1.0\n")
        wheel.writestr(
            "pytest-1.0.dist-info/WHEEL", "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n"
        )
        wheel.writestr("pytest-1.0.dist-info/RECORD", "")

    workspace = tmp_path / "workspace"
    for package in ("ctl", "provider"):
        (workspace / package).mkdir(parents=True)
    (workspace / "pyproject.toml").write_text(
        '[project]\nname = "fixture-root"\nversion = "1.0"\nrequires-python = ">=3.11,!=3.15.*"\n'
        '[tool.uv.workspace]\nmembers = ["ctl", "provider"]\n[dependency-groups]\nci-image = []\n'
    )
    (workspace / "ctl" / "pyproject.toml").write_text(
        '[project]\nname = "apache-airflow-ctl"\nversion = "1.0"\nrequires-python = ">=3.11"\n'
        f'[dependency-groups]\ndev = ["pytest @ {wheel_path.as_uri()}"]\n'
    )
    (workspace / "provider" / "pyproject.toml").write_text(
        '[project]\nname = "apache-airflow-providers-fixture"\nversion = "1.0"\n'
        'requires-python = ">=3.11,!=3.14.*"\n'
    )
    subprocess.run([uv, "lock", "--python", str(resolver_python)], cwd=workspace, check=True)
    monkeypatch.setenv("UV_PYTHON", str(target_python))
    return workspace, uv, target_python


def test_airflow_ctl_tests_uses_target_in_a_strict_workspace(strict_workspace, monkeypatch, capfd):
    workspace, _, _ = strict_workspace
    monkeypatch.setattr(testing_commands, "AIRFLOW_ROOT_PATH", workspace)
    monkeypatch.setattr(testing_commands, "AIRFLOW_CTL_ROOT_PATH", workspace / "ctl")

    result = CliRunner().invoke(testing_commands.airflow_ctl_tests, ["--python", "3.14"])

    stdout, stderr = capfd.readouterr()
    assert result.exit_code == 0, (result.output, stderr)
    assert "pytest fixture on (3, 14)" in stdout


@pytest.mark.parametrize("upgrade", (False, True))
def test_filtered_image_resolves_for_a_strict_workspace(strict_workspace, tmp_path, monkeypatch, upgrade):
    workspace, uv, target_python = strict_workspace
    image_scripts = AIRFLOW_ROOT_PATH / "scripts" / "docker"
    installer = tmp_path / "install.sh"
    installer.write_text(
        (image_scripts / "install_airflow_when_building_images.sh")
        .read_text()
        .split("\ncommon::get_colors\n", 1)[0]
    )
    shutil.copy(image_scripts / "filter_ci_install_set.py", tmp_path)
    (tmp_path / "common.sh").write_text("set -euo pipefail\n")

    # Run the real lock and export, then observe the target passed to each installation command.
    (tmp_path / "python3").symlink_to(target_python)
    uv_wrapper = tmp_path / "uv"
    uv_wrapper.write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$1" == "pip" ]]; then printf "%s\\n" "$*"; exit 0; fi\n'
        f'exec {shlex.quote(uv)} "$@"\n'
    )
    uv_wrapper.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("UPGRADE_RANDOM_INDICATOR_STRING", "upgrade" if upgrade else "")
    monkeypatch.setenv("PACKAGING_TOOL_CMD", "uv")
    monkeypatch.setenv("AIRFLOW_FALLBACK_NO_CONSTRAINTS_INSTALLATION", "false")
    for color in ("BLUE", "RED", "RESET", "YELLOW"):
        monkeypatch.setenv(f"COLOR_{color}", "")

    result = subprocess.run(
        ["bash", "-c", 'source "$1"; install_from_sources', "bash", str(installer)],
        cwd=workspace,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    installs = [line for line in result.stdout.splitlines() if line.startswith("pip ")]
    assert len(installs) == 3
    assert all(f"--python {tmp_path / 'python3'}" in line for line in installs)
    assert not any("/provider" in line for line in installs)
