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
"""Tests for the change detection in compile_ui_assets.py.

The script refuses to be imported as a module, so these tests run the real
script as a subprocess against a stubbed ``pnpm`` and a stubbed
``common_prek_utils`` that points all paths into a temporary directory.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[3] / "ci" / "prek" / "compile_ui_assets.py"

UI_RELATIVE_PATH = Path("airflow-core/src/airflow/ui")
SIMPLE_AUTH_MANAGER_UI_RELATIVE_PATH = Path("airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui")

PNPM_STUB = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import os
    import sys
    from pathlib import Path

    with open(os.environ["PNPM_CALL_LOG"], "a") as call_log:
        call_log.write(f"{Path.cwd().name}: {' '.join(sys.argv[1:])}\\n")

    output_directory = Path("node_modules/pkg") if sys.argv[1] == "install" else Path("dist")
    output_directory.mkdir(parents=True, exist_ok=True)
    (output_directory / "index.js").write_text("")
    """
)


class ScriptHarness:
    def __init__(self, tmp_path: Path):
        airflow_root = tmp_path / "airflow_root"
        self.ui_directory = airflow_root / UI_RELATIVE_PATH
        self.simple_auth_manager_ui_directory = airflow_root / SIMPLE_AUTH_MANAGER_UI_RELATIVE_PATH
        for ui_directory in (self.ui_directory, self.simple_auth_manager_ui_directory):
            (ui_directory / "src").mkdir(parents=True)
            (ui_directory / "src" / "main.ts").write_text("export const main = 1;\n")

        script_dir = tmp_path / "prek"
        script_dir.mkdir()
        self.script_path = script_dir / SCRIPT_PATH.name
        shutil.copy(SCRIPT_PATH, self.script_path)
        (script_dir / "common_prek_utils.py").write_text(
            textwrap.dedent(
                f"""\
                from pathlib import Path

                AIRFLOW_ROOT_PATH = Path({os.fspath(airflow_root)!r})
                AIRFLOW_CORE_SOURCES_PATH = AIRFLOW_ROOT_PATH / "airflow-core" / "src"
                """
            )
        )

        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        pnpm_stub = bin_dir / "pnpm"
        pnpm_stub.write_text(PNPM_STUB)
        pnpm_stub.chmod(0o755)
        self.call_log = tmp_path / "pnpm_calls.txt"
        self.env = {
            **os.environ,
            "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
            "PNPM_CALL_LOG": os.fspath(self.call_log),
        }

    def run_script(self) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, os.fspath(self.script_path)],
            env=self.env,
            capture_output=True,
            text=True,
            check=True,
        )

    def read_pnpm_calls(self) -> list[str]:
        return self.call_log.read_text().splitlines() if self.call_log.exists() else []


@pytest.fixture
def harness(tmp_path):
    return ScriptHarness(tmp_path)


@pytest.mark.parametrize(
    "changed_file",
    [
        pytest.param(Path(".pnpm-store/v10/files/00/abc"), id="pnpm-store"),
        pytest.param(Path("node_modules/pkg/index.js"), id="node-modules"),
    ],
)
def test_ignores_dependency_store_changes(harness, changed_file):
    harness.run_script()
    calls_after_first_build = harness.read_pnpm_calls()

    for ui_directory in (harness.ui_directory, harness.simple_auth_manager_ui_directory):
        (ui_directory / changed_file).parent.mkdir(parents=True, exist_ok=True)
        (ui_directory / changed_file).write_text("changed")
    result = harness.run_script()

    assert harness.read_pnpm_calls() == calls_after_first_build
    assert result.stdout.count("has not changed! Skip regeneration.") == 2


def test_rebuilds_when_sources_change(harness):
    harness.run_script()
    calls_after_first_build = harness.read_pnpm_calls()

    (harness.ui_directory / "src" / "main.ts").write_text("export const main = 2;\n")
    harness.run_script()

    assert harness.read_pnpm_calls() == [
        *calls_after_first_build,
        "ui: install --frozen-lockfile --config.confirmModulesPurge=false",
        "ui: run build",
    ]
