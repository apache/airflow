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
"""Tests for the dev-server supervision in compile_ui_assets_dev.py.

The script refuses to be imported as a module, so these tests run the real
script as a subprocess against a stubbed ``pnpm`` and a stubbed
``common_prek_utils`` that points all paths into a temporary directory.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[3] / "ci" / "prek" / "compile_ui_assets_dev.py"

UI_RELATIVE_PATH = Path("airflow-core/src/airflow/ui")
SIMPLE_AUTH_MANAGER_UI_RELATIVE_PATH = Path("airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui")

PNPM_STUB = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    import signal
    import sys
    import time
    from pathlib import Path

    ORPHAN_SELF_DESTRUCT_SECONDS = 60

    if sys.argv[1] == "install":
        print("pnpm install stub")
        sys.exit(0)

    print("pnpm dev stub starting", flush=True)
    Path("dev_server.pid").write_text("started")

    def handle_sigterm(signum, frame):
        Path("terminated.txt").write_text("terminated")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

    behavior_file = Path("dev_behavior.txt")
    behavior = behavior_file.read_text().strip() if behavior_file.exists() else "sleep"
    if behavior.startswith("exit:"):
        deadline = time.monotonic() + 30
        while not Path("exit_now.txt").exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        sys.exit(int(behavior.removeprefix("exit:")))
    time.sleep(ORPHAN_SELF_DESTRUCT_SECONDS)
    """
)


def wait_for_file(path: Path, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() > deadline:
            raise AssertionError(f"{path} did not appear within {timeout} seconds")
        time.sleep(0.1)


class DevScriptHarness:
    def __init__(self, tmp_path: Path):
        airflow_root = tmp_path / "airflow_root"
        self.ui_directory = airflow_root / UI_RELATIVE_PATH
        self.simple_auth_manager_ui_directory = airflow_root / SIMPLE_AUTH_MANAGER_UI_RELATIVE_PATH
        self.ui_directory.mkdir(parents=True)
        self.simple_auth_manager_ui_directory.mkdir(parents=True)
        self.ui_out_file = airflow_root / ".build" / "ui" / "asset_compile_dev_mode.out"
        self.simple_auth_manager_ui_out_file = (
            airflow_root / ".build" / "ui" / "simple_auth_manager_asset_compile_dev_mode.out"
        )

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
        self.env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}
        self.process: subprocess.Popen | None = None

    def set_dev_behavior(self, ui_directory: Path, behavior: str) -> None:
        (ui_directory / "dev_behavior.txt").write_text(behavior)

    def run_script(self) -> subprocess.Popen:
        self.process = subprocess.Popen(
            [sys.executable, os.fspath(self.script_path)],
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        return self.process

    def wait_for_dev_servers_started(self) -> None:
        wait_for_file(self.ui_directory / "dev_server.pid")
        wait_for_file(self.simple_auth_manager_ui_directory / "dev_server.pid")

    def kill_process_group(self) -> None:
        if self.process is None:
            return
        for kill_signal in (signal.SIGTERM, signal.SIGKILL):
            if self.process.poll() is None:
                try:
                    os.killpg(self.process.pid, kill_signal)
                    self.process.wait(timeout=10)
                except (OSError, subprocess.TimeoutExpired):
                    continue


@pytest.fixture
def harness(tmp_path):
    harness = DevScriptHarness(tmp_path)
    yield harness
    harness.kill_process_group()


@pytest.mark.parametrize(
    "exiting_server_name, exit_code",
    [
        pytest.param("airflow-ui", 7, id="airflow-ui"),
        pytest.param("airflow-ui", 0, id="airflow-ui-clean-exit"),
        pytest.param("simple-auth-manager-ui", 3, id="simple-auth-manager-ui"),
    ],
)
def test_reports_dev_server_exit_and_terminates_the_other_one(harness, exiting_server_name, exit_code):
    servers = {
        "airflow-ui": (harness.ui_directory, harness.ui_out_file),
        "simple-auth-manager-ui": (
            harness.simple_auth_manager_ui_directory,
            harness.simple_auth_manager_ui_out_file,
        ),
    }
    exiting_directory, exiting_out_file = servers[exiting_server_name]
    ((surviving_directory, _),) = (server for name, server in servers.items() if name != exiting_server_name)
    harness.set_dev_behavior(exiting_directory, f"exit:{exit_code}")

    process = harness.run_script()
    harness.wait_for_dev_servers_started()
    (exiting_directory / "exit_now.txt").write_text("exit")
    _, stderr = process.communicate(timeout=90)

    assert process.returncode == 1
    assert f"The {exiting_server_name} dev server exited unexpectedly with code {exit_code}." in stderr
    assert os.fspath(exiting_out_file) in stderr
    wait_for_file(surviving_directory / "terminated.txt")


def test_keeps_running_while_dev_servers_are_alive(harness):
    process = harness.run_script()
    harness.wait_for_dev_servers_started()

    time.sleep(2.5)

    assert process.poll() is None


def test_terminates_dev_servers_on_keyboard_interrupt(harness):
    process = harness.run_script()
    harness.wait_for_dev_servers_started()
    time.sleep(0.5)

    os.kill(process.pid, signal.SIGINT)
    _, stderr = process.communicate(timeout=30)

    assert process.returncode == 130
    assert "Traceback" not in stderr
    wait_for_file(harness.ui_directory / "terminated.txt")
    wait_for_file(harness.simple_auth_manager_ui_directory / "terminated.txt")
