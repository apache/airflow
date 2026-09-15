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
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
UI_WORKFLOW = ".github/workflows/ui-e2e-tests.yml"


def load_yaml(path):
    return yaml.load((ROOT / path).read_text(), Loader=yaml.BaseLoader)


def find_step(path, *, name, job):
    steps = load_yaml(path)["jobs"][job]["steps"]
    return next(step for step in steps if step.get("name") == name)


def run_shell(script, env):
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", script],
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


@pytest.fixture
def fake_tools(tmp_path):
    tools = tmp_path / "bin"
    tools.mkdir()
    log = tmp_path / "commands.log"
    command = tools / "command"
    command.write_text(
        r"""#!/usr/bin/env bash
name="${0##*/}"
printf '%s\n' "${name} $*" >> "${COMMAND_LOG}"
"""
    )
    command.chmod(0o755)
    (tools / "pnpm").symlink_to(command)
    return {"PATH": f"{tools}:{os.environ['PATH']}", "COMMAND_LOG": str(log)}


def read_commands(env):
    return [shlex.split(line) for line in Path(env["COMMAND_LOG"]).read_text().splitlines()]


@pytest.mark.parametrize(
    "browser,expected",
    [("all", []), ("chromium", ["chromium"]), ("firefox", ["firefox"]), ("webkit", ["webkit"])],
)
def test_browser_install_preserves_requested_engine(fake_tools, browser, expected):
    step = find_step(
        UI_WORKFLOW,
        name="Install selected Playwright browser and dependencies",
        job="test-ui-e2e-tests",
    )
    result = run_shell(step["run"], {**fake_tools, "BROWSER": browser})
    assert result.returncode == 0, result.stderr
    assert read_commands(fake_tools) == [["pnpm", "exec", "playwright", "install", "--with-deps", *expected]]


@pytest.mark.parametrize("browser", ("", "chrome", "chromium firefox", "$(echo chromium)", "--help"))
def test_browser_install_rejects_unknown_values(fake_tools, browser):
    step = find_step(
        UI_WORKFLOW,
        name="Install selected Playwright browser and dependencies",
        job="test-ui-e2e-tests",
    )
    result = run_shell(step["run"], {**fake_tools, "BROWSER": browser})
    assert result.returncode != 0
    assert not Path(fake_tools["COMMAND_LOG"]).exists()


def test_ui_jobs_keep_frozen_installs_and_tests_without_duplicate_stashes():
    steps = load_yaml(".github/workflows/basic-tests.yml")["jobs"]["tests-ui"]["steps"]
    commands = [step.get("run", "") for step in steps]
    for directory in (
        "airflow-core/src/airflow/ui",
        "airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui",
    ):
        assert f"cd {directory} && pnpm install --frozen-lockfile" in commands
        assert f"cd {directory} && pnpm test" in commands
    assert not any("stash/" in step.get("uses", "") for step in steps)
    setup = next(step for step in steps if step.get("uses", "").startswith("actions/setup-node@"))
    assert setup["with"]["cache"] == "pnpm"
    assert len(setup["with"]["cache-dependency-path"].splitlines()) == 2


def test_translation_check_remains_independent_of_ui_selection():
    job = load_yaml(".github/workflows/basic-tests.yml")["jobs"]["check-translation-completness"]
    assert "if" not in job and "needs" not in job
    assert job["name"] == "Check translation completeness"
    assert job["steps"][-1]["run"] == "breeze ui check-translation-completeness || true"
