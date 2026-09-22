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

ROOT = Path(__file__).resolve().parents[3]


def run_shell(script, env):
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-c", script],
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
if [[ -n "${FAIL_MATCH:-}" && "${name} $*" == *"${FAIL_MATCH}"* ]]; then
    exit 42
fi
"""
    )
    command.chmod(0o755)
    for name in ("sudo", "df"):
        (tools / name).symlink_to(command)
    return {"PATH": f"{tools}:{os.environ['PATH']}", "COMMAND_LOG": str(log)}


def read_commands(env):
    return [shlex.split(line) for line in Path(env["COMMAND_LOG"]).read_text().splitlines()]


def test_parallel_cleanup_keeps_exact_targets_and_concurrency_bound(fake_tools):
    script = (ROOT / "scripts/tools/free_up_disk_space.sh").read_text()
    result = run_shell(script, fake_tools)
    assert result.returncode == 0, result.stderr
    commands = read_commands(fake_tools)
    deletes = [command for command in commands if command[:3] == ["sudo", "rm", "-rf"]]
    expected = {
        "/usr/share/dotnet/",
        "/usr/local/graalvm/",
        "/usr/local/.ghcup/",
        "/usr/local/share/powershell",
        "/usr/local/share/chromium",
        "/usr/local/share/boost",
        "/usr/local/lib/android",
        "/opt/hostedtoolcache",
        "/opt/ghc",
    }
    assert len(deletes) == len(expected)
    assert {command[-1] for command in deletes} == expected
    assert "xargs -0 -r -n 1 -P 4 sudo rm -rf --" in script
    apt_position = commands.index(["sudo", "apt-get", "clean"])
    assert all(commands.index(command) < apt_position for command in deletes)


@pytest.mark.parametrize("failure", ("/usr/local/lib/android", "sudo apt-get clean"))
def test_parallel_cleanup_propagates_errors(fake_tools, failure):
    result = run_shell(
        (ROOT / "scripts/tools/free_up_disk_space.sh").read_text(), {**fake_tools, "FAIL_MATCH": failure}
    )
    assert result.returncode != 0
