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

SCRIPT = Path(__file__).resolve().parents[2] / "tools" / "free_up_disk_space.sh"

EXPECTED_TARGETS = [
    "/usr/share/dotnet/",
    "/usr/local/graalvm/",
    "/usr/local/.ghcup/",
    "/usr/local/share/powershell",
    "/usr/local/share/chromium",
    "/usr/local/share/boost",
    "/usr/local/lib/android",
    "/opt/hostedtoolcache",
    "/opt/ghc",
]


@pytest.fixture
def fake_tools(tmp_path):
    """Shadow ``sudo`` and ``df`` with a logger that can be told to fail for a matching command."""
    tools = tmp_path / "bin"
    tools.mkdir()
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
    return {"PATH": f"{tools}:{os.environ['PATH']}", "COMMAND_LOG": str(tmp_path / "commands.log")}


def run_script(env):
    return subprocess.run(
        ["bash", "--noprofile", "--norc", str(SCRIPT)],
        env={**os.environ, **env},
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def read_commands(env):
    return [shlex.split(line) for line in Path(env["COMMAND_LOG"]).read_text().splitlines()]


def get_deleted_paths(commands):
    return [command[-1] for command in commands if command[:3] == ["sudo", "rm", "-rf"]]


def test_deletes_expected_targets_before_apt_clean(fake_tools):
    result = run_script(fake_tools)

    assert result.returncode == 0, result.stderr
    commands = read_commands(fake_tools)
    assert sorted(get_deleted_paths(commands)) == sorted(EXPECTED_TARGETS)
    apt_clean_position = commands.index(["sudo", "apt-get", "clean"])
    assert not get_deleted_paths(commands[apt_clean_position:])


def test_failed_deletion_does_not_stop_remaining_cleanup(fake_tools):
    result = run_script({**fake_tools, "FAIL_MATCH": "/usr/local/lib/android"})

    assert result.returncode == 0, result.stderr
    commands = read_commands(fake_tools)
    assert sorted(get_deleted_paths(commands)) == sorted(EXPECTED_TARGETS)
    assert ["sudo", "apt-get", "clean"] in commands
