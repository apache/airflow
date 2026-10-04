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
import subprocess
from pathlib import Path

import pytest

DOCKERFILE = Path(__file__).parents[3] / "Dockerfile.ci"


@pytest.mark.parametrize("with_lock", [False, True])
def test_dependency_context_preserves_manifests_without_source(tmp_path, with_lock):
    source = tmp_path / "checkout with spaces"
    destination = tmp_path / "manifests"
    for directory in ("", "task-sdk", "providers/example", "shared/example", "providers/example with spaces"):
        member = source / directory
        member.mkdir(parents=True, exist_ok=True)
        (member / "pyproject.toml").write_text(f"# manifest for {directory}\n")
        (member / "source.py").write_text("# source must not enter the dependency context\n")
    if with_lock:
        (source / "uv.lock").write_text("version = 1\n")

    stage = DOCKERFILE.read_text().split("as dependency-manifests\n", 1)[1].split("\nFROM ", 1)[0]
    command = stage.split("RUN --mount=type=bind,target=/source \\\n", 1)[1]
    command = command.replace("/dependency-manifests", '"${MANIFESTS}"').replace("/source", '"${SOURCE}"')
    subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", command],
        env={**os.environ, "SOURCE": str(source), "MANIFESTS": str(destination)},
        cwd=source,
        check=True,
    )
    expected = {
        "pyproject.toml",
        "task-sdk/pyproject.toml",
        "providers/example/pyproject.toml",
        "providers/example with spaces/pyproject.toml",
        "shared/example/pyproject.toml",
    }
    if with_lock:
        expected.add("uv.lock")
    assert {
        path.relative_to(destination).as_posix() for path in destination.rglob("*") if path.is_file()
    } == expected
    for relative in expected:
        assert (destination / relative).read_bytes() == (source / relative).read_bytes()


@pytest.mark.parametrize("upgrade", ["", "fresh-resolution"])
@pytest.mark.parametrize("uv_exit", [0, 1])
def test_dependency_preinstallation_is_optional(tmp_path, upgrade, uv_exit):
    executable = tmp_path / "uv"
    calls = tmp_path / "calls"
    executable.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$CALLS"\nexit "$UV_EXIT"\n')
    executable.chmod(0o755)
    section = DOCKERFILE.read_text().split("# Only preinstall locked third-party dependencies.", 1)[1]
    command = section.split("RUN --mount=type=cache", 1)[1].split("\n\n", 1)[0].split("\\\n", 1)[1]
    subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", command],
        env={
            **os.environ,
            "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}",
            "CALLS": str(calls),
            "UV_EXIT": str(uv_exit),
            "UPGRADE_RANDOM_INDICATOR_STRING": upgrade,
        },
        check=True,
    )
    if upgrade:
        assert not calls.exists()
    else:
        assert "--no-install-workspace" in calls.read_text().splitlines()
