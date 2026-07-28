#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Regenerate the supervisor schema snapshot at
``task-sdk/src/airflow/sdk/execution_time/schema/schema.json``.

The snapshot is the head-version JSON Schema for every Pydantic class
on the supervisor schema wire (the union members of ``ToTask``,
``ToSupervisor``, ``ToManager``, ``ToDagProcessor``).

The actual dump is delegated to ``dump_supervisor_schemas.py`` (the
sibling stdout-only script). If the committed snapshot differs from
the dumped content the hook rewrites it and exits non-zero (standard
"regenerated files, please re-stage" pattern).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from common_prek_utils import console

REPO_ROOT = Path(__file__).parents[3].resolve()
SNAPSHOT_PATH = REPO_ROOT.joinpath(
    "task-sdk",
    "src",
    "airflow",
    "sdk",
    "execution_time",
    "schema",
    "schema.json",
)
DUMP_SCRIPT = Path(__file__).parent.joinpath("dump_supervisor_schemas.py")


def dump_snapshot(cwd: Path) -> str:
    """Run ``dump_supervisor_schemas.py`` in *cwd* and return its stdout."""
    result = subprocess.run(
        [
            "uv",
            "run",
            "-p",
            "3.12",
            "--frozen",
            "--no-progress",
            "--project",
            "task-sdk",
            "-s",
            str(DUMP_SCRIPT),
        ],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Snapshot dump failed: {result.stderr}")
    return result.stdout


def main() -> int:
    try:
        new_content = dump_snapshot(REPO_ROOT)
    except Exception as e:
        console.print(f"[bold red]ERROR:[/] {e}")
        return 1

    if SNAPSHOT_PATH.exists():
        old_content = SNAPSHOT_PATH.read_text()
        if old_content == new_content:
            return 0
    else:
        SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)

    SNAPSHOT_PATH.write_text(new_content)
    rel = SNAPSHOT_PATH.relative_to(REPO_ROOT)
    console.print(f"[yellow]Regenerated[/] [cyan]{rel}[/]. Please review the diff and re-stage the file.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
