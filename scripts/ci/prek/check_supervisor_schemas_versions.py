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
Fail when a supervisor schema has changed without a matching
``VersionChange`` entry under
``task-sdk/src/airflow/sdk/execution_time/schema/versions/``.

Mirrors :mod:`scripts.ci.prek.check_execution_api_versions` for the
supervisor bundle. The check is per-commit: every PR that mutates a
registered supervisor schema must add an instruction to the in-progress head
``v<YYYY>_<MM>_<DD>.py`` file. The release-time version-file bump itself
is one-per-release; this hook is what keeps the in-progress file
honest between releases.

The comparison is done by dumping the snapshot JSON in this worktree
and in a temporary worktree of the upstream target branch, then
diffing them. Both sides invoke the sibling ``dump_supervisor_schemas.py``
script so the comparison is dump-version stable.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from common_prek_utils import (
    console,
    create_temporary_worktree,
    fetch_target_branch,
    get_changed_files,
)

SUPERVISOR_SCHEMAS_PREFIX = "task-sdk/src/airflow/sdk/execution_time/schema/"
VERSIONS_PREFIX = SUPERVISOR_SCHEMAS_PREFIX + "versions/"
TASK_SDK_COMMS_PATH = "task-sdk/src/airflow/sdk/execution_time/comms.py"
CORE_PROCESSOR_PATH = "airflow-core/src/airflow/dag_processing/processor.py"

DUMP_SCRIPT = Path(__file__).parent / "dump_supervisor_schemas.py"


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


def _upstream_has_schema(ref: str) -> bool:
    """Return True if the target branch carries the schema package."""
    # ``git cat-file -e`` exits zero iff the path exists at the ref.
    result = subprocess.run(
        ["git", "cat-file", "-e", f"{ref}:{VERSIONS_PREFIX}__init__.py"],
        capture_output=True,
        check=False,
    )
    return result.returncode == 0


def dump_snapshot_from_main(ref: str) -> str:
    """Dump snapshot from target branch using a temporary worktree."""
    with create_temporary_worktree(ref) as worktree_path:
        return dump_snapshot(worktree_path)


def main() -> int:
    changed_files = get_changed_files(sys.argv[1:])

    # Files under schema/ that reference the bundle's
    # registered models. Schema changes in those models' homes
    # (``comms.py``, ``processor.py``) trigger this hook too because
    # the snapshot embeds their head shape.
    schema_source_files = [
        f
        for f in changed_files
        if f.startswith(SUPERVISOR_SCHEMAS_PREFIX) or f == TASK_SDK_COMMS_PATH or f == CORE_PROCESSOR_PATH
    ]
    version_files = [f for f in changed_files if f.startswith(VERSIONS_PREFIX)]

    if not schema_source_files:
        return 0
    if version_files:
        # Contributor added a version-change entry: trust them.
        return 0

    target_ref = fetch_target_branch()
    if not _upstream_has_schema(target_ref):
        # The package is being introduced in this PR -- nothing on the
        # target branch to compare against. The check will start firing
        # normally once the package is on the target branch.
        console.print(
            "[yellow]Skipping supervisor-schemas version check:[/] target branch "
            "has no schema package yet. The check activates once "
            "this PR merges."
        )
        return 0

    try:
        main_snapshot = dump_snapshot_from_main(target_ref)
    except Exception as e:
        console.print(f"[bold red]ERROR:[/] Failed to generate upstream snapshot for comparison: {e}")
        return 1

    try:
        current_snapshot = dump_snapshot(Path.cwd())
    except Exception as e:
        console.print(f"[bold red]ERROR:[/] Failed to generate current snapshot: {e}")
        return 1

    if current_snapshot != main_snapshot:
        console.print("[bold red]ERROR:[/] Supervisor schema has changed but no version file was updated.")
        console.print("")
        console.print("The following files were changed:")
        for f in schema_source_files:
            console.print(f"  - [magenta]{f}[/]")
        console.print("")
        console.print(
            f"Snapshot diff against [cyan]{target_ref}[/] detected differences.\n"
            "\n"
            "Append a ``VersionChange`` subclass to the in-progress head "
            "``v<YYYY>_<MM>_<DD>.py`` file under:\n"
            f"  [cyan]{VERSIONS_PREFIX}[/]\n"
            "\n"
            "See [cyan]task-sdk/src/airflow/sdk/execution_time/schema/AGENTS.md[/]."
        )
        return 1
    console.print("[green]Snapshot unchanged:[/] Source changes do not affect the supervisor schema.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
