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
Fail when the YAML Dag schema changes without a matching version update.

This mirrors :mod:`scripts.ci.prek.check_supervisor_schemas_versions` for the
YAML Cadwyn bundle. Documents pin their format to a published ``$schema``, so a
change to the generated snapshot that is not paired with a version update would
silently reinterpret already-pinned documents once the current head date ships.

The check is per-commit; if a schema-affecting source files or the committed
snapshot changed, but nothing under ``versions/`` did, the hook asks the author
to record the change as a new dated version. A ``versions/`` change is trusted.

The comparison is against the committed ``schema.json`` on the target branch;
the paired ``generate-yaml-importer-schema-snapshot`` hook guarantees the
snapshot tracks the models, so the committed file is the source of truth here.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from common_prek_utils import console, get_remote_for_main

SCHEMA_DIR = "task-sdk/src/airflow/sdk/importers/yaml_importer/"
SNAPSHOT_PATH = SCHEMA_DIR + "schema.json"
MODELS_PATH = SCHEMA_DIR + "models.py"
VERSIONS_PREFIX = SCHEMA_DIR + "versions/"


def get_target_branch() -> str:
    """Branch to compare against. GITHUB_BASE_REF for PRs, DEFAULT_BRANCH in CI, else main."""
    return os.environ.get("GITHUB_BASE_REF") or os.environ.get("DEFAULT_BRANCH") or "main"


def get_changed_files(filenames: list[str]) -> list[str]:
    """Get changed files. Uses filenames from prek when provided, else staged files for local runs."""
    if filenames:
        return filenames
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [f for f in result.stdout.strip().splitlines() if f]


def _target_ref() -> str:
    """Fetch and return the target-branch ref to compare against."""
    remote = get_remote_for_main()
    target_branch = get_target_branch()
    subprocess.run(["git", "fetch", remote, target_branch], capture_output=True, check=False)
    return f"{remote}/{target_branch}"


def _snapshot_on_target(ref: str) -> str | None:
    """Return the committed ``schema.json`` at *ref*, or ``None`` if it does not exist there yet."""
    result = subprocess.run(
        ["git", "show", f"{ref}:{SNAPSHOT_PATH}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout if result.returncode == 0 else None


def main() -> int:
    changed_files = get_changed_files(sys.argv[1:])
    schema_source_files = [f for f in changed_files if f in (MODELS_PATH, SNAPSHOT_PATH)]
    version_files = [f for f in changed_files if f.startswith(VERSIONS_PREFIX)]

    if not schema_source_files:
        return 0
    if version_files:
        # A version update accompanies the change -- trust the contributor.
        return 0

    ref = _target_ref()
    target_snapshot = _snapshot_on_target(ref)
    if target_snapshot is None:
        # The snapshot is being introduced in this PR -- nothing on the target
        # branch to compare against. The check activates once it merges.
        console.print(
            "[yellow]Skipping YAML importer schema version check:[/] target branch "
            "has no schema snapshot yet. The check activates once this PR merges."
        )
        return 0

    if Path(SNAPSHOT_PATH).read_text() != target_snapshot:
        console.print("[bold red]ERROR:[/] YAML importer schema changed but no version was updated.")
        console.print("")
        console.print("The following files were changed:")
        for f in schema_source_files:
            console.print(f"  - [magenta]{f}[/]")
        console.print("")
        console.print(
            f"The committed snapshot differs from [cyan]{ref}[/].\n"
            "\n"
            "Once a schema date is released, changing it reinterprets documents pinned to that\n"
            "version. Record the change as a new dated version carrying a ``VersionChange`` under:\n"
            f"  [cyan]{VERSIONS_PREFIX}[/]\n"
            "(as the supervisor and execution-api bundles do), then regenerate the snapshot."
        )
        return 1

    console.print("[green]Snapshot unchanged:[/] source changes do not affect the YAML importer schema.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
