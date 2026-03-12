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
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import console

DATAMODELS_PREFIX = "airflow-core/src/airflow/api_fastapi/execution_api/datamodels/"
VERSIONS_PREFIX = "airflow-core/src/airflow/api_fastapi/execution_api/versions/"


def get_changed_files_ci() -> list[str]:
    """Get changed files in a CI environment by comparing against the target branch."""
    target_branch = os.environ.get("GITHUB_BASE_REF", "main")
    fetch_result = subprocess.run(
        ["git", "fetch", "origin", target_branch],
        capture_output=True,
        text=True,
        check=False,
    )
    if fetch_result.returncode != 0:
        console.print(
            f"[yellow]WARNING: Failed to fetch origin/{target_branch}: {fetch_result.stderr.strip()}[/]"
        )
    result = subprocess.run(
        ["git", "diff", "--name-only", f"origin/{target_branch}...HEAD"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        # Shallow clone (fetch-depth: 1) may not have enough history to compute the
        # merge base required by the three-dot diff.  Deepen the fetch and retry once.
        console.print(
            f"[yellow]WARNING: git diff against origin/{target_branch} failed (exit {result.returncode}), "
            "retrying with deeper fetch...[/]"
        )
        subprocess.run(
            ["git", "fetch", "--deepen=50", "origin", target_branch],
            capture_output=True,
            text=True,
            check=False,
        )
        try:
            result = subprocess.run(
                ["git", "diff", "--name-only", f"origin/{target_branch}...HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            console.print(f"[bold red]ERROR:[/] git diff failed (exit {e.returncode})")
            if e.stdout:
                console.print(f"[dim]stdout:[/]\n{e.stdout.strip()}")
            if e.stderr:
                console.print(f"[dim]stderr:[/]\n{e.stderr.strip()}")
            raise
    return [f for f in result.stdout.strip().splitlines() if f]


def get_changed_files_local() -> list[str]:
    """Get staged files in a local (non-CI) environment."""
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
        check=True,
    )
    return [f for f in result.stdout.strip().splitlines() if f]


def main() -> int:
    is_ci = os.environ.get("CI")
    if is_ci:
        changed_files = get_changed_files_ci()
    else:
        changed_files = get_changed_files_local()

    datamodel_files = [
        f for f in changed_files if f.startswith(DATAMODELS_PREFIX) and not f.endswith("__init__.py")
    ]
    version_files = [f for f in changed_files if f.startswith(VERSIONS_PREFIX)]

    if datamodel_files and not version_files:
        console.print(
            "[bold red]ERROR:[/] Changes to execution API datamodels require corresponding changes in versions."
        )
        console.print("")
        console.print("The following datamodel files were changed:")
        for f in datamodel_files:
            console.print(f"  - [magenta]{f}[/]")
        console.print("")
        console.print(
            "But no files were changed under:\n"
            f"  [cyan]{VERSIONS_PREFIX}[/]\n"
            "\n"
            "Please add or update a version file to reflect the datamodel changes.\n"
            "See [cyan]contributing-docs/19_execution_api_versioning.rst[/] for details."
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
