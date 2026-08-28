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

import json
import subprocess
import sys
from pathlib import Path

from common_prek_utils import (
    console,
    create_temporary_worktree,
    fetch_target_branch,
    get_changed_files,
)

DATAMODELS_PREFIX = "airflow-core/src/airflow/api_fastapi/execution_api/datamodels/"
VERSIONS_PREFIX = "airflow-core/src/airflow/api_fastapi/execution_api/versions/"


def generate_schema(cwd: Path) -> dict:
    """Generate OpenAPI schema from repo at cwd."""
    script_path = Path(__file__).parent / "generate_execution_api_schema.py"
    result = subprocess.run(
        ["uv", "run", "-p", "3.12", "--no-progress", "--project", "airflow-core", "-s", str(script_path)],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Schema generation failed: {result.stderr}")
    return json.loads(result.stdout)


def generate_schema_from_main(ref: str) -> dict:
    """Generate schema from target branch using worktree."""
    with create_temporary_worktree(ref) as worktree_path:
        return generate_schema(worktree_path)


def normalize_schema(schema: dict) -> dict:
    """Normalize schema for comparison by removing non-semantic differences."""
    normalized = json.loads(json.dumps(schema, sort_keys=True))
    if "info" in normalized:
        normalized.pop("info", None)
    if "servers" in normalized:
        normalized.pop("servers", None)
    return normalized


def schemas_equal(schema1: dict, schema2: dict) -> bool:
    """Compare two schemas for semantic equality."""
    return normalize_schema(schema1) == normalize_schema(schema2)


def main() -> int:
    changed_files = get_changed_files(sys.argv[1:])

    datamodel_files = [
        f for f in changed_files if f.startswith(DATAMODELS_PREFIX) and not f.endswith("__init__.py")
    ]
    version_files = [f for f in changed_files if f.startswith(VERSIONS_PREFIX)]

    if datamodel_files and not version_files:
        target_ref = fetch_target_branch()
        try:
            main_schema = generate_schema_from_main(target_ref)
        except Exception as e:
            console.print(f"[yellow]WARNING: Could not generate schema from main: {e}[/]")
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

        try:
            current_schema = generate_schema(Path.cwd())
        except Exception as e:
            console.print(f"[bold red]ERROR:[/] Failed to generate current schema: {e}")
            return 1

        if not schemas_equal(current_schema, main_schema):
            console.print(
                "[bold red]ERROR:[/] Execution API schema has changed but no version file was updated."
            )
            console.print("")
            console.print("The following datamodel files were changed:")
            for f in datamodel_files:
                console.print(f"  - [magenta]{f}[/]")
            console.print("")
            console.print(
                f"Schema diff against [cyan]{target_ref}[/] detected differences.\n"
                "\n"
                "Please add or update a version file under:\n"
                f"  [cyan]{VERSIONS_PREFIX}[/]\n"
                "\n"
                "See [cyan]contributing-docs/19_execution_api_versioning.rst[/] for details."
            )
            return 1
        console.print("[green]Schema unchanged:[/] Datamodel changes do not affect API contract.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
