#!/usr/bin/env python3
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
"""
Keep the Go SDK's Dag and task spec structs in step with the serialization schema.

``go-sdk/airflow/spec.gen.go`` declares ``airflow.DagSpec`` and
``airflow.TaskSpec``, the two structs a Dag author fills in. Both are generated
from ``airflow-core/src/airflow/serialization/schema.json``, which Python owns:
``go-sdk/internal/genspec`` rewrites that schema into the authoring shape and
go-jsonschema writes the structs from it.

The generated file is committed, so nothing regenerates it when the schema moves
on the Python side. Without this check a property added, renamed or retyped there
would leave the Go structs silently behind, and a Dag authored in Go would keep
serializing the old shape.

The check regenerates the file and asks Git whether it changed. A drifted file is
left regenerated in the working tree, so the fix is to commit it.

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/check_go_sdk_spec_drift.py

Exits 0 if the committed structs match the schema, 1 otherwise.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
GO_SDK_MODULE = pathlib.Path("go-sdk")
GENERATED_SPECS = GO_SDK_MODULE / "airflow" / "spec.gen.go"


def regenerate_specs(module_dir: pathlib.Path, go_binary: str = "go") -> tuple[int, str]:
    """Run the spec generators. Returns ``(returncode, combined_output)``."""
    completed = subprocess.run(
        [go_binary, "generate", "./airflow/..."],
        cwd=module_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode, (completed.stdout + completed.stderr).strip()


def read_drift(repo_root: pathlib.Path, git_binary: str = "git") -> tuple[int, str]:
    """Ask Git what regeneration changed. Returns ``(returncode, diff)``."""
    completed = subprocess.run(
        [git_binary, "diff", "--", str(GENERATED_SPECS)],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode, completed.stdout.strip()


def format_report(
    generate_returncode: int, generate_output: str, diff_returncode: int, diff: str
) -> tuple[int, str]:
    """Turn a regeneration result and the diff that followed it into ``(exit_code, report)``."""
    if generate_returncode != 0:
        return 1, "\n".join(
            [
                f"ERROR: regenerating {GENERATED_SPECS} failed.",
                "",
                "genspec fails on a schema construct it has no rule for, which is how a change on",
                "the Python side that needs a new rule surfaces; the generators also need the Go",
                "toolchain and the network to fetch go-jsonschema. `go generate` reported:",
                "",
                generate_output or "(no output)",
            ]
        )
    # An unreadable diff is not an absent one: reporting success here would let the
    # check pass on every drift.
    if diff_returncode != 0:
        return 1, f"ERROR: `git diff` failed, so whether {GENERATED_SPECS} drifted is unknown."
    if not diff:
        return 0, f"OK: {GENERATED_SPECS} matches the serialization schema."
    return 1, "\n".join(
        [
            f"ERROR: {GENERATED_SPECS} is out of date.",
            "",
            "It is generated from airflow-core/src/airflow/serialization/schema.json by",
            "go-sdk/internal/genspec, and one of the two has moved since the file was committed.",
            "The regenerated file is in your working tree.",
            "",
            "Review it — a property that should not reach an author belongs in the exclusion",
            "list in go-sdk/internal/genspec/authoring.go — then commit it:",
            "",
            f"    git add {GENERATED_SPECS}",
            "",
            "Regeneration changed:",
            "",
            diff,
        ]
    )


def main() -> int:
    module_dir = REPO_ROOT / GO_SDK_MODULE
    if not (REPO_ROOT / GENERATED_SPECS).is_file():
        print(f"ERROR: {GENERATED_SPECS} not found — has the generated file moved?")
        return 1
    if shutil.which("go") is None:
        if os.environ.get("CI"):
            print("ERROR: `go` is not on PATH but this is a CI run — the toolchain is required here.")
            return 1
        print(f"SKIPPED: `go` is not on PATH, cannot verify that {GENERATED_SPECS} is current.")
        return 0
    generate_returncode, generate_output = regenerate_specs(module_dir)
    diff_returncode, diff = read_drift(REPO_ROOT)
    exit_code, report = format_report(generate_returncode, generate_output, diff_returncode, diff)
    print(report)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
