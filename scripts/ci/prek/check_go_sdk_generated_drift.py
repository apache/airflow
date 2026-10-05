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
Keep the Go SDK's generated files in step with the schemas they generate from.

Two of the Go SDK's surfaces are generated from schemas Python owns, and both are
committed, so nothing regenerates them when the schema moves:

* ``go-sdk/airflow/spec.gen.go`` — ``airflow.DagSpec`` and ``airflow.TaskSpec``, the
  structs a Dag author fills in, from ``go-sdk/schema/dag-schema.json``.
* ``go-sdk/pkg/execution/genmodels/*.gen.go`` — the coordinator-protocol messages,
  from ``go-sdk/schema/supervisor-schema.json``.

Both of those are go-sdk's vendored copies of a schema another distribution owns;
``sync-go-sdk-schemas`` is what keeps a copy equal to its original. This check takes
the copy as given and asks only whether the committed Go is what the copy generates.

Without it a property added, renamed or retyped upstream leaves the Go side silently
behind: a Dag authored in Go keeps serializing the old shape, and msgpack drops a
message field the Go struct does not declare. That is not theoretical — ``models.gen.go``
sat two fields behind its snapshot across two releases, which is what #73954 is about.

The check regenerates each target and asks Git whether it changed. A drifted file is
left regenerated in the working tree, so the fix is to commit it.

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/check_go_sdk_generated_drift.py

Exits 0 if every committed file matches its schema, 1 otherwise.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess
import sys
from typing import NamedTuple

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
GO_SDK_MODULE = pathlib.Path("go-sdk")


class Target(NamedTuple):
    """One `go generate` target and the committed files it writes."""

    package: str
    committed: tuple[pathlib.Path, ...]
    schema: str
    # remedy is what to weigh before committing the regenerated file, or "" when there
    # is nothing to decide and the file just has to be brought forward.
    remedy: str


TARGETS = (
    Target(
        package="./airflow/...",
        committed=(GO_SDK_MODULE / "airflow" / "spec.gen.go",),
        schema="go-sdk/schema/dag-schema.json",
        remedy=(
            "Review it — a property that should not reach an author belongs in the "
            "exclusion list in go-sdk/internal/genspec/authoring.go — then commit it:"
        ),
    ),
    Target(
        package="./pkg/execution/genmodels/...",
        committed=(
            GO_SDK_MODULE / "pkg" / "execution" / "genmodels" / "models.gen.go",
            GO_SDK_MODULE / "pkg" / "execution" / "genmodels" / "discriminators.gen.go",
            GO_SDK_MODULE / "pkg" / "execution" / "genmodels" / "defaults.gen.go",
        ),
        schema="go-sdk/schema/supervisor-schema.json",
        remedy="Commit it:",
    ),
)


def regenerate(module_dir: pathlib.Path, package: str, go_binary: str = "go") -> tuple[int, str]:
    """Run one target's generators. Returns ``(returncode, combined_output)``."""
    completed = subprocess.run(
        [go_binary, "generate", package],
        cwd=module_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode, (completed.stdout + completed.stderr).strip()


def read_drift(
    repo_root: pathlib.Path, paths: tuple[pathlib.Path, ...], git_binary: str = "git"
) -> tuple[int, str]:
    """Ask Git what regeneration changed. Returns ``(returncode, diff)``."""
    completed = subprocess.run(
        [git_binary, "diff", "--", *(str(path) for path in paths)],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode, completed.stdout.strip()


def format_report(
    target: Target, generate_returncode: int, generate_output: str, diff_returncode: int, diff: str
) -> tuple[int, str]:
    """Turn one target's regeneration result and the diff that followed into ``(exit_code, report)``."""
    written = ", ".join(str(path) for path in target.committed)
    stageable = " ".join(str(path) for path in target.committed)
    if generate_returncode != 0:
        return 1, "\n".join(
            [
                f"ERROR: regenerating {written} failed.",
                "",
                "A generator fails on a schema construct it has no rule for, which is how a",
                "newly vendored schema that needs a new rule surfaces; the generators also",
                "need the Go toolchain and the network to fetch go-jsonschema.",
                "`go generate` reported:",
                "",
                generate_output or "(no output)",
            ]
        )
    # An unreadable diff is not an absent one: reporting success here would let the
    # check pass on every drift.
    if diff_returncode != 0:
        return 1, f"ERROR: `git diff` failed, so whether {written} drifted is unknown."
    if not diff:
        return 0, f"OK: {written} — up to date with {target.schema}."
    return 1, "\n".join(
        [
            f"ERROR: out of date: {written}.",
            "",
            f"The committed output no longer matches what {target.schema} and the",
            "generators produce. The regenerated output is in your working tree.",
            "",
            target.remedy,
            "",
            f"    git add {stageable}",
            "",
            "Regeneration changed:",
            "",
            diff,
        ]
    )


def main() -> int:
    module_dir = REPO_ROOT / GO_SDK_MODULE
    for target in TARGETS:
        for path in target.committed:
            if not (REPO_ROOT / path).exists():
                print(f"ERROR: {path} not found — has the generated file moved?")
                return 1
    if shutil.which("go") is None:
        if os.environ.get("CI"):
            print("ERROR: `go` is not on PATH but this is a CI run — the toolchain is required here.")
            return 1
        print("SKIPPED: `go` is not on PATH, cannot verify that the generated files are current.")
        return 0
    exit_code = 0
    for target in TARGETS:
        generate_returncode, generate_output = regenerate(module_dir, target.package)
        diff_returncode, diff = read_drift(REPO_ROOT, target.committed)
        target_exit_code, report = format_report(
            target, generate_returncode, generate_output, diff_returncode, diff
        )
        print(report)
        exit_code = exit_code or target_exit_code
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
