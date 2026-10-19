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
Refresh the Go SDK's vendored copies of the two schemas Python owns.

``go-sdk`` generates its Dag-authoring structs from ``airflow-core``'s Dag serialization
schema and its coordinator-protocol models from the supervisor wire-schema snapshot the
Python Task SDK owns. It used to reach both by relative path across the monorepo, which
made a generated file only explainable from a directory the published Go module does not
contain, and made every change to a Python schema a change that has to carry regenerated
Go with it. It now vendors them under ``go-sdk/schema/``, the way ``ts-sdk`` and
``java-sdk`` already vendor theirs.

Vendoring splits the one question ("is the Go behind Python?") into two, and both now
run on every commit that touches either side:

* this hook — is the copy equal to the source? Copying is mechanical, so it copies for
  you and fails, triggered by either the Python source or the vendored copy changing.
  A Python-only schema PR used to leave this unrun (it was a go-sdk maintainer's manual
  step) and the vendored copy going stale was not caught by anything else either — a Go
  copy did exactly that undetected once. Failing here, on the PR that caused it, is the
  fix.
* ``check-go-sdk-generated-drift`` — are the generated files what the copy generates?
  Also triggered by the Python source now, so it runs in the same commit as this hook
  and regenerates from whatever this hook leaves the copy holding; a new schema
  construct may need a generator rule or an authoring exclusion, so what to do about it
  is a decision, not a copy, which is why that part stays a second, separate hook.

Run it from the repo root, through prek:

    prek run sync-go-sdk-schemas

or directly:

    uv run --project scripts python scripts/ci/prek/sync_go_sdk_schemas.py

Exits 0 when both copies already matched their source, 1 when it refreshed one.
"""

from __future__ import annotations

import pathlib
import shutil
import sys
from typing import NamedTuple

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]


class VendoredSchema(NamedTuple):
    """One schema Python owns and the copy ``go generate`` reads instead."""

    source: pathlib.Path
    vendored: pathlib.Path
    # generated is what the copy feeds, and regenerate the recipe that rewrites it.
    generated: str
    regenerate: str
    # also is what else has to move when this schema does, or "" when nothing does.
    also: str


VENDORED_SCHEMAS = (
    VendoredSchema(
        source=pathlib.Path("airflow-core/src/airflow/serialization/schema.json"),
        vendored=pathlib.Path("go-sdk/schema/dag-schema.json"),
        generated="go-sdk/airflow/spec.gen.go",
        regenerate="just generate-specs",
        also=(
            "A property that should not reach a Dag author belongs in the exclusion list "
            "in go-sdk/internal/genspec/authoring.go."
        ),
    ),
    VendoredSchema(
        source=pathlib.Path("task-sdk/src/airflow/sdk/execution_time/schema/schema.json"),
        vendored=pathlib.Path("go-sdk/schema/supervisor-schema.json"),
        generated="go-sdk/pkg/execution/genmodels/*.gen.go",
        regenerate="just generate-models",
        also=(
            "When api_version moved, SupervisorSchemaVersion in "
            "go-sdk/pkg/execution/messages.go has to move with it; "
            "TestSupervisorSchemaVersionMatchesSnapshot fails until it does."
        ),
    ),
)


def refresh(schema: VendoredSchema, repo_root: pathlib.Path = REPO_ROOT) -> bool:
    """Copy ``source`` over ``vendored`` when the two differ. Returns whether it copied."""
    source_path = repo_root / schema.source
    vendored_path = repo_root / schema.vendored
    if not source_path.is_file():
        raise SystemExit(f"{schema.source} is missing; cannot refresh {schema.vendored}")
    if vendored_path.is_file() and vendored_path.read_bytes() == source_path.read_bytes():
        return False
    vendored_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, vendored_path)
    return True


def format_report(refreshed: tuple[VendoredSchema, ...]) -> tuple[int, str]:
    """Turn what was refreshed into ``(exit_code, report)``."""
    if not refreshed:
        copies = ", ".join(str(schema.vendored) for schema in VENDORED_SCHEMAS)
        return 0, f"{copies} match the schemas they are vendored from."
    lines = []
    for schema in refreshed:
        lines.extend(
            [
                f"Refreshed {schema.vendored} from {schema.source}.",
                "",
                f"Review the diff, regenerate what it feeds, and commit both it and {schema.generated}:",
                "",
                f"    (cd go-sdk && {schema.regenerate})",
                "",
            ]
        )
        if schema.also:
            lines.extend([schema.also, ""])
    return 1, "\n".join(lines).rstrip()


def main() -> int:
    refreshed = tuple(schema for schema in VENDORED_SCHEMAS if refresh(schema))
    exit_code, report = format_report(refreshed)
    print(report, file=sys.stderr if exit_code else sys.stdout)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
