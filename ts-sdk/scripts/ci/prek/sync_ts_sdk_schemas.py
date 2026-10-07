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
Refresh the TypeScript SDK's vendored copies of the two schemas Python owns.

``ts-sdk`` generates its Dag field interfaces from ``airflow-core``'s Dag serialization
schema and its supervisor wire types from the snapshot the Python Task SDK owns. It
vendors both under ``ts-sdk/schema/`` so a published npm package can be built without the
monorepo, the way ``go-sdk`` and ``java-sdk`` already vendor theirs.

Vendoring splits the one question ("is the TypeScript behind Python?") into two, and both
now run on every commit that touches either side:

* this hook — is the copy equal to the source? Copying is mechanical, so it copies for
  you and fails, triggered by either the Python source or the vendored copy changing.
  A Python-only schema change used to leave the supervisor copy unrun and nothing else
  caught it either, so ``src/generated/supervisor.ts`` shipped stale. Failing here, on
  the PR that caused it, is the fix.
* ``check-ts-sdk-dag-schema`` / ``check-ts-sdk-supervisor-schema`` — are the generated
  files what the copy generates? They regenerate from whatever this hook leaves the copy
  holding, which is why they stay separate hooks: a new schema construct may need a
  generator change, which is a decision, not a copy.

Run it from the repo root, through prek:

    prek run sync-ts-sdk-schemas

or directly:

    ./ts-sdk/scripts/ci/prek/sync_ts_sdk_schemas.py

Exits 0 when both copies already matched their source, 1 when it refreshed one.
"""

from __future__ import annotations

import pathlib
import shutil
import sys
from typing import NamedTuple

REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]


class VendoredSchema(NamedTuple):
    """One schema Python owns and the copy ``pnpm run generate:*`` reads instead."""

    source: pathlib.Path
    vendored: pathlib.Path
    # generated is what the copy feeds, and regenerate the recipe that rewrites it.
    generated: str
    regenerate: str


VENDORED_SCHEMAS = (
    VendoredSchema(
        source=pathlib.Path("airflow-core/src/airflow/serialization/schema.json"),
        vendored=pathlib.Path("ts-sdk/schema/dag-schema.json"),
        generated="ts-sdk/src/generated/dag-schema-fields.ts",
        regenerate="pnpm run generate:dag-schema",
    ),
    VendoredSchema(
        source=pathlib.Path("task-sdk/src/airflow/sdk/execution_time/schema/schema.json"),
        vendored=pathlib.Path("ts-sdk/schema/supervisor-schema.json"),
        generated="ts-sdk/src/generated/supervisor.ts",
        regenerate="pnpm run generate:supervisor",
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
                f"    (cd ts-sdk && {schema.regenerate})",
                "",
            ]
        )
    return 1, "\n".join(lines).rstrip()


def main() -> int:
    refreshed = tuple(schema for schema in VENDORED_SCHEMAS if refresh(schema))
    exit_code, report = format_report(refreshed)
    print(report, file=sys.stderr if exit_code else sys.stdout)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
