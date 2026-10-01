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
# dependencies = ["PyYAML>=6.0", "rich>=13.6.0"]
# ///
"""Regenerate the TypeScript SDK compatibility table in ``ts-sdk/README.md``.

Renders the Markdown matrix from ``ts-sdk/capabilities.yaml`` between the AUTO-GENERATED markers.
Exits non-zero when the file changed so the contributor re-stages it. The hook also verifies that
the manifest's supervisor schema version matches the generated TypeScript runtime constant.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from common_prek_utils import console, insert_documentation
from lang_sdk_compat_matrix import (
    AIRFLOW_ROOT_PATH,
    LANG_SDKS,
    README_MATRIX_FOOTER,
    README_MATRIX_HEADER,
    CapabilitiesDoc,
    load_capabilities,
    render_markdown_table,
)

SDK_ID = "ts"
TS_SUPERVISOR = AIRFLOW_ROOT_PATH / "ts-sdk" / "src" / "generated" / "supervisor.ts"
SCHEMA_VERSION_PATTERN = re.compile(r'^export const SUPERVISOR_API_VERSION = "(?P<version>[^"]+)" as const;$')


def read_ts_schema_version() -> str | None:
    """Read ``SUPERVISOR_API_VERSION`` from the generated TypeScript runtime source."""
    if not TS_SUPERVISOR.exists():
        return None
    for line in TS_SUPERVISOR.read_text().splitlines():
        if match := SCHEMA_VERSION_PATTERN.fullmatch(line):
            return match["version"]
    return None


def check_schema_version(doc: CapabilitiesDoc) -> bool:
    """Whether the manifest agrees with the generated TypeScript supervisor schema version.

    Unlike the Java sibling hook (which treats an unreadable gradle property as an implicit pass),
    an unreadable constant here is deliberately treated as a failure: silently skipping the check
    would let capabilities.yaml drift from the runtime undetected. It is reported separately from a
    genuine mismatch because editing capabilities.yaml cannot fix a missing constant.
    """
    ts_version = read_ts_schema_version()
    declared = doc["supervisor_schema_version"]
    if ts_version is None:
        console.print(
            "[red]Could not read SUPERVISOR_API_VERSION from ts-sdk/src/generated/supervisor.ts: "
            "the file is missing, or the generated declaration no longer matches this hook's "
            "pattern. Regenerate it with 'pnpm run generate:supervisor' in ts-sdk/ (or update "
            "SCHEMA_VERSION_PATTERN if scripts/generate-supervisor.mjs changed its output). "
            "Editing ts-sdk/capabilities.yaml cannot fix this.[/]"
        )
        return False
    if ts_version == declared:
        return True
    console.print(
        f"[red]ts-sdk/capabilities.yaml declares supervisor_schema_version {declared!r} but "
        f"src/generated/supervisor.ts SUPERVISOR_API_VERSION is {ts_version!r}. "
        "Update capabilities.yaml to match.[/]"
    )
    return False


def main() -> int:
    sdk = next(entry for entry in LANG_SDKS if entry["id"] == SDK_ID)
    doc = load_capabilities(sdk["capabilities_yaml"], expected_sdk=SDK_ID)
    if not check_schema_version(doc):
        return 1
    changed = insert_documentation(
        sdk["readme"],
        render_markdown_table(doc),
        README_MATRIX_HEADER,
        README_MATRIX_FOOTER,
        extra_information="the TypeScript SDK compatibility matrix",
    )
    if changed:
        console.print(
            "[yellow]Regenerated the TypeScript SDK compatibility matrix in ts-sdk/README.md; re-stage it.[/]"
        )
        return 1
    return 0


if __name__ in ("__main__", "__mp_main__"):
    raise SystemExit(main())
