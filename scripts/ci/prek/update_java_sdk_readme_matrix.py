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
"""Regenerate the Java SDK compatibility table in ``java-sdk/README.md`` and the Dokka module doc.

Renders the Markdown matrix from ``java-sdk/capabilities.yaml`` between the AUTO-GENERATED markers
in both ``java-sdk/README.md`` and ``java-sdk/sdk/module.md`` (the latter is included in the Dokka
API reference via ``includes.from("module.md")``). Exits non-zero when either file changed so the
contributor re-stages it.
"""

from __future__ import annotations

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

SDK_ID = "java"

# The Dokka module documentation that surfaces the matrix in the Java API reference.
DOKKA_MODULE_DOC = AIRFLOW_ROOT_PATH / "java-sdk" / "sdk" / "module.md"

GRADLE_PROPERTIES = AIRFLOW_ROOT_PATH / "java-sdk" / "gradle.properties"
SCHEMA_VERSION_PROPERTY = "airflowSupervisorSchemaVersion"


def read_gradle_schema_version() -> str | None:
    """The ``airflowSupervisorSchemaVersion`` from gradle.properties (source of truth for the JAR)."""
    for line in GRADLE_PROPERTIES.read_text().splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() == SCHEMA_VERSION_PROPERTY:
            return value.strip()
    return None


def check_schema_version(doc: CapabilitiesDoc) -> bool:
    """Whether the manifest agrees with the schema version the JAR manifest is stamped with."""
    gradle_version = read_gradle_schema_version()
    declared = doc["supervisor_schema_version"]
    if gradle_version is None or gradle_version == declared:
        return True
    console.print(
        f"[red]java-sdk/capabilities.yaml declares supervisor_schema_version {declared!r} but "
        f"gradle.properties {SCHEMA_VERSION_PROPERTY} is {gradle_version!r} (the JAR manifest uses "
        f"the latter). Update capabilities.yaml to match.[/]"
    )
    return False


def main() -> int:
    sdk = next(entry for entry in LANG_SDKS if entry["id"] == SDK_ID)
    doc = load_capabilities(sdk["capabilities_yaml"], expected_sdk=SDK_ID)
    if not check_schema_version(doc):
        return 1
    table = render_markdown_table(doc)
    changed = False
    for target, label in (
        (sdk["readme"], "java-sdk/README.md"),
        (DOKKA_MODULE_DOC, "java-sdk/sdk/module.md"),
    ):
        if insert_documentation(
            target,
            table,
            README_MATRIX_HEADER,
            README_MATRIX_FOOTER,
            extra_information="the Java SDK compatibility matrix",
        ):
            console.print(
                f"[yellow]Regenerated the Java SDK compatibility matrix in {label}; re-stage it.[/]"
            )
            changed = True
    return 1 if changed else 0


if __name__ in ("__main__", "__mp_main__"):
    raise SystemExit(main())
