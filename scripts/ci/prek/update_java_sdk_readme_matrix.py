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
# dependencies = ["rich>=13.6.0"]
# ///
"""Regenerate the Java SDK compatibility table in ``java-sdk/README.md`` and the Dokka module doc.

Renders the Markdown matrix from ``java-sdk/generated/lang-sdk/capabilities.json`` between the
AUTO-GENERATED markers in both ``java-sdk/README.md`` and ``java-sdk/sdk/module.md`` (the latter is
included in the Dokka API reference via ``includes.from("module.md")``). Exits non-zero when either
file changed so the contributor re-stages it.
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
    load_capabilities,
    render_markdown_table,
)

SDK_ID = "java"

# The Dokka module documentation that surfaces the matrix in the Java API reference.
DOKKA_MODULE_DOC = AIRFLOW_ROOT_PATH / "java-sdk" / "sdk" / "module.md"


def main() -> int:
    sdk = next(entry for entry in LANG_SDKS if entry["id"] == SDK_ID)
    table = render_markdown_table(load_capabilities(sdk["capabilities_json"], expected_sdk=SDK_ID))
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
