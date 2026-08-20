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
"""Regenerate the consolidated Language SDK compatibility matrix in the user-facing docs.

Reads every ``<sdk>/capabilities.yaml`` listed in the ``LANG_SDKS`` registry (an SDK that has
not published one yet simply shows the "absent" mark) and rewrites the ``list-table`` in
``airflow-core/docs/authoring-and-scheduling/language-sdks/index.rst`` between the
AUTO-GENERATED markers. Exits non-zero when the file changed so the contributor re-stages it.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final, NamedTuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

import lang_sdk_compat_matrix as matrix
from common_prek_utils import console, insert_documentation

CENTRAL_MATRIX_HEADER: Final = ".. BEGIN AUTO-GENERATED LANG-SDK COMPAT MATRIX"
CENTRAL_MATRIX_FOOTER: Final = ".. END AUTO-GENERATED LANG-SDK COMPAT MATRIX"
ABSENT_MARK: Final = matrix.NO_VERSION_MARK
SDK_DISPLAY_NAMES: Final[dict[str, str]] = {
    "go": "Go",
    "java": "Java",
    "ts": "TypeScript",
}

INDEX_RST: Final = (
    matrix.AIRFLOW_ROOT_PATH
    / "airflow-core"
    / "docs"
    / "authoring-and-scheduling"
    / "language-sdks"
    / "index.rst"
)


class SdkCapabilities(NamedTuple):
    display_name: str
    doc: matrix.CapabilitiesDoc | None


def load_sdk_capabilities(sdks: Sequence[matrix.LangSdk]) -> list[SdkCapabilities]:
    """Load every published manifest in registry order."""
    return [
        SdkCapabilities(
            display_name=SDK_DISPLAY_NAMES[sdk["id"]],
            doc=(
                matrix.load_capabilities(sdk["capabilities_yaml"], expected_sdk=sdk["id"])
                if sdk["capabilities_yaml"].exists()
                else None
            ),
        )
        for sdk in sdks
    ]


def _render_row(label: str, values: Sequence[str]) -> list[str]:
    return [f"   * - {label}\n", *[f"     - {value}".rstrip() + "\n" for value in values]]


def _render_header_row(label: str, column_count: int) -> list[str]:
    return _render_row(f"**{label}**", [""] * column_count)


def _get_state_cell(doc: matrix.CapabilitiesDoc | None, name: str) -> str:
    if doc is None:
        return ABSENT_MARK
    return matrix._state_mark(doc["states"][name])


def _get_capability_cell(doc: matrix.CapabilitiesDoc | None, capability: matrix.Capability) -> str:
    if doc is None:
        return ABSENT_MARK
    return matrix._capability_mark(doc, capability)


def render_central_table(sdk_capabilities: Sequence[SdkCapabilities]) -> list[str]:
    """Render the consolidated cross-SDK RST ``list-table``."""
    column_count = len(sdk_capabilities)
    widths = " ".join(["30"] + ["15"] * column_count)
    lines = [
        "\n",
        ".. list-table:: Language SDK compatibility matrix\n",
        "   :header-rows: 1\n",
        f"   :widths: {widths}\n",
        "\n",
    ]
    lines += _render_row("Dimension", [sdk.display_name for sdk in sdk_capabilities])
    lines += _render_row(
        "Min. Airflow version",
        [sdk.doc["min_airflow_version"] if sdk.doc else ABSENT_MARK for sdk in sdk_capabilities],
    )
    lines += _render_row(
        "Supervisor schema",
        [sdk.doc["supervisor_schema_version"] if sdk.doc else ABSENT_MARK for sdk in sdk_capabilities],
    )
    lines += _render_header_row(matrix.STATES_GROUP_LABEL, column_count)
    for state, tier in matrix.STATE_DIMENSIONS:
        lines += _render_row(
            f"``{state}`` ({tier})", [_get_state_cell(sdk.doc, state) for sdk in sdk_capabilities]
        )
    current_group = ""
    for capability in matrix.CAPABILITY_DIMENSIONS:
        if capability.group != current_group:
            current_group = capability.group
            lines += _render_header_row(matrix.GROUP_LABELS[capability.group], column_count)
        lines += _render_row(
            f"``{capability.name}`` ({matrix._tier_label(capability)})",
            [_get_capability_cell(sdk.doc, capability) for sdk in sdk_capabilities],
        )
    lines += [
        "\n",
        f"*Marks:* ``{matrix.SUPPORTED_MARK}`` supported · "
        f"``{matrix.UNSUPPORTED_MARK}`` not supported · ``{matrix.NA_MARK}`` not applicable · "
        f"``{ABSENT_MARK}`` not published. A tier marked ``†`` applies only when "
        f"``{matrix.NATIVE_DAG_GATE}`` is supported.\n",
        "\n",
    ]
    return lines


def main() -> int:
    changed = insert_documentation(
        INDEX_RST,
        render_central_table(load_sdk_capabilities(matrix.LANG_SDKS)),
        CENTRAL_MATRIX_HEADER,
        CENTRAL_MATRIX_FOOTER,
        extra_information="the Language SDK compatibility matrix",
    )
    if changed:
        console.print(
            "[yellow]Regenerated the Language SDK compatibility matrix in index.rst; re-stage the file.[/]"
        )
        return 1
    return 0


if __name__ in ("__main__", "__mp_main__"):
    raise SystemExit(main())
