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
# requires-python = ">=3.11,<3.12"
# dependencies = ["PyYAML>=6.0", "rich>=13.6.0"]
# ///
"""Regenerate the consolidated Language SDK compatibility matrix in the user-facing docs.

Reads every ``<sdk>/capabilities.yaml`` listed in the ``LANG_SDKS`` registry (an SDK that has
not published one yet simply shows the "absent" mark) and rewrites the ``list-table`` blocks in
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
ABSENT_MARK: Final = "n/p"  # the SDK has not published a manifest at all

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
    doc: matrix.Capabilities | None


def load_sdk_capabilities(sdks: Sequence[matrix.LangSdk]) -> list[SdkCapabilities]:
    """Load every published manifest in registry order."""
    return [
        SdkCapabilities(
            display_name=sdk["display_name"],
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


def _render_table_header(
    title: str, sdk_capabilities: Sequence[SdkCapabilities], first_column: str = "Dimension"
) -> list[str]:
    widths = " ".join(["30"] + ["15"] * len(sdk_capabilities))
    return [
        "\n",
        f".. list-table:: {title}\n",
        "   :header-rows: 1\n",
        f"   :widths: {widths}\n",
        "\n",
        *_render_row(first_column, [sdk.display_name for sdk in sdk_capabilities]),
    ]


def _get_cell(sdk: SdkCapabilities, row: matrix.MatrixRow) -> str:
    """Render a mark, with the SDK release that shipped a supported dimension, e.g. ``✓ 1.0.0``."""
    if sdk.doc is None:
        return ABSENT_MARK
    mark = matrix.get_mark(sdk.doc, row)
    sdk_version = sdk.doc.get_entry(row).get("sdk_version")
    return f"{mark} {sdk_version}" if mark == matrix.SUPPORTED_MARK and sdk_version else mark


def render_central_table(sdk_capabilities: Sequence[SdkCapabilities]) -> list[str]:
    """Render the consolidated cross-SDK RST ``list-table`` for each part of the matrix."""
    lines = _render_table_header("Language SDK versions", sdk_capabilities, first_column="Version")
    lines += _render_row(
        "Min. Airflow version",
        [sdk.doc.min_airflow_version if sdk.doc else ABSENT_MARK for sdk in sdk_capabilities],
    )
    lines += _render_row(
        "Current Supervisor Schema Version",
        [sdk.doc.supervisor_schema_version if sdk.doc else ABSENT_MARK for sdk in sdk_capabilities],
    )
    for part in matrix.build_matrix_parts():
        lines += _render_table_header(part.title, sdk_capabilities)
        for group in part.groups:
            if len(part.groups) > 1:
                lines += _render_row(f"**{group.label}**", [""] * len(sdk_capabilities))
            for row in group.rows:
                lines += _render_row(
                    f"``{row.name}`` ({matrix.format_tier(row)})",
                    [_get_cell(sdk, row) for sdk in sdk_capabilities],
                )
    lines += [
        "\n",
        f"*Marks:* ``{matrix.SUPPORTED_MARK}`` supported (with the SDK release that shipped it, when known) · "
        f"``{matrix.UNSUPPORTED_MARK}`` not supported · ``{matrix.NA_MARK}`` not applicable · "
        f"``{ABSENT_MARK}`` not published. A tier marked ``†`` applies only when "
        f"``{matrix.DAG_DEFINITION_GATE}`` is supported.\n",
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
