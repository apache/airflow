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
#   "pyyaml>=6.0.3",
#   "rich>=13.6.0",
# ]
# ///
"""
Ensure ``howto/connection:`` labels in RST docs stay consistent with provider.yaml.

Source of truth: ``connection-type`` values in each provider's ``provider.yaml``.

Checks:
  ORPHAN LABEL  — an RST anchor ``.. _howto/connection:{X}:`` where X is not a
                  registered connection-type.
  MULTIPLE LABELS — more than one top-level ``howto/connection:`` anchor per file.
  BROKEN REF    — a ``:ref:`` targeting ``howto/connection:*`` that has no
                  matching anchor anywhere.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.resolve()))

from common_prek_utils import AIRFLOW_PROVIDERS_ROOT_PATH, AIRFLOW_ROOT_PATH, get_all_provider_info_dicts

console = Console(color_system="standard", width=200)

KNOWN_EXCEPTIONS: set[str] = {
    "pgvector",
    "sql",
}

TOP_LEVEL_ANCHOR_RE = re.compile(r"^\.\.\s+_howto/connection:([a-zA-Z0-9_-]+):\s*$", re.MULTILINE)
ANY_ANCHOR_RE = re.compile(r"^\.\.\s+_(howto/connection:[^\s]+?):\s*$", re.MULTILINE)
REF_RE = re.compile(r":ref:`(?:[^`]*<(howto/connection:[^>]+)>|(howto/connection:[^`]+))`")


def collect_connection_types() -> set[str]:
    conn_types: set[str] = set()
    for _pid, info in get_all_provider_info_dicts().items():
        for ct in info.get("connection-types", []):
            conn_types.add(ct["connection-type"])
    return conn_types


def collect_rst_files() -> list[Path]:
    rst_files: list[Path] = list(AIRFLOW_PROVIDERS_ROOT_PATH.rglob("*.rst"))
    core_docs = AIRFLOW_ROOT_PATH / "airflow-core" / "docs"
    if core_docs.is_dir():
        rst_files.extend(core_docs.rglob("*.rst"))
    return rst_files


def main() -> int:
    errors: list[str] = []
    valid_conn_types = collect_connection_types() | KNOWN_EXCEPTIONS

    rst_files = collect_rst_files()
    all_anchors: set[str] = set()
    top_level_per_file: dict[Path, list[str]] = {}

    for rst_file in rst_files:
        content = rst_file.read_text()
        for full_label in ANY_ANCHOR_RE.findall(content):
            all_anchors.add(full_label)
        top_labels = TOP_LEVEL_ANCHOR_RE.findall(content)
        if top_labels:
            top_level_per_file[rst_file] = top_labels

    for rst_file, labels in top_level_per_file.items():
        rel = rst_file.relative_to(AIRFLOW_ROOT_PATH)
        for label in labels:
            if label not in valid_conn_types:
                errors.append(
                    f"ORPHAN LABEL: {rel} — "
                    f"'howto/connection:{label}' does not match any connection-type in provider.yaml"
                )
        if len(labels) > 1:
            errors.append(
                f"MULTIPLE LABELS: {rel} — "
                f"found {len(labels)} top-level labels ({', '.join(labels)}), expected at most 1"
            )

    for rst_file in rst_files:
        content = rst_file.read_text()
        for match in REF_RE.finditer(content):
            target = match.group(1) or match.group(2)
            if target not in all_anchors:
                rel = rst_file.relative_to(AIRFLOW_ROOT_PATH)
                errors.append(f"BROKEN REF: {rel} — :ref:`{target}` has no matching anchor in any RST file")

    if errors:
        console.print()
        for error in errors:
            console.print(f"  [red]✗[/] {error}")
        console.print()
        console.print(f"[red]Connection doc label check failed with {len(errors)} error(s).[/]")
        return 1

    console.print("[green]All connection doc labels and cross-references are consistent.[/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
