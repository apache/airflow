#!/usr/bin/env python
#
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
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Verify ``FanOutMapper.default_downstream_mapper_by_window_name`` stays in sync
between core and the Task SDK.

The default downstream-mapper table is defined twice — once in the core class
hierarchy and once in the SDK copy — because the two hierarchies are
independent (the SDK cannot import core) and the lookup is by ``Window`` class
*name*. Both copies must list the same ``Window`` name -> mapper class mapping,
otherwise a ``FanOutMapper`` resolves a different default depending on whether
it runs in Dag-author code (SDK) or after deserialization (core).

This check parses the ``default_downstream_mapper_by_window_name`` class
attribute from both files via AST and asserts the two mappings are identical.

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/check_partition_mapper_defaults_in_sync.py

Exits 0 if the two tables match, 1 (with a diff) otherwise.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from common_prek_utils import (
    AIRFLOW_CORE_SOURCES_PATH,
    AIRFLOW_TASK_SDK_SOURCES_PATH,
)
from rich.console import Console

console = Console(color_system="standard", width=200)

CLASS_NAME = "FanOutMapper"
ATTR_NAME = "default_downstream_mapper_by_window_name"

CORE_FILE = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "partition_mappers" / "temporal.py"
SDK_FILE = (
    AIRFLOW_TASK_SDK_SOURCES_PATH / "airflow" / "sdk" / "definitions" / "partition_mappers" / "temporal.py"
)


def _attr_value(stmt: ast.stmt) -> ast.expr | None:
    """Return the value node of a statement that assigns ``ATTR_NAME``, or None."""
    if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name) and stmt.target.id == ATTR_NAME:
        return stmt.value
    if (
        isinstance(stmt, ast.Assign)
        and len(stmt.targets) == 1
        and isinstance(stmt.targets[0], ast.Name)
        and stmt.targets[0].id == ATTR_NAME
    ):
        return stmt.value
    return None


def _find_attr_value(file_path: Path) -> ast.Dict:
    """Return the AST node assigned to ``FanOutMapper.default_downstream_mapper_by_window_name``."""
    tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    for node in ast.walk(tree):
        if not (isinstance(node, ast.ClassDef) and node.name == CLASS_NAME):
            continue
        for stmt in node.body:
            value = _attr_value(stmt)
            if value is None:
                continue
            if not isinstance(value, ast.Dict):
                raise ValueError(
                    f"{file_path}: {CLASS_NAME}.{ATTR_NAME} is not a dict literal; "
                    f"this check parses a dict literal and must be updated."
                )
            return value
        raise ValueError(f"{file_path}: {CLASS_NAME} has no {ATTR_NAME} attribute.")
    raise ValueError(f"{file_path}: no class {CLASS_NAME} found.")


def _extract_mapping(file_path: Path) -> dict[str, str]:
    """Extract the ``{window_name: mapper_class_name}`` mapping from a dict literal."""
    dict_node = _find_attr_value(file_path)
    mapping: dict[str, str] = {}
    for key_node, value_node in zip(dict_node.keys, dict_node.values):
        if not (isinstance(key_node, ast.Constant) and isinstance(key_node.value, str)):
            raise ValueError(
                f"{file_path}: {CLASS_NAME}.{ATTR_NAME} has a non-string-literal key; "
                f"this check expects window class names as string literals."
            )
        if not isinstance(value_node, ast.Name):
            raise ValueError(
                f"{file_path}: {CLASS_NAME}.{ATTR_NAME}[{key_node.value!r}] is not a bare "
                f"class name; this check expects a mapper class reference."
            )
        mapping[key_node.value] = value_node.id
    return mapping


def main() -> int:
    try:
        core_mapping = _extract_mapping(CORE_FILE)
        sdk_mapping = _extract_mapping(SDK_FILE)
    except ValueError as exc:
        console.print(f"[red]Could not read the default mapper table:[/red] {exc}")
        return 1

    if core_mapping == sdk_mapping:
        return 0

    console.print(f"[red]{CLASS_NAME}.{ATTR_NAME} is out of sync between core and the Task SDK.[/red]\n")
    all_windows = sorted(core_mapping.keys() | sdk_mapping.keys())
    for window in all_windows:
        core_val = core_mapping.get(window, "<missing>")
        sdk_val = sdk_mapping.get(window, "<missing>")
        marker = "  " if core_val == sdk_val else "->"
        color = "" if core_val == sdk_val else "[red]"
        end = "" if core_val == sdk_val else "[/red]"
        console.print(f"{color}{marker} {window}: core={core_val} sdk={sdk_val}{end}")
    console.print(
        f"\nMake both tables list the same window -> mapper entries:\n  core: {CORE_FILE}\n  sdk:  {SDK_FILE}"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
