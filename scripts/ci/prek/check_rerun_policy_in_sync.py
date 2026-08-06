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
Verify ``RerunPolicy`` stays in sync between core and the Task SDK.

The enum is defined twice — once in core and once in the SDK — because the two
sides are independent (the SDK cannot import core) and the value is serialized
by its string value to round-trip across the Dag-parse / scheduler boundary. If
the member cases drift, a Dag authored against the SDK could serialize a value
the core side cannot resolve, and the behavior methods would disagree.

This check parses both files via AST and asserts the following are identical:

- ``RerunPolicy`` enum member values (``{member_name: value}``)
- the bodies of the behavior methods (``fires_immediately``, ``drops_event``)

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/check_rerun_policy_in_sync.py

Exits 0 if synced, 1 (with a diff) otherwise.
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

CORE_FILE = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "partition_mappers" / "rerun_policy.py"
SDK_FILE = (
    AIRFLOW_TASK_SDK_SOURCES_PATH
    / "airflow"
    / "sdk"
    / "definitions"
    / "partition_mappers"
    / "rerun_policy.py"
)

BEHAVIOR_METHODS = ["fires_immediately", "drops_event"]


def _find_class(tree: ast.Module, name: str, file_path: Path) -> ast.ClassDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    raise ValueError(f"{file_path}: no class {name!r} found.")


def _extract_members(cls: ast.ClassDef) -> dict[str, str]:
    members: dict[str, str] = {}
    for stmt in cls.body:
        if (
            isinstance(stmt, ast.Assign)
            and len(stmt.targets) == 1
            and isinstance(stmt.targets[0], ast.Name)
            and isinstance(stmt.value, ast.Constant)
            and isinstance(stmt.value.value, str)
        ):
            members[stmt.targets[0].id] = stmt.value.value
    return members


def _extract_method_body(cls: ast.ClassDef, name: str, file_path: Path) -> str:
    for stmt in cls.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)) and stmt.name == name:
            # Drop the docstring so wording differences do not trip the check.
            body = stmt.body[1:] if ast.get_docstring(stmt) is not None else stmt.body
            return "\n".join(ast.unparse(node) for node in body)
    raise ValueError(f"{file_path}: RerunPolicy has no method {name!r}.")


def _build_surface(file_path: Path) -> dict[str, object]:
    cls = _find_class(
        ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path)), "RerunPolicy", file_path
    )
    return {
        "members": _extract_members(cls),
        "methods": {name: _extract_method_body(cls, name, file_path) for name in BEHAVIOR_METHODS},
    }


def main() -> int:
    try:
        core_surface = _build_surface(CORE_FILE)
        sdk_surface = _build_surface(SDK_FILE)
    except ValueError as exc:
        console.print(f"[red]Could not read the RerunPolicy definitions:[/red] {exc}")
        return 1

    if core_surface == sdk_surface:
        return 0

    console.print("[red]RerunPolicy is out of sync between core and the Task SDK.[/red]\n")
    for key in sorted(set(core_surface) | set(sdk_surface)):
        core_val = core_surface.get(key, "<missing>")
        sdk_val = sdk_surface.get(key, "<missing>")
        if core_val != sdk_val:
            console.print(f"[red]-> {key}: core={core_val!r} sdk={sdk_val!r}[/red]")
    console.print(f"\nMake both copies match:\n  core: {CORE_FILE}\n  sdk:  {SDK_FILE}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
