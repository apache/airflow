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
Verify the shared surface of ``window.py`` stays in sync between core and the Task SDK.

The ``Window`` class hierarchy is defined twice — once in core and once in the SDK —
because the two hierarchies are independent (the SDK cannot import core). The SDK copy
is the author-facing API used at Dag-parse time; the core copy carries the scheduler-side
runtime logic. Both must agree on the shared surface so serialization round-trips and
direction semantics behave identically wherever the classes are instantiated.

This check parses both files via AST and asserts the following are identical:

- ``Window.Direction`` enum member values
- The set of shared class names (Direction, Window, HourWindow, DayWindow,
  WeekWindow, MonthWindow, QuarterWindow, YearWindow)
- ``Window.__init__`` ``direction`` kwarg default
- ``Window.__init__`` body, ``Window.serialize`` body, ``Window.deserialize`` body
- Each subclass ``expected_decoded_type`` ClassVar value

Core-only symbols (``ABC`` base, ``abstractmethod to_upstream``, helper functions,
imports, docstrings) are intentionally excluded from the comparison.

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/check_window_in_sync.py

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

CORE_FILE = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "partition_mappers" / "window.py"
SDK_FILE = (
    AIRFLOW_TASK_SDK_SOURCES_PATH / "airflow" / "sdk" / "definitions" / "partition_mappers" / "window.py"
)

EXPECTED_CLASS_NAMES = [
    "Direction",
    "Window",
    "HourWindow",
    "DayWindow",
    "WeekWindow",
    "MonthWindow",
    "QuarterWindow",
    "YearWindow",
]


def _parse(file_path: Path) -> ast.Module:
    return ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))


def _find_class(tree: ast.Module, name: str, file_path: Path) -> ast.ClassDef:
    """Return the ClassDef named *name* from *tree*; raise ValueError if absent."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    raise ValueError(f"{file_path}: no class {name!r} found.")


def _extract_enum_members(tree: ast.Module, file_path: Path) -> dict[str, str]:
    """Return the ``{member_name: value}`` dict for ``Direction``."""
    cls = _find_class(tree, "Direction", file_path)
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


def _find_method(
    class_def: ast.ClassDef, method_name: str, file_path: Path
) -> ast.FunctionDef | ast.AsyncFunctionDef:
    """Return the method named *method_name* from *class_def*; raise ValueError if absent."""
    for stmt in class_def.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)) and stmt.name == method_name:
            return stmt
    raise ValueError(f"{file_path}: class {class_def.name!r} has no method {method_name!r}.")


def _extract_method_source(class_def: ast.ClassDef, method_name: str, file_path: Path) -> str:
    """Return the unparsed body of *method_name* in *class_def*."""
    method = _find_method(class_def, method_name, file_path)
    return "\n".join(ast.unparse(stmt) for stmt in method.body)


def _extract_direction_default(class_def: ast.ClassDef, file_path: Path) -> str:
    """Return the ``ast.unparse`` of the ``direction`` kwarg default in ``Window.__init__``."""
    init = _find_method(class_def, "__init__", file_path)
    args = init.args
    for i, kwarg in enumerate(args.kwonlyargs):
        if kwarg.arg == "direction":
            default = args.kw_defaults[i]
            if default is None:
                raise ValueError(
                    f"{file_path}: Window.__init__ 'direction' kwarg has no default — "
                    "expected Direction.FORWARD."
                )
            return ast.unparse(default)
    raise ValueError(f"{file_path}: Window.__init__ has no 'direction' kwonly parameter.")


def _extract_decoded_types(tree: ast.Module, file_path: Path) -> dict[str, str]:
    """Return ``{class_name: ast.unparse(expected_decoded_type value)}`` for the Window class hierarchy."""
    result: dict[str, str] = {}
    for name in EXPECTED_CLASS_NAMES:
        try:
            cls = _find_class(tree, name, file_path)
        except ValueError:
            continue
        for stmt in cls.body:
            target_name = None
            value_node = None
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                target_name = stmt.target.id
                value_node = stmt.value
            elif (
                isinstance(stmt, ast.Assign)
                and len(stmt.targets) == 1
                and isinstance(stmt.targets[0], ast.Name)
            ):
                target_name = stmt.targets[0].id
                value_node = stmt.value
            if target_name == "expected_decoded_type" and value_node is not None:
                result[name] = ast.unparse(value_node)
                break
    return result


def _build_surface(file_path: Path) -> dict[str, object]:
    """Build the comparable surface dict for one ``window.py`` file."""
    tree = _parse(file_path)
    present_classes = sorted(
        name
        for name in EXPECTED_CLASS_NAMES
        if any(isinstance(node, ast.ClassDef) and node.name == name for node in ast.walk(tree))
    )
    window_cls = _find_class(tree, "Window", file_path)
    return {
        "class_names": present_classes,
        "enum_members": _extract_enum_members(tree, file_path),
        "direction_default": _extract_direction_default(window_cls, file_path),
        "init_body": _extract_method_source(window_cls, "__init__", file_path),
        "serialize_body": _extract_method_source(window_cls, "serialize", file_path),
        "deserialize_body": _extract_method_source(window_cls, "deserialize", file_path),
        "decoded_types": _extract_decoded_types(tree, file_path),
    }


def main() -> int:
    try:
        core_surface = _build_surface(CORE_FILE)
        sdk_surface = _build_surface(SDK_FILE)
    except ValueError as exc:
        console.print(f"[red]Could not read the window definitions:[/red] {exc}")
        return 1

    if core_surface == sdk_surface:
        return 0

    console.print("[red]window.py shared surface is out of sync between core and the Task SDK.[/red]\n")
    all_keys = sorted(set(core_surface) | set(sdk_surface))
    for key in all_keys:
        core_val = core_surface.get(key, "<missing>")
        sdk_val = sdk_surface.get(key, "<missing>")
        marker = "  " if core_val == sdk_val else "->"
        color = "" if core_val == sdk_val else "[red]"
        end = "" if core_val == sdk_val else "[/red]"
        console.print(f"{color}{marker} {key}: core={core_val!r} sdk={sdk_val!r}{end}")
    console.print(f"\nMake both copies match:\n  core: {CORE_FILE}\n  sdk:  {SDK_FILE}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
