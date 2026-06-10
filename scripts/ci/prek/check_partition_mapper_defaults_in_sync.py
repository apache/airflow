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
Verify partition-mapper definitions stay in sync between core and the Task SDK.

Checks two things:

1. ``FanOutMapper.default_downstream_mapper_by_window_name`` — the default
   downstream-mapper table is defined twice (core and SDK) because the two
   hierarchies are independent. Both copies must list the same
   ``Window`` name -> mapper class mapping.

2. ``SegmentWindow`` and ``FixedKeyMapper`` — for each class, the field-name
   set and the ``raise ValueError(...)`` message-template set must be identical
   between core and the SDK. This catches wording drift (e.g. "non-empty" vs
   "non-empty strings") that would otherwise silently diverge.

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/check_partition_mapper_defaults_in_sync.py

Exits 0 if everything matches, 1 (with a diff) otherwise.
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

CORE_WINDOW_FILE = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "partition_mappers" / "window.py"
SDK_WINDOW_FILE = (
    AIRFLOW_TASK_SDK_SOURCES_PATH / "airflow" / "sdk" / "definitions" / "partition_mappers" / "window.py"
)
CORE_FIXED_KEY_FILE = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "partition_mappers" / "fixed_key.py"
SDK_FIXED_KEY_FILE = (
    AIRFLOW_TASK_SDK_SOURCES_PATH / "airflow" / "sdk" / "definitions" / "partition_mappers" / "fixed_key.py"
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


def _joinedstr_to_template(node: ast.JoinedStr) -> str:
    """
    Convert an f-string AST node to a template string.

    Literal text fragments are kept as-is; each interpolated expression
    ``{...}`` is replaced by a ``{}`` placeholder. This lets us compare
    f-string message templates between core and SDK without caring about the
    exact expression inside the braces (e.g. ``{type(item).__name__!r}`` vs
    a future refactoring).
    """
    parts: list[str] = []
    for value in node.values:
        if isinstance(value, ast.Constant):
            parts.append(str(value.value))
        else:
            parts.append("{}")
    return "".join(parts)


def _extract_raise_messages(subtree: ast.AST) -> set[str]:
    """
    Collect ``raise ValueError(...)`` message templates from an arbitrary AST subtree.

    For each ``raise ValueError(...)`` whose first argument is a plain string
    constant, an f-string, or adjacent f-string/constant concatenation
    (``BinOp(Add, ...)``), extracts the template:

    - ``ast.Constant`` → the literal string value.
    - ``ast.JoinedStr`` (f-string) → literal fragments joined, interpolated
      expressions replaced by ``{}``.
    - ``ast.BinOp(Add, ...)`` → recursively concatenated from both sides.
    """
    messages: set[str] = set()

    def _collect_bin(n: ast.expr) -> str:
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            return n.value
        if isinstance(n, ast.JoinedStr):
            return _joinedstr_to_template(n)
        if isinstance(n, ast.BinOp) and isinstance(n.op, ast.Add):
            return _collect_bin(n.left) + _collect_bin(n.right)
        return "{}"

    for node in ast.walk(subtree):
        if not isinstance(node, ast.Raise):
            continue
        exc = node.exc
        if exc is None:
            continue
        if not (
            isinstance(exc, ast.Call)
            and isinstance(exc.func, ast.Name)
            and exc.func.id == "ValueError"
            and exc.args
        ):
            continue
        arg = exc.args[0]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            messages.add(arg.value)
        elif isinstance(arg, ast.JoinedStr):
            messages.add(_joinedstr_to_template(arg))
        elif isinstance(arg, ast.BinOp) and isinstance(arg.op, ast.Add):
            messages.add(_collect_bin(arg))

    return messages


def _collect_converter_names(class_node: ast.ClassDef) -> set[str]:
    """
    Return function names referenced via ``converter=<Name>`` in attrs field definitions.

    Scans the top-level statements of *class_node* for annotated assignments of
    the form ``<name>: <ann> = attrs.field(converter=<Name>)`` (or
    ``attr.field`` / bare ``field(...)``).  Also handles unannotated
    ``Assign`` nodes with an ``attrs.field(converter=<Name>)`` call on the RHS.
    Only bare ``ast.Name`` references are collected; lambdas and attribute
    lookups are ignored (they cannot resolve to a module-level function by
    name).
    """
    names: set[str] = set()

    def _check_call(call_node: ast.expr) -> None:
        """If *call_node* is an attrs.field / attr.field / field() call, collect converter=<Name>."""
        if not isinstance(call_node, ast.Call):
            return
        func = call_node.func
        is_field_call = (
            # attrs.field(...) or attr.field(...)
            (isinstance(func, ast.Attribute) and func.attr == "field")
            # bare field(...)
            or (isinstance(func, ast.Name) and func.id == "field")
        )
        if not is_field_call:
            return
        for kw in call_node.keywords:
            if kw.arg == "converter" and isinstance(kw.value, ast.Name):
                names.add(kw.value.id)

    for stmt in class_node.body:
        if isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
            _check_call(stmt.value)
        elif isinstance(stmt, ast.Assign):
            for target_value in [stmt.value]:
                _check_call(target_value)

    return names


def extract_class_error_messages(file_path: Path, class_name: str) -> set[str]:
    """
    Return the set of ``raise ValueError(...)`` message templates for *class_name*.

    Scans two sources:

    1. The class body itself (all depths) — covers validator methods and nested
       helpers declared inside the class.
    2. Module-level functions referenced via ``converter=<Name>`` in
       ``attrs.field(...)`` declarations inside the class body.  These
       converters live outside the class but are logically part of its
       construction-time validation.

    For each ``raise ValueError(...)`` whose first argument is a plain string
    constant or an f-string, extracts the template:

    - ``ast.Constant`` → the literal string value.
    - ``ast.JoinedStr`` (f-string) → literal fragments joined, interpolated
      expressions replaced by ``{}``.
    """
    tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))

    target_class: ast.ClassDef | None = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            target_class = node
            break

    if target_class is None:
        return set()

    # 1. Collect messages from the class body.
    messages = _extract_raise_messages(target_class)

    # 2. Follow converter= references to module-level functions.
    converter_names = _collect_converter_names(target_class)
    if converter_names:
        # Only look at top-level (module-body) function definitions to avoid
        # accidentally matching a same-named method inside another class.
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in converter_names:
                messages |= _extract_raise_messages(node)

    return messages


def extract_class_field_names(file_path: Path, class_name: str) -> set[str]:
    """
    Return the set of annotated attribute names declared in *class_name*'s body.

    Collects names from annotated assignments (``name: type = ...`` or
    ``name: type``) at the top level of the class body. ClassVar annotations
    are excluded because they are class-level constants, not instance fields.
    """
    tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    fields: set[str] = set()

    for node in ast.walk(tree):
        if not (isinstance(node, ast.ClassDef) and node.name == class_name):
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.AnnAssign):
                continue
            if not isinstance(stmt.target, ast.Name):
                continue
            # Skip ClassVar[...] annotations
            ann = stmt.annotation
            is_classvar = False
            if isinstance(ann, ast.Subscript):
                if isinstance(ann.value, ast.Name) and ann.value.id == "ClassVar":
                    is_classvar = True
                elif isinstance(ann.value, ast.Attribute) and ann.value.attr == "ClassVar":
                    is_classvar = True
            elif isinstance(ann, ast.Name) and ann.id == "ClassVar":
                is_classvar = True
            if is_classvar:
                continue
            fields.add(stmt.target.id)
        break

    return fields


def main() -> int:
    failed = False

    try:
        core_mapping = _extract_mapping(CORE_FILE)
        sdk_mapping = _extract_mapping(SDK_FILE)
    except ValueError as exc:
        console.print(f"[red]Could not read the default mapper table:[/red] {exc}")
        return 1

    if core_mapping != sdk_mapping:
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
        failed = True

    checks = [
        ("SegmentWindow", CORE_WINDOW_FILE, SDK_WINDOW_FILE),
        ("FixedKeyMapper", CORE_FIXED_KEY_FILE, SDK_FIXED_KEY_FILE),
    ]

    for class_name, core_file, sdk_file in checks:
        core_fields = extract_class_field_names(core_file, class_name)
        sdk_fields = extract_class_field_names(sdk_file, class_name)
        if core_fields != sdk_fields:
            console.print(f"[red]{class_name}: field names are out of sync between core and SDK.[/red]")
            core_only = core_fields - sdk_fields
            sdk_only = sdk_fields - core_fields
            if core_only:
                console.print(f"  core-only fields: {sorted(core_only)}")
            if sdk_only:
                console.print(f"  sdk-only  fields: {sorted(sdk_only)}")
            console.print(f"  core: {core_file}\n  sdk:  {sdk_file}")
            failed = True

        core_msgs = extract_class_error_messages(core_file, class_name)
        sdk_msgs = extract_class_error_messages(sdk_file, class_name)
        if core_msgs != sdk_msgs:
            console.print(
                f"[red]{class_name}: raise ValueError(...) message templates are out of sync.[/red]"
            )
            core_only = core_msgs - sdk_msgs
            sdk_only = sdk_msgs - core_msgs
            if core_only:
                console.print(f"  core-only messages: {sorted(core_only)}")
            if sdk_only:
                console.print(f"  sdk-only  messages: {sorted(sdk_only)}")
            console.print(f"  core: {core_file}\n  sdk:  {sdk_file}")
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
