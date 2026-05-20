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
"""
Verify that the duplicate ``BaseTaskInstanceDTO`` definitions in airflow-core
and task-sdk stay structurally identical.

``BaseTaskInstanceDTO`` is duplicated (not shared) in:

- ``airflow-core/src/airflow/executors/workloads/task.py``
- ``task-sdk/src/airflow/sdk/execution_time/workloads/task.py``

This hook compares the *fields* (annotated assignments) and bases of both
``BaseTaskInstanceDTO`` classes. The concrete ``TaskInstanceDTO`` subclasses
in each file are allowed to differ (airflow-core adds an executor-specific
``key`` property that depends on ``airflow.models``, which the Task SDK
does not have access to).
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

AIRFLOW_ROOT = Path(__file__).parents[3].resolve()
CORE_FILE = AIRFLOW_ROOT / "airflow-core" / "src" / "airflow" / "executors" / "workloads" / "task.py"
SDK_FILE = AIRFLOW_ROOT / "task-sdk" / "src" / "airflow" / "sdk" / "execution_time" / "workloads" / "task.py"
CLASS_NAME = "BaseTaskInstanceDTO"


def _find_class(tree: ast.AST, class_name: str) -> ast.ClassDef | None:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return node
    return None


def _field_signature(class_node: ast.ClassDef) -> list[tuple[str, str, str | None]]:
    """Return a normalized list of ``(name, annotation, default)`` for each field."""
    fields: list[tuple[str, str, str | None]] = []
    for stmt in class_node.body:
        if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            name = stmt.target.id
            annotation = ast.unparse(stmt.annotation)
            default = ast.unparse(stmt.value) if stmt.value is not None else None
            fields.append((name, annotation, default))
    return fields


def _bases(class_node: ast.ClassDef) -> list[str]:
    return [ast.unparse(base) for base in class_node.bases]


def _extract(file_path: Path) -> tuple[list[str], list[tuple[str, str, str | None]]]:
    source = file_path.read_text()
    tree = ast.parse(source, filename=str(file_path))
    class_node = _find_class(tree, CLASS_NAME)
    if class_node is None:
        print(f"ERROR: Could not find class {CLASS_NAME} in {file_path}", file=sys.stderr)
        sys.exit(1)
    return _bases(class_node), _field_signature(class_node)


def main() -> None:
    core_bases, core_fields = _extract(CORE_FILE)
    sdk_bases, sdk_fields = _extract(SDK_FILE)

    if core_bases == sdk_bases and core_fields == sdk_fields:
        sys.exit(0)

    print(
        f"\nERROR: {CLASS_NAME} definitions in airflow-core and task-sdk are out of sync!",
        file=sys.stderr,
    )
    print(f"\n  airflow-core: {CORE_FILE.relative_to(AIRFLOW_ROOT)}", file=sys.stderr)
    print(f"  task-sdk:     {SDK_FILE.relative_to(AIRFLOW_ROOT)}", file=sys.stderr)

    if core_bases != sdk_bases:
        print("\nClass bases differ:", file=sys.stderr)
        print(f"  airflow-core: {core_bases}", file=sys.stderr)
        print(f"  task-sdk:     {sdk_bases}", file=sys.stderr)

    if core_fields != sdk_fields:
        core_set = {f[0]: f for f in core_fields}
        sdk_set = {f[0]: f for f in sdk_fields}
        only_in_core = sorted(set(core_set) - set(sdk_set))
        only_in_sdk = sorted(set(sdk_set) - set(core_set))
        differing = sorted(name for name in set(core_set) & set(sdk_set) if core_set[name] != sdk_set[name])
        if only_in_core:
            print(f"\n  Fields only in airflow-core: {only_in_core}", file=sys.stderr)
        if only_in_sdk:
            print(f"\n  Fields only in task-sdk: {only_in_sdk}", file=sys.stderr)
        for name in differing:
            print(
                f"\n  Field {name!r} differs:"
                f"\n    airflow-core: {core_set[name]}"
                f"\n    task-sdk:     {sdk_set[name]}",
                file=sys.stderr,
            )

    print(
        f"\nUpdate both files together so the two {CLASS_NAME} definitions stay in sync.",
        file=sys.stderr,
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
