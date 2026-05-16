#!/usr/bin/env python3
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

from __future__ import annotations

import ast
import sys
from pathlib import Path


def class_extends_error_mixin(
    class_def: ast.ClassDef,
    class_registry: dict[str, list[tuple[str, ast.ClassDef]]],
    current_package: str,
    visited: set[str] | None = None,
) -> bool:
    """Return True if class def found to extend AirflowErrorCodeMixin, False otherwise."""
    if visited is None:
        visited = set()

    if class_def.name in visited:
        return False
    visited.add(class_def.name)

    for base in class_def.bases:
        node = base.value if isinstance(base, ast.Subscript) else base
        if isinstance(node, ast.Name) and node.id == "AirflowErrorCodeMixin":
            return True

    for base in class_def.bases:
        node = base.value if isinstance(base, ast.Subscript) else base
        if isinstance(node, ast.Name):
            parent_class_name = node.id
            parent_candidates: list[tuple[str, ast.ClassDef]] = class_registry.get(parent_class_name, [])
            parent_def = None
            for package, candidate in parent_candidates:
                if package == current_package:
                    parent_def = candidate
                    break
            if parent_def:
                if class_extends_error_mixin(
                    class_def=parent_def,
                    class_registry=class_registry,
                    current_package=current_package,
                    visited=visited,
                ):
                    return True

    return False


def extract_static_properties(class_def: ast.ClassDef, required_props: list[str]) -> dict:
    """Extract class level static properties."""
    properties = {}
    for stmt in class_def.body:
        if not isinstance(stmt, ast.Assign):
            continue
        for target in stmt.targets:
            if isinstance(target, ast.Name) and target.id in required_props:
                try:
                    properties[target.id] = ast.literal_eval(stmt.value)
                except Exception:
                    properties[target.id] = ""
    return properties


def check_raise_statement(
    node: ast.Raise,
    filename: str,
    line_no: int,
    class_registry: dict[str, list[tuple[str, ast.ClassDef]]],
    current_package: str,
) -> list[str]:
    """Gather raise statements and check respective exception classes for missing properties."""
    errors: list[str] = []

    if node.exc is None or not isinstance(node.exc, ast.Call):
        return errors

    func_name = getattr(node.exc.func, "id", getattr(node.exc.func, "attr", None))
    if not func_name:
        return errors

    matching_classes: list[tuple[str, ast.ClassDef]] = class_registry.get(func_name, [])
    class_def: ast.ClassDef | None = None
    for package, package_class_def in matching_classes:
        if package == current_package:
            class_def = package_class_def
            break

    if class_def and class_extends_error_mixin(class_def, class_registry, current_package):
        required_props = ["user_facing_error_message", "first_steps", "description", "documentation"]
        properties = extract_static_properties(class_def, required_props)

        for prop in required_props:
            if not properties.get(prop):
                errors.append(
                    f"{filename}:{line_no} [{current_package}] "
                    f"{prop} is missing in exception class {func_name}."
                )

    return errors


def get_package_name(filepath: Path) -> str:
    """Identify if a file belongs to tasksdk or airflow-core."""
    parts = filepath.parts
    if "task-sdk" in parts or "sdk" in parts:
        return "task-sdk"
    if "airflow-core" in parts or "core" in parts:
        return "airflow-core"
    if "providers" in parts:
        return "providers"
    raise ValueError("Unsupported package name")


def validate_files(paths: list[Path]) -> list[str]:
    """This acts as the entrypoint function for this static check."""
    errors = []

    class_registry: dict[str, list[tuple[str, ast.ClassDef]]] = {}
    raise_statements: list[tuple[ast.Raise, str, str]] = []

    for filepath in paths:
        if not filepath.suffix == ".py":
            continue
        try:
            tree = ast.parse(filepath.read_text(encoding="utf-8"), filename=str(filepath))
            package_name: str = get_package_name(filepath)

            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    if node.name not in class_registry:
                        class_registry[node.name] = []
                    class_registry[node.name].append((package_name, node))

                elif isinstance(node, ast.Raise):
                    raise_statements.append((node, str(filepath), package_name))

        except Exception as e:
            errors.append(f"{filepath}: Error parsing file: {e}")

    for raise_node, filename, current_package in raise_statements:
        errors.extend(
            check_raise_statement(
                node=raise_node,
                filename=filename,
                line_no=raise_node.lineno,
                class_registry=class_registry,
                current_package=current_package,
            )
        )

    return errors


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: check_airflow_error_codes.py <file1.py> [file2.py ...]", file=sys.stderr)
        sys.exit(1)

    validation_errors = validate_files([Path(arg) for arg in sys.argv[1:]])
    if validation_errors:
        for error in validation_errors:
            print(error)
        sys.exit(1)

    sys.exit(0)
