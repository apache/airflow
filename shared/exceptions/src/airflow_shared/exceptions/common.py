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
from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any


class AirflowErrorCodeMixin:
    """Mixin class providing ability to pass error_code to an exception."""

    user_facing_error_message: str | None = None
    first_steps: str | None = None
    description: str | None = None
    documentation: str | None = None

    @staticmethod
    def get_error_code(class_name):
        class_name = re.sub(r"^(Airflow)|Exception$", "", class_name)
        parts = re.findall(r"[A-Z]+(?=[A-Z][a-z]|$)|[A-Z][a-z0-9]*", class_name)
        return f"AERR-{'-'.join(part.upper() for part in parts)}"

    @property
    def error_code(self) -> str:
        class_name = self.__class__.__name__
        return self.get_error_code(class_name)

    def __str__(self):
        base = super().__str__()
        error_code_str = f"[{self.error_code}]"
        return f"{error_code_str} {base}" if base else error_code_str

    def __repr__(self):
        args_repr = ", ".join(repr(a) for a in self.args)
        return f"{type(self).__name__}({args_repr}, error_code={self.error_code!r})"


def _get_literal_value(node: ast.expr) -> Any:
    try:
        return ast.literal_eval(node)
    except ValueError:
        return f"<Unresolved AST: {type(node).__name__}>"


def _is_exception_subclass(inheritance_dict: dict, class_name: str, visited: set) -> bool:
    """Return True if class is subclass of an exception class, False otherwise."""
    if "Exception" in class_name or "Error" in class_name:
        return True
    if class_name in visited:
        return False

    visited.add(class_name)
    parents = inheritance_dict.get(class_name, [])
    for parent in parents:
        if _is_exception_subclass(inheritance_dict, parent, visited):
            return True
    return False


def _has_error_code_mixin(
    inheritance_dict: dict, imported_names: dict, class_name: str, visited: set[str] | None = None
) -> bool:
    """Return True if class inherits from AirflowErrorCodeMixin, False otherwise."""
    if visited is None:
        visited = set()

    # check for current class name
    if class_name == "AirflowErrorCodeMixin" or class_name.endswith(".AirflowErrorCodeMixin"):
        return True
    resolved_current = imported_names.get(class_name, class_name)
    if resolved_current == "AirflowErrorCodeMixin":
        return True

    if class_name in visited:
        return False
    visited.add(class_name)

    # recursively check for parent class names
    parents = inheritance_dict.get(class_name, [])
    for parent in parents:
        if parent == "AirflowErrorCodeMixin" or parent.endswith(".AirflowErrorCodeMixin"):
            return True
        if imported_names.get(parent) == "AirflowErrorCodeMixin":
            return True
        resolved_parent = imported_names.get(parent, parent)
        if _has_error_code_mixin(inheritance_dict, imported_names, resolved_parent, visited):
            return True

    return False


def extract_error_metadata(base_path: Path, error_meta_props: list[str]) -> list[dict[str, Any]]:
    """Fetch exception classes using AirflowErrorCodeMixin, extract error meta properties and return as a dict."""
    error_meta_list = []
    file_paths = base_path.rglob("*.py")
    for file_path in file_paths:
        try:
            with open(file_path, encoding="utf-8") as f:
                root = ast.parse(f.read(), filename=str(file_path))
        except (SyntaxError, UnicodeDecodeError):
            continue

        imported_names = {}
        inheritance_map = {}
        class_nodes: list[ast.ClassDef] = []

        for node in ast.walk(root):
            # fill imported names
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported_names[alias.asname or alias.name] = alias.name
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    imported_names[alias.asname or alias.name] = alias.name

            # fill inheritance map
            if isinstance(node, ast.ClassDef):
                class_nodes.append(node)
                bases = []
                for base in node.bases:
                    if isinstance(base, ast.Name):
                        bases.append(base.id)
                    elif isinstance(base, ast.Attribute) and isinstance(base.value, ast.Name):
                        bases.append(f"{base.value.id}.{base.attr}")
                inheritance_map[node.name] = bases

        # fill error_meta_list based on available props
        for node in class_nodes:
            if _is_exception_subclass(inheritance_map, node.name, set()) and _has_error_code_mixin(
                inheritance_map, imported_names, node.name
            ):
                error_meta = {}
                for body_node in node.body:
                    if isinstance(body_node, ast.Assign):
                        # multiple targets possible for ast.Assign
                        for target in body_node.targets:
                            if isinstance(target, ast.Name) and target.id in error_meta_props:
                                error_meta[target.id] = _get_literal_value(body_node.value)
                    elif isinstance(body_node, ast.AnnAssign):
                        # only single target for ast.AnnAssign, always
                        if isinstance(body_node.target, ast.Name) and body_node.target.id in error_meta_props:
                            if body_node.value:
                                error_meta[body_node.target.id] = _get_literal_value(body_node.value)
                error_meta["exception_type"] = node.name
                error_meta["error_code"] = AirflowErrorCodeMixin.get_error_code(node.name)
                error_meta_list.append(error_meta)

    return error_meta_list


def get_error_meta_list(paths: list[Path]):
    """Return unique list of error code metadata extracted from relevant exception classes."""
    error_meta_dict = {}
    required_props = [
        "user_facing_error_message",
        "first_steps",
        "description",
        "documentation",
        "exception_type",
        "error_code",
    ]
    for class_path in paths:
        error_meta_list = extract_error_metadata(
            class_path,
            required_props,
        )
        # gather items into error meta dict with error code as key for uniqueness
        for item in error_meta_list:
            missing_props = set(required_props) - set(item)
            if missing_props:
                raise ValueError(f"Missing error meta properties {', '.join(missing_props)} in {item}")
            error_code = item["error_code"]
            error_meta_dict[error_code] = item
    return list(error_meta_dict.values())
