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
"""Test that the sdk package root's export surfaces stay consistent with each other."""

from __future__ import annotations

import ast
from pathlib import Path

import airflow.sdk


def _parse_sdk_init() -> tuple[dict[str, tuple[str, str]], set[str], set[str], set[str]]:
    """Parse the sdk package root into its lazy imports, __all__, TYPE_CHECKING and annotated names."""
    source = Path(airflow.sdk.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    lazy_imports: dict[str, tuple[str, str]] = {}
    all_names: set[str] = set()
    type_checking_names: set[str] = set()
    annotated_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if getattr(target, "id", "") == "__all__":
                    all_names = set(ast.literal_eval(node.value))
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            annotated_names.add(node.target.id)
            if node.target.id == "__lazy_imports":
                lazy_imports = ast.literal_eval(node.value)
        if isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING":
            for sub in ast.walk(node):
                if isinstance(sub, ast.ImportFrom):
                    type_checking_names.update(alias.asname or alias.name for alias in sub.names)
    return lazy_imports, all_names, type_checking_names, annotated_names


def test_lazy_imports_are_declared_in_all() -> None:
    """Every lazy import must be part of __all__ so it is part of the declared public API."""
    lazy_imports, all_names, _, _ = _parse_sdk_init()
    undeclared = set(lazy_imports) - all_names
    assert undeclared == set(), f"lazy imports missing from __all__: {sorted(undeclared)}"


def test_lazy_imports_are_reexported_for_type_checkers() -> None:
    """Every lazy import must be re-exported by the TYPE_CHECKING block, or declared as a module-level annotated name like conf."""
    lazy_imports, _, type_checking_names, annotated_names = _parse_sdk_init()
    undeclared = set(lazy_imports) - (type_checking_names | annotated_names)
    assert undeclared == set(), f"lazy imports missing from the TYPE_CHECKING block: {sorted(undeclared)}"
