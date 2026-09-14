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
#   "rich>=13.6.0",
# ]
# ///
"""
Ensure every common.ai operator is listed in ``docs/operators/index.rst``.

The "Choosing the right operator" table in that file is hand-maintained and has already
silently gone stale once — ``LLMSchemaCompareOperator`` was missing from it until #71738.
Nothing else in CI catches this class of gap.

Only classes *defined in* the operators modules are considered, so imported helpers
(mixins, ``BaseOperator`` itself, pydantic models, operator links) are never flagged.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.resolve()))

from common_prek_utils import AIRFLOW_PROVIDERS_ROOT_PATH

console = Console(color_system="standard", width=200)

PROVIDER_PATH = AIRFLOW_PROVIDERS_ROOT_PATH / "common" / "ai"
OPERATORS_PATH = PROVIDER_PATH / "src" / "airflow" / "providers" / "common" / "ai" / "operators"
INDEX_RST = PROVIDER_PATH / "docs" / "operators" / "index.rst"

# Bases that mark a class as an operator. Local operators subclass each other (e.g.
# LLMFileAnalysisOperator -> LLMOperator), so any name ending in "Operator" counts as a base too.
_OPERATOR_BASE_SUFFIX = "Operator"
# Classes that are operator-like by name but are not operators users would document.
_EXCLUDED = {"BaseOperator"}


def extract_base_names(class_def: ast.ClassDef) -> list[str]:
    names = []
    for base in class_def.bases:
        if isinstance(base, ast.Name):
            names.append(base.id)
        elif isinstance(base, ast.Attribute):
            names.append(base.attr)
    return names


def find_operator_classes() -> list[str]:
    """Return operator class names defined directly in the operators package."""
    operators: list[str] = []
    for module_path in sorted(OPERATORS_PATH.glob("*.py")):
        if module_path.name == "__init__.py":
            continue
        tree = ast.parse(module_path.read_text(), filename=str(module_path))
        for node in tree.body:
            if not isinstance(node, ast.ClassDef) or node.name in _EXCLUDED:
                continue
            if any(base.endswith(_OPERATOR_BASE_SUFFIX) for base in extract_base_names(node)):
                operators.append(node.name)
    return operators


def main() -> int:
    if not INDEX_RST.is_file():
        console.print(f"[red]Cannot find {INDEX_RST}[/]")
        return 1

    index_contents = INDEX_RST.read_text()
    # Whole-word match: a bare substring check would let LLMOperator pass on a page that only
    # mentions LLMBranchOperator, and would not notice a name gaining a typo'd suffix.
    undocumented_operators = sorted(
        name for name in find_operator_classes() if not re.search(rf"\b{re.escape(name)}\b", index_contents)
    )

    if undocumented_operators:
        console.print()
        for name in undocumented_operators:
            console.print(f"  [red]✗[/] {name} is not mentioned in {INDEX_RST}")
        console.print()
        console.print(
            "[red]Add the operator(s) above to the 'Choosing the right operator' table "
            f"in {INDEX_RST.relative_to(AIRFLOW_PROVIDERS_ROOT_PATH.parent)}.[/]"
        )
        return 1

    console.print("[green]All common.ai operators are listed in the docs index.[/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
