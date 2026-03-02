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

"""
Validate that TYPE_CHECKING block in sdk.py matches runtime import maps.

This pre-commit hook ensures that:
1. All items in _IMPORT_MAP, _RENAME_MAP, and _MODULE_MAP are in TYPE_CHECKING
2. All items in TYPE_CHECKING are in one of the runtime maps
3. No mismatches between type hints and runtime behavior

Usage:
    python scripts/ci/prek/check_common_compat_lazy_imports.py
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


def extract_runtime_maps(py_file: Path) -> tuple[set[str], set[str], set[str]]:
    """
    Extract all names from _IMPORT_MAP, _RENAME_MAP, and _MODULE_MAP.

    Returns tuple of (import_names, rename_names, module_names)
    """
    content = py_file.read_text()
    tree = ast.parse(content)

    import_map = set()
    rename_map = set()
    module_map = set()

    for node in tree.body:
        # Handle both annotated assignments and regular assignments
        targets: list[ast.Name | ast.expr] = []
        value = None

        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            targets = [node.target]
            value = node.value
        elif isinstance(node, ast.Assign):
            targets = node.targets
            value = node.value

        for target in targets:
            if not isinstance(target, ast.Name):
                continue

            if target.id == "_IMPORT_MAP" and value:
                data = ast.literal_eval(value)
                import_map = set(data.keys())
            elif target.id == "_RENAME_MAP" and value:
                data = ast.literal_eval(value)
                rename_map = set(data.keys())
            elif target.id == "_MODULE_MAP" and value:
                data = ast.literal_eval(value)
                module_map = set(data.keys())

    return import_map, rename_map, module_map


def extract_type_checking_names(py_file: Path) -> set[str]:
    """
    Extract all imported names from TYPE_CHECKING block.
    """
    content = py_file.read_text()
    tree = ast.parse(content)

    type_checking_names = set()

    for node in ast.walk(tree):
        # Find: if TYPE_CHECKING:
        if isinstance(node, ast.If):
            # Check if condition is comparing TYPE_CHECKING
            if isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING":
                # Extract all imports in this block
                for stmt in node.body:
                    if isinstance(stmt, ast.ImportFrom):
                        # from X import Y as Z
                        for alias in stmt.names:
                            # Use alias.asname if present, else alias.name
                            name = alias.asname if alias.asname else alias.name
                            type_checking_names.add(name)
                    elif isinstance(stmt, ast.Import):
                        # import X as Y or import X.Y as Z
                        for alias in stmt.names:
                            # For module imports like "import airflow.sdk.io as io"
                            # Use the alias name (io) not the full module path
                            name = alias.asname if alias.asname else alias.name.split(".")[-1]
                            type_checking_names.add(name)

    return type_checking_names


def main():
    """Validate TYPE_CHECKING block matches runtime maps."""
    sdk_py = (
        Path(__file__).parent.parent.parent.parent
        / "providers"
        / "common"
        / "compat"
        / "src"
        / "airflow"
        / "providers"
        / "common"
        / "compat"
        / "sdk.py"
    )

    if not sdk_py.exists():
        print(f"❌ ERROR: {sdk_py} not found")
        sys.exit(1)

    # Extract runtime maps
    import_names, rename_names, module_names = extract_runtime_maps(sdk_py)
    runtime_names = import_names | rename_names | module_names

    # Extract TYPE_CHECKING imports
    type_checking_names = extract_type_checking_names(sdk_py)

    # Check for discrepancies
    missing_in_type_checking = runtime_names - type_checking_names
    extra_in_type_checking = type_checking_names - runtime_names

    errors = []

    if missing_in_type_checking:
        errors.append("\n❌ Items in runtime maps but MISSING in TYPE_CHECKING block:")
        for name in sorted(missing_in_type_checking):
            # Determine which map it's from
            map_name = []
            if name in import_names:
                map_name.append("_IMPORT_MAP")
            if name in rename_names:
                map_name.append("_RENAME_MAP")
            if name in module_names:
                map_name.append("_MODULE_MAP")
            errors.append(f"  - {name} (in {', '.join(map_name)})")

    if extra_in_type_checking:
        errors.append("\n❌ Items in TYPE_CHECKING block but NOT in any runtime map:")
        for name in sorted(extra_in_type_checking):
            errors.append(f"  - {name}")

    if errors:
        print("\n".join(errors))
        print("\n❌ FAILED: TYPE_CHECKING block and runtime maps are out of sync!")
        print("\nTo fix:")
        print(f"1. Add missing items to TYPE_CHECKING block in {sdk_py}")
        print("2. Remove extra items from TYPE_CHECKING block")
        print(
            "3. Ensure every item in _IMPORT_MAP/_RENAME_MAP/_MODULE_MAP has a corresponding TYPE_CHECKING import"
        )
        sys.exit(1)

    print("✅ SUCCESS: TYPE_CHECKING block matches runtime maps")
    print(f"   - {len(import_names)} items in _IMPORT_MAP")
    print(f"   - {len(rename_names)} items in _RENAME_MAP")
    print(f"   - {len(module_names)} items in _MODULE_MAP")
    print(f"   - {len(type_checking_names)} items in TYPE_CHECKING")
    sys.exit(0)


if __name__ == "__main__":
    main()
