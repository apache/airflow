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
Check and generate lazy_compat.pyi from lazy_compat.py.

This script can be used as:
1. Pre-commit hook - checks if .pyi is in sync with _IMPORT_MAP
2. Manual generation - generates .pyi file from _IMPORT_MAP

Usage:
    python scripts/ci/prek/check_common_compat_lazy_imports.py                # Check only (pre-commit)
    python scripts/ci/prek/check_common_compat_lazy_imports.py --generate     # Generate .pyi file
    python scripts/ci/prek/check_common_compat_lazy_imports.py --validate     # Generate with import validation
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


def extract_import_map(py_file: Path) -> dict[str, str | tuple[str, ...]]:
    """
    Extract _IMPORT_MAP from lazy_compat.py.

    :param py_file: Path to lazy_compat.py
    :return: Dictionary mapping class names to module paths
    """
    content = py_file.read_text()
    tree = ast.parse(content)

    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == "_IMPORT_MAP" and node.value:
                return ast.literal_eval(node.value)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "_IMPORT_MAP":
                    return ast.literal_eval(node.value)

    raise ValueError("Could not find _IMPORT_MAP in lazy_compat.py")


def extract_rename_map(py_file: Path) -> dict[str, tuple[str, str, str]]:
    """
    Extract _RENAME_MAP from lazy_compat.py.

    :param py_file: Path to lazy_compat.py
    :return: Dictionary mapping new class names to (new_path, old_path, old_name)
    """
    content = py_file.read_text()
    tree = ast.parse(content)

    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == "_RENAME_MAP" and node.value:
                return ast.literal_eval(node.value)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "_RENAME_MAP":
                    return ast.literal_eval(node.value)

    raise ValueError("Could not find _RENAME_MAP in lazy_compat.py")


def extract_module_map(py_file: Path) -> dict[str, str | tuple[str, ...]]:
    """
    Extract _MODULE_MAP from lazy_compat.py.

    :param py_file: Path to lazy_compat.py
    :return: Dictionary mapping module names to module paths
    """
    content = py_file.read_text()
    tree = ast.parse(content)

    for node in tree.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == "_MODULE_MAP" and node.value:
                return ast.literal_eval(node.value)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "_MODULE_MAP":
                    return ast.literal_eval(node.value)

    raise ValueError("Could not find _MODULE_MAP in lazy_compat.py")


def generate_pyi_content(
    rename_map: dict[str, tuple[str, str, str]],
    import_map: dict[str, str | tuple[str, ...]],
    module_map: dict[str, str | tuple[str, ...]],
) -> str:
    """
    Generate .pyi stub content from rename, import and module maps.

    :param rename_map: Dictionary mapping new names to (new_path, old_path, old_name)
    :param import_map: Dictionary mapping class names to module paths
    :param module_map: Dictionary mapping module names to module paths
    :return: Content for the .pyi file
    """
    header = '''# Licensed to the Apache Software Foundation (ASF) under one
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
Type stubs for IDE autocomplete - always uses Airflow 3 paths.

This file is auto-generated from lazy_compat.py.
    - run scripts/ci/prek/check_common_compat_lazy_imports.py --generate instead.
"""

'''

    imports_by_module: dict[str, list[str]] = {}

    # Process renamed imports from _RENAME_MAP (use new names and new paths for type hints)
    for new_name, (new_path, _old_path, _old_name) in sorted(rename_map.items()):
        if new_path not in imports_by_module:
            imports_by_module[new_path] = []
        imports_by_module[new_path].append(new_name)

    # Process regular imports from _IMPORT_MAP
    for name, paths in sorted(import_map.items()):
        module_path = paths[0] if isinstance(paths, tuple) else paths
        if module_path not in imports_by_module:
            imports_by_module[module_path] = []
        imports_by_module[module_path].append(name)

    lines = []

    # Generate regular imports (always use multiline for ruff compatibility)
    for module_path in sorted(imports_by_module.keys()):
        names = sorted(imports_by_module[module_path])
        # Always use multiline format for ruff
        lines.append(f"from {module_path} import (")
        for i, name in enumerate(names):
            comma = "," if i < len(names) - 1 else ""
            lines.append(f"    {name} as {name}{comma}")
        lines.append(")")

    # Generate module imports (import the module itself)
    for module_name, paths in sorted(module_map.items()):
        module_path = paths[0] if isinstance(paths, tuple) else paths
        lines.append(f"import {module_path} as {module_name}")

    # Generate __all__ (include renamed, attributes and modules)
    all_names = sorted(list(rename_map.keys()) + list(import_map.keys()) + list(module_map.keys()))
    lines.append("")
    lines.append(f"__all__: list[str] = {all_names!r}")

    return header + "\n".join(lines) + "\n"


def validate_imports(
    rename_map: dict[str, tuple[str, str, str]],
    import_map: dict[str, str | tuple[str, ...]],
    module_map: dict[str, str | tuple[str, ...]],
    skip_on_error: bool = False,
) -> list[str]:
    """
    Validate that all imports in the maps are actually importable.

    This is optional and only runs if skip_on_error=False. It requires Airflow
    and all providers to be installed, so it's meant for manual validation only.

    :param rename_map: The rename map to validate
    :param import_map: The import map to validate
    :param module_map: The module map to validate
    :param skip_on_error: If True, skip validation and return empty list
    :return: List of errors (empty if all valid or skipped)
    """
    if skip_on_error:
        print("\nSkipping import validation (requires full Airflow installation)")
        return []

    import importlib

    errors = []
    print("\nValidating imports (requires Airflow + providers installed)...")

    # Validate renamed imports
    for new_name, (new_path, old_path, old_name) in rename_map.items():
        importable = False
        last_error = None

        # Try new path with new name (Airflow 3.x)
        try:
            module = importlib.import_module(new_path)
            if hasattr(module, new_name):
                importable = True
            else:
                last_error = f"Module {new_path} does not have attribute {new_name}"
        except (ImportError, ModuleNotFoundError) as e:
            last_error = str(e)

        # Try old path with old name (Airflow 2.x)
        if not importable:
            try:
                module = importlib.import_module(old_path)
                if hasattr(module, old_name):
                    importable = True
                else:
                    last_error = f"Module {old_path} does not have attribute {old_name}"
            except (ImportError, ModuleNotFoundError) as e:
                last_error = str(e)

        if not importable:
            errors.append(
                f"  ✗ {new_name} (renamed from {old_name}): Could not import. Last error: {last_error}"
            )
        else:
            print(f"  ✓ {new_name} (renamed from {old_name})")

    # Validate attribute imports
    for name, paths in import_map.items():
        if isinstance(paths, str):
            paths = (paths,)

        importable = False
        last_error = None

        for module_path in paths:
            try:
                module = importlib.import_module(module_path)
                if hasattr(module, name):
                    importable = True
                    break
                last_error = f"Module {module_path} does not have attribute {name}"
            except (ImportError, ModuleNotFoundError) as e:
                last_error = str(e)
                continue

        if not importable:
            errors.append(f"  ✗ {name}: Could not import from any path. Last error: {last_error}")
        else:
            print(f"  ✓ {name}")

    # Validate module imports
    for module_name, paths in module_map.items():
        if isinstance(paths, str):
            paths = (paths,)

        importable = False
        last_error = None

        for module_path in paths:
            try:
                importlib.import_module(module_path)
                importable = True
                break
            except (ImportError, ModuleNotFoundError) as e:
                last_error = str(e)
                continue

        if not importable:
            errors.append(
                f"  ✗ {module_name} (module): Could not import from any path. Last error: {last_error}"
            )
        else:
            print(f"  ✓ {module_name} (module)")

    return errors


def main() -> int:
    """Generate and check lazy_compat.pyi."""
    repo_root = Path(__file__).parent.parent.parent.parent
    lazy_compat_py = (
        repo_root
        / "providers"
        / "common"
        / "compat"
        / "src"
        / "airflow"
        / "providers"
        / "common"
        / "compat"
        / "lazy_compat.py"
    )
    lazy_compat_pyi = lazy_compat_py.with_suffix(".pyi")

    if not lazy_compat_py.exists():
        print(f"ERROR: Could not find {lazy_compat_py}")
        return 1

    should_generate = "--generate" in sys.argv or "--validate" in sys.argv
    should_validate = "--validate" in sys.argv

    try:
        rename_map = extract_rename_map(lazy_compat_py)
        print(f"Found {len(rename_map)} renames in _RENAME_MAP")
    except Exception as e:
        print(f"ERROR: Failed to extract _RENAME_MAP: {e}")
        return 1

    try:
        import_map = extract_import_map(lazy_compat_py)
        print(f"Found {len(import_map)} imports in _IMPORT_MAP")
    except Exception as e:
        print(f"ERROR: Failed to extract _IMPORT_MAP: {e}")
        return 1

    try:
        module_map = extract_module_map(lazy_compat_py)
        print(f"Found {len(module_map)} modules in _MODULE_MAP")
    except Exception as e:
        print(f"ERROR: Failed to extract _MODULE_MAP: {e}")
        return 1

    total_imports = len(rename_map) + len(import_map) + len(module_map)

    errors = validate_imports(rename_map, import_map, module_map, skip_on_error=not should_validate)
    if errors:
        print("\n❌ Import validation failed:")
        for error in errors:
            print(error)
        print("\nPlease fix the import paths in _IMPORT_MAP and _MODULE_MAP and try again.")
        return 1

    if should_validate:
        print(f"\n✓ All {total_imports} imports validated successfully")

    # Check if .pyi exists and is in sync
    if not should_generate:
        # Check-only mode (pre-commit)
        if not lazy_compat_pyi.exists():
            print(f"ERROR: {lazy_compat_pyi.name} does not exist")
            print("Run: python scripts/ci/prek/check_common_compat_lazy_imports.py --generate")
            return 1

        pyi_content = lazy_compat_pyi.read_text()

        # Count total imports in .pyi (each "X as X" pattern + "import X as Y", excluding __all__)
        import re

        # Match "import X.Y.Z as module_name" pattern (standalone module imports)
        module_import_pattern = r"^import\s+[\w.]+\s+as\s+(\w+)"
        pyi_module_imports = set(re.findall(module_import_pattern, pyi_content, re.MULTILINE))

        # Remove __all__ and standalone import lines to avoid false matches
        pyi_for_attr_search = pyi_content
        pyi_for_attr_search = re.sub(r"__all__:.*", "", pyi_for_attr_search, flags=re.DOTALL)
        pyi_for_attr_search = re.sub(
            r"^import\s+[\w.]+\s+as\s+\w+.*$", "", pyi_for_attr_search, flags=re.MULTILINE
        )

        # Match all "Name as Name" patterns
        attr_import_pattern = r"(\w+)\s+as\s+\1"
        pyi_attr_imports = set(re.findall(attr_import_pattern, pyi_for_attr_search))

        # Combine all expected imports
        map_renames = set(rename_map.keys())
        map_attrs = set(import_map.keys())
        map_modules = set(module_map.keys())
        all_expected_attrs = map_renames | map_attrs

        # Check for discrepancies
        missing_attrs = all_expected_attrs - pyi_attr_imports
        extra_attrs = pyi_attr_imports - all_expected_attrs
        missing_modules = map_modules - pyi_module_imports
        extra_modules = pyi_module_imports - map_modules

        if not (missing_attrs or extra_attrs or missing_modules or extra_modules):
            print(f"✓ lazy_compat.pyi is in sync with lazy_compat.py ({total_imports} imports)")
            return 0

        # Out of sync
        if missing_attrs:
            print(
                f"ERROR: lazy_compat.pyi is missing {len(missing_attrs)} attributes from "
                "_RENAME_MAP/_IMPORT_MAP:"
            )
            for name in sorted(missing_attrs)[:10]:
                print(f"  - {name}")
            if len(missing_attrs) > 10:
                print(f"  ... and {len(missing_attrs) - 10} more")

        if extra_attrs:
            print(
                f"ERROR: lazy_compat.pyi has {len(extra_attrs)} extra attributes not in "
                "_RENAME_MAP/_IMPORT_MAP:"
            )
            for name in sorted(extra_attrs)[:10]:
                print(f"  + {name}")
            if len(extra_attrs) > 10:
                print(f"  ... and {len(extra_attrs) - 10} more")

        if missing_modules:
            print(f"ERROR: lazy_compat.pyi is missing {len(missing_modules)} modules from _MODULE_MAP:")
            for name in sorted(missing_modules):
                print(f"  - {name} (module)")

        if extra_modules:
            print(f"ERROR: lazy_compat.pyi has {len(extra_modules)} extra modules not in _MODULE_MAP:")
            for name in sorted(extra_modules):
                print(f"  + {name} (module)")

        print("\nRun: python scripts/ci/prek/check_common_compat_lazy_imports.py --generate")
        return 1

    # Generate mode
    new_pyi_content = generate_pyi_content(rename_map, import_map, module_map)
    lazy_compat_pyi.write_text(new_pyi_content)
    print(f"✓ Generated {lazy_compat_pyi.name} with {total_imports} imports")
    return 0


if __name__ == "__main__":
    sys.exit(main())
