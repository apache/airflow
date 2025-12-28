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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
#   "tomli>=2.0.1",
# ]
# ///
"""
Check shared projects in the "shared" subfolder for compliance.
"""

from __future__ import annotations

import ast
import itertools
import re
import sys
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import AIRFLOW_ROOT_PATH, console

SHARED_DIR = AIRFLOW_ROOT_PATH / "shared"
TASK_SDK_DIR = AIRFLOW_ROOT_PATH / "task-sdk"
AIRFLOW_CORE_DIR = AIRFLOW_ROOT_PATH / "airflow-core"
DEVEL_COMMON_DIR = AIRFLOW_ROOT_PATH / "devel-common"


def check_pyproject_exists(shared_path: Path) -> bool:
    pyproject_path = shared_path / "pyproject.toml"
    if pyproject_path.exists():
        console.print(
            f"  pyproject.toml exists for [magenta]{shared_path.name}[/magenta] [bold green]OK[/bold green]"
        )
        return True
    console.print(f"  [red]pyproject.toml missing for for [magenta]{shared_path.name}[/magenta][/red]")
    return False


def check_project_name(shared_path: Path, pyproject: dict) -> tuple[bool, str]:
    name = pyproject.get("project", {}).get("name", "")
    # Normalize directory name: convert underscores to hyphens for package name
    normalized_name = shared_path.name.replace("_", "-")
    expected_name = f"apache-airflow-shared-{normalized_name}"

    m = re.match(r"apache-airflow-shared-(.+)", name)
    if m and name == expected_name:
        console.print(
            f"  project name matches [magenta]{shared_path.name}[/magenta] and is '{expected_name}' [bold green]OK[/bold green]"
        )
        # Return the original directory name (with underscores), not the normalized package name
        return True, shared_path.name
    console.print(
        f"  [red]project name '{name}' does not match for [magenta]{shared_path.name}[/magenta] or is not '{expected_name}'[/red]"
    )
    return False, shared_path.name


def check_src_airflow_shared_pkg(shared_path: Path, pkg_name: str) -> bool:
    pkg_path = shared_path / "src" / "airflow_shared" / pkg_name
    if pkg_path.exists():
        console.print(
            f"  src/airflow_shared/{pkg_name} exists for [magenta]{shared_path.name}[/magenta] [bold green]OK[/bold green]"
        )
        return True
    console.print(
        f"  [red]{pkg_path} for [magenta]{shared_path.name}[/magenta] does not exist (should be sub-package of airflow_shared)[/red]"
    )
    return False


def check_no_init_in_airflow_shared(shared_path: Path) -> bool:
    airflow_shared_path = shared_path / "src" / "airflow_shared"
    init_file = airflow_shared_path / "__init__.py"
    if not init_file.exists():
        console.print(
            f"  src/airflow_shared/__init__.py does not exist for [magenta]{shared_path.name}[/magenta] [bold green]OK[/bold green]"
        )
        return True
    console.print(
        f"  [red]{init_file} should NOT exist for [magenta]{shared_path.name}[/magenta] (implicit namespace package)[/red]"
    )
    return False


def check_tests_pkg(shared_path: Path, pkg_name: str) -> bool:
    tests_pkg_path = shared_path / "tests" / pkg_name
    if tests_pkg_path.exists():
        console.print(
            f"  tests/{pkg_name} exists for [magenta]{shared_path.name}[/magenta] [bold green]OK[/bold green]"
        )
        return True
    console.print(
        f"  [red]{tests_pkg_path} does not exist for [magenta]{shared_path.name}[/magenta] (should be test package)[/red]"
    )
    return False


def check_private_classifier(pyproject: dict) -> bool:
    classifiers = pyproject.get("project", {}).get("classifiers", [])
    if classifiers == ["Private :: Do Not Upload"]:
        console.print(
            "  'Private :: Do Not Upload' classifier is only classifier [bold green]OK[/bold green]"
        )
        return True
    console.print(f"  [red]classifiers are not exactly ['Private :: Do Not Upload']: {classifiers}[/red]")
    return False


def check_build_system(pyproject: dict, shared_path: Path) -> bool:
    build_system = pyproject.get("build-system", {})
    if build_system == {"requires": ["hatchling"], "build-backend": "hatchling.build"}:
        console.print(
            f"  build-system is correct for [magenta]{shared_path.name}[/magenta] [bold green]OK[/bold green]"
        )
        return True
    console.print(
        f"  [red]build-system is not correct for [magenta]{shared_path.name}[/magenta]: {build_system}[/red]"
    )
    return False


def check_hatch_build_targets(pyproject: dict, shared_path: Path) -> bool:
    hatch_build_targets = pyproject.get("tool", {}).get("hatch", {}).get("build", {}).get("targets", {})
    if hatch_build_targets.get("wheel", {}).get("packages", []) == ["src/airflow_shared"]:
        console.print(
            f"  tool.hatch.build.targets.wheel is correct for [magenta]{shared_path.name}[/magenta] [bold green]OK[/bold green]"
        )
        return True
    console.print(
        f"  [red]tool.hatch.build.targets.wheel is not correct for [magenta]{shared_path.name}[/magenta]: {hatch_build_targets}[/red]"
    )
    return False


def check_ruff(pyproject: dict, shared_path: Path) -> tuple[bool, dict]:
    ruff = pyproject.get("tool", {}).get("ruff", {})
    if ruff.get("extend", None) == "../../pyproject.toml" and ruff.get("src", None) == ["src"]:
        console.print(
            f"  tool.ruff is correct for [magenta]{shared_path.name}[/magenta] [bold green]OK[/bold green]"
        )
        return True, ruff
    console.print(f"  [red]tool.ruff is not correct for [magenta]{shared_path.name}[/magenta]: {ruff}[/red]")
    return False, ruff


def check_ruff_lint_rules(ruff: dict, shared_path: Path) -> bool:
    ruff_lint = ruff.get("lint", {})
    per_file_ignores = ruff_lint.get("per-file-ignores", {})
    if per_file_ignores.get("!src/*", None) == ["D", "S101", "TRY002"]:
        console.print(
            f"  tool.ruff.lint rules are correct for [magenta]{shared_path.name}[/magenta] [bold green]OK[/bold green]"
        )
        return True
    console.print(
        f"  [red]tool.ruff.lint rules are not correct for [magenta]{shared_path.name}[/magenta]: {ruff_lint}[/red]"
    )
    return False


def _parse_python_file(py_file: Path, base_path: Path) -> ast.Module | None:
    """Parse a Python file and return an AST tree, handling errors gracefully."""
    try:
        with open(py_file, encoding="utf-8") as f:
            return ast.parse(f.read(), filename=str(py_file))
    except SyntaxError as e:
        console.print(f"  [yellow]Warning: Could not parse {py_file.relative_to(base_path)}: {e}[/yellow]")
        return None
    except Exception as e:
        console.print(f"  [yellow]Warning: Error reading {py_file.relative_to(base_path)}: {e}[/yellow]")
        return None


def _check_imports_in_files(
    py_files: list[Path],
    base_path: Path,
    import_predicate,
    dist_name: str,
) -> list[tuple[Path, int, str]]:
    """
    Check imports in Python files based on a predicate function.

    Args:
        py_files: List of Python files to check
        base_path: Base path for relative path calculation
        import_predicate: Function that takes (node, alias/module) and returns (should_report, import_stmt)
        dist_name: Distribution name for logging

    Returns:
        List of violations (file_path, lineno, import_stmt)
    """
    violations = []
    console.print(f"  Checking imports in {len(py_files)} files for [magenta]{dist_name}[/magenta]")
    for py_file in py_files:
        tree = _parse_python_file(py_file, base_path)
        if tree is None:
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    should_report, import_stmt = import_predicate(node, alias.name, is_from_import=False)
                    if should_report:
                        violations.append((py_file, node.lineno, import_stmt))

            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    should_report, import_stmt = import_predicate(node, node.module, is_from_import=True)
                    if should_report:
                        violations.append((py_file, node.lineno, import_stmt))

    return violations


def check_no_airflow_dependencies(pyproject: dict, shared_path: Path) -> bool:
    """Check that airflow packages are not listed as dependencies."""
    forbidden_packages = {"apache-airflow", "apache-airflow-core", "apache-airflow-task-sdk"}
    violations = []

    # Check regular dependencies
    dependencies = pyproject.get("project", {}).get("dependencies", [])
    for dep in dependencies:
        # Parse dependency string (may include version specifiers)
        dep_name = dep.split("[")[0].split(">")[0].split("<")[0].split("=")[0].split("!")[0].strip()
        if dep_name in forbidden_packages:
            violations.append(("dependencies", dep))

    # Check optional dependencies
    optional_deps = pyproject.get("project", {}).get("optional-dependencies", {})
    for group_name, deps in optional_deps.items():
        for dep in deps:
            dep_name = dep.split("[")[0].split(">")[0].split("<")[0].split("=")[0].split("!")[0].strip()
            if dep_name in forbidden_packages:
                violations.append((f"optional-dependencies.{group_name}", dep))

    # Check dependency groups (PEP 735)
    dependency_groups = pyproject.get("dependency-groups", {})
    for group_name, deps in dependency_groups.items():
        for dep in deps:
            dep_name = dep.split("[")[0].split(">")[0].split("<")[0].split("=")[0].split("!")[0].strip()
            if dep_name in forbidden_packages:
                violations.append((f"dependency-groups.{group_name}", dep))

    if violations:
        console.print(
            f"  [red]Found forbidden airflow dependencies in [magenta]{shared_path.name}[/magenta]:[/red]"
        )
        for location, dep in violations:
            console.print(f"    [red]{location}: {dep}[/red]")
        console.print()
        console.print(f"  [red]Shared distributions must not depend on {', '.join(forbidden_packages)}[/red]")
        return False

    console.print(
        f"  No forbidden airflow dependencies in [magenta]{shared_path.name}[/magenta] [bold green]OK[/bold green]"
    )
    return True


def check_no_airflow_imports(shared_path: Path) -> bool:
    """Check that no Python files in src/ import from airflow using AST."""
    src_path = shared_path / "src"
    test_path = shared_path / "tests"
    if not src_path.exists():
        console.print(
            f"  [yellow]src/ directory does not exist for [magenta]{shared_path.name}[/magenta][/yellow]"
        )
        return True

    def airflow_import_predicate(node, module_name, is_from_import):
        """Check if import is from airflow package."""
        if module_name == "airflow" or module_name.startswith("airflow."):
            if is_from_import:
                imported_names = ", ".join(alias.name for alias in node.names)
                return True, f"from {module_name} import {imported_names}"
            return True, f"import {module_name}"
        return False, ""

    py_files = list(itertools.chain(src_path.rglob("*.py"), test_path.rglob("*.py")))
    violations = _check_imports_in_files(py_files, shared_path, airflow_import_predicate, shared_path.name)

    if violations:
        console.print(f"  [red]Found airflow imports in [magenta]{shared_path.name}[/magenta]:[/red]")
        for file_path, lineno, import_stmt in violations:
            rel_path = file_path.relative_to(shared_path)
            console.print(f"    [red]{rel_path}:{lineno}: {import_stmt}[/red]")
        console.print()
        console.print(
            f"  [red]Please remove airflow imports from [magenta]{shared_path.name}[/magenta][/red] "
            f"in the way described below:"
        )
        console.print()
        console.print("  [yellow]How to fix: When you see `airflow` import in `src`:[/]")
        console.print()
        console.print(
            "[bright_blue]"
            "    * When you want to use `from airflow.**._shared.ANOTHER_DISTRIBUTIONS - use \n"
            "      relative import `from ..ANOTHER_DISTRIBUTION` (might need ../../ or ../../..)"
        )
        console.print(
            "  [bright_blue]"
            "    * When you want to use `from airflow.MODULE`, move the module to another shared \n"
            "      distribution (sometimes it might cause loss of DRY)"
        )
        console.print()
        console.print("  [yellow]How to fix: When you see `airflow` import in `tests`:[/]")
        console.print()
        console.print(
            "[bright_blue]"
            "    * When you want to use `from airflow.**._shared.MY_DISTRIBUTION`\n"
            "      use `from airflow_shared.MY_DISTRIBUTION import ...` "
        )
        console.print(
            "[bright_blue]"
            "    * When you want to use `from airflow.MODULE` where it is some test-related code \n"
            "      this code should likely be moved to `devel-common`"
        )
        return False

    console.print(
        f"  No airflow imports found in [magenta]{shared_path.name}[/magenta] [bold green]OK[/bold green]"
    )
    return True


def check_shared_distribution(shared_path: Path) -> bool:
    pyproject_path = shared_path / "pyproject.toml"
    console.print(f"\n[bold blue]Checking:[/bold blue] [magenta]{shared_path.name}[/magenta] shared project")
    if not check_pyproject_exists(shared_path):
        return False
    with open(pyproject_path, "rb") as f:
        pyproject = tomllib.load(f)
    ok, pkg_name = check_project_name(shared_path, pyproject)
    if not ok:
        return False
    if not check_src_airflow_shared_pkg(shared_path, pkg_name):
        return False
    if not check_no_init_in_airflow_shared(shared_path):
        return False
    if not check_tests_pkg(shared_path, pkg_name):
        return False
    if not check_private_classifier(pyproject):
        return False
    if not check_build_system(pyproject, shared_path):
        return False
    if not check_hatch_build_targets(pyproject, shared_path):
        return False
    ok, ruff = check_ruff(pyproject, shared_path)
    if not ok:
        return False
    if not check_ruff_lint_rules(ruff, shared_path):
        return False
    if not check_no_airflow_dependencies(pyproject, shared_path):
        return False
    if not check_no_airflow_imports(shared_path):
        return False
    console.print(f"[bold green]Summary: {shared_path.name} is OK[/bold green]")
    return True


def check_no_airflow_shared_imports(dist_path: Path, dist_name: str) -> bool:
    """Check that no Python files use airflow_shared imports."""
    src_path = dist_path / "src"
    if not src_path.exists():
        console.print(f"  [yellow]src/ directory does not exist for [magenta]{dist_name}[/magenta][/yellow]")
        return True

    def airflow_shared_import_predicate(node, module_name, is_from_import):
        """Check if import is from airflow_shared package."""
        if module_name == "airflow_shared" or module_name.startswith("airflow_shared."):
            if is_from_import:
                imported_names = ", ".join(alias.name for alias in node.names)
                return True, f"from {module_name} import {imported_names}"
            return True, f"import {module_name}"
        return False, ""

    py_files = list(src_path.rglob("*.py"))
    violations = _check_imports_in_files(py_files, dist_path, airflow_shared_import_predicate, dist_name)

    if violations:
        console.print(f"  [red]Found airflow_shared imports in [magenta]{dist_name}[/magenta]:[/red]")
        for file_path, lineno, import_stmt in violations:
            rel_path = file_path.relative_to(dist_path)
            console.print(f"    [red]{rel_path}:{lineno}: {import_stmt}[/red]")
        console.print()
        console.print(
            f"  [red]Please do not use airflow_shared imports in [magenta]{dist_name}[/magenta][/red]"
        )
        console.print(
            "  [yellow]Use proper _shared imports instead (e.g., airflow._shared.* or airflow.sdk._shared.*)[/yellow]"
        )
        return False

    console.print(
        f"  No airflow_shared imports found in [magenta]{dist_name}[/magenta] [bold green]OK[/bold green]"
    )
    return True


def check_only_allowed_shared_imports(dist_path: Path, dist_name: str, allowed_prefix: str) -> bool:
    """Check that only imports with the allowed _shared prefix are used."""
    src_path = dist_path / "src"
    if not src_path.exists():
        console.print(f"  [yellow]src/ directory does not exist for [magenta]{dist_name}[/magenta][/yellow]")
        return True

    def allowed_shared_import_predicate(node, module_name, is_from_import):
        """Check if _shared import uses the correct prefix."""
        if "._shared" in module_name or module_name.endswith("._shared"):
            if not module_name.startswith(allowed_prefix):
                if is_from_import:
                    imported_names = ", ".join(alias.name for alias in node.names)
                    return True, f"from {module_name} import {imported_names}"
                return True, f"import {module_name}"
        return False, ""

    py_files = list(src_path.rglob("*.py"))
    violations = _check_imports_in_files(py_files, dist_path, allowed_shared_import_predicate, dist_name)

    if violations:
        console.print(f"  [red]Found disallowed _shared imports in [magenta]{dist_name}[/magenta]:[/red]")
        for file_path, lineno, import_stmt in violations:
            rel_path = file_path.relative_to(dist_path)
            console.print(f"    [red]{rel_path}:{lineno}: {import_stmt}[/red]")
        console.print()
        console.print(
            f"  [red]Only imports starting with '{allowed_prefix}' are allowed in [magenta]{dist_name}[/magenta][/red]"
        )
        return False

    console.print(
        f"  Only allowed _shared imports found in [magenta]{dist_name}[/magenta] [bold green]OK[/bold green]"
    )
    return True


def check_distribution(dist_path: Path, dist_name: str, allowed_shared_prefix: str) -> bool:
    """
    Check a distribution for proper _shared imports usage.

    Args:
        dist_path: Path to the distribution directory
        dist_name: Name of the distribution for display
        allowed_shared_prefix: Allowed prefix for _shared imports (e.g., 'airflow.sdk._shared')

    Returns:
        True if all checks pass, False otherwise
    """
    console.print(f"\n[bold blue]Checking:[/bold blue] [magenta]{dist_name}[/magenta] distribution")

    if not dist_path.exists():
        console.print(f"  [yellow]{dist_name} directory does not exist[/yellow]")
        return True

    all_ok = True

    # Check 1: No airflow_shared imports
    if not check_no_airflow_shared_imports(dist_path, dist_name):
        all_ok = False

    # Check 2: Only allowed _shared imports
    if not check_only_allowed_shared_imports(dist_path, dist_name, allowed_shared_prefix):
        all_ok = False

    if all_ok:
        console.print(f"[bold green]Summary: {dist_name} is OK[/bold green]")

    return all_ok


def check_task_sdk_distribution() -> bool:
    """Check task-sdk distribution for proper _shared imports usage."""
    return check_distribution(TASK_SDK_DIR, "task-sdk", "airflow.sdk._shared")


def check_airflow_core_distribution() -> bool:
    """Check airflow-core distribution for proper _shared imports usage."""
    return check_distribution(AIRFLOW_CORE_DIR, "airflow-core", "airflow._shared")


def check_no_airflow_imports_devel_common(dist_path: Path) -> bool:
    """Check that no Python files in devel-common use airflow imports."""
    src_path = dist_path / "src"
    if not src_path.exists():
        console.print("  [yellow]src/ directory does not exist for [magenta]devel-common[/magenta][/yellow]")
        return True

    def airflow_import_predicate(node, module_name, is_from_import):
        """Check if import is from airflow package."""
        if module_name == "airflow" or module_name.startswith("airflow."):
            if is_from_import:
                imported_names = ", ".join(alias.name for alias in node.names)
                return True, f"from {module_name} import {imported_names}"
            return True, f"import {module_name}"
        return False, ""

    py_files = list(src_path.rglob("*.py"))
    violations = _check_imports_in_files(py_files, dist_path, airflow_import_predicate, "devel-common")

    if violations:
        console.print("  [red]Found airflow imports in [magenta]devel-common[/magenta]:[/red]")
        for file_path, lineno, import_stmt in violations:
            rel_path = file_path.relative_to(dist_path)
            console.print(f"    [red]{rel_path}:{lineno}: {import_stmt}[/red]")
        console.print()
        console.print("  [red]Please remove airflow imports from [magenta]devel-common[/magenta][/red]")
        console.print(
            "  [yellow]devel-common should not depend on airflow packages to remain independent[/yellow]\n\n"
            "  [yellow]Those imports should be converted to `from airflow_shared` or "
            "moved to the devel-common distribution.[/yellow]"
        )
        return False

    console.print("  No airflow imports found in [magenta]devel-common[/magenta] [bold green]OK[/bold green]")
    return True


def check_devel_common_distribution() -> bool:
    """Check devel-common distribution for proper imports usage."""
    console.print("\n[bold blue]Checking:[/bold blue] [magenta]devel-common[/magenta] distribution")

    if not DEVEL_COMMON_DIR.exists():
        console.print("  [yellow]devel-common directory does not exist[/yellow]")
        return True

    all_ok = True

    # Check: No airflow imports
    if not check_no_airflow_imports_devel_common(DEVEL_COMMON_DIR):
        all_ok = False

    if all_ok:
        console.print("[bold green]Summary: devel-common is OK[/bold green]")

    return all_ok


def main() -> None:
    all_ok = True

    # Check shared distributions
    if SHARED_DIR.exists():
        for shared_project in SHARED_DIR.iterdir():
            if shared_project.is_dir():
                ok = check_shared_distribution(shared_project)
                if not ok:
                    all_ok = False
    else:
        console.print("[yellow]No shared directory found.[/yellow]")
        sys.exit(1)

    # Check task-sdk distribution
    if not check_task_sdk_distribution():
        all_ok = False

    # Check airflow-core distribution
    if not check_airflow_core_distribution():
        all_ok = False

    # Check devel-common distribution
    if not check_devel_common_distribution():
        all_ok = False

    if not all_ok:
        sys.exit(2)


if __name__ == "__main__":
    main()
