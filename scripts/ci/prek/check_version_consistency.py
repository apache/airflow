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
#   "packaging>=25.0",
#   "tomli>=2.0.1",
# ]
# ///
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib

from packaging.specifiers import SpecifierSet
from packaging.version import Version

sys.path.insert(0, str(Path(__file__).parent.resolve()))

from common_prek_utils import (
    AIRFLOW_CORE_SOURCES_PATH,
    AIRFLOW_ROOT_PATH,
    AIRFLOW_TASK_SDK_SOURCES_PATH,
    console,
)


def read_airflow_version() -> str:
    """Read Airflow version from airflow-core/src/airflow/__init__.py"""
    ast_obj = ast.parse((AIRFLOW_CORE_SOURCES_PATH / "airflow" / "__init__.py").read_text())
    for node in ast_obj.body:
        if isinstance(node, ast.Assign):
            if node.targets[0].id == "__version__":  # type: ignore[attr-defined]
                return ast.literal_eval(node.value)

    raise RuntimeError("Couldn't find __version__ in airflow-core/src/airflow/__init__.py")


def read_task_sdk_version() -> str:
    """Read Task SDK version from task-sdk/src/airflow/sdk/__init__.py"""
    ast_obj = ast.parse((AIRFLOW_TASK_SDK_SOURCES_PATH / "airflow" / "sdk" / "__init__.py").read_text())
    for node in ast_obj.body:
        if isinstance(node, ast.Assign):
            if node.targets[0].id == "__version__":  # type: ignore[attr-defined]
                return ast.literal_eval(node.value)

    raise RuntimeError("Couldn't find __version__ in task-sdk/src/airflow/sdk/__init__.py")


def read_airflow_version_from_pyproject() -> str:
    """Read Airflow version from airflow-core/pyproject.toml"""
    pyproject_path = AIRFLOW_ROOT_PATH / "airflow-core" / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        data = tomllib.load(f)
    version = data.get("project", {}).get("version")
    if not version:
        raise RuntimeError("Couldn't find version in airflow-core/pyproject.toml")
    return str(version)


def read_task_sdk_dependency_constraint() -> str:
    """Read Task SDK dependency constraint from airflow-core/pyproject.toml"""
    pyproject_path = AIRFLOW_ROOT_PATH / "airflow-core" / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        data = tomllib.load(f)
    dependencies = data.get("project", {}).get("dependencies", [])
    for dep in dependencies:
        # Extract package name (everything before >=, >, <, ==, etc.)
        package_name = re.split(r"[<>=!]", dep)[0].strip().strip("\"'")
        if package_name == "apache-airflow-task-sdk":
            # Extract the version constraint part
            constraint_match = re.search(r"apache-airflow-task-sdk\s*(.*)", dep)
            if constraint_match:
                return constraint_match.group(1).strip().strip("\"'")
            return dep
    raise RuntimeError("Couldn't find apache-airflow-task-sdk dependency in airflow-core/pyproject.toml")


def read_root_pyproject_version() -> str:
    """Read version from root pyproject.toml"""
    pyproject_path = AIRFLOW_ROOT_PATH / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        data = tomllib.load(f)
    version = data.get("project", {}).get("version")
    if not version:
        raise RuntimeError("Couldn't find version in pyproject.toml")
    return str(version)


def read_root_airflow_core_dependency() -> str:
    """Read apache-airflow-core dependency from root pyproject.toml"""
    pyproject_path = AIRFLOW_ROOT_PATH / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        data = tomllib.load(f)
    dependencies = data.get("project", {}).get("dependencies", [])
    for dep in dependencies:
        # Extract package name (everything before >=, >, <, ==, etc.)
        package_name = re.split(r"[<>=!]", dep)[0].strip().strip("\"'")
        if package_name == "apache-airflow-core":
            # Extract the version constraint part
            constraint_match = re.search(r"apache-airflow-core\s*(.*)", dep)
            if constraint_match:
                return constraint_match.group(1).strip().strip("\"'")
            return dep
    raise RuntimeError("Couldn't find apache-airflow-core dependency in pyproject.toml")


def read_root_task_sdk_dependency_constraint() -> str:
    """Read Task SDK dependency constraint from root pyproject.toml"""
    pyproject_path = AIRFLOW_ROOT_PATH / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        data = tomllib.load(f)
    dependencies = data.get("project", {}).get("dependencies", [])
    for dep in dependencies:
        # Extract package name (everything before >=, >, <, ==, etc.)
        package_name = re.split(r"[<>=!]", dep)[0].strip().strip("\"'")
        if package_name == "apache-airflow-task-sdk":
            # Extract the version constraint part
            constraint_match = re.search(r"apache-airflow-task-sdk\s*(.*)", dep)
            if constraint_match:
                return constraint_match.group(1).strip().strip("\"'")
            return dep
    raise RuntimeError("Couldn't find apache-airflow-task-sdk dependency in pyproject.toml")


def check_version_in_constraint(version: str, constraint: str) -> bool:
    """Check if version satisfies the constraint"""
    try:
        spec = SpecifierSet(constraint)
        return Version(version) in spec
    except Exception as e:
        console.print(f"[red]Error parsing constraint '{constraint}': {e}[/red]")
        return False


def get_minimum_version_from_constraint(constraint: str) -> str | None:
    """
    Extract the minimum version from a constraint string.
    Returns the highest >= or > version requirement, or None if not found.
    """
    try:
        spec = SpecifierSet(constraint)
        min_version = None

        for specifier in spec:
            if specifier.operator in (">=", ">"):
                if min_version is None or Version(specifier.version) > Version(min_version):
                    min_version = specifier.version

        return min_version
    except Exception:
        return None


def check_constraint_matches_version(version: str, constraint: str) -> tuple[bool, str | None]:
    """
    Check if the constraint's minimum version matches the actual version.
    Returns (is_match, min_version_from_constraint)
    """
    min_version = get_minimum_version_from_constraint(constraint)
    if min_version is None:
        return (False, None)

    # Check if the minimum version in the constraint matches the actual version
    return (Version(min_version) == Version(version), min_version)


def main():
    errors: list[str] = []

    # Read versions
    try:
        airflow_version_init = read_airflow_version()
        airflow_version_pyproject = read_airflow_version_from_pyproject()
        root_pyproject_version = read_root_pyproject_version()
        root_airflow_core_dep = read_root_airflow_core_dependency()
        task_sdk_version_init = read_task_sdk_version()
        task_sdk_constraint = read_task_sdk_dependency_constraint()
        root_task_sdk_constraint = read_root_task_sdk_dependency_constraint()
    except Exception as e:
        console.print(f"[red]Error reading versions: {e}[/red]")
        sys.exit(1)

    # Check Airflow version consistency
    if airflow_version_init != airflow_version_pyproject:
        errors.append(
            f"Airflow version mismatch:\n"
            f"  airflow-core/src/airflow/__init__.py: {airflow_version_init}\n"
            f"  airflow-core/pyproject.toml: {airflow_version_pyproject}"
        )

    # Check root pyproject.toml version matches Airflow version
    if airflow_version_init != root_pyproject_version:
        errors.append(
            f"Root pyproject.toml version mismatch:\n"
            f"  airflow-core/src/airflow/__init__.py: {airflow_version_init}\n"
            f"  pyproject.toml: {root_pyproject_version}"
        )

    # Check root pyproject.toml apache-airflow-core dependency matches Airflow version exactly
    expected_core_dep = f"=={airflow_version_init}"
    if root_airflow_core_dep != expected_core_dep:
        errors.append(
            f"Root pyproject.toml apache-airflow-core dependency mismatch:\n"
            f"  Expected: apache-airflow-core=={airflow_version_init}\n"
            f"  Found: apache-airflow-core{root_airflow_core_dep}"
        )

    # Check Task SDK version is within constraint in airflow-core/pyproject.toml
    if not check_version_in_constraint(task_sdk_version_init, task_sdk_constraint):
        errors.append(
            f"Task SDK version does not satisfy constraint in airflow-core/pyproject.toml:\n"
            f"  task-sdk/src/airflow/sdk/__init__.py: {task_sdk_version_init}\n"
            f"  airflow-core/pyproject.toml constraint: apache-airflow-task-sdk{task_sdk_constraint}"
        )

    # Check Task SDK constraint minimum version matches actual version in airflow-core/pyproject.toml
    constraint_matches, min_version = check_constraint_matches_version(
        task_sdk_version_init, task_sdk_constraint
    )
    if not constraint_matches:
        errors.append(
            f"Task SDK constraint minimum version does not match actual version in airflow-core/pyproject.toml:\n"
            f"  task-sdk/src/airflow/sdk/__init__.py: {task_sdk_version_init}\n"
            f"  airflow-core/pyproject.toml constraint minimum: {min_version}\n"
            f"  Expected constraint to have minimum version: >= {task_sdk_version_init}"
        )

    # Check Task SDK version is within constraint in root pyproject.toml
    if not check_version_in_constraint(task_sdk_version_init, root_task_sdk_constraint):
        errors.append(
            f"Task SDK version does not satisfy constraint in pyproject.toml:\n"
            f"  task-sdk/src/airflow/sdk/__init__.py: {task_sdk_version_init}\n"
            f"  pyproject.toml constraint: apache-airflow-task-sdk{root_task_sdk_constraint}"
        )

    # Check Task SDK constraint minimum version matches actual version in root pyproject.toml
    root_constraint_matches, root_min_version = check_constraint_matches_version(
        task_sdk_version_init, root_task_sdk_constraint
    )
    if not root_constraint_matches:
        errors.append(
            f"Task SDK constraint minimum version does not match actual version in pyproject.toml:\n"
            f"  task-sdk/src/airflow/sdk/__init__.py: {task_sdk_version_init}\n"
            f"  pyproject.toml constraint minimum: {root_min_version}\n"
            f"  Expected constraint to have minimum version: >= {task_sdk_version_init}"
        )

    # Verify constraints match between airflow-core and root pyproject.toml
    if task_sdk_constraint != root_task_sdk_constraint:
        errors.append(
            f"Task SDK constraint mismatch between pyproject.toml files:\n"
            f"  airflow-core/pyproject.toml: apache-airflow-task-sdk{task_sdk_constraint}\n"
            f"  pyproject.toml: apache-airflow-task-sdk{root_task_sdk_constraint}"
        )

    # Report results
    if errors:
        console.print("[red]Version consistency check failed:[/red]\n")
        for error in errors:
            console.print(f"[red]{error}[/red]\n")
        console.print(
            "[yellow]Please ensure versions are consistent:\n"
            "  1. Set the Airflow version in airflow-core/src/airflow/__init__.py\n"
            "  2. Set the Airflow version in airflow-core/pyproject.toml\n"
            "  3. Set the Airflow version in pyproject.toml\n"
            "  4. Set apache-airflow-core==<version> in pyproject.toml dependencies\n"
            "  5. Set the Task SDK version in task-sdk/src/airflow/sdk/__init__.py\n"
            "  6. Update the Task SDK version constraint in airflow-core/pyproject.toml to include the Task SDK version\n"
            "  7. Update the Task SDK version constraint in pyproject.toml to include the Task SDK version[/yellow]"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
