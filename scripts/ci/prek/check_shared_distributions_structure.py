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
    console.print(f"[bold green]Summary: {shared_path.name} is OK[/bold green]")
    return True


def main() -> None:
    if not SHARED_DIR.exists():
        print("No shared directory found.")
        sys.exit(1)
    all_ok = True
    for shared_project in SHARED_DIR.iterdir():
        if shared_project.is_dir():
            ok = check_shared_distribution(shared_project)
            if not ok:
                all_ok = False
    if not all_ok:
        sys.exit(2)


if __name__ == "__main__":
    main()
