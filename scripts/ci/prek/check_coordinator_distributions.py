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
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.6.0",
#   "tomli>=2.0.1; python_version < '3.11'",
# ]
# ///
"""Validate sdk/coordinators distributions metadata and basic docs."""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

from common_prek_utils import console, coordinator_distribution_dirs

PACKAGE_PREFIX = "apache-airflow-coordinators-"


def _load_pyproject(path: Path) -> dict:
    return tomllib.loads(path.read_text())


def check_coordinator_distribution(dist_dir: Path) -> list[str]:
    errors: list[str] = []
    coordinator_id = dist_dir.name
    expected_package_name = f"{PACKAGE_PREFIX}{coordinator_id}"

    pyproject_path = dist_dir / "pyproject.toml"
    pyproject = _load_pyproject(pyproject_path)
    project = pyproject.get("project", {})
    package_name = project.get("name")
    if package_name != expected_package_name:
        errors.append(
            f"{pyproject_path}: [project].name must be {expected_package_name!r}, got {package_name!r}"
        )

    for required_file in ("LICENSE", "NOTICE", "README.rst"):
        if not (dist_dir / required_file).exists():
            errors.append(f"{dist_dir}: missing required file {required_file}")

    readme_path = dist_dir / "README.rst"
    if readme_path.exists():
        readme = readme_path.read_text()
        if expected_package_name not in readme:
            errors.append(f"{readme_path}: missing package name {expected_package_name!r}")
        if "pip install" not in readme:
            errors.append(f"{readme_path}: missing installation instructions")

    source_dir = dist_dir / "src" / "airflow" / "sdk" / "coordinators" / coordinator_id
    if not source_dir.exists():
        errors.append(f"{dist_dir}: missing source package {source_dir.relative_to(dist_dir)}")

    tests_dir = dist_dir / "tests"
    if not tests_dir.exists():
        errors.append(f"{dist_dir}: missing tests directory")

    return errors


def main() -> int:
    errors: list[str] = []
    for dist_dir in coordinator_distribution_dirs():
        errors.extend(check_coordinator_distribution(dist_dir))

    if errors:
        console.print("[red]Errors found in coordinator distributions[/]")
        for error in errors:
            console.print(f"[red]{error}[/]")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
