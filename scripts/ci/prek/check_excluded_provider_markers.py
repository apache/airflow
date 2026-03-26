#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "packaging>=25",
#   "pyyaml",
#   "rich>=13.6.0",
# ]
# ///
"""
Validate that every dependency on a Python-version-excluded provider
in pyproject.toml carries the correct ``python_version`` environment marker.

Provider exclusions are authoritative in each provider's ``provider.yaml``
(``excluded-python-versions`` field).  Any dependency string in the
meta-package ``pyproject.toml`` that names an excluded provider without a
matching ``python_version != "X.Y"`` marker is flagged as an error.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml
from common_prek_utils import AIRFLOW_PROVIDERS_ROOT_PATH, AIRFLOW_ROOT_PATH
from packaging.requirements import InvalidRequirement, Requirement
from rich.console import Console

console = Console(width=400, color_system="standard")

PYPROJECT_TOML_PATH = AIRFLOW_ROOT_PATH / "pyproject.toml"


def _load_toml(path: Path) -> dict:
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib  # type: ignore[no-redef]
    return tomllib.loads(path.read_text())


def _get_excluded_providers() -> dict[str, list[str]]:
    """Return {normalized-package-name: [excluded python versions]} from provider.yaml files."""
    excluded: dict[str, list[str]] = {}
    for provider_yaml in AIRFLOW_PROVIDERS_ROOT_PATH.rglob("provider.yaml"):
        if provider_yaml.is_relative_to(AIRFLOW_PROVIDERS_ROOT_PATH / "src"):
            continue
        data = yaml.safe_load(provider_yaml.read_text())
        versions = data.get("excluded-python-versions", [])
        if versions:
            package_name = data["package-name"].lower().replace("_", "-")
            excluded[package_name] = [str(v) for v in versions]
    return excluded


def _check_dependency(dep_str: str, excluded_providers: dict[str, list[str]]) -> list[str]:
    """Check a single dependency string.  Return list of error messages."""
    try:
        req = Requirement(dep_str)
    except InvalidRequirement:
        return []
    package_name = req.name.lower().replace("_", "-")
    if package_name not in excluded_providers:
        return []
    errors = []
    for version in excluded_providers[package_name]:
        env = {"python_version": version}
        if req.marker is None or req.marker.evaluate(env):
            errors.append(
                f'Dependency on "{package_name}" is missing python_version !="{version}" marker: {dep_str}'
            )
    return errors


def main() -> int:
    excluded_providers = _get_excluded_providers()
    if not excluded_providers:
        return 0

    console.print("[bright_blue]Checking excluded-provider markers in pyproject.toml")
    for pkg, versions in sorted(excluded_providers.items()):
        console.print(f"  [bright_blue]{pkg}[/] excluded for Python {', '.join(versions)}")

    toml_data = _load_toml(PYPROJECT_TOML_PATH)
    all_errors: list[str] = []

    # Check [project.dependencies]
    for dep in toml_data.get("project", {}).get("dependencies", []):
        all_errors.extend(_check_dependency(dep, excluded_providers))

    # Check [project.optional-dependencies]
    for _extra_name, deps in toml_data.get("project", {}).get("optional-dependencies", {}).items():
        for dep in deps:
            all_errors.extend(_check_dependency(dep, excluded_providers))

    if all_errors:
        console.print(f"\n[red]Found {len(all_errors)} missing marker(s) in {PYPROJECT_TOML_PATH.name}:\n")
        for error in all_errors:
            console.print(f"  [red]✗[/] {error}")
        console.print(
            "\n[yellow]Each dependency on a provider with excluded-python-versions in "
            "provider.yaml must have a matching python_version marker.[/]\n"
            "[yellow]Example: 'apache-airflow-providers-amazon>=9.0.0; python_version !=\"3.14\"'[/]"
        )
        return 1

    console.print("[green]All excluded-provider markers are correct.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
