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
Validate that every dependency on a restricted provider in pyproject.toml carries
the correct environment marker.

Provider restrictions are authoritative in each provider's ``provider.yaml``. Any
dependency string in the meta-package ``pyproject.toml`` that names a restricted
provider without the matching marker is flagged as an error.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TypedDict

import yaml
from common_prek_utils import AIRFLOW_PROVIDERS_ROOT_PATH, AIRFLOW_ROOT_PATH, EXCLUDED_PLATFORM_MACHINES
from packaging.requirements import InvalidRequirement, Requirement
from rich.console import Console

console = Console(width=400, color_system="standard")

PYPROJECT_TOML_PATH = AIRFLOW_ROOT_PATH / "pyproject.toml"


class ProviderRestrictions(TypedDict):
    python: list[str]
    machines: list[str]
    floor: str


def _load_toml(path: Path) -> dict:
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib  # type: ignore[no-redef]
    return tomllib.loads(path.read_text())


def _get_excluded_providers() -> dict[str, ProviderRestrictions]:
    """Return provider Python floors and excluded versions and machines.

    ``python`` holds excluded Python versions, ``machines`` holds the
    ``platform_machine`` values derived from excluded platforms, and ``floor`` holds
    the provider's minimum full Python version.
    """
    excluded: dict[str, ProviderRestrictions] = {}
    for provider_yaml in AIRFLOW_PROVIDERS_ROOT_PATH.rglob("provider.yaml"):
        if provider_yaml.is_relative_to(AIRFLOW_PROVIDERS_ROOT_PATH / "src"):
            continue
        data = yaml.safe_load(provider_yaml.read_text())
        versions = [str(v) for v in data.get("excluded-python-versions", [])]
        machines = [
            machine
            for platform in data.get("excluded-platforms", [])
            for machine in EXCLUDED_PLATFORM_MACHINES.get(platform, [])
        ]
        floor = str(data.get("min-python-version", ""))
        if versions or machines or floor:
            package_name = data["package-name"].lower().replace("_", "-")
            excluded[package_name] = {"python": versions, "machines": machines, "floor": floor}
    return excluded


def _check_dependency(dep_str: str, excluded_providers: dict[str, ProviderRestrictions]) -> list[str]:
    """Check a single dependency string.  Return list of error messages."""
    try:
        req = Requirement(dep_str)
    except InvalidRequirement:
        return []
    package_name = req.name.lower().replace("_", "-")
    if package_name not in excluded_providers:
        return []
    errors = []
    exclusions = excluded_providers[package_name]
    if floor := exclusions.get("floor"):
        major, minor, patch = (int(part) for part in str(floor).split("."))
        below_floor = f"{major}.{minor}.{patch - 1}" if patch else f"{major}.{minor - 1}.999999"
        below_env = {"python_full_version": below_floor, "python_version": f"{major}.{minor}"}
        floor_env = {"python_full_version": str(floor), "python_version": f"{major}.{minor}"}
        if req.marker is None or req.marker.evaluate(below_env) or not req.marker.evaluate(floor_env):
            errors.append(
                f'Dependency on "{package_name}" is missing python_full_version >="{floor}" marker: {dep_str}'
            )
    for version in exclusions.get("python", []):
        env = {"python_version": version}
        if req.marker is None or req.marker.evaluate(env):
            errors.append(
                f'Dependency on "{package_name}" is missing python_version !="{version}" marker: {dep_str}'
            )
    for machine in exclusions.get("machines", []):
        env = {"platform_machine": machine}
        if req.marker is None or req.marker.evaluate(env):
            errors.append(
                f'Dependency on "{package_name}" is missing platform_machine !="{machine}" marker: {dep_str}'
            )
    return errors


def main() -> int:
    excluded_providers = _get_excluded_providers()
    if not excluded_providers:
        return 0

    console.print("[bright_blue]Checking excluded-provider markers in pyproject.toml")
    for pkg, exclusions in sorted(excluded_providers.items()):
        details = []
        if exclusions["floor"]:
            details.append(f"Python >= {exclusions['floor']}")
        if exclusions["python"]:
            details.append(f"Python {', '.join(exclusions['python'])}")
        if exclusions["machines"]:
            details.append(f"machine {', '.join(exclusions['machines'])}")
        console.print(f"  [bright_blue]{pkg}[/] excluded for {'; '.join(details)}")

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
            "\n[yellow]Each dependency on a provider with min-python-version, "
            "excluded-python-versions, or excluded-platforms in provider.yaml must have a matching marker.[/]\n"
            "[yellow]Example: 'apache-airflow-providers-example>=1.0.0; "
            'python_full_version >="3.10.1"\'[/]\n'
            "[yellow]Example: 'apache-airflow-providers-amazon>=9.0.0; python_version !=\"3.14\"'[/]\n"
            '[yellow]Example: \'apache-airflow-providers-ibm-mq>=0.1.0; platform_machine !="aarch64" '
            'and platform_machine !="arm64"\'[/]'
        )
        return 1

    console.print("[green]All excluded-provider markers are correct.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
