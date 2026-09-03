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
#   "rich>=13.6.0",
#   "tomli>=2.0.1",
# ]
# ///
"""
Validate that every external dependency declared in a ``pyproject.toml`` has a lower bound.

An unbounded requirement lets the resolver answer with any version that happens to be on
PyPI, so what a constraints file pins - and what a user ends up installing - depends on how
the resolution went rather than on what the code needs. Naming the oldest supported version
makes that answer deterministic and documents the floor the code is tested against.

Checked locations: ``project.dependencies``, ``project.optional-dependencies``,
``dependency-groups`` and ``build-system.requires``.

Requirements resolved from the uv workspace rather than from PyPI are exempt - their source
is the checkout, so a version range would say nothing. Direct URL requirements are exempt
too, since the URL already names the exact artifact.
"""

from __future__ import annotations

import sys
from functools import cache
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH, console
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name
from rich.markup import escape

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

# Operators that place a floor under the resolved version; anything else (``!=``, ``<``,
# ``<=``) leaves the resolver free to reach back to the oldest release ever published.
LOWER_BOUND_OPERATORS = {">=", ">", "==", "===", "~="}


def _load_toml(path: Path) -> dict:
    return tomllib.loads(path.read_text())


@cache
def get_workspace_distribution_names() -> frozenset[str]:
    """Return the canonical names of the distributions uv resolves from this checkout."""
    root_pyproject = AIRFLOW_ROOT_PATH / "pyproject.toml"
    root_data = _load_toml(root_pyproject)
    members = root_data.get("tool", {}).get("uv", {}).get("workspace", {}).get("members", [])
    names = set()
    for member in members:
        for member_path in sorted(AIRFLOW_ROOT_PATH.glob(f"{member}/pyproject.toml")):
            if name := _load_toml(member_path).get("project", {}).get("name"):
                names.add(canonicalize_name(name))
    return frozenset(names)


def extract_requirements(data: dict) -> list[tuple[str, str]]:
    """Return ``(section, requirement)`` pairs for every dependency table we guard."""
    requirements: list[tuple[str, str]] = []
    project = data.get("project") or {}
    for dependency in project.get("dependencies") or []:
        requirements.append(("project.dependencies", dependency))
    for extra, dependencies in (project.get("optional-dependencies") or {}).items():
        for dependency in dependencies:
            requirements.append((f'project.optional-dependencies."{extra}"', dependency))
    for group, dependencies in (data.get("dependency-groups") or {}).items():
        for dependency in dependencies:
            # A group may also pull in another group via ``{include-group = "..."}``.
            if isinstance(dependency, str):
                requirements.append((f'dependency-groups."{group}"', dependency))
    for dependency in (data.get("build-system") or {}).get("requires") or []:
        requirements.append(("build-system.requires", dependency))
    return requirements


def check_requirement(section: str, dependency: str, workspace_names: frozenset[str]) -> str | None:
    """Return an error message when ``dependency`` needs a lower bound, otherwise ``None``."""
    try:
        requirement = Requirement(dependency)
    except InvalidRequirement as error:
        return f"[{section}] {dependency!r} is not a valid requirement: {error}"
    if (
        requirement.url
        or canonicalize_name(requirement.name) in workspace_names
        or any(specifier.operator in LOWER_BOUND_OPERATORS for specifier in requirement.specifier)
    ):
        return None
    return f"[{section}] {dependency!r} has no lower bound - add one, for example {requirement.name}>=X.Y.Z"


def check_pyproject_file(path: Path, workspace_names: frozenset[str]) -> list[str]:
    return [
        error
        for section, dependency in extract_requirements(_load_toml(path))
        if (error := check_requirement(section, dependency, workspace_names))
    ]


def main() -> int:
    workspace_names = get_workspace_distribution_names()
    failed = False
    for file in sys.argv[1:]:
        path = Path(file)
        if errors := check_pyproject_file(path, workspace_names):
            failed = True
            console.print(f"\n[red]Missing lower bounds in {file}:[/]\n")
            for error in errors:
                console.print(f"  {escape(error)}")
    if failed:
        console.print(
            "\n[bright_yellow]Every dependency resolved from PyPI needs a lower bound.[/]\n"
            "Without one the resolver may pick any published version, so constraints pin\n"
            "whatever the resolution happened to produce rather than the oldest version the\n"
            "code supports. Use the oldest version you are willing to test against.\n"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
