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
#   "requests>=2.31.0",
#   "rich>=13.6.0",
#   "tomli>=2.0.1",
# ]
# ///
"""
Raise the lower bounds of a curated set of dependencies to the newest release older than a cooldown.

Policy lives in ``[tool.airflow.dependency-floors]`` of the root ``pyproject.toml``; see
dev/breeze/doc/adr/0018-raise-dependency-floors-automatically.md.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta
from fnmatch import fnmatchcase
from pathlib import Path

from check_dependency_lower_bounds import extract_requirements
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(minute|hour|day)s?\s*$")


@dataclass(frozen=True)
class FloorConfig:
    min_age: timedelta
    packages: tuple[str, ...]
    groups: tuple[tuple[str, ...], ...]
    exclude: dict[str, str]


def parse_duration(value: str) -> timedelta:
    """Parse the ``N days|hours|minutes`` form uv accepts in ``exclude-newer-package``."""
    match = _DURATION_RE.match(value)
    if not match:
        raise ValueError(f"Invalid duration {value!r}: expected e.g. '180 days', '12 hours'")
    return timedelta(**{f"{match.group(2)}s": float(match.group(1))})


def _canonicalize_pattern(pattern: str) -> str:
    # canonicalize_name keeps "*" and "?" intact, so globs normalize like names do.
    return canonicalize_name(pattern)


def is_curated(name: str, config: FloorConfig) -> bool:
    canonical = canonicalize_name(name)
    return any(fnmatchcase(canonical, pattern) for pattern in config.packages)


def load_config(pyproject_path: Path) -> FloorConfig:
    data = tomllib.loads(pyproject_path.read_text())
    section = data.get("tool", {}).get("airflow", {}).get("dependency-floors")
    if section is None:
        raise ValueError(f"No [tool.airflow.dependency-floors] section in {pyproject_path}")
    config = FloorConfig(
        min_age=parse_duration(section["min-age"]),
        packages=tuple(_canonicalize_pattern(p) for p in section.get("packages", [])),
        groups=tuple(tuple(canonicalize_name(m) for m in group) for group in section.get("groups", [])),
        exclude={canonicalize_name(k): v for k, v in section.get("exclude", {}).items()},
    )
    for group in config.groups:
        for member in group:
            if not is_curated(member, config):
                raise ValueError(f"Group member {member!r} is not covered by 'packages'")
    return config


# A site with any of these means we are deliberately holding the package back.
HOLD_BACK_OPERATORS = {"<", "<=", "!=", "==", "~=", "==="}


@dataclass(frozen=True)
class RequirementSite:
    path: Path
    section: str
    raw: str
    requirement: Requirement


def find_requirements(
    pyproject_paths: Iterable[Path], workspace_names: frozenset[str]
) -> dict[str, list[RequirementSite]]:
    sites: dict[str, list[RequirementSite]] = defaultdict(list)
    for path in pyproject_paths:
        for section, raw in extract_requirements(tomllib.loads(path.read_text())):
            if section == "build-system.requires":
                continue
            try:
                requirement = Requirement(raw)
            except InvalidRequirement:
                # check-dependency-lower-bounds reports these
                continue
            name = canonicalize_name(requirement.name)
            if requirement.url or name in workspace_names:
                continue
            sites[name].append(RequirementSite(path=path, section=section, raw=raw, requirement=requirement))
    return dict(sites)


def get_exclusion_reason(name: str, sites: list[RequirementSite], config: FloorConfig) -> str | None:
    if reason := config.exclude.get(canonicalize_name(name)):
        return reason
    for site in sites:
        held = [str(s) for s in site.requirement.specifier if s.operator in HOLD_BACK_OPERATORS]
        if held:
            return f"held back by {','.join(held)} in {site.path}"
    return None
