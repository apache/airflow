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

import os
import re
import subprocess
import sys
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from fnmatch import fnmatchcase
from pathlib import Path

import requests
from check_dependency_lower_bounds import extract_requirements, get_workspace_distribution_names
from common_prek_utils import AIRFLOW_ROOT_PATH, console
from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

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


def _get_display_path(path: Path) -> Path:
    # Reasons end up in the upgrade PR description, so do not leak the local checkout location.
    try:
        return path.relative_to(AIRFLOW_ROOT_PATH)
    except ValueError:
        return path


UV_HOLD_TABLES = ("constraint-dependencies", "override-dependencies")


def find_uv_holds(root_pyproject: Path) -> dict[str, list[RequirementSite]]:
    """Requirements in the root ``[tool.uv]`` tables that cap packages without declaring a dependency."""
    tool_uv = tomllib.loads(root_pyproject.read_text()).get("tool", {}).get("uv", {})
    holds: dict[str, list[RequirementSite]] = defaultdict(list)
    for table in UV_HOLD_TABLES:
        for raw in tool_uv.get(table, []):
            requirement = Requirement(raw)
            holds[canonicalize_name(requirement.name)].append(
                RequirementSite(
                    path=root_pyproject, section=f"tool.uv.{table}", raw=raw, requirement=requirement
                )
            )
    return dict(holds)


def _get_site_location(site: RequirementSite) -> str:
    if site.section.startswith("tool.uv."):
        return f"[tool.uv] {site.section.removeprefix('tool.uv.')}"
    return str(_get_display_path(site.path))


def get_exclusion_reason(name: str, sites: list[RequirementSite], config: FloorConfig) -> str | None:
    if reason := config.exclude.get(canonicalize_name(name)):
        return reason
    for site in sites:
        held = [str(s) for s in site.requirement.specifier if s.operator in HOLD_BACK_OPERATORS]
        if held:
            return f"held back by {','.join(held)} in {_get_site_location(site)}"
    return None


def _parse_upload_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def get_eligible_versions(releases: dict[str, list[dict]], min_age: timedelta, now: datetime) -> set[Version]:
    cutoff = now - min_age
    eligible: set[Version] = set()
    for version_str, files in releases.items():
        if not files or any(f.get("yanked") for f in files):
            continue
        try:
            version = Version(version_str)
        except InvalidVersion:
            continue
        if version.is_prerelease or version.is_devrelease:
            continue
        if min(_parse_upload_time(f["upload_time_iso_8601"]) for f in files) <= cutoff:
            eligible.add(version)
    return eligible


def find_target_version(releases: dict[str, list[dict]], min_age: timedelta, now: datetime) -> Version | None:
    return max(get_eligible_versions(releases, min_age, now), default=None)


def find_group_target(
    releases_by_member: dict[str, dict[str, list[dict]]], min_age: timedelta, now: datetime
) -> Version | None:
    eligible = [get_eligible_versions(r, min_age, now) for r in releases_by_member.values()]
    return max(set.intersection(*eligible), default=None) if eligible else None


FLOOR_OPERATORS = {">=", ">"}


def get_floor(requirement: Requirement) -> Version | None:
    floors = [Version(s.version) for s in requirement.specifier if s.operator in FLOOR_OPERATORS]
    return max(floors, default=None)


def rewrite_requirement(raw: str, target: Version) -> str | None:
    """Return ``raw`` with its floor raised to ``target``, or None when it would not be raised."""
    requirement = Requirement(raw)
    floor = get_floor(requirement)
    if floor is None or target <= floor:
        return None
    rewritten = raw
    for specifier in requirement.specifier:
        if specifier.operator in FLOOR_OPERATORS and Version(specifier.version) == floor:
            pattern = re.compile(
                rf"{re.escape(specifier.operator)}(\s*){re.escape(specifier.version)}(?![\w.])"
            )
            rewritten = pattern.sub(lambda m: f">={m.group(1)}{target}", rewritten, count=1)
    return rewritten


@dataclass(frozen=True)
class Edit:
    path: Path
    old: str
    new: str


@dataclass(frozen=True)
class Bump:
    unit: str
    packages: tuple[str, ...]
    old_floors: tuple[str, ...]
    target: Version
    edits: tuple[Edit, ...]


def build_bump(unit: str, sites: list[RequirementSite], target: Version) -> Bump | None:
    edits: dict[tuple[Path, str], Edit] = {}
    old_floors: set[str] = set()
    packages: set[str] = set()
    for site in sites:
        if (new := rewrite_requirement(site.raw, target)) is not None:
            edits[(site.path, site.raw)] = Edit(path=site.path, old=site.raw, new=new)
            old_floors.add(str(get_floor(site.requirement)))
            packages.add(canonicalize_name(site.requirement.name))
    if not edits:
        return None
    return Bump(
        unit=unit,
        packages=tuple(sorted(packages)),
        old_floors=tuple(sorted(old_floors, key=Version)),
        target=target,
        edits=tuple(edits.values()),
    )


# Which quoted occurrences of ``edit.new`` each applied edit produced, so a revert restores only
# those and never lowers an identical requirement that was already at the target.
_APPLIED_OCCURRENCES: dict[Edit, list[int]] = {}


def _find_quoted(content: str, text: str) -> list[int]:
    """Return the start offsets of ``text`` quoted with either TOML quote character."""
    return sorted(
        match.start()
        for quote in ('"', "'")
        for match in re.finditer(re.escape(f"{quote}{text}{quote}"), content)
    )


def _replace_at(content: str, starts: list[int], old: str, new: str) -> str:
    for start in sorted(starts, reverse=True):
        quote = content[start]
        content = content[:start] + f"{quote}{new}{quote}" + content[start + len(old) + 2 :]
    return content


def apply_bump(bump: Bump) -> None:
    for edit in bump.edits:
        if not _find_quoted(edit.path.read_text(), edit.old):
            raise ValueError(f"Requirement {edit.old!r} not found in {edit.path}")
    for edit in bump.edits:
        content = edit.path.read_text()
        old_starts = _find_quoted(content, edit.old)
        updated = _replace_at(content, old_starts, edit.old, edit.new)
        shift = len(edit.new) - len(edit.old)
        new_starts = {start + shift * index for index, start in enumerate(old_starts)}
        _APPLIED_OCCURRENCES[edit] = [
            index for index, start in enumerate(_find_quoted(updated, edit.new)) if start in new_starts
        ]
        edit.path.write_text(updated)


def revert_bump(bump: Bump) -> None:
    for edit in reversed(bump.edits):
        content = edit.path.read_text()
        occurrences = _find_quoted(content, edit.new)
        starts = [occurrences[index] for index in _APPLIED_OCCURRENCES.pop(edit)]
        edit.path.write_text(_replace_at(content, starts, edit.new, edit.old))


RESOLVE_COMMANDS: tuple[tuple[str, ...], ...] = (
    ("uv", "lock", "--dry-run"),
    ("uv", "lock", "--dry-run", "--resolution", "lowest-direct"),
)
ERROR_TAIL_LINES = 20


@dataclass(frozen=True)
class ResolveResult:
    ok: bool
    error: str = ""


class LockAlreadyBrokenError(Exception):
    """The workspace does not resolve even without any floor bump."""


def resolve_check(root: Path) -> ResolveResult:
    for command in RESOLVE_COMMANDS:
        result = subprocess.run(list(command), cwd=root, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            tail = "\n".join(result.stderr.strip().splitlines()[-ERROR_TAIL_LINES:])
            return ResolveResult(ok=False, error=f"$ {' '.join(command)}\n{tail}")
    return ResolveResult(ok=True)


def _check_with(bumps: list[Bump], check: Callable[[], ResolveResult]) -> ResolveResult:
    applied: list[Bump] = []
    try:
        for bump in bumps:
            apply_bump(bump)
            applied.append(bump)
        return check()
    finally:
        for bump in reversed(applied):
            revert_bump(bump)


def _find_failing(
    bumps: list[Bump], check: Callable[[], ResolveResult], context: list[Bump]
) -> list[tuple[Bump, str]]:
    """Return the bumps that break resolution when applied on top of ``context`` (known-good bumps)."""
    result = _check_with(context + bumps, check)
    if result.ok:
        return []
    if len(bumps) == 1:
        return [(bumps[0], result.error)]
    middle = len(bumps) // 2
    left, right = bumps[:middle], bumps[middle:]
    failing_left = _find_failing(left, check, context)
    failing_units = {bump.unit for bump, _ in failing_left}
    # The right half is judged on top of the surviving left half, so a failure that needs
    # one bump from each half is pinned on the right-hand one instead of on both halves.
    surviving_left = [bump for bump in left if bump.unit not in failing_units]
    return failing_left + _find_failing(right, check, context + surviving_left)


def apply_with_rollback(
    bumps: list[Bump], check: Callable[[], ResolveResult]
) -> tuple[list[Bump], list[tuple[Bump, str]]]:
    rolled_back: list[tuple[Bump, str]] = []
    remaining = list(bumps)
    while remaining:
        if _check_with(remaining, check).ok:
            break
        if not rolled_back and not (baseline := check()).ok:
            raise LockAlreadyBrokenError(baseline.error)
        failing = _find_failing(remaining, check, context=[])
        failing_units = {bump.unit for bump, _ in failing}
        rolled_back.extend(failing)
        remaining = [bump for bump in remaining if bump.unit not in failing_units]
    for bump in remaining:
        apply_bump(bump)
    return remaining, rolled_back


PYPI_TIMEOUT_SECONDS = 30
# Without a cut-off an unreachable PyPI would cost PYPI_TIMEOUT_SECONDS for every curated package.
MAX_CONSECUTIVE_CONNECTION_FAILURES = 3
REPORT_ENV = "DEPENDENCY_FLOORS_REPORT"


@dataclass
class Report:
    raised: list[Bump]
    skipped: dict[str, str]
    rolled_back: list[tuple[Bump, str]]


def fetch_releases(name: str) -> dict[str, list[dict]]:
    response = requests.get(f"https://pypi.org/pypi/{name}/json", timeout=PYPI_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()["releases"]


def _get_units(names: list[str], config: FloorConfig) -> list[tuple[str, tuple[str, ...]]]:
    """Group curated names into bump units; a group is a unit only when a member is present."""
    units: list[tuple[str, tuple[str, ...]]] = []
    grouped: set[str] = set()
    for group in config.groups:
        if any(member in names for member in group):
            units.append(("+".join(group), group))
            grouped.update(group)
    units.extend((name, (name,)) for name in names if name not in grouped)
    return units


def run(
    root: Path,
    pyproject_paths: list[Path],
    workspace_names: frozenset[str],
    now: datetime,
    fetch: Callable[[str], dict[str, list[dict]]],
    check: Callable[[], ResolveResult],
) -> Report:
    config = load_config(root / "pyproject.toml")
    sites = find_requirements(pyproject_paths, workspace_names)
    uv_holds = find_uv_holds(root / "pyproject.toml")
    curated = sorted(name for name in sites if is_curated(name, config))
    skipped: dict[str, str] = {}
    bumps: list[Bump] = []
    connection_failures = 0
    for unit, members in _get_units(curated, config):
        member_sites = [site for member in members for site in sites.get(member, [])]
        reasons = (
            get_exclusion_reason(member, sites.get(member, []) + uv_holds.get(member, []), config)
            for member in members
        )
        if reason := next((r for r in reasons if r), None):
            skipped[unit] = reason
            continue
        if connection_failures >= MAX_CONSECUTIVE_CONNECTION_FAILURES:
            skipped[unit] = f"PyPI unreachable ({connection_failures} consecutive connection failures)"
            continue
        try:
            releases = {member: fetch(member) for member in members}
            target = find_group_target(releases, config.min_age, now)
        except (requests.HTTPError, KeyError, TypeError, ValueError) as error:
            # PyPI answered, just not with usable data for this package.
            skipped[unit] = f"PyPI metadata unavailable: {error!r}"
            continue
        except OSError as error:
            # requests' own exceptions subclass OSError, so this covers connection errors and timeouts.
            connection_failures += 1
            skipped[unit] = f"PyPI metadata unavailable: {error!r}"
            continue
        connection_failures = 0
        if target is None:
            skipped[unit] = f"no release older than {config.min_age.days} days"
            continue
        if bump := build_bump(unit, member_sites, target):
            bumps.append(bump)
    raised, rolled_back = apply_with_rollback(bumps, check)
    return Report(raised=raised, skipped=skipped, rolled_back=rolled_back)


def render_report(report: Report) -> str:
    lines = ["### Dependency floors", ""]
    lines.append("Raised:" if report.raised else "Raised: none")
    for bump in report.raised:
        names = ", ".join(f"`{p}`" for p in bump.packages)
        files = ", ".join(sorted({str(_get_display_path(edit.path)) for edit in bump.edits}))
        lines.append(f"- {names}: {', '.join(bump.old_floors)} → {bump.target} ({files})")
    if report.rolled_back:
        lines += ["", "Rolled back (resolution failed):"]
        for bump, error in report.rolled_back:
            lines.append(f"- `{bump.unit}` → {bump.target}")
            lines += ["  ```", *(f"  {line}" for line in error.splitlines()), "  ```"]
    if report.skipped:
        lines += ["", "Skipped:"]
        lines += [f"- `{unit}`: {reason}" for unit, reason in sorted(report.skipped.items())]
    return "\n".join(lines) + "\n"


def get_workspace_pyprojects(root: Path) -> list[Path]:
    """Return the root and member pyproject.toml files whose floors the root ``uv.lock`` validates.

    Members with a ``uv.lock`` of their own (dev/breeze) are left out: the resolve check only
    covers the root lock, so raising their floors would leave their lock stale.
    """
    data = tomllib.loads((root / "pyproject.toml").read_text())
    members = data.get("tool", {}).get("uv", {}).get("workspace", {}).get("members", [])
    member_pyprojects = sorted(
        path
        for member in members
        for path in root.glob(f"{member}/pyproject.toml")
        if path.parent != root and not (path.parent / "uv.lock").exists()
    )
    return list(dict.fromkeys([root / "pyproject.toml", *member_pyprojects]))


def main() -> int:
    root = AIRFLOW_ROOT_PATH
    try:
        report = run(
            root,
            get_workspace_pyprojects(root),
            get_workspace_distribution_names(),
            datetime.now(timezone.utc),
            fetch_releases,
            lambda: resolve_check(root),
        )
    except (ValueError, LockAlreadyBrokenError) as error:
        console.print(f"[red]Dependency floors not updated: {error}[/]")
        # Say so in the upgrade PR too, or a broken step would go unnoticed run after run.
        _write_report(f"### Dependency floors\n\nNot updated: {error}\n")
        return 1
    text = render_report(report)
    console.print(text)
    _write_report(text)
    return 0


def _write_report(text: str) -> None:
    if report_path := os.environ.get(REPORT_ENV):
        Path(report_path).write_text(text)


if __name__ == "__main__":
    sys.exit(main())
