#!/usr/bin/env python3
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
"""
Compute the CI image install set for one Python minor without excluded workspace members.

``uv sync --all-packages`` installs every workspace member, including the dev groups of providers
excluded for a Python minor (``Requires-Python`` carries ``!=<minor>``). Their third-party
dependencies would still be installed, and some do not build for the new interpreter.

The helper resolves the requirements of the members that are not excluded, not the lock's export
graph. Each kept member becomes a local ``file://`` requirement, and the requirements of each kept
member's ``dev`` group (the root's ``ci-image`` group for the root) are added verbatim, with
excluded members removed. ``uv pip compile`` resolves that input for the target interpreter and
platform, so markers apply per edge. The lock's pins are passed as constraints, so every version
matches the lock. Output is the third-party pins and the editable member directories.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from glob import glob
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

MEMBER_DEFAULT_GROUP = "dev"
ROOT_CI_GROUP = "ci-image"
MINOR_VERSION = re.compile(r"\d+\.\d+")
REQUIREMENT_NAME = re.compile(r"\s*([A-Za-z0-9][A-Za-z0-9._-]*)")
RESOLVED_MEMBER = re.compile(r"^([^\s@]+)\s*@\s*(file://\S+)$")
PINNED_NAME = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[[^\]]*\])?\s*(?:==|@)")
PINNED_VERSION = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s;]+)")
GLOB_CHARACTERS = frozenset("*?[")
PYTHON_PLATFORMS = {"x86_64": "x86_64-manylinux_2_28", "aarch64": "aarch64-manylinux_2_28"}


def normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def requirement_name(entry: str) -> str:
    match = REQUIREMENT_NAME.match(entry)
    if not match:
        raise ValueError(f"Cannot read a requirement name from {entry!r}")
    return normalize_name(match.group(1))


def is_python_minor_excluded(requires_python: str, minor: str) -> bool:
    # The lookahead keeps longer versions such as !=3.150 or !=3.15.1 from matching a whole-minor exclusion.
    return re.search(rf"!=\s*{re.escape(minor)}(\.\*)?(?![\d.])", requires_python) is not None


def load_manifest(directory: Path) -> dict[str, Any]:
    with (directory / "pyproject.toml").open("rb") as manifest:
        return tomllib.load(manifest)


def has_glob_characters(pattern: str) -> bool:
    return any(character in GLOB_CHARACTERS for character in pattern)


def find_workspace_members(root: Path, patterns: list[str]) -> list[Path]:
    members: list[Path] = []
    for pattern in patterns:
        matches = sorted(glob(str(root / pattern)))
        if not matches and not has_glob_characters(pattern):
            raise ValueError(f"Workspace member {pattern!r} does not exist under {root}")
        for match in matches:
            candidate = Path(match)
            if (candidate / "pyproject.toml").is_file():
                members.append(candidate)
            elif not has_glob_characters(pattern):
                raise ValueError(f"Workspace member {pattern!r} has no pyproject.toml")
    return members


def expand_group(groups: dict[str, list[Any]], group: str, seen: set[str] | None = None) -> list[str]:
    """Return the raw requirement strings of a dependency group, following ``include-group`` entries."""
    if seen is None:
        seen = set()
    if group in seen:
        return []
    seen.add(group)
    entries: list[str] = []
    for entry in groups.get(group, []):
        if isinstance(entry, dict):
            entries.extend(expand_group(groups, entry["include-group"], seen))
        else:
            entries.append(entry)
    return entries


def split_members(root: Path, minor: str) -> tuple[dict[str, Path], set[str]]:
    """Return the members kept for installation (name -> directory) and the excluded member names."""
    patterns = load_manifest(root)["tool"]["uv"]["workspace"]["members"]
    kept: dict[str, Path] = {}
    excluded: set[str] = set()
    for member in find_workspace_members(root, patterns):
        project = load_manifest(member).get("project", {})
        name = normalize_name(project.get("name", member.name))
        if is_python_minor_excluded(project.get("requires-python", ""), minor):
            excluded.add(name)
        else:
            kept[name] = member
    return kept, excluded


def build_requirement_input(root: Path, kept: dict[str, Path], excluded: set[str]) -> list[str]:
    """Return the requirements to resolve: kept members as local references and their dev requirements."""
    lines = [f"{name} @ {path.resolve().as_uri()}" for name, path in sorted(kept.items())]
    for path in kept.values():
        groups = load_manifest(path).get("dependency-groups", {})
        group = ROOT_CI_GROUP if path == root else MEMBER_DEFAULT_GROUP
        for entry in expand_group(groups, group):
            if requirement_name(entry) not in excluded:
                lines.append(entry)
    return lines


def resolve_requirements(
    requirements: list[str],
    constraints: Path,
    minor: str,
    platform: str,
    workdir: Path,
    root: Path,
    find_links: list[Path],
) -> list[str]:
    """Resolve the requirements for the target interpreter with the lock's pins as constraints.

    The command runs from the workspace root so that the root's ``exclude-newer`` applies to the resolver.
    """
    # uv runs from the workspace root, so relative paths would resolve there rather than here.
    workdir = workdir.resolve()
    constraints = constraints.resolve()
    requirements_in = workdir / "requirements.in"
    resolved = workdir / "resolved.txt"
    requirements_in.write_text("\n".join(requirements) + "\n", encoding="utf-8")
    find_links_args = [argument for directory in find_links for argument in ("--find-links", str(directory))]
    subprocess.run(
        [
            "uv",
            "pip",
            "compile",
            str(requirements_in),
            "--constraints",
            str(constraints),
            "--python-version",
            minor,
            "--python-platform",
            platform,
            *find_links_args,
            "--no-header",
            "--no-annotate",
            "--output-file",
            str(resolved),
        ],
        check=True,
        cwd=root,
    )
    return resolved.read_text(encoding="utf-8").splitlines()


def split_resolved(lines: list[str]) -> tuple[list[str], list[Path]]:
    """Separate third-party pins from the resolved local member directories (``-e file://`` or ``name @ file://``)."""
    pins: list[str] = []
    members: list[Path] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("-e "):
            members.append(member_path(stripped[len("-e ") :]))
            continue
        match = RESOLVED_MEMBER.match(stripped)
        if match:
            members.append(member_path(match.group(2)))
        elif "file://" in stripped:
            raise ValueError(f"Cannot read a local member reference from the resolved output: {stripped!r}")
        else:
            pins.append(line)
    return pins, members


def member_path(file_uri: str) -> Path:
    return Path(unquote(urlparse(file_uri.strip()).path))


def verify_resolved_pins(pins: list[str], lock_pins: str, excluded: set[str]) -> None:
    """Fail when the resolver returns an excluded member, a package the lock does not contain, or a locked version differing."""
    locked_names: set[str] = set()
    locked_versions: dict[str, set[str]] = {}
    for line in lock_pins.splitlines():
        stripped = line.strip()
        name_match = PINNED_NAME.match(stripped)
        if name_match:
            locked_names.add(normalize_name(name_match.group(1)))
        version_match = PINNED_VERSION.match(stripped)
        if version_match:
            locked_versions.setdefault(normalize_name(version_match.group(1)), set()).add(
                version_match.group(2)
            )
    problems: list[str] = []
    for line in pins:
        stripped = line.strip()
        name_match = PINNED_NAME.match(stripped)
        if not name_match:
            problems.append(f"unreadable resolved requirement {stripped!r}")
            continue
        name = normalize_name(name_match.group(1))
        if name in excluded:
            problems.append(f"excluded member {name} was resolved from an index")
        elif name not in locked_names:
            problems.append(f"{name} is not in the lock")
        else:
            version_match = PINNED_VERSION.match(stripped)
            if version_match and version_match.group(2) not in locked_versions.get(name, set()):
                problems.append(
                    f"{name}=={version_match.group(2)} differs from the locked versions "
                    f"{sorted(locked_versions.get(name, set()))}"
                )
    if problems:
        raise ValueError("Resolved pins disagree with the lock: " + "; ".join(problems))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--root", type=Path, required=True, help="Workspace root holding the root pyproject.toml"
    )
    parser.add_argument(
        "--constraints", type=Path, required=True, help="Lock pins (uv export) used as constraints"
    )
    parser.add_argument("--python-minor", required=True, help="Target Python major.minor, e.g. 3.15")
    parser.add_argument(
        "--platform", choices=sorted(PYTHON_PLATFORMS), required=True, help="Target CPU architecture"
    )
    parser.add_argument("--workdir", type=Path, required=True, help="Scratch directory for resolver inputs")
    parser.add_argument("--output", type=Path, required=True, help="Where to write the third-party pins")
    parser.add_argument(
        "--editables", type=Path, required=True, help="Where to write the kept member directories"
    )
    parser.add_argument(
        "--find-links",
        type=Path,
        action="append",
        default=[],
        help="Directory of local wheels the resolver may use, e.g. temporary wheels for a missing release",
    )
    args = parser.parse_args(argv)
    if not MINOR_VERSION.fullmatch(args.python_minor):
        parser.error(f"--python-minor must be major.minor, got {args.python_minor!r}")
    root = args.root.resolve()
    args.workdir.mkdir(parents=True, exist_ok=True)

    kept, excluded = split_members(root, args.python_minor)
    requirements = build_requirement_input(root, kept, excluded)
    resolved = resolve_requirements(
        requirements,
        args.constraints,
        args.python_minor,
        PYTHON_PLATFORMS[args.platform],
        args.workdir,
        root,
        [directory.resolve() for directory in args.find_links],
    )
    pins, member_dirs = split_resolved(resolved)
    verify_resolved_pins(pins, args.constraints.read_text(encoding="utf-8"), excluded)
    kept_dirs = {path.resolve() for path in kept.values()}
    unexpected = {path for path in member_dirs if path.resolve() not in kept_dirs}
    if unexpected:
        raise ValueError(f"Resolver returned members that were not kept: {sorted(map(str, unexpected))}")
    missing = sorted(str(path) for path in kept_dirs - {path.resolve() for path in member_dirs})
    if missing:
        raise ValueError(f"Kept members missing from the resolved set: {missing}")

    args.output.write_text("".join(f"{pin}\n" for pin in pins), encoding="utf-8")
    args.editables.write_text("".join(f"{path}\n" for path in sorted(kept_dirs)), encoding="utf-8")
    print(
        f"Excluded members for Python {args.python_minor}: {len(excluded)}; "
        f"third-party pins: {len(pins)}; editable members: {len(kept_dirs)}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
