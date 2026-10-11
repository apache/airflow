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
"""
Select the workspace members the CI image installs for the Python interpreter running this script.

A provider is excluded when its ``requires-python`` rejects the interpreter. Breeze writes that field
from ``excluded-python-versions`` in ``provider.yaml``, and every ``pyproject.toml`` is in the build
context, including the stage that carries only the dependency manifests. Only the clauses Breeze writes
are understood; any other clause fails the build rather than being guessed at. Only providers are
evaluated, so a core distribution can never drop out of the install set by accident.
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from pathlib import Path

PROVIDER_DISTRIBUTION_PREFIX = "apache-airflow-providers-"
# The two clause forms dev/breeze/src/airflow_breeze/utils/packages.py writes into a provider's
# requires-python: the minimum version, and one wildcard exclusion per excluded Python version.
LOWER_BOUND_CLAUSE = re.compile(r">=\s*(?P<release>\d+(?:\.\d+)*)")
EXCLUSION_CLAUSE = re.compile(r"!=\s*(?P<release>\d+(?:\.\d+)*)\.\*")
# Exit codes the shell probe distinguishes: anything else means the probe itself failed.
EXIT_HAS_EXCLUDED_MEMBERS = 0
EXIT_NO_EXCLUDED_MEMBERS = 10


def find_workspace_members(root: Path, entries: list[str]) -> list[Path]:
    """Resolve the ``[tool.uv.workspace] members`` entries, which the root lists one directory each."""
    members: set[Path] = set()
    for entry in entries:
        member = root / entry
        if not member.is_dir():
            raise ValueError(f"Workspace member {entry!r} does not exist under {root}")
        if not (member / "pyproject.toml").is_file():
            raise ValueError(f"Workspace member {entry!r} has no pyproject.toml")
        members.add(member)
    return sorted(members)


def load_manifest(directory: Path) -> dict:
    path = directory / "pyproject.toml"
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as error:
        raise ValueError(f"Cannot parse {path}: {error}") from None


def parse_release(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.split("."))


def pad_release(release: tuple[int, ...], length: int) -> tuple[int, ...]:
    return release + (0,) * (length - len(release))


def is_python_version_allowed(requires_python: str, python_version: tuple[int, ...]) -> bool:
    """Evaluate ``requires-python`` for ``python_version`` as PEP 440 does, for the clauses Breeze writes."""
    for clause in (clause.strip() for clause in requires_python.split(",")):
        if lower_bound := LOWER_BOUND_CLAUSE.fullmatch(clause):
            release = parse_release(lower_bound["release"])
            length = max(len(release), len(python_version))
            if pad_release(python_version, length) < pad_release(release, length):
                return False
        elif exclusion := EXCLUSION_CLAUSE.fullmatch(clause):
            prefix = parse_release(exclusion["release"])
            if pad_release(python_version, len(prefix))[: len(prefix)] == prefix:
                return False
        else:
            raise ValueError(f"Unsupported requires-python clause {clause!r}")
    return True


def split_members(root: Path, python_version: tuple[int, ...]) -> tuple[dict[str, Path], set[str]]:
    """Split the workspace into the members kept for ``python_version`` and the excluded provider names."""
    entries = load_manifest(root).get("tool", {}).get("uv", {}).get("workspace", {}).get("members", [])
    kept: dict[str, Path] = {}
    excluded: set[str] = set()
    for member in find_workspace_members(root, entries):
        project = load_manifest(member).get("project", {})
        name = project.get("name")
        if not name:
            raise ValueError(f"No project.name in {member / 'pyproject.toml'}")
        requires_python = project.get("requires-python")
        if name.startswith(PROVIDER_DISTRIBUTION_PREFIX) and requires_python:
            try:
                allowed = is_python_version_allowed(requires_python, python_version)
            except ValueError as error:
                raise ValueError(f"{member / 'pyproject.toml'}: {error}") from None
            if not allowed:
                excluded.add(name)
                continue
        kept[name] = member
    return kept, excluded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--root", type=Path, required=True, help="Workspace root")
    parser.add_argument(
        "--python-version",
        type=parse_release,
        default=sys.version_info[:3],
        help="Python version to select members for, e.g. 3.15.0 (default: the interpreter running this)",
    )
    parser.add_argument("--packages", type=Path, help="Where to write the kept distribution names")
    parser.add_argument("--editables", type=Path, help="Where to write the kept member directories")
    parser.add_argument(
        "--probe",
        action="store_true",
        help=f"Exit {EXIT_HAS_EXCLUDED_MEMBERS} when some member is excluded, "
        f"{EXIT_NO_EXCLUDED_MEMBERS} when none is, and non-zero otherwise on failure",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()

    kept, excluded = split_members(root, args.python_version)
    print(
        f"Providers excluded for Python {'.'.join(map(str, args.python_version))}: "
        f"{', '.join(sorted(excluded)) or 'none'}; kept members: {len(kept)}",
        file=sys.stderr,
    )
    if args.probe:
        return EXIT_HAS_EXCLUDED_MEMBERS if excluded else EXIT_NO_EXCLUDED_MEMBERS
    if args.packages:
        args.packages.write_text("".join(f"{name}\n" for name in sorted(kept)), encoding="utf-8")
    if args.editables:
        args.editables.write_text(
            "".join(f"{path}\n" for path in sorted(str(path) for path in kept.values())),
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
