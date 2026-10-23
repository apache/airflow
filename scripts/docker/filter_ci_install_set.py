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
Select the workspace members the CI image installs for one Python minor.

``uv sync --all-packages`` installs every workspace member, including providers excluded for this
Python minor, and the third-party packages only those providers reach - some of which have no wheels
for the new interpreter. Listing the members that are kept lets the installer export the lock for
exactly those: ``uv export --package <member>`` emits the lock's own closure, already carrying the
hashes and the environment markers that encode each provider's exclusions, so the install filters
itself on the target interpreter. No second resolution runs, so nothing can drift from the lock.

Exclusions are read from ``generated/provider_dependencies.json``, which the provider tooling
regenerates from each ``provider.yaml``. Only providers appear there, so a core distribution can
never drop out of the install set by accident.
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from glob import glob
from pathlib import Path

PROVIDER_DEPENDENCIES_PATH = Path("generated") / "provider_dependencies.json"
GLOB_CHARACTERS = frozenset("*?[")
# Exit codes the shell probe distinguishes: anything else means the probe itself failed.
EXIT_HAS_EXCLUDED_MEMBERS = 0
EXIT_NO_EXCLUDED_MEMBERS = 10


def has_glob_characters(pattern: str) -> bool:
    return any(character in GLOB_CHARACTERS for character in pattern)


def find_workspace_members(root: Path, patterns: list[str]) -> list[Path]:
    """Expand the ``[tool.uv.workspace] members`` patterns into directories holding a manifest."""
    members: list[Path] = []
    for pattern in patterns:
        if has_glob_characters(pattern):
            matches = [Path(match) for match in glob(str(root / pattern))]
            members.extend(match for match in matches if (match / "pyproject.toml").is_file())
            continue
        member = root / pattern
        if not member.is_dir():
            raise ValueError(f"Workspace member {pattern!r} does not exist under {root}")
        if not (member / "pyproject.toml").is_file():
            raise ValueError(f"Workspace member {pattern!r} has no pyproject.toml")
        members.append(member)
    return sorted(set(members))


def load_manifest(directory: Path) -> dict:
    return tomllib.loads((directory / "pyproject.toml").read_text(encoding="utf-8"))


def read_distribution_name(directory: Path) -> str:
    name = load_manifest(directory).get("project", {}).get("name")
    if not name:
        raise ValueError(f"No project.name in {directory / 'pyproject.toml'}")
    return name


def build_provider_distribution_name(provider_id: str) -> str:
    """Turn a provider id from the generated metadata (``apache.kafka``) into its distribution name."""
    return "apache-airflow-providers-" + provider_id.replace(".", "-")


def read_excluded_distributions(root: Path, minor: str) -> set[str]:
    """Return the distribution names of providers excluded for ``minor``."""
    metadata = json.loads((root / PROVIDER_DEPENDENCIES_PATH).read_text(encoding="utf-8"))
    return {
        build_provider_distribution_name(provider_id)
        for provider_id, details in metadata.items()
        if minor in details.get("excluded-python-versions", [])
    }


def split_members(root: Path, minor: str) -> tuple[dict[str, Path], set[str]]:
    """Split the workspace into the members kept for ``minor`` and the excluded distribution names."""
    patterns = load_manifest(root).get("tool", {}).get("uv", {}).get("workspace", {}).get("members", [])
    excluded = read_excluded_distributions(root, minor)
    kept: dict[str, Path] = {}
    seen_excluded: set[str] = set()
    for member in find_workspace_members(root, patterns):
        name = read_distribution_name(member)
        if name in excluded:
            seen_excluded.add(name)
        else:
            kept[name] = member
    return kept, seen_excluded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--root", type=Path, required=True, help="Workspace root")
    parser.add_argument("--python-minor", required=True, help="Target Python major.minor, e.g. 3.15")
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

    kept, excluded = split_members(root, args.python_minor)
    if args.probe:
        return EXIT_HAS_EXCLUDED_MEMBERS if excluded else EXIT_NO_EXCLUDED_MEMBERS
    if args.packages:
        args.packages.write_text("".join(f"{name}\n" for name in sorted(kept)), encoding="utf-8")
    if args.editables:
        args.editables.write_text(
            "".join(f"{path}\n" for path in sorted(str(path) for path in kept.values())),
            encoding="utf-8",
        )
    print(
        f"Excluded members for Python {args.python_minor}: {len(excluded)}; kept members: {len(kept)}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
