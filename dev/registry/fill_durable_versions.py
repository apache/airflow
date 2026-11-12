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
"""Date each durable module in modules.json against the provider tags that first shipped it.

Runs on the host rather than in Breeze with the rest of the module extraction, because it reads
the ``providers-<id>/<version>`` git tags and Breeze does not mount ``.git``.

Usage:
    python dev/registry/fill_durable_versions.py --modules dev/registry/modules.json
"""

from __future__ import annotations

import argparse
import functools
import json
import subprocess
import sys
from pathlib import Path

from packaging.version import InvalidVersion, Version

AIRFLOW_ROOT = Path(__file__).parent.parent.parent

# Inputs to the dating, not part of the published /api/modules.json contract.
EXTRACTION_ONLY_FIELDS = ("durable_mechanism", "durable_gating_class")

# Patterns matching the mechanism, never its registry declaration, and earliest match wins.
# KubernetesPodOperator reconnected via task_state_store in 10.20.0 but only declared
# `__supports_durable_execution` in 10.21.0; AgentOperator took `durable` in common-ai 0.1.0
# but only moved to task_state_store in 0.7.0.
DURABLE_TOKENS = {
    "marker": r"task_state_store|durable: bool",
    "resumable_mixin": "execute_resumable",
}


class GitCommandError(RuntimeError):
    """A git call failed for a reason other than one the caller expected."""


def run_git(*args: str, allowed_returncodes: tuple[int, ...] = (0,)) -> list[str]:
    try:
        result = subprocess.run(["git", *args], capture_output=True, text=True, cwd=AIRFLOW_ROOT, check=False)
    except OSError as exc:
        raise GitCommandError(f"Could not run git {' '.join(args)}: {exc}") from exc
    if result.returncode not in allowed_returncodes:
        raise GitCommandError(
            f"git {' '.join(args)} failed with exit code {result.returncode}: {result.stderr.strip()}"
        )
    return [line for line in result.stdout.splitlines() if line]


@functools.cache
def get_released_tags(provider_id: str) -> tuple[str, ...]:
    tags = []
    for tag in run_git("tag", "--list", f"providers-{provider_id}/*"):
        try:
            version = Version(tag.rsplit("/", 1)[-1])
        except InvalidVersion:
            continue
        if not version.is_prerelease:
            tags.append(tag)
    return tuple(tags)


def build_source_path(provider_id: str, qualified_class: str) -> str:
    module_path = qualified_class.rsplit(".", 1)[0].replace(".", "/")
    return f"providers/{provider_id.replace('-', '/')}/src/{module_path}.py"


def find_first_releases(provider_id: str, token: str, paths: set[str]) -> dict[str, str]:
    """Map each path to the earliest released version of `provider_id` whose copy holds `token`.

    One `git grep` searches every release tag at once, so this costs a single call per provider
    rather than one per version, and it reports every match rather than assuming the capability
    was added once and kept.
    """
    tags = get_released_tags(provider_id)
    if not tags:
        return {}
    # git grep exits 1 when nothing matches, which is an ordinary answer here.
    matches = run_git("grep", "-lE", token, *tags, "--", *sorted(paths), allowed_returncodes=(0, 1))
    earliest: dict[str, Version] = {}
    for line in matches:
        tag, _, path = line.partition(":")
        try:
            version = Version(tag.rsplit("/", 1)[-1])
        except InvalidVersion:
            continue
        if path in paths and (path not in earliest or version < earliest[path]):
            earliest[path] = version
    return {path: str(version) for path, version in earliest.items()}


def durable_group(module: dict) -> tuple[str, str, str] | None:
    """Return the (provider, token, source path) a module's durable capability is dated from."""
    provider_id = module.get("durable_gating_provider")
    qualified_class = module.get("durable_gating_class")
    token = DURABLE_TOKENS.get(module.get("durable_mechanism") or "")
    if not (provider_id and qualified_class and token):
        return None
    return provider_id, token, build_source_path(provider_id, qualified_class)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--modules", required=True, nargs="+", type=Path, help="modules.json to update in place"
    )
    args = parser.parse_args()

    # Nothing can be dated without tags, but the extraction-only fields still have to go.
    has_tags = bool(run_git("tag", "--list", "providers-*"))
    if not has_tags:
        print("No providers-* tags in this checkout; leaving durable_since_version unset.")

    for modules_file in args.modules:
        if not modules_file.exists():
            print(f"Skipping missing {modules_file}")
            continue
        catalog = json.loads(modules_file.read_text())

        wanted: dict[tuple[str, str], set[str]] = {}
        if has_tags:
            for module in catalog["modules"]:
                group = durable_group(module)
                if group:
                    provider_id, token, path = group
                    wanted.setdefault((provider_id, token), set()).add(path)

        versions = {
            (provider_id, path): version
            for (provider_id, token), paths in wanted.items()
            for path, version in find_first_releases(provider_id, token, paths).items()
        }

        filled = 0
        for module in catalog["modules"]:
            group = durable_group(module) if has_tags else None
            if group:
                provider_id, _, path = group
                module["durable_since_version"] = versions.get((provider_id, path))
                filled += (provider_id, path) in versions
            for field in EXTRACTION_ONLY_FIELDS:
                module.pop(field, None)
        modules_file.write_text(json.dumps(catalog, indent=2))
        print(f"{modules_file}: dated {filled} durable modules")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except GitCommandError as error:
        print(error, file=sys.stderr)
        sys.exit(1)
