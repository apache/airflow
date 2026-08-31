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
"""
Check that each UI workspace's ``pnpm.overrides`` in package.json is mirrored by the
``overrides:`` block pnpm writes at the top of that workspace's pnpm-lock.yaml.

Six UI workspaces pin security-advisory overrides this way. In some grouped dependabot
updates the regenerated lockfile comes back without the ``overrides:`` section at all,
while package.json still declares it. That silently drops the pins, and a frozen install
of the mismatched pair fails anyway with a bare ERR_PNPM_LOCKFILE_CONFIG_MISMATCH buried
in a hook log. This check compares the two sides directly and names exactly what drifted,
so the failure is legible without having to reproduce the frozen install locally first.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import yaml
from common_prek_utils import AIRFLOW_ROOT_PATH, console
from rich.markup import escape

# Workspaces that pin security-advisory overrides via `pnpm.overrides` in package.json,
# mirrored into `overrides:` at the top of their own pnpm-lock.yaml. Keep this list in
# sync with .github/dependabot.yml's npm entries and .pre-commit-config.yaml's excludes
# for these same lockfiles.
UI_WORKSPACES = (
    "airflow-core/src/airflow/ui",
    "airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui",
    "providers/edge3/src/airflow/providers/edge3/plugins/www",
    "providers/fab/src/airflow/providers/fab/www",
    "providers/common/ai/src/airflow/providers/common/ai/plugins/www",
    "dev/react-plugin-tools/react_plugin_template",
)


def _load_package_json_overrides(workspace: Path) -> dict[str, str]:
    package_json = json.loads((workspace / "package.json").read_text())
    return package_json.get("pnpm", {}).get("overrides", {}) or {}


def _load_lockfile_overrides(workspace: Path) -> dict[str, str]:
    lockfile_path = workspace / "pnpm-lock.yaml"
    if not lockfile_path.exists():
        return {}
    lockfile = yaml.safe_load(lockfile_path.read_text())
    return (lockfile or {}).get("overrides") or {}


def diff_workspace_overrides(workspace_rel: str) -> list[str]:
    """Return human-readable diff lines for one workspace, empty when the two sides agree."""
    workspace = AIRFLOW_ROOT_PATH / workspace_rel
    package_json_overrides = _load_package_json_overrides(workspace)
    lockfile_overrides = _load_lockfile_overrides(workspace)

    if not package_json_overrides and not lockfile_overrides:
        return []

    missing_from_lockfile = sorted(set(package_json_overrides) - set(lockfile_overrides))
    extra_in_lockfile = sorted(set(lockfile_overrides) - set(package_json_overrides))
    mismatched_values = sorted(
        key
        for key in set(package_json_overrides) & set(lockfile_overrides)
        if package_json_overrides[key] != lockfile_overrides[key]
    )

    errors = []
    if missing_from_lockfile:
        errors.append(
            "declared in package.json pnpm.overrides but missing from the pnpm-lock.yaml "
            f"overrides: block: {', '.join(missing_from_lockfile)}"
        )
    if extra_in_lockfile:
        errors.append(
            "present in the pnpm-lock.yaml overrides: block but not in package.json "
            f"pnpm.overrides: {', '.join(extra_in_lockfile)}"
        )
    for key in mismatched_values:
        errors.append(
            f"{key!r} differs: package.json wants {package_json_overrides[key]!r}, "
            f"lockfile has {lockfile_overrides[key]!r}"
        )
    return errors


def main() -> int:
    failed = False
    for workspace_rel in UI_WORKSPACES:
        if errors := diff_workspace_overrides(workspace_rel):
            failed = True
            console.print(f"\n[red]pnpm.overrides drift in {workspace_rel}:[/]\n")
            for error in errors:
                console.print(f"  {escape(error)}")
    if failed:
        console.print(
            "\n[bright_yellow]Each affected workspace's `pnpm.overrides` in package.json is a "
            "security-advisory pin, and pnpm mirrors it into the `overrides:` block at the top of "
            "that workspace's own pnpm-lock.yaml. A frozen install of a mismatched pair fails anyway, "
            "but as a bare ERR_PNPM_LOCKFILE_CONFIG_MISMATCH - this check just names the drift instead.\n"
            "Regenerate the lockfile from the workspace's pinned pnpm version (see its "
            "package.json `packageManager` field) so it picks the overrides back up, for example:\n"
            "  npx pnpm@<pinned-version> --dir <workspace> install --no-frozen-lockfile\n"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
