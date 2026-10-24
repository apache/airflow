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
"""
Validate ``.github/fork-ci-gate.toml`` (AIP-120 fork CI gate).

The file must parse with the same rules the gate job uses, and every ``[pilot]`` join instant
and ``enabled_since`` value that is new or changed relative to ``main`` must lie in the future.
The gate reads the file from the base branch, so an entry only takes effect when it merges,
while the instant is compared with pull request creation: an instant before the merge would
gate pull requests that already ran project CI. See "Contributor pilot" in
dev/breeze/doc/ci/08_fork_ci_for_external_contributors.md.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = ".github/fork-ci-gate.toml"
UPSTREAM_URL = "https://github.com/apache/airflow.git"
GATE_MODULE_PATH = REPO_ROOT / "dev" / "breeze" / "src" / "airflow_breeze" / "utils" / "fork_ci_gate.py"


def load_gate_module() -> ModuleType:
    # fork_ci_gate.py is standard-library only by design, so it can be loaded without breeze.
    spec = importlib.util.spec_from_file_location("fork_ci_gate", GATE_MODULE_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {GATE_MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def find_violations(base_text: str | None, new_text: str, *, now: datetime) -> list[str]:
    gate = load_gate_module()
    try:
        new = gate.parse_config(new_text)
    except ValueError as e:
        return [str(e)]
    try:
        base = gate.parse_config(base_text) if base_text is not None else None
    except ValueError:
        base = None

    violations = []
    # Moving enabled_since later (switching the gate on) must not gate pull requests opened
    # before the merge. Moving it earlier is the backlog reset and gates older pull requests on
    # purpose. Without a parsable base there is nothing to compare with.
    if base is not None and new.enabled_since > base.enabled_since and new.enabled_since <= now:
        violations.append(
            f"enabled_since moved later to {new.enabled_since.isoformat()}, which is not in the future. "
            "Use an instant after the expected merge."
        )
    if base is not None and not base.enabled and new.enabled and new.enabled_since <= now:
        violations.append(
            f"enabled is switched on while enabled_since is {new.enabled_since.isoformat()}, which is not "
            "in the future; that would gate pull requests that already ran project CI. Set enabled_since "
            "to an instant after the expected merge in the same change."
        )
    # A base branch without the file, such as a release branch the file is backported to, has no
    # gate yet, so no pull request against it was ever gated and every entry takes effect only
    # when the file arrives there.
    # An unparsable base still checks every entry, as all of them count as new.
    if base_text is None:
        return violations
    base_pilot = base.pilot if base is not None else {}
    for login, joined_at in new.pilot.items():
        if base_pilot.get(login) != joined_at and joined_at <= now:
            violations.append(
                f"[pilot] entry {login!r} has join instant {joined_at.isoformat()}, which is not in the "
                "future. Use an instant after the expected merge, such as the next day at 00:00 UTC."
            )
    return violations


def _git(*args: str) -> str | None:
    result = subprocess.run(["git", "-C", str(REPO_ROOT), *args], capture_output=True, text=True, check=False)
    return result.stdout if result.returncode == 0 else None


def resolve_base_ref() -> str | None:
    """Return the revision to compare the configuration with, or None when there is none."""
    # CI checks out the pull request's merge commit shallowly, without any base branch ref, so
    # the base branch named by the pull_request event is fetched explicitly.
    base_branch = os.environ.get("GITHUB_BASE_REF")
    if (
        base_branch
        and _git("fetch", "--quiet", "--depth=1", "origin", f"refs/heads/{base_branch}") is not None
    ):
        return "FETCH_HEAD"
    head = (_git("rev-parse", "HEAD") or "").strip()
    for candidate in ("upstream/main", "origin/main", "main"):
        if _git("rev-parse", "--verify", "--quiet", candidate) is not None:
            merge_base = (_git("merge-base", "HEAD", candidate) or "").strip()
            # On a push to a fork's own main, that main is HEAD itself and compares with nothing.
            if merge_base and merge_base != head:
                return merge_base
    # A push in a contributor's fork has no base branch and a shallow checkout without main, and
    # its origin is the fork: compare with apache/airflow's main, the branch the change targets.
    if _git("fetch", "--quiet", "--depth=1", UPSTREAM_URL, "refs/heads/main") is not None:
        return "FETCH_HEAD"
    return None


def main() -> int:
    new_text = (REPO_ROOT / CONFIG_PATH).read_text()
    base_ref = resolve_base_ref()
    if base_ref is None:
        # Comparing with the change itself would accept any new past instant, so fail instead.
        print(
            f"{CONFIG_PATH}: no base revision to compare with; fetch apache/airflow's main and retry.",
            file=sys.stderr,
        )
        return 1
    base_text = _git("show", f"{base_ref}:{CONFIG_PATH}")
    violations = find_violations(base_text, new_text, now=datetime.now(tz=UTC))
    for violation in violations:
        print(f"{CONFIG_PATH}: {violation}", file=sys.stderr)
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
