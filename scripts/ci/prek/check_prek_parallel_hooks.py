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
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "pyyaml>=6.0.3",
# ]
# ///
"""Fail when a hook of the root prek config sets a priority that is not allowed.

prek runs the hooks that share a ``priority`` value in parallel. In the root
``.pre-commit-config.yaml``, only the hooks in ``PARALLEL_HOOKS`` may set a priority, and they
must all use ``PARALLEL_PRIORITY``. ``scripts/ci/prek/AGENTS.md`` lists the rules that a hook must
follow before it can be added to ``PARALLEL_HOOKS``.

The check rejects every value other than ``PARALLEL_PRIORITY`` because hooks that set no priority
get their position in the file, counted from 0, as their priority. For example, a hook that sets
``50`` would run together with the 51st hook.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

ROOT_CONFIG = Path(".pre-commit-config.yaml")
PARALLEL_PRIORITY = 1000
PARALLEL_HOOKS = frozenset(
    {
        "bandit",
        "check-apache-license-rat",
        "check-builtin-literals",
        "check-contextmanager-class-decorators",
        "check-for-inclusive-language",
        "check-go-sdk-serialization-conformance",
        "check-lazy-logging",
        "check-metrics-synced-with-registry",
        "check-template-fields-valid",
        "check-test-only-imports-in-src",
        "check-ts-sdk-serialization-conformance-shared",
        "check-xcom-model-columns",
        "codespell",
        "debug-statements",
        "lint-dockerfile",
        "lint-markdown",
        "lychee-docker",
        "prevent-deprecated-sqlalchemy-usage",
        "python-no-log-warn",
        "shellcheck",
        "yamllint",
        "zizmor",
    }
)


def find_unaudited_priority_hooks(config_path: Path) -> list[str]:
    config = yaml.safe_load(config_path.read_text()) or {}
    return [
        f"{hook['id']} (priority {hook['priority']!r})"
        for repo in config.get("repos", [])
        for hook in repo.get("hooks", [])
        if "priority" in hook and (hook["priority"] != PARALLEL_PRIORITY or hook["id"] not in PARALLEL_HOOKS)
    ]


def main() -> int:
    errors = find_unaudited_priority_hooks(ROOT_CONFIG)
    if errors:
        print(f"ERROR: These hooks in {ROOT_CONFIG} set a priority that is not allowed:\n")
        print("\n".join(f"  {error}" for error in errors))
        print(
            "\nOnly the hooks listed in PARALLEL_HOOKS in scripts/ci/prek/check_prek_parallel_hooks.py"
            f" may set a priority, and the value must be {PARALLEL_PRIORITY}. Those hooks run in parallel."
            " Before you add a hook to PARALLEL_HOOKS, check that it follows the rules in"
            " scripts/ci/prek/AGENTS.md."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
