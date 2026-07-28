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
# requires-python = ">=3.10"
# dependencies = []
# ///
"""Fail when any shared/<dist> workspace member is missing its mypy prek config.

Every shared library has its own `mypy-shared-<dist>` prek hook backed by a
dedicated `.pre-commit-config.yaml`. When a new shared distribution is added
under `shared/`, the contributor must also add the matching prek config so the
mypy hook runs for that library. This check enforces that rule.
"""

from __future__ import annotations

import sys

from common_prek_utils import AIRFLOW_ROOT_PATH

SHARED_DIR = AIRFLOW_ROOT_PATH / "shared"

EXPECTED_TEMPLATE = """\
# <license header>
---
default_stages: [pre-commit, pre-push]
minimum_prek_version: '0.3.4'
default_language_version:
  python: python3
repos:
  - repo: local
    hooks:
      - id: mypy-shared-{dist}
        name: Run mypy for shared-{dist}
        language: python
        entry: >-
          ../../scripts/ci/prek/run_mypy_full_dist_local_venv_or_breeze_in_ci.py
          shared/{dist}
        pass_filenames: false
        files: ^.*\\.py$
        require_serial: true
"""


def main() -> int:
    missing: list[str] = []
    for dist_dir in sorted(SHARED_DIR.iterdir()):
        if not (dist_dir / "pyproject.toml").exists():
            continue
        dist = dist_dir.name
        hook_id = f"mypy-shared-{dist}"
        config = dist_dir / ".pre-commit-config.yaml"
        if not config.exists() or hook_id not in config.read_text():
            missing.append(dist)

    if missing:
        print(
            "ERROR: The following shared/<dist> workspace members are missing their "
            f"dedicated mypy prek hook: {', '.join(missing)}\n"
        )
        print(
            "Every shared library must ship its own mypy-shared-<dist> hook so it is\n"
            "type-checked in isolation (dedicated virtualenv + mypy cache under .build/).\n"
        )
        print(
            "Create shared/<dist>/.pre-commit-config.yaml with the following contents\n"
            "(add the ASF license header at the top) for each missing distribution:\n"
        )
        for dist in missing:
            print(f"--- shared/{dist}/.pre-commit-config.yaml ---")
            print(EXPECTED_TEMPLATE.format(dist=dist))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
