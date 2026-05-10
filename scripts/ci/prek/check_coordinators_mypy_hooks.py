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
"""Fail when any sdk/coordinators/<dist> workspace member is missing its mypy prek config."""

from __future__ import annotations

import sys

from common_prek_utils import coordinator_distribution_dirs

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
      - id: mypy-coordinators-{dist}
        name: Run mypy for coordinators-{dist}
        language: python
        entry: >-
          ../../../scripts/ci/prek/run_mypy_full_dist_local_venv_or_breeze_in_ci.py
          sdk/coordinators/{dist}
        pass_filenames: false
        files: ^.*\\.py$
        require_serial: true
"""


def missing_mypy_hook_distribution_ids() -> list[str]:
    missing: list[str] = []
    for dist_dir in coordinator_distribution_dirs():
        dist = dist_dir.name
        hook_id = f"mypy-coordinators-{dist}"
        config = dist_dir / ".pre-commit-config.yaml"
        if not config.exists() or hook_id not in config.read_text():
            missing.append(dist)
    return missing


def main() -> int:
    missing = missing_mypy_hook_distribution_ids()
    if missing:
        print(
            "ERROR: The following sdk/coordinators/<dist> workspace members are missing their "
            f"dedicated mypy prek hook: {', '.join(missing)}\n"
        )
        print(
            "Every coordinator distribution must ship its own mypy-coordinators-<dist> hook so it is\n"
            "type-checked in isolation (dedicated virtualenv + mypy cache under .build/).\n"
        )
        print(
            "Create sdk/coordinators/<dist>/.pre-commit-config.yaml with the following contents\n"
            "(add the ASF license header at the top) for each missing distribution:\n"
        )
        for dist in missing:
            print(f"--- sdk/coordinators/{dist}/.pre-commit-config.yaml ---")
            print(EXPECTED_TEMPLATE.format(dist=dist))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
