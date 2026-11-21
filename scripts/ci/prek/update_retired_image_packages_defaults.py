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
# requires-python = ">=3.11,<3.12"
# dependencies = []
# ///
"""
Keep the defaults of the "Delete retired image packages" workflow in sync.

A run with the defaults must never delete images that CI still builds, so its ``keep-branches``
default lists every maintained branch, which is every branch with a
``scheduled-upgrade-check-<branch>.yml`` workflow, and its ``keep-python`` default lists
ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS from global_constants.py.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH, read_allowed_python_major_minor_versions

WORKFLOWS_PATH = AIRFLOW_ROOT_PATH / ".github" / "workflows"
WORKFLOW_PATH = WORKFLOWS_PATH / "delete-retired-image-packages.yml"
SCHEDULED_UPGRADE_CHECK = re.compile(r"^scheduled-upgrade-check-(?P<branch>.+)\.yml$")


def find_maintained_branches(workflows_path: Path) -> list[str]:
    branches = sorted(
        match["branch"]
        for path in workflows_path.iterdir()
        if (match := SCHEDULED_UPGRADE_CHECK.match(path.name))
    )
    if "main" not in branches:
        raise RuntimeError(f"No scheduled-upgrade-check-main.yml in {workflows_path}")
    return ["main", *(branch for branch in branches if branch != "main")]


def set_input_default(text: str, input_name: str, value: str) -> str:
    pattern = re.compile(
        rf"(^ {{6}}{re.escape(input_name)}:\n(?: {{8}}(?!default:).*\n)*? {{8}}default: )\"[^\"\n]*\"$",
        re.MULTILINE,
    )
    updated, count = pattern.subn(lambda match: f'{match[1]}"{value}"', text)
    if count != 1:
        raise RuntimeError(f"Could not find the default of the {input_name!r} input")
    return updated


def main() -> int:
    original = WORKFLOW_PATH.read_text()
    updated = set_input_default(original, "keep-branches", " ".join(find_maintained_branches(WORKFLOWS_PATH)))
    updated = set_input_default(updated, "keep-python", " ".join(read_allowed_python_major_minor_versions()))
    if updated == original:
        return 0
    WORKFLOW_PATH.write_text(updated)
    print(f"Updated the keep defaults in {WORKFLOW_PATH.relative_to(AIRFLOW_ROOT_PATH)}.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
