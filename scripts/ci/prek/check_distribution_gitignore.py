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
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Check that all distributions have a .gitignore file containing *.iml.

Every directory with a pyproject.toml (a distribution) must have a .gitignore
file that includes the ``*.iml`` pattern to prevent IntelliJ IDEA module files
from being committed.
"""

from __future__ import annotations

import sys

from common_prek_utils import AIRFLOW_ROOT_PATH, console

errors: list[str] = []

for pyproject_path in sorted(AIRFLOW_ROOT_PATH.rglob("pyproject.toml")):
    relative = pyproject_path.relative_to(AIRFLOW_ROOT_PATH)
    # Skip directories that are not part of the source tree
    if any(part.startswith(".") or part in ("out", "node_modules", "files") for part in relative.parts):
        continue
    dist_dir = pyproject_path.parent
    gitignore_path = dist_dir / ".gitignore"
    if not gitignore_path.exists():
        errors.append(f"{gitignore_path.relative_to(AIRFLOW_ROOT_PATH)}: .gitignore file missing")
        continue
    lines = gitignore_path.read_text().splitlines()
    if "*.iml" not in lines:
        errors.append(f"{gitignore_path.relative_to(AIRFLOW_ROOT_PATH)}: missing '*.iml' entry")

if errors:
    console.print("[red]Errors found in distribution .gitignore files:[/]")
    for error in errors:
        console.print(f"  [red]✗[/] {error}")
    console.print()
    console.print(
        "[bright_blue]Each distribution directory (containing pyproject.toml) must have a "
        ".gitignore file with a '*.iml' entry.[/]"
    )
    sys.exit(1)

sys.exit(0)
