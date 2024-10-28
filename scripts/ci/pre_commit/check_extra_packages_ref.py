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
Checks if all the libraries in setup.py are listed in installation.rst file
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from tabulate import tabulate

AIRFLOW_ROOT_PATH = Path(__file__).parents[3].resolve()
COMMON_PRECOMMIT_PATH = Path(__file__).parent.resolve()
EXTRA_PACKAGES_REF_FILE = (
    AIRFLOW_ROOT_PATH / "docs" / "apache-airflow" / "extra-packages-ref.rst"
)
PYPROJECT_TOML_FILE_PATH = AIRFLOW_ROOT_PATH / "pyproject.toml"

sys.path.insert(
    0, COMMON_PRECOMMIT_PATH.as_posix()
)  # make sure common_precommit_utils is imported
from common_precommit_utils import console

sys.path.insert(0, AIRFLOW_ROOT_PATH.as_posix())  # make sure airflow root is imported
from hatch_build import ALL_DYNAMIC_EXTRAS

doc_ref_content = EXTRA_PACKAGES_REF_FILE.read_text()

errors: list[str] = []
regular_suggestions: list[str] = []
devel_suggestions: list[str] = []
suggestions: list[tuple] = []
suggestions_devel: list[tuple] = []
suggestions_providers: list[tuple] = []

for dependency in ALL_DYNAMIC_EXTRAS:
    console.print(f"[bright_blue]Checking if {dependency} is mentioned in refs[/]")
    find_matching = re.search(
        rf"^\| {dependency} *\|", doc_ref_content, flags=re.MULTILINE
    )
    if not find_matching:
        errors.append(
            f"[red]ERROR: {dependency} is not listed in {EXTRA_PACKAGES_REF_FILE}[/]"
        )
        is_devel_dep = dependency.startswith("devel") or dependency in ["doc", "doc-gen"]
        short_dep = dependency.replace("devel-", "")
        if is_devel_dep:
            suggestions_devel.append(
                (
                    dependency,
                    f"pip install -e '.[{dependency}]'",
                    f"Adds all test libraries needed to test {short_dep}",
                )
            )
        else:
            suggestions.append(
                (
                    dependency,
                    f"pip install apache-airflow[{dependency}]",
                    f"{dependency.capitalize()} hooks and operators",
                )
            )

HEADERS = ["extra", "install command", "enables"]
if errors:
    console.print("\n".join(errors))
    console.print()
    console.print("[bright_blue]Suggested tables to add to references::[/]")
    if suggestions:
        console.print("[bright_blue]Regular dependencies[/]")
        console.print(
            tabulate(suggestions, headers=HEADERS, tablefmt="grid"), markup=False
        )
    if suggestions_devel:
        console.print("[bright_blue]Devel dependencies[/]")
        console.print(
            tabulate(suggestions_devel, headers=HEADERS, tablefmt="grid"), markup=False
        )
    if suggestions_providers:
        console.print("[bright_blue]Devel dependencies[/]")
        console.print(
            tabulate(suggestions_providers, headers=HEADERS, tablefmt="grid"),
            markup=False,
        )
    sys.exit(1)
else:
    console.print(
        f"[green]Checked: {len(ALL_DYNAMIC_EXTRAS)} dependencies are mentioned[/]"
    )
