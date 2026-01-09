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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "hatchling==1.27.0",
#   "rich>=13.6.0",
#   "tabulate>=0.9.0",
# ]
# ///
"""
Checks if all the libraries in setup.py are listed in installation.rst file
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH
from tabulate import tabulate

try:
    import tomllib
except ImportError:
    import tomli as tomllib


COMMON_PREK_PATH = Path(__file__).parent.resolve()
EXTRA_PACKAGES_REF_FILE = AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "extra-packages-ref.rst"
PYPROJECT_TOML_FILE_PATH = AIRFLOW_ROOT_PATH / "pyproject.toml"

sys.path.insert(0, COMMON_PREK_PATH.as_posix())  # make sure common_prek_utils is imported
from common_prek_utils import console

sys.path.insert(0, AIRFLOW_ROOT_PATH.as_posix())  # make sure airflow root is imported

doc_ref_content = EXTRA_PACKAGES_REF_FILE.read_text()

errors: list[str] = []
suggestions: list[tuple] = []

pyproject_toml_content = tomllib.loads(PYPROJECT_TOML_FILE_PATH.read_text(encoding="utf-8"))
all_extras = pyproject_toml_content["project"]["optional-dependencies"]

for dependency in all_extras:
    console.print(f"[bright_blue]Checking if {dependency} is mentioned in refs[/]")
    find_matching = re.search(rf"^\| {dependency} *\|", doc_ref_content, flags=re.MULTILINE)
    if not find_matching:
        errors.append(f"[red]ERROR: {dependency} is not listed in {EXTRA_PACKAGES_REF_FILE}[/]")
        suggestions.append(
            (
                dependency,
                f"``pip install apache-airflow[{dependency}]``",
                f"{dependency.capitalize()} hooks and operators",
            )
        )

HEADERS = ["extra", "install command", "enables"]
if suggestions:
    console.print("\n".join(errors))
    console.print()
    console.print("[bright_blue]Suggest to add this to the relevant reference. For example:[/]")
    console.print(tabulate(suggestions, headers=HEADERS, tablefmt="grid"), markup=False)
    sys.exit(1)
else:
    console.print(f"[green]Checked: {len(all_extras)} dependencies are mentioned[/]")
