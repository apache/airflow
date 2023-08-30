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
from __future__ import annotations

import re
import sys
from pathlib import Path

from rich.console import Console

console = Console(width=400, color_system="standard")

AIRFLOW_SOURCES = Path(__file__).parents[3].resolve()
README_SECTIONS_TO_EXTRACT = [
    "Apache Airflow",
    "Requirements",
    "Getting started",
    "Installing from PyPI",
    "Official source code",
    "Contributing",
    "Who uses Apache Airflow",
    "Who maintains Apache Airflow",
]

PYPI_README_HEADER = (
    "<!-- "
    "PLEASE DO NOT MODIFY THIS FILE. IT HAS BEEN GENERATED AUTOMATICALLY FROM THE `README.md` FILE OF THE\n"
    "PROJECT BY THE `generate-pypi-readme` PRE-COMMIT. YOUR CHANGES HERE WILL BE AUTOMATICALLY OVERWRITTEN."
    "-->\n"
)

# Function to extract sections based on start and end comments
def extract_section(content, section_name):
    start_comment = (
        f"<!-- START {section_name}, please keep comment here to allow auto update of PyPI readme.md -->"
    )
    end_comment = (
        f"<!-- END {section_name}, please keep comment here to allow auto update of PyPI readme.md -->"
    )
    section_match = re.search(
        rf"{re.escape(start_comment)}(.*?)\n{re.escape(end_comment)}", content, re.DOTALL
    )
    if section_match:
        return section_match.group(1)
    else:
        raise Exception(f"Cannot find section {section_name} in README.md")


if __name__ == "__main__":
    readme_file = AIRFLOW_SOURCES / "README.md"
    pypi_readme_file = AIRFLOW_SOURCES / "generated" / "PYPI_README.md"

    if not pypi_readme_file.exists():
        pypi_readme_content = PYPI_README_HEADER
    else:
        pypi_readme_content = pypi_readme_file.read_text()
    with readme_file.open("r") as readme:
        readme_content = readme.read()
    generated_pypi_readme_content = PYPI_README_HEADER
    for section in README_SECTIONS_TO_EXTRACT:
        section_content = extract_section(readme_content, section)
        generated_pypi_readme_content += section_content
    if pypi_readme_content != generated_pypi_readme_content:
        with pypi_readme_file.open("w") as generated_readme:
            generated_readme.write(generated_pypi_readme_content)
        console.print("\n[yellow]PyPI README.md content regenerated after update..\n")
        console.print("[red]Aborting the commit")
        console.print("You should check the changes made. Then simply 'git add --update .' and re-commit")
        sys.exit(1)
