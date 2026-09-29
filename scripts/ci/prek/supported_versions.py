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
#   "tabulate>=0.9.0",
# ]
# ///

from __future__ import annotations

import re
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH
from tabulate import tabulate

HEADERS = (
    "Version",
    "Current Patch/Minor",
    "State",
    "First Release",
    "Limited Maintenance",
    "EOL/Terminated",
)

SUPPORTED_VERSIONS = (
    ("3", "3.3.2", "Maintenance", "Apr 22, 2025", "TBD", "TBD"),
    ("2", "2.11.2", "EOL", "Dec 17, 2020", "Oct 22, 2025", "Apr 22, 2026"),
    ("1.10", "1.10.15", "EOL", "Aug 27, 2018", "Dec 17, 2020", "June 17, 2021"),
    ("1.9", "1.9.0", "EOL", "Jan 03, 2018", "Aug 27, 2018", "Aug 27, 2018"),
    ("1.8", "1.8.2", "EOL", "Mar 19, 2017", "Jan 03, 2018", "Jan 03, 2018"),
    ("1.7", "1.7.1.2", "EOL", "Mar 28, 2016", "Mar 19, 2017", "Mar 19, 2017"),
)


# The README pins the latest stable release in the Requirements table header and in the
# ``pip install`` examples; these are not covered by the generated table, so they are rewritten
# here to keep a release PR that only edits SUPPORTED_VERSIONS from leaving them stale.
STABLE_VERSION_HEADER_PATTERN = re.compile(r"Stable version \((?P<version>[^)]*)\)(?P<padding> *)")
STABLE_VERSION_PIN_PATTERNS = (
    re.compile(r"(?P<prefix>pip install 'apache-airflow(?:\[[^\]]*\])?==)[^']*(?P<suffix>')"),
    re.compile(r"(?P<prefix>/apache/airflow/constraints-)[0-9][^/]*(?P<suffix>/constraints-)"),
)


def get_stable_version() -> str:
    return SUPPORTED_VERSIONS[0][1]


def replace_text_between(file: Path, start: str, end: str, replacement_text: str):
    original_text = file.read_text()
    leading_text = original_text.split(start)[0]
    trailing_text = original_text.split(end)[1]
    file.write_text(leading_text + start + replacement_text + end + trailing_text)


def update_stable_version_in_readme(text: str, stable_version: str) -> str:
    def replace_header(match: re.Match) -> str:
        # Keep the markdown table cell width unchanged whenever the padding allows it.
        width = len(match.group(0))
        header = f"Stable version ({stable_version})"
        return header + " " * max(width - len(header), 1 if match.group("padding") else 0)

    if not STABLE_VERSION_HEADER_PATTERN.search(text):
        raise RuntimeError(f"Pattern {STABLE_VERSION_HEADER_PATTERN.pattern!r} not found in README.md")
    text = STABLE_VERSION_HEADER_PATTERN.sub(replace_header, text)
    for pattern in STABLE_VERSION_PIN_PATTERNS:
        if not pattern.search(text):
            raise RuntimeError(f"Pattern {pattern.pattern!r} not found in README.md")
        text = pattern.sub(rf"\g<prefix>{stable_version}\g<suffix>", text)
    return text


if __name__ == "__main__":
    readme = AIRFLOW_ROOT_PATH / "README.md"
    readme.write_text(update_stable_version_in_readme(readme.read_text(), get_stable_version()))
    replace_text_between(
        file=AIRFLOW_ROOT_PATH / "README.md",
        start="<!-- Beginning of auto-generated table -->\n",
        end="<!-- End of auto-generated table -->\n",
        replacement_text="\n"
        + tabulate(
            SUPPORTED_VERSIONS, tablefmt="github", headers=HEADERS, stralign="left", disable_numparse=True
        )
        + "\n\n",
    )
    replace_text_between(
        file=AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "installation" / "supported-versions.rst",
        start=" .. Beginning of auto-generated table\n",
        end=" .. End of auto-generated table\n",
        replacement_text="\n"
        + tabulate(
            SUPPORTED_VERSIONS, tablefmt="rst", headers=HEADERS, stralign="left", disable_numparse=True
        )
        + "\n\n",
    )
