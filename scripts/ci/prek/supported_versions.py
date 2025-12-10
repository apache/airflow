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
    ("3", "3.1.4", "Supported", "Apr 22, 2025", "TBD", "TBD"),
    ("2", "2.11.0", "Supported", "Dec 17, 2020", "Oct 22, 2025", "Apr 22, 2026"),
    ("1.10", "1.10.15", "EOL", "Aug 27, 2018", "Dec 17, 2020", "June 17, 2021"),
    ("1.9", "1.9.0", "EOL", "Jan 03, 2018", "Aug 27, 2018", "Aug 27, 2018"),
    ("1.8", "1.8.2", "EOL", "Mar 19, 2017", "Jan 03, 2018", "Jan 03, 2018"),
    ("1.7", "1.7.1.2", "EOL", "Mar 28, 2016", "Mar 19, 2017", "Mar 19, 2017"),
)


def replace_text_between(file: Path, start: str, end: str, replacement_text: str):
    original_text = file.read_text()
    leading_text = original_text.split(start)[0]
    trailing_text = original_text.split(end)[1]
    file.write_text(leading_text + start + replacement_text + end + trailing_text)


if __name__ == "__main__":
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
