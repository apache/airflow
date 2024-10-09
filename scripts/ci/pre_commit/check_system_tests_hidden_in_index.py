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

import sys
from pathlib import Path
from typing import Any

from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )


console = Console(color_system="standard", width=200)

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3].resolve()
DOCS_ROOT = AIRFLOW_SOURCES_ROOT / "docs"

PREFIX = "apache-airflow-providers-"


errors: list[Any] = []


def check_system_test_entry_hidden(provider_index: Path):
    console.print(f"[bright_blue]Checking {provider_index}")
    provider_folder = provider_index.parent.name
    if not provider_folder.startswith(PREFIX):
        console.print(f"[red]Bad provider index passed: {provider_index}")
        errors.append(provider_index)
    provider_path = provider_folder[len(PREFIX) :].replace("-", "/")
    expected_text = f"""
.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/{provider_path}/index>
"""
    index_text = provider_index.read_text()
    system_tests_path = AIRFLOW_SOURCES_ROOT / "providers" / "tests" / "system" / provider_path
    index_text_manual = index_text.split(
        ".. THE REMAINDER OF THE FILE IS AUTOMATICALLY GENERATED. IT WILL BE OVERWRITTEN AT RELEASE TIME!"
    )[0]
    if system_tests_path.exists():
        if expected_text not in index_text_manual:
            console.print(f"[red]The {provider_index} does not contain System Tests TOC.\n")
            console.print(
                f"[yellow]Make sure to add those lines to {provider_index} BEFORE (!) the line "
                f"starting with  '.. THE REMINDER OF THE FILE':\n"
            )
            console.print(expected_text, markup=False)
            errors.append(provider_index)
        else:
            console.print(f"[green]All ok. The {provider_index} contains hidden index.\n")
    else:
        console.print(f"[yellow]All ok. The {provider_index} does not contain system tests.\n")


if __name__ == "__main__":
    for file in sys.argv[1:]:
        check_system_test_entry_hidden(Path(file))
    sys.exit(1 if errors else 0)
