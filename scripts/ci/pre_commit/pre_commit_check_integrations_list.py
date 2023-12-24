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
Module to check integration tests are listed in documentation.

Compare the contents of the integrations table and the docker-compose
integration files, if there is a mismatch, the table is generated.
"""
from __future__ import annotations

import sys
import re
from pathlib import Path

# make sure common_precommit_utils is imported
sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_precommit_utils import (
    AIRFLOW_SOURCES_ROOT_PATH,
    console,
    insert_documentation,
)
from tabulate import tabulate

DOCUMENTATION_PATH = AIRFLOW_SOURCES_ROOT_PATH / "TESTING.rst"
INTEGRATION_TESTS_PATH = AIRFLOW_SOURCES_ROOT_PATH / "scripts" / "ci" / "docker-compose"
INTEGRATION_TEST_PREFIX = "integration-*.yml"
DOCS_MARKER_START = ".. BEGIN AUTO-GENERATED INTEGRATION LIST"
DOCS_MARKER_END = ".. END AUTO-GENERATED INTEGRATION LIST"
_LIST_MATCH = r"\|[^|\n]+"


def get_ci_integrations(
    tests_path: Path = INTEGRATION_TESTS_PATH,
    integration_prefix: str = INTEGRATION_TEST_PREFIX,
) -> list[str]:
    """Get list of integrations from matching filenames."""
    if not tests_path.is_dir() and tests_path.exists():
        console.print(f"[red]Bad tests path: {tests_path}. [/]")
        sys.exit(1)

    integrations_files = [_i for _i in tests_path.glob(integration_prefix)]

    if len(integrations_files) == 0:
        console.print(
            f"[red]No integrations found."
            f"Pattern '{integration_prefix}' did not match any files under {tests_path}. [/]"
        )
        sys.exit(1)

    # parse into list of ids
    integrations = []
    for _i in integrations_files:
        try:
            integrations.append(_i.stem.split("-")[1])
        except IndexError:
            console.print(
                f"[red]Tried to parse {_i.stem}, but did not contain '-' separator. [/]"
            )
            continue

    return integrations


def get_docs_integrations(docs_path: Path = DOCUMENTATION_PATH):
    """Get integrations listed in docs."""
    table_lines = []
    _list_start_line = None
    with open(docs_path, encoding="utf8") as f:
        for line_n, line in enumerate(f):
            if DOCS_MARKER_END in line:
                break
            if DOCS_MARKER_START in line:
                _list_start_line = line_n
            if _list_start_line is None:
                continue
            if line_n > _list_start_line:
                table_lines.append(line)

    if len(table_lines) == 0:
        console.print("[red]No integrations table in docs.[/]")
        sys.exit(1)

    table_cells = []
    for line in table_lines:
        m = re.findall(_LIST_MATCH, line)
        if len(m) == 0:
            continue
        for _i in m:
            table_cells.append(_i.strip("|").strip())

    def _list_matcher(j):
        """Filter callable to exclude header and empty cells."""
        if len(j) == 0:
            return False
        elif j in ["Description", "Identifier"]:
            return False
        else:
            return True

    table_cells = list(filter(_list_matcher, table_cells))
    return table_cells


def update_integration_tests_array(integrations: list[str]):
    rows = []
    description = ""
    for integration in sorted(integrations):
        # TODO: replace description
        rows.append((integration, description))
    formatted_table = (
        "\n"
        + tabulate(rows, tablefmt="grid", headers=("Identifier", "Description"))
        + "\n\n"
    )
    insert_documentation(
        file_path=AIRFLOW_SOURCES_ROOT_PATH / "TESTING.rst",
        content=formatted_table.splitlines(keepends=True),
        header=DOCS_MARKER_START,
        footer=DOCS_MARKER_END,
    )


def print_diff_and_fail(source, target, msg):
    difference = source - target
    if difference:
        console.print(msg)
        for i in difference:
            console.print(f"[red]\t- {i}[/]")
    return list(difference)


def main():
    docs_integrations = get_docs_integrations()
    ci_integrations = get_ci_integrations()

    if len(ci_integrations) == 0:
        console.print("[red]No integrations found.[/]")
        sys.exit(1)

    _ci_items = set(ci_integrations)
    _docs_items = set(docs_integrations)
    diff = []
    diff.append(
        print_diff_and_fail(
            _ci_items, _docs_items, "[red]Found in ci files, but not in docs: [/]"
        )
    )
    diff.append(
        print_diff_and_fail(
            _docs_items, _ci_items, "[red]Found in docs, but not in ci files: [/]"
        )
    )
    if diff:
        sys.exit(1)

    # update_integration_tests_array(integrations)


if __name__ == "__main__":
    main()
