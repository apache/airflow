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

import re
import sys
from pathlib import Path
from typing import Any

import yaml

# make sure common_precommit_utils is imported
sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_precommit_utils import (
    AIRFLOW_SOURCES_ROOT_PATH,
    console,
    insert_documentation,
)
from tabulate import tabulate

DOCUMENTATION_PATH = (
    AIRFLOW_SOURCES_ROOT_PATH / "contributing-docs" / "testing" / "integration_tests.rst"
)
INTEGRATION_TESTS_PATH = AIRFLOW_SOURCES_ROOT_PATH / "scripts" / "ci" / "docker-compose"
INTEGRATION_TEST_PREFIX = "integration-*.yml"
DOCS_MARKER_START = ".. BEGIN AUTO-GENERATED INTEGRATION LIST"
DOCS_MARKER_END = ".. END AUTO-GENERATED INTEGRATION LIST"
_LIST_MATCH = r"\|[^|\n]+"


def get_ci_integrations(
    tests_path: Path = INTEGRATION_TESTS_PATH,
    integration_prefix: str = INTEGRATION_TEST_PREFIX,
) -> dict[str, Path]:
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
    integrations = {}
    for _i in integrations_files:
        try:
            _key = _i.stem.split("-")[1]
            integrations[_key] = _i
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
        table_cells.append(m[0].strip("|").strip())

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


def update_integration_tests_array(contents: dict[str, list[str]]):
    """Generate docs table."""
    rows = []
    sorted_contents = dict(sorted(contents.items()))
    for integration, description in sorted_contents.items():
        formatted_hook_description = (
            description[0] if len(description) == 1 else "* " + "\n* ".join(description)
        )
        rows.append((integration, formatted_hook_description))
    formatted_table = (
        "\n"
        + tabulate(rows, tablefmt="grid", headers=("Identifier", "Description"))
        + "\n\n"
    )
    insert_documentation(
        file_path=AIRFLOW_SOURCES_ROOT_PATH
        / "contributing-docs"
        / "testing"
        / "integration_tests.rst",
        content=formatted_table.splitlines(keepends=True),
        header=DOCS_MARKER_START,
        footer=DOCS_MARKER_END,
    )


def print_diff(source, target, msg):
    difference = source - target
    if difference:
        console.print(msg)
        for i in difference:
            console.print(f"[red]\t- {i}[/]")
    return list(difference)


def _get_breeze_description(
    parsed_compose: dict[str, Any], label_key: str = "breeze.description"
):
    """Extract all breeze.description labels per image."""
    image_label_map = {}
    # possible key error handled outside
    for _img_name, img in parsed_compose["services"].items():
        try:
            for _label_name, label in img["labels"].items():
                if _label_name == label_key:
                    image_label_map[_img_name] = label
        except KeyError:
            # service has no 'lables' entry
            continue
    return image_label_map


def get_integration_descriptions(integrations: dict[str, Path]) -> dict[str, list[Any]]:
    """Pull breeze description from docker-compose files."""
    table = {}
    for integration, path in integrations.items():
        with open(path) as f:
            _compose = yaml.safe_load(f)

        try:
            _labels = _get_breeze_description(_compose)
        except KeyError:
            console.print(f"[red]No 'services' entry in compose file {path}.[/]")
            sys.exit(1)
        table[integration] = list(_labels.values())
    return table


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
        print_diff(_ci_items, _docs_items, "[red]Found in ci files, but not in docs: [/]")
    )
    diff.append(
        print_diff(_docs_items, _ci_items, "[red]Found in docs, but not in ci files: [/]")
    )
    if diff:
        console.print(
            "[yellow]Regenerating documentation table. Don't forget to review and commit possible changes.[/]"
        )

    table_contents = get_integration_descriptions(ci_integrations)
    update_integration_tests_array(table_contents)


if __name__ == "__main__":
    main()
