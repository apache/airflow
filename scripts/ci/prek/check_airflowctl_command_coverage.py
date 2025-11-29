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
#   "rich>=13.6.0",
# ]
# ///
"""
Check that all airflowctl CLI commands have integration test coverage by comparing  commands from operations.py against test_commands in conftest.py.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import AIRFLOW_ROOT_PATH, console

OPERATIONS_FILE = AIRFLOW_ROOT_PATH / "airflow-ctl" / "src" / "airflowctl" / "api" / "operations.py"
CONFTEST_FILE = AIRFLOW_ROOT_PATH / "airflow-ctl-tests" / "tests" / "airflowctl_tests" / "conftest.py"

# Operations excluded from CLI (see cli_config.py)
EXCLUDED_OPERATION_CLASSES = {"BaseOperations", "LoginOperations", "VersionOperations"}
EXCLUDED_METHODS = {
    "__init__",
    "__init_subclass__",
    "error",
    "_check_flag_and_exit_if_server_response_error",
    "bulk",
}

# Commands intentionally not tested - grouped by reason:
# - Assets: require specific asset state, DAG dependencies, or queued events
# - Backfill: require inter-command data passing (IDs from create)
# - DAGs: destructive (delete) or require specific error/warning/version state
# - Connections: create_defaults may conflict with test environment
EXCLUDED_COMMANDS = {
    # Assets - require asset state/dependencies
    "assets materialize",
    "assets get",
    "assets get-by-alias",
    "assets list-by-alias",
    "assets create-event",
    "assets get-queued-events",
    "assets get-dag-queued-events",
    "assets get-dag-queued-event",
    "assets delete-queued-events",
    "assets delete-dag-queued-events",
    "assets delete-queued-event",
    # Backfill - require IDs from create command
    "backfill cancel",
    "backfill create",
    "backfill create-dry-run",
    "backfill get",
    "backfill pause",
    "backfill unpause",
    # DAGs - destructive or require specific state
    "dags delete",
    "dags get-details",
    "dags get-tags",
    "dags get-import-error",
    "dags list-import-errors",
    "dags get-stats",
    "dags get-version",
    "dags list-version",
    "dags list-warning",
    "dags trigger",  # covered by dagrun trigger
    # Connections
    "connections create-defaults",
    "connections test",  # requires server-side configuration (core.test_connection)
}


def parse_operations() -> dict[str, list[str]]:
    """Parse operations.py to extract CLI commands grouped by operation class."""
    commands: dict[str, list[str]] = {}

    with open(OPERATIONS_FILE) as f:
        tree = ast.parse(f.read(), filename=str(OPERATIONS_FILE))

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name.endswith("Operations"):
            if node.name in EXCLUDED_OPERATION_CLASSES:
                continue

            group_name = node.name.replace("Operations", "").lower()
            commands[group_name] = []

            for child in node.body:
                if isinstance(child, ast.FunctionDef):
                    method_name = child.name
                    if method_name in EXCLUDED_METHODS or method_name.startswith("_"):
                        continue
                    subcommand = method_name.replace("_", "-")
                    commands[group_name].append(subcommand)

    return commands


def parse_tested_commands() -> set[str]:
    """Parse conftest.py to extract tested commands from test_commands fixture."""
    tested: set[str] = set()

    with open(CONFTEST_FILE) as f:
        content = f.read()

    # Match command patterns like "assets list", "connections create --..."
    pattern = r'"([a-z]+(?:-[a-z]+)?\s+[a-z]+(?:-[a-z]+)?)'
    for match in re.findall(pattern, content):
        parts = match.split()
        if len(parts) >= 2:
            tested.add(f"{parts[0]} {parts[1]}")

    return tested


def main():
    available = parse_operations()
    tested = parse_tested_commands()

    missing = []
    for group, subcommands in sorted(available.items()):
        for subcommand in sorted(subcommands):
            cmd = f"{group} {subcommand}"
            if cmd not in tested and cmd not in EXCLUDED_COMMANDS:
                missing.append(cmd)

    if missing:
        console.print("[red]ERROR: Commands not covered by integration tests:[/]")
        for cmd in missing:
            console.print(f"  [red]- {cmd}[/]")
        console.print()
        console.print("[yellow]Fix by either:[/]")
        console.print("  1. Add test to airflow-ctl-tests/tests/airflowctl_tests/conftest.py")
        console.print("  2. Add to EXCLUDED_COMMANDS in scripts/ci/prek/check_airflowctl_command_coverage.py")
        sys.exit(1)

    total = sum(len(cmds) for cmds in available.values())
    console.print(
        f"[green]All {total} CLI commands covered ({len(tested)} tested, {len(EXCLUDED_COMMANDS)} excluded)[/]"
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
