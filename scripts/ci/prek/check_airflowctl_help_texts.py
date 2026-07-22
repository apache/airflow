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
#   "pyyaml>=6.0.3",
# ]
# ///
"""
Check that auto-generated airflowctl CLI commands have corresponding help texts in the help_texts.yaml file.
"""

from __future__ import annotations

import sys

import yaml
from common_prek_utils import AIRFLOW_ROOT_PATH, console, parse_operations

OPERATIONS_FILE = AIRFLOW_ROOT_PATH / "airflow-ctl" / "src" / "airflowctl" / "api" / "operations.py"
HELP_TEXTS_FILE = AIRFLOW_ROOT_PATH / "airflow-ctl" / "src" / "airflowctl" / "ctl" / "help_texts.yaml"
# Operations excluded from CLI (see cli_config.py)
EXCLUDED_OPERATION_CLASSES = {"BaseOperations", "LoginOperations", "VersionOperations"}
EXCLUDED_METHODS = {
    "__init__",
    "__init_subclass__",
    "error",
    "_check_flag_and_exit_if_server_response_error",
    "bulk",
    "export",
}


def load_help_texts_yaml() -> dict:
    """Load the help texts yaml for the auto-generated commands."""
    with open(HELP_TEXTS_FILE) as yaml_file:
        help_texts = yaml.safe_load(yaml_file)
    return help_texts


def main():
    available = parse_operations(
        operations_file=OPERATIONS_FILE,
        exclude_operation_classes=EXCLUDED_OPERATION_CLASSES,
        exclude_methods=EXCLUDED_METHODS,
    )
    help_texts = load_help_texts_yaml()
    missing = []
    for group, subcommands in sorted(available.items()):
        for subcommand in sorted(subcommands):
            help_text = help_texts.get(group, {}).get(subcommand)
            if help_text is None:
                missing.append(f"{group} {subcommand}")

    if missing:
        console.print("[red]ERROR: Commands do not have help texts:[/]")
        for cmd in missing:
            console.print(f"  [red]- {cmd}[/]")
        console.print()
        console.print("[yellow]Fix by:[/]")
        console.print(f"Adding help texts to {HELP_TEXTS_FILE}")
        sys.exit(1)

    total = sum(len(cmds) for cmds in available.values())
    console.print(f"[green]All {total} CLI help texts are covered [/]")
    sys.exit(0)


if __name__ == "__main__":
    main()
