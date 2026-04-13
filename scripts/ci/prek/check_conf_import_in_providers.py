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
Check that provider files import ``conf`` only from ``airflow.providers.common.compat.sdk``.

Providers must not import ``conf`` directly from ``airflow.configuration`` or
``airflow.sdk.configuration``.  The compat SDK re-exports ``conf`` and ensures
the code works across Airflow 2 and 3.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from common_prek_utils import console, get_imports_from_file

# Fully-qualified import names that are forbidden (as returned by get_imports_from_file)
FORBIDDEN_CONF_IMPORTS = {
    "airflow.configuration.conf",
    "airflow.sdk.configuration.conf",
}

# Executor files run inside Airflow-Core, so they may use airflow.configuration directly.
# Only airflow.sdk.configuration remains forbidden for them.
EXECUTOR_ALLOWED_CONF_IMPORTS = {
    "airflow.configuration.conf",
}

ALLOWED_IMPORT = "from airflow.providers.common.compat.sdk import conf"


def is_excluded(path: Path) -> bool:
    """Check if a file is the compat SDK module itself (which must define the re-export)."""
    return path.as_posix().endswith("airflow/providers/common/compat/sdk.py")


def is_executor_file(path: Path) -> bool:
    """Check if a file is an executor module (lives under an ``executors/`` directory)."""
    return "executors" in path.parts


def find_forbidden_conf_imports(path: Path) -> list[str]:
    """Return forbidden conf imports found in *path*."""
    imports = get_imports_from_file(path, only_top_level=False)
    forbidden = FORBIDDEN_CONF_IMPORTS
    if is_executor_file(path):
        forbidden = forbidden - EXECUTOR_ALLOWED_CONF_IMPORTS
    return [imp for imp in imports if imp in forbidden]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check that provider files import conf only from the compat SDK."
    )
    parser.add_argument("files", nargs="*", type=Path, help="Python source files to check.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.files:
        console.print("[yellow]No files provided.[/]")
        return 0

    provider_files = [path for path in args.files if not is_excluded(path)]

    if not provider_files:
        return 0

    errors: list[str] = []

    for path in provider_files:
        try:
            forbidden = find_forbidden_conf_imports(path)
        except Exception as e:
            console.print(f"[red]Failed to parse {path}: {e}[/]")
            return 2

        if forbidden:
            errors.append(f"\n[red]{path}:[/]")
            for imp in forbidden:
                errors.append(f"  - {imp}")

    if errors:
        console.print("\n[red]Some provider files import conf from forbidden modules![/]\n")
        console.print(
            "[yellow]Provider files must import conf from the compat SDK:[/]\n"
            f"  {ALLOWED_IMPORT}\n"
            "\n[yellow]The following imports are forbidden:[/]\n"
            "  - from airflow.configuration import conf\n"
            "  - from airflow.sdk.configuration import conf\n"
        )
        console.print("[red]Found forbidden imports in:[/]")
        for error in errors:
            console.print(error)
        return 1

    console.print("[green]All provider files import conf from the correct module![/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
