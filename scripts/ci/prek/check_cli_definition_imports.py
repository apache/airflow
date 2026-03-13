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
Check that CLI definition files only import from allowed modules.

CLI definition files (matching pattern */cli/definition.py) should only import
from 'airflow.configuration' or 'airflow.cli.cli_config' to avoid circular imports
and ensure clean separation of concerns.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import console, get_imports_from_file

# Allowed modules that can be imported in CLI definition files
ALLOWED_MODULES = {
    "airflow.configuration",
    "airflow.cli.cli_config",
}

# Standard library and __future__ modules are also allowed
STDLIB_PREFIXES = (
    "argparse",
    "getpass",
    "textwrap",
    "typing",
    "collections",
    "functools",
    "itertools",
    "pathlib",
    "os",
    "sys",
    "re",
    "json",
    "dataclasses",
    "enum",
)


def get_provider_path_from_file(file_path: Path) -> str | None:
    """
    Extract the provider path from a CLI definition file.

    For example:
    - providers/celery/src/airflow/providers/celery/cli/definition.py -> celery
    - providers/cncf/kubernetes/src/airflow/providers/cncf/kubernetes/cli/definition.py -> cncf.kubernetes
    """
    path_str = file_path.as_posix()

    # Find the substring between "airflow/providers/" and "/cli/definition"
    start_marker = "airflow/providers/"
    end_marker = "/cli/definition"

    start_idx = path_str.find(start_marker)
    end_idx = path_str.find(end_marker)

    if start_idx == -1 or end_idx == -1 or start_idx >= end_idx:
        return None

    # Extract the provider path and replace '/' with '.'
    provider_path = path_str[start_idx + len(start_marker) : end_idx]
    return provider_path.replace("/", ".")


def is_allowed_import(import_name: str, file_path: Path) -> bool:
    """Check if an import is allowed in CLI definition files."""
    # Check if it's one of the allowed Airflow modules
    for allowed_module in ALLOWED_MODULES:
        if import_name == allowed_module or import_name.startswith(f"{allowed_module}."):
            return True

    # Check if it's a standard library module
    for prefix in STDLIB_PREFIXES:
        if import_name == prefix or import_name.startswith(f"{prefix}."):
            return True

    # Allow imports from the provider's own version_compat module
    provider_path = get_provider_path_from_file(file_path)
    if provider_path:
        version_compat_module = f"airflow.providers.{provider_path}.version_compat"
        if import_name == version_compat_module or import_name.startswith(f"{version_compat_module}."):
            return True

    return False


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check that CLI definition files only import from allowed modules."
    )
    parser.add_argument("files", nargs="*", type=Path, help="Python source files to check.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.files:
        console.print("[yellow]No files provided.[/]")
        return 0

    errors: list[str] = []

    # Filter files to only check */cli/definition.py files
    cli_definition_files = [
        path
        for path in args.files
        if path.name == "definition.py" and len(path.parts) >= 2 and path.parts[-2] == "cli"
    ]

    if not cli_definition_files:
        console.print("[yellow]No CLI definition files found to check.[/]")
        return 0

    console.print(f"[blue]Checking {len(cli_definition_files)} CLI definition file(s)...[/]")
    console.print(cli_definition_files)

    for path in cli_definition_files:
        try:
            imports = get_imports_from_file(path, only_top_level=True)
        except Exception as e:
            console.print(f"[red]Failed to parse {path}: {e}[/]")
            return 2

        forbidden_imports = []
        for imp in imports:
            if not is_allowed_import(imp, path):
                forbidden_imports.append(imp)

        if forbidden_imports:
            errors.append(f"\n[red]{path}:[/]")
            for imp in forbidden_imports:
                errors.append(f"  - {imp}")

    if errors:
        console.print("\n[red] Some CLI definition files contain forbidden imports![/]\n")
        console.print(
            f"[yellow]CLI definition files (*/cli/definition.py) should only import from:[/]\n"
            "  - airflow.configuration\n"
            "  - airflow.cli.cli_config\n"
            "  - Their own provider's version_compat module\n"
            f"  - Standard library modules ({', '.join(STDLIB_PREFIXES)})\n"
        )
        console.print("[red]Found forbidden imports in:[/]")
        for error in errors:
            console.print(error)
        console.print(
            "\n[yellow]This restriction exists to:[/]\n"
            "  - Keep CLI definitions lightweight and declarative to avoid slowdowns\n"
            "  - Ensure clean separation between CLI structure and implementation\n"
        )
        return 1

    console.print("[green] All CLI definition files import only from allowed modules![/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
