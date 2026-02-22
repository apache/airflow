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
from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import console


def check_file_for_prohibited_imports(file_path: Path) -> list[tuple[int, str]]:
    """
    Check file for airflow-core and airflow.sdk imports.
    Shared libraries should not depend on either core or task-sdk.
    Returns list of (line_num, import_statement).
    """
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []

    violations = []

    for node in ast.walk(tree):
        # Check `from airflow.x import y` statements
        if isinstance(node, ast.ImportFrom):
            if node.module and node.module.startswith("airflow."):
                # Allow airflow_shared imports (which show as airflow._shared at runtime)
                if not (
                    node.module.startswith("airflow_shared") or node.module.startswith("airflow._shared")
                ):
                    import_names = ", ".join(alias.name for alias in node.names)
                    statement = f"from {node.module} import {import_names}"
                    violations.append((node.lineno, statement))

        # Check `import airflow.x` statements
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("airflow."):
                    # Allow airflow_shared imports (which show as airflow._shared at runtime)
                    if not (
                        alias.name.startswith("airflow_shared") or alias.name.startswith("airflow._shared")
                    ):
                        statement = f"import {alias.name}"
                        if alias.asname:
                            statement += f" as {alias.asname}"
                        violations.append((node.lineno, statement))

    return violations


def main():
    parser = argparse.ArgumentParser(description="Check for core/task-sdk imports in shared library files")
    parser.add_argument("files", nargs="*", help="Files to check")
    args = parser.parse_args()

    if not args.files:
        return

    total_violations = 0

    for file_path in [Path(f) for f in args.files]:
        violations = check_file_for_prohibited_imports(file_path)
        if violations:
            console.print(f"[red]{file_path}[/red]:")
            for line_num, statement in violations:
                console.print(f"  [yellow]Line {line_num}[/yellow]: {statement}")
            total_violations += len(violations)

    if total_violations:
        console.print()
        console.print(f"[red]Found {total_violations} prohibited import(s) in shared library files[/red]")
        console.print("[yellow]Shared libraries must not import from airflow-core or airflow.sdk[/yellow]")
        console.print("[yellow]Only airflow_shared (airflow._shared at runtime) imports are allowed[/yellow]")
        sys.exit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
