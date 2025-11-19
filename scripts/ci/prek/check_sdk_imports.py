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


def check_file_for_sdk_imports(file_path: Path) -> list[tuple[int, str]]:
    """Check file for airflow.sdk imports. Returns list of (line_num, import_statement)."""
    try:
        with open(file_path, encoding="utf-8") as f:
            source = f.read()
            tree = ast.parse(source, filename=str(file_path))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []

    mismatches = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and ("airflow.sdk" in node.module):
                import_names = ", ".join(alias.name for alias in node.names)
                statement = f"from {node.module} import {import_names}"
                mismatches.append((node.lineno, statement))

    return mismatches


def main():
    parser = argparse.ArgumentParser(description="Check for SDK imports in airflow-core files")
    parser.add_argument("files", nargs="*", help="Files to check")
    args = parser.parse_args()

    if not args.files:
        return

    files_to_check = [Path(f) for f in args.files if f.endswith(".py")]
    total_violations = 0

    for file_path in files_to_check:
        mismatches = check_file_for_sdk_imports(file_path)
        if mismatches:
            console.print(f"[red]{file_path}[/red]:")
            for line_num, statement in mismatches:
                console.print(f"  [yellow]Line {line_num}[/yellow]: {statement}")
            total_violations += len(mismatches)

    if total_violations:
        console.print()
        console.print(f"[red]Found {total_violations} SDK import(s) in core files[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
