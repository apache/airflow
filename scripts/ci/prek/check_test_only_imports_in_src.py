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
r"""Check that production source files do not import test-only modules at runtime.

Detects two categories of forbidden imports in production source code
(anything under ``*/src/``):

1. **tests_common** — the ``apache-airflow-devel-common`` package is dev-only
   and never published to PyPI.
2. **\*.tests.\*** — any import whose module path contains a ``.tests.``
   component (e.g. ``from providers.cncf.kubernetes.tests.foo import bar``).
   Test directories are not shipped in package wheels.
"""

# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import console

# Top-level modules that are dev-only and must never be imported at runtime.
FORBIDDEN_MODULES = ("tests_common",)

# Pattern matching a ``.tests.`` component anywhere in a dotted module path,
# or a module path that starts with ``tests.`` or equals ``tests``.
_TESTS_PATH_RE = re.compile(r"(^|\.)(tests)(\..*|$)")


def _is_forbidden(module: str) -> bool:
    """Return True if *module* is a forbidden import for production code."""
    # Check top-level forbidden modules (e.g. tests_common).
    if module.split(".")[0] in FORBIDDEN_MODULES:
        return True
    # Check for a ``.tests.`` component anywhere in the path,
    # or a path starting with ``tests.`` / equal to ``tests``.
    if _TESTS_PATH_RE.search(module):
        return True
    return False


def check_file(file_path: Path) -> list[tuple[int, str]]:
    """Return list of ``(line_number, import_statement)`` violations."""
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []

    violations: list[tuple[int, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if _is_forbidden(node.module):
                names = ", ".join(alias.name for alias in node.names)
                violations.append((node.lineno, f"from {node.module} import {names}"))

        elif isinstance(node, ast.Import):
            for alias in node.names:
                if _is_forbidden(alias.name):
                    stmt = f"import {alias.name}"
                    if alias.asname:
                        stmt += f" as {alias.asname}"
                    violations.append((node.lineno, stmt))

    return violations


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check that production source files do not import test-only modules at runtime"
    )
    parser.add_argument("files", nargs="*", help="Files to check")
    args = parser.parse_args()

    if not args.files:
        return

    total_violations = 0

    for file_path in [Path(f) for f in args.files]:
        violations = check_file(file_path)
        if violations:
            if console:
                console.print(f"[red]{file_path}[/red]:")
                for line_num, statement in violations:
                    console.print(f"  [yellow]Line {line_num}[/yellow]: {statement}")
            else:
                print(f"{file_path}:")
                for line_num, statement in violations:
                    print(f"  Line {line_num}: {statement}")
            total_violations += len(violations)

    if total_violations:
        msg = (
            f"\nFound {total_violations} prohibited test-only import(s) "
            f"in production source files\n"
            "Forbidden patterns: tests_common.*, *.tests.*, tests.*\n"
            "These modules are dev-only and not available at runtime."
        )
        if console:
            console.print()
            console.print(
                f"[red]Found {total_violations} prohibited test-only import(s) "
                f"in production source files[/red]"
            )
            console.print(
                "[yellow]Forbidden patterns: tests_common.*, *.tests.*, tests.*\n"
                "These modules are dev-only and not available at runtime.[/yellow]"
            )
        else:
            print(msg)
        sys.exit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
