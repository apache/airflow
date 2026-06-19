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
"""Check that ``HTTPException`` is imported from ``fastapi`` in fastapi-using trees.

The hook is wired into per-distribution ``.pre-commit-config.yaml`` files
(``airflow-core``, ``providers/amazon``, ``providers/common/ai``,
``providers/edge3``, ``providers/fab``, ``providers/keycloak``), each
scoped to the subtree that actually wires a FastAPI app. In
``airflow-core`` that includes ``api_fastapi/`` and
``utils/serve_logs/`` (the worker log-serving FastAPI app). Provider
trees that mix client and server code (e.g. edge3's ``cli/`` is a
client) are scoped to the server-side subfolders only to avoid false
positives on stdlib HTTP usage in the client. Within those scopes,
every ``HTTPException`` must come from ``fastapi`` (which re-exports
the Starlette class). Two common mistakes this hook catches:

* ``from starlette.exceptions import HTTPException`` — a different class at
  runtime; ``isinstance(exc, fastapi.HTTPException)`` and
  ``pytest.raises(fastapi.HTTPException)`` will not match it.
* ``from http.client import HTTPException`` — an unrelated stdlib exception
  whose constructor signature differs, so the route returns 500 instead of
  the intended HTTP status.
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
import sys
from pathlib import Path

from common_prek_utils import console


def _is_fastapi_module(module: str) -> bool:
    """Return True if *module* is ``fastapi`` or a submodule of it."""
    return module == "fastapi" or module.startswith("fastapi.")


def check_file(file_path: Path) -> list[tuple[int, str]]:
    """Return list of ``(line_number, import_statement)`` violations."""
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []

    violations: list[tuple[int, str]] = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or not node.module:
            continue
        if _is_fastapi_module(node.module):
            continue
        bad_aliases = [alias for alias in node.names if alias.name == "HTTPException"]
        if not bad_aliases:
            continue
        rendered = ", ".join(
            alias.name if not alias.asname else f"{alias.name} as {alias.asname}" for alias in bad_aliases
        )
        violations.append((node.lineno, f"from {node.module} import {rendered}"))

    return violations


def main() -> None:
    parser = argparse.ArgumentParser(description="Check that HTTPException is imported from fastapi")
    parser.add_argument("files", nargs="*", help="Files to check")
    args = parser.parse_args()

    if not args.files:
        return

    total_violations = 0

    for file_path in [Path(f) for f in args.files]:
        violations = check_file(file_path)
        if not violations:
            continue
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
        message = (
            f"Found {total_violations} HTTPException import(s) not coming from `fastapi`.\n"
            "Use `from fastapi import HTTPException` instead. Importing it from "
            "`starlette.exceptions`, `http.client`, or any other module yields a "
            "different class at runtime and breaks `isinstance` / `pytest.raises` "
            "checks against `fastapi.HTTPException` (and, for `http.client`, calls "
            "the wrong constructor so the route returns 500 instead of the intended "
            "status)."
        )
        if console:
            console.print()
            console.print(f"[red]{message}[/red]")
        else:
            print()
            print(message)
        sys.exit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
