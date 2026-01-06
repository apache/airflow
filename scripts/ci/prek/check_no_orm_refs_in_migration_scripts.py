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
Check that there are no imports of ORM classes in any of the alembic migration scripts.
This is to prevent the addition of migration code directly referencing any ORM definition,
which could potentially break downgrades. For more details, refer to the relevant discussion
thread at this link: https://github.com/apache/airflow/issues/59871
"""

from __future__ import annotations

import ast
import os
import sys
from pathlib import Path
from pprint import pformat
from typing import Final

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import AIRFLOW_CORE_SOURCES_PATH, console

_MIGRATIONS_DIRPATH: Final[Path] = Path(
    os.path.join(AIRFLOW_CORE_SOURCES_PATH, "airflow/migrations/versions")
)
# Tuple of fully qualified references within `airflow.models.*` which are allowed to be imported by the
# migration scripts. Should only allow references to primitives or constants, and NOT ORM class definitions.
_MODELS_IMPORTS_ALLOWLIST: Final[tuple[str, ...]] = ("airflow.models.ID_LEN",)


def main() -> None:
    if len(sys.argv) > 1:
        migration_scripts = [Path(f) for f in sys.argv[1:]]
    else:
        migration_scripts = list(_MIGRATIONS_DIRPATH.glob("**/*.py"))
    console.print(
        f"Checking the following modified migration scripts: {pformat([str(path) for path in migration_scripts])}"
    )
    violations = []
    for script_path in migration_scripts:
        violations.extend(_find_models_import_violations(script_path=script_path))
    if violations:
        for err in violations:
            console.print(f"[red]{err}")
        console.print("\n[red]ORM references detected in one or more migration scripts[/]")
        sys.exit(1)
    console.print("[green]No ORM references detected in migration scripts.")


def _find_models_import_violations(script_path: Path) -> list[str]:
    """
    Return a list of invalid imports of ORM definitions for the given migration script, if any.
    For simplicity and forward compatibility when individual tables are added / removed / renamed,
    this function uses the heuristic of checking for any non-allowlisted imports from within the
    `airflow.models` module.
    """
    script_source = script_path.read_text(encoding="utf-8")
    bad_imports = []
    for node in ast.walk(ast.parse(script_source)):
        if details := _is_violating_orm_import(node=node):
            line_no, src = details
            bad_imports.append(
                f"Found bad import on line {line_no} in migration script {str(script_path)}: '{src}'"
            )
    return bad_imports


def _is_violating_orm_import(node: ast.AST) -> tuple[int, str] | None:
    """
    Return a tuple of line number and line text for the given node, if it is an import of any non-allowlisted object
    from within `airflow.models`, otherwise return `None`.
    """
    # Match "from x import y [as z]"
    if isinstance(node, ast.ImportFrom) and node.module:
        fully_qualified_reference = ".".join([node.module] + [alias.name for alias in node.names])
    # Match "import x.y.z [as w]"
    elif isinstance(node, ast.Import):
        fully_qualified_reference = ".".join([alias.name for alias in node.names])
    else:
        return None

    if not fully_qualified_reference.startswith("airflow.models"):
        return None
    if fully_qualified_reference in _MODELS_IMPORTS_ALLOWLIST:
        return None
    return (node.lineno, ast.unparse(node))


if __name__ == "__main__":
    main()
