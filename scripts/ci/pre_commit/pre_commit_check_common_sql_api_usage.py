#!/usr/bin/env python
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
from __future__ import annotations

import ast
import pathlib
import sys
from pathlib import Path

from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To execute this script, run ./{__file__} [FILE] ..."
    )

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3].resolve()
COMMON_SQL_ROOT = AIRFLOW_SOURCES_ROOT / "airflow" / "providers" / "common" / "sql"

COMMON_SQL_API_FILE = COMMON_SQL_ROOT / "api.txt"
COMMON_SQL_README_API_FILE = COMMON_SQL_ROOT / "README_API.md"

COMMON_SQL_PACKAGE_PREFIX = "airflow.providers.common.sql."

console = Console(width=400, color_system="standard")

ALL_PUBLIC_COMMON_SQL_API_IMPORTS = {
    line.split("(")[0]
    for line in COMMON_SQL_API_FILE.read_text().splitlines()
    if line.strip() and not line.strip().startswith("#")
}


def get_imports_from_file(filepath: Path):
    content = filepath.read_text()
    doc_node = ast.parse(content, str(filepath))
    import_names: set[str] = set()
    for current_node in ast.walk(doc_node):
        if not isinstance(current_node, (ast.Import, ast.ImportFrom)):
            continue
        for alias in current_node.names:
            name = alias.name
            fullname = f"{current_node.module}.{name}" if isinstance(current_node, ast.ImportFrom) else name
            import_names.add(fullname)
    return import_names


if __name__ == "__main__":
    errors = 0
    for file_name in sys.argv[1:]:
        file_path = AIRFLOW_SOURCES_ROOT / pathlib.Path(file_name)
        try:
            file_path.relative_to(COMMON_SQL_ROOT)
            console.print(f"[bright_blue]Skipping {file_path} as it is part of the common.sql provider[/]")
            continue
        except ValueError:
            # The is_relative_to is only available in 3.9+, so we have to attempt to get relative path, and
            # it will fail in case it is not (we only want to check those paths that are not in common.sql
            pass
        imports = get_imports_from_file(file_path)
        header_printed = False
        for _import in imports:
            if _import.startswith(COMMON_SQL_PACKAGE_PREFIX):
                if not header_printed:
                    console.print(f"\n[bright_blue]The common.sql imports in {file_path}:[/]")
                    header_printed = True
                if _import not in ALL_PUBLIC_COMMON_SQL_API_IMPORTS:
                    console.print(
                        f"[red]NOK. The import {_import} is not part of the public common.sql API.[/]"
                    )
                    errors += 1
                else:
                    console.print(f"[green]OK. The import {_import} is part of the public common.sql API.[/]")
    if errors == 0:
        console.print(
            "[green]All OK. All usages of the common.sql provider in other providers use public API[/]"
        )
        sys.exit(0)
    console.print(
        "\n[red]Some of the above providers are not using the public API of common.sql.[/]"
        f"\nYou can only use public API as defined in {COMMON_SQL_API_FILE}."
        f"\n[yellow]Please fix it and make sure only public API is used.[/]"
        f"\n[bright_blue]You can read more about it in the {COMMON_SQL_README_API_FILE}.[/]\n"
    )
    sys.exit(1)
