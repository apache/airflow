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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import ast
import sys
from pathlib import Path

from rich.console import Console

console = Console(color_system="standard", width=200)


def check_session_query(mod: ast.Module, file_path: str) -> bool:
    errors = False
    for node in ast.walk(mod):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "query":
                console.print(f"Deprecated query-obj found at line {node.lineno} in {file_path}.")
                errors = True
        if isinstance(node, ast.ImportFrom):
            if (
                node.module == "sqlalchemy.orm.query"
                or node.module == "sqlalchemy"
                or node.module == "sqlalchemy.orm"
            ):
                for alias in node.names:
                    if alias.name == "Query":
                        console.print(f"Deprecated Query class found at line {node.lineno} in {file_path}.")
                        errors = True

    return errors


def main():
    exit_code = 0
    for file in sys.argv[1:]:
        file_path = Path(file)
        ast_module = ast.parse(file_path.read_text(encoding="utf-8"), file)
        errors = check_session_query(ast_module, file_path)
        if errors:
            exit_code = 1
            console.print("[yellow]Please update SQLAlchemy 2.0 style.\n")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
