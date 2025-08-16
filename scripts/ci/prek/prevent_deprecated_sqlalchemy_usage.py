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
import sys
from pathlib import Path

from rich.console import Console

console = Console(color_system="standard", width=200)


def check_session_query(mod: ast.Module) -> int:
    errors = False
    for node in ast.walk(mod):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if (
                node.func.attr == "query"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "session"
            ):
                console.print(
                    f"\nUse of legacy `session.query` detected on line {node.lineno}. "
                    f"\nSQLAlchemy 2.0 deprecates the `Query` object"
                    f"use the `select()` construct instead."
                )
                errors = True
    return errors


def main():
    for file in sys.argv[1:]:
        file_path = Path(file)
        ast_module = ast.parse(file_path.read_text(encoding="utf-8"), file)
        errors = check_session_query(ast_module)
        return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
