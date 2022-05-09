#!/usr/bin/env python3
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

import ast
import os
import sys
from glob import glob
from typing import Dict, List, Sequence, Union

from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module. "
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )

console = Console(color_system="standard", width=180)

FstringInfo = Dict[str, Union[str, int]]


class LogFstringFinder(ast.NodeVisitor):
    logging_levels: Sequence[str] = ("critical", "error", "warning", "info", "debug")
    has_fstrings_in_logging: bool = False
    fstrings_logging_info: List[FstringInfo] = []

    def __init__(self, file_name: str) -> None:
        self.file_name = file_name

    def _add_fstring_info(self, node):
        fstring_info: FstringInfo = {"file_name": self.file_name, "line_number": node.lineno}
        LogFstringFinder.fstrings_logging_info.append(fstring_info)

    def visit_Call(self, node: ast.Call) -> None:
        # Validate callable name is a predefined logging level other than `notset` and argument to the
        # logging call is an f-string.
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr in self.logging_levels
            and isinstance(node.args[0], ast.JoinedStr)
        ):
            # Check for logging calls of `self.log.info()`, `self.logger.warning()`, etc. form or similar.
            if (
                isinstance(node.func.value, ast.Attribute)
                and getattr(node.func.value.value, "id") == "self"
                and node.func.value.attr.startswith("log")
            ):
                LogFstringFinder.has_fstrings_in_logging = True
                self._add_fstring_info(node)
            # Check if logging call does not utilize a class method such as ``logging.info()``.
            elif isinstance(node.func.value, ast.Name) and node.func.value.id.startswith("log"):
                LogFstringFinder.has_fstrings_in_logging = True
                self._add_fstring_info(node)


def check_logging_for_fstrings(provider_files: List[str]):
    for file_name in provider_files:
        if file_name.endswith("/__pycache__"):
            continue

        if file_name.endswith(".py"):
            try:
                with open(file_name, encoding="utf-8") as f:
                    tree = ast.parse(f.read(), file_name)
            except Exception:
                print(f"Error when opening file: {file_name}", file=sys.stderr)
                raise

            visitor = LogFstringFinder(file_name)
            visitor.visit(tree)

    if visitor.has_fstrings_in_logging:
        console.print(
            "[bold red]\nF-strings found in logging calls within the following file(s). "
            "Please convert these calls to use %-formatting instead.[/]\n"
        )
        for fstring_log in visitor.fstrings_logging_info:
            console.print(f"{fstring_log['file_name']}:{fstring_log['line_number']}")

        sys.exit(1)


if __name__ == "__main__":
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))
    all_airflow_python_files = sorted(glob(f"{root_dir}/airflow/**/*", recursive=True))
    check_logging_for_fstrings(all_airflow_python_files)
