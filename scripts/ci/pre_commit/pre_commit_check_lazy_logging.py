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
from __future__ import annotations

import ast
import pathlib
import re
import sys

import astor as astor
from rich.console import Console

console = Console(color_system="standard", width=200)

LOGGIN_MATCHER = re.compile(r'^log.?[a-z]*\.[a-z]*\(f.*["\']')
SELF_LOG_MATCHER = re.compile(r'^self\.log\.[a-z]*\(f.*["\']')


class LogFinder(astor.TreeWalk):
    module_printed: bool = False
    name: str = ''
    error_count = 0

    def pre_Call(self):
        if isinstance(self.cur_node.func, ast.Attribute) and (
            isinstance(self.cur_node.func.value, ast.Name)
            and (
                self.cur_node.func.value.id == "logger"
                or self.cur_node.func.value.id == "logging"
                or self.cur_node.func.value.id == "log"
            )
            or (self.cur_node.func.attr in ['log', 'debug', 'warning', 'info', "error", "critical"])
        ):
            line = astor.to_source(self.cur_node, add_line_information=True)
            if LOGGIN_MATCHER.match(line) or SELF_LOG_MATCHER.match(line):
                if not self.module_printed:
                    self.module_printed = True
                    console.print(f"[red]Error:[/] {self.name}")
                console.print(f"{self.name}:{self.cur_node.lineno} -> {line}", end="")
                self.error_count += 1


def check_logging() -> int:
    total_error_count = 0
    for file_name in sys.argv[1:]:
        file_path = pathlib.Path(file_name)
        module = ast.parse(file_path.read_text("utf-8"), str(file_path))
        finder = LogFinder()
        finder.name = file_name
        finder.walk(node=module)
        total_error_count += finder.error_count
    if total_error_count > 0:
        console.print(
            "\n[yellow]Please convert all the logging instructions above "
            "to use '%-formatting' rather than f-strings."
        )
        console.print("Why?: https://docs.python.org/3/howto/logging.html#logging-variable-data\n")
    return 1 if total_error_count else 0


if __name__ == "__main__":
    sys.exit(check_logging())
