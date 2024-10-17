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

import re
import sys
from pathlib import Path

from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    msg = (
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )
    raise SystemExit(msg)


console = Console(color_system="standard", width=200)


def _check_file(file: Path) -> list:
    content = file.read_text()
    return re.findall(r" +\:type .+?\:", content)


def _join_with_newline(list_):
    return "\n".join(list_)


if __name__ == "__main__":
    error_list = []
    for file in sys.argv[1:]:
        matches = _check_file(Path(file))
        if matches:
            error_list.append((file, matches))
    if error_list:
        error_message = "\n".join([f"{f}: \n{_join_with_newline(m)}" for f, m in error_list])
        console.print(
            f"""
[red]Found files with types specified in docstring.
This is no longer needed since sphinx can now infer types from type annotations.[/]
{error_message}
"""
        )
        sys.exit(1)
