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

errors: list[str] = []

SKIP_COMP_CHECK = "# ignore airflow compat check"
GET_AIRFLOW_APP_MATCHER = re.compile(r".*get_airflow_app\(\)")


def _check_file(_file: Path):
    lines = _file.read_text().splitlines()

    for index, line in enumerate(lines):
        if SKIP_COMP_CHECK in line:
            continue

        if GET_AIRFLOW_APP_MATCHER.match(line):
            errors.append(
                f"[red]In {_file}:{index} there is a forbidden construct "
                "(Airflow 2.4+ only):[/]\n\n"
                f"{lines[index]}\n\n"
                "[yellow]You should not use airflow.utils.airflow_flask_app.get_airflow_app() in providers "
                "as it is not available in Airflow 2.4+. Use current_app instead.[/]"
            )


if __name__ == "__main__":
    for file in sys.argv[1:]:
        _check_file(Path(file))
    if errors:
        console.print("[red]Found Airflow 2.3+ compatibility problems in providers:[/]\n")
        for error in errors:
            console.print(f"{error}")
        sys.exit(1)
