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
import re
import sys
from pathlib import Path
from typing import List

from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )


console = Console(color_system="standard", width=200)

errors: List[str] = []

GET_ATTR_MATCHER = re.compile(r".*getattr\((ti|TI), ['\"]run_id['\"]\).*")
TI_RUN_ID_MATCHER = re.compile(r".*(ti|TI)\.run_id.*")


def _check_file(_file: Path):
    lines = _file.read_text().splitlines()

    for index, line in enumerate(lines):
        if GET_ATTR_MATCHER.match(line) or TI_RUN_ID_MATCHER.match(line):
            errors.append(
                f"[red]In {_file}:{index} there is a forbidden construct "
                f"(Airflow 2.2+ only):[/]\n\n"
                f"{lines[index]}\n\n"
                f"[yellow]You should not retrieve run_id from Task Instance in providers as it "
                f"is not available in Airflow 2.1[/]\n\n"
                f"Use one of: \n\n"
                f"     context['run_id']\n\n"
                f"     getattr(ti, 'run_id', '<DEFAULT>')\n\n"
            )


if __name__ == '__main__':
    for file in sys.argv[1:]:
        _check_file(Path(file))
    if errors:
        console.print("[red]Found forbidden usage of TaskInstance's run_id in providers:[/]\n")
        for error in errors:
            console.print(f"{error}")
        sys.exit(1)
