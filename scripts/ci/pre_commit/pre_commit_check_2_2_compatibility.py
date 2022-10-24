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
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )

console = Console(color_system="standard", width=200)

errors: list[str] = []

SKIP_COMP_CHECK = "# ignore airflow compat check"
TRY_NUM_MATCHER = re.compile(r".*context.*\[[\"']try_number[\"']].*")
GET_MANDATORY_MATCHER = re.compile(r".*conf\.get_mandatory_value")
GET_AIRFLOW_APP_MATCHER = re.compile(r".*get_airflow_app\(\)")
HOOK_PARAMS_MATCHER = re.compile(r".*get_hook\(hook_params")


def _check_file(_file: Path):
    lines = _file.read_text().splitlines()

    for index, line in enumerate(lines):
        if SKIP_COMP_CHECK in line:
            continue

        if "XCom.get_value(" in line:
            if "if ti_key is not None:" not in lines[index - 1]:
                errors.append(
                    f"[red]In {_file}:{index} there is a forbidden construct "
                    "(Airflow 2.3.0 only):[/]\n\n"
                    f"{lines[index-1]}\n{lines[index]}\n\n"
                    "[yellow]When you use XCom.get_value( in providers, it should be in the form:[/]\n\n"
                    "if ti_key is not None:\n"
                    "    value = XCom.get_value(...., ti_key=ti_key)\n\n"
                    "See: https://airflow.apache.org/docs/apache-airflow-providers/"
                    "howto/create-update-providers.html#using-providers-with-dynamic-task-mapping\n"
                )
        if "ti.map_index" in line:
            errors.append(
                f"[red]In {_file}:{index} there is a forbidden construct "
                "(Airflow 2.3+ only):[/]\n\n"
                f"{lines[index]}\n\n"
                "[yellow]You should not use map_index field in providers "
                "as it is only available in Airflow 2.3+[/]"
            )

        if TRY_NUM_MATCHER.match(line):
            errors.append(
                f"[red]In {_file}:{index} there is a forbidden construct "
                "(Airflow 2.3+ only):[/]\n\n"
                f"{lines[index]}\n\n"
                "[yellow]You should not expect try_number field for context in providers "
                "as it is only available in Airflow 2.3+[/]"
            )

        if GET_MANDATORY_MATCHER.match(line):
            errors.append(
                f"[red]In {_file}:{index} there is a forbidden construct "
                "(Airflow 2.3+ only):[/]\n\n"
                f"{lines[index]}\n\n"
                "[yellow]You should not use conf.get_mandatory_value in providers "
                "as it is only available in Airflow 2.3+[/]"
            )

        if HOOK_PARAMS_MATCHER.match(line):
            errors.append(
                f"[red]In {_file}:{index} there is a forbidden construct "
                "(Airflow 2.3+ only):[/]\n\n"
                f"{lines[index]}\n\n"
                "[yellow]You should not use 'hook_params' in get_hook as it has been added in providers "
                "as it is not available in Airflow 2.3+. Use get_hook() instead.[/]"
            )

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
        console.print("[red]Found Airflow 2.2 compatibility problems in providers:[/]\n")
        for error in errors:
            console.print(f"{error}")
        sys.exit(1)
