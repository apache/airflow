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

WATCHER_APPEND_INSTRUCTION = "list(dag.tasks) >> watcher()"

PYTEST_FUNCTION = """
def test_run():
    from airflow.utils.state import State

    dag.clear(dag_run_state=State.NONE)
    dag.run()
"""


def _check_file(file: Path):
    content = file.read_text()
    if "from tests.system.utils.watcher import watcher" in content:
        index = content.find(WATCHER_APPEND_INSTRUCTION)
        if index == -1:
            errors.append(
                f"[red]The example {file} imports tests.system.utils.watcher "
                f"but does not use it properly![/]\n\n"
                "[yellow]Make sure you have:[/]\n\n"
                f"        {WATCHER_APPEND_INSTRUCTION}\n\n"
                "[yellow]as last instruction in your example DAG.[/]\n"
            )
        else:
            operator_leftshift_index = content.find("<<", index + len(WATCHER_APPEND_INSTRUCTION))
            operator_rightshift_index = content.find(">>", index + len(WATCHER_APPEND_INSTRUCTION))
            if operator_leftshift_index != -1 or operator_rightshift_index != -1:
                errors.append(
                    f"[red]In the  example {file} "
                    f"watcher is not last instruction in your DAG (there are << "
                    f"or >> operators after it)![/]\n\n"
                    "[yellow]Make sure you have:[/]\n"
                    f"        {WATCHER_APPEND_INSTRUCTION}\n\n"
                    "[yellow]as last instruction in your example DAG.[/]\n"
                )
        if PYTEST_FUNCTION not in content:
            errors.append(
                f"[yellow]The  example {file} missed the pytest function at the end.[/]\n\n"
                "All example tests should have this function added:\n\n" + PYTEST_FUNCTION + "\n\n"
                "[yellow]Automatically adding it now!\n"
            )
            file.write_text(content + "\n" + PYTEST_FUNCTION)


if __name__ == '__main__':
    for file in sys.argv[1:]:
        _check_file(Path(file))
    if errors:
        console.print("[red]There were some errors in the example files[/]\n")
        for error in errors:
            console.print(error)
        sys.exit(1)
