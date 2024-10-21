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
        f"To execute this script, run ./{__file__} [FILE] ..."
    )


console = Console(color_system="standard", width=200)

errors: list[str] = []

WATCHER_APPEND_INSTRUCTION = "list(dag.tasks) >> watcher()"

PYTEST_FUNCTION = """
from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
"""
PYTEST_FUNCTION_PATTERN = re.compile(
    r"from tests_common\.test_utils\.system_tests import get_test_run(?:  # noqa: E402)?\s+"
    r"(?:# .+\))?\s+"
    r"test_run = get_test_run\(dag\)"
)


def _check_file(file: Path):
    content = file.read_text()
    if "from tests_common.test_utils.watcher import watcher" in content:
        index = content.find(WATCHER_APPEND_INSTRUCTION)
        if index == -1:
            errors.append(
                f"[red]The example {file} imports tests_common.test_utils.watcher "
                f"but does not use it properly![/]\n\n"
                "[yellow]Make sure you have:[/]\n\n"
                f"        {WATCHER_APPEND_INSTRUCTION}\n\n"
                "[yellow]as the last instruction in your example DAG.[/]\n"
            )
        else:
            operator_leftshift_index = content.find("<<", index + len(WATCHER_APPEND_INSTRUCTION))
            operator_rightshift_index = content.find(">>", index + len(WATCHER_APPEND_INSTRUCTION))
            if operator_leftshift_index != -1 or operator_rightshift_index != -1:
                errors.append(
                    f"[red]In the example {file} "
                    f"watcher is not the last instruction in your DAG "
                    f"(there are << or >> operators after it)![/]\n\n"
                    "[yellow]Make sure you have:[/]\n"
                    f"        {WATCHER_APPEND_INSTRUCTION}\n\n"
                    "[yellow]as the last instruction in your example DAG.[/]\n"
                )
    if not PYTEST_FUNCTION_PATTERN.search(content):
        errors.append(
            f"[yellow]The example {file} missed the pytest function at the end.[/]\n\n"
            "All example tests should have this function added:\n\n" + PYTEST_FUNCTION + "\n\n"
            "[yellow]Automatically adding it now![/]\n"
        )
        file.write_text(content + "\n" + PYTEST_FUNCTION)


if __name__ == "__main__":
    for file in sys.argv[1:]:
        _check_file(Path(file))
    if errors:
        console.print("[red]There were some errors in the example files[/]\n")
        for error in errors:
            console.print(error)
        sys.exit(1)
