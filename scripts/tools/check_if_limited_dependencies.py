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

import os
import sys
from pathlib import Path
from subprocess import CalledProcessError, check_call

from rich.console import Console

program = f"./{__file__}" if not __file__.startswith("./") else __file__

if __name__ != "__main__":
    raise Exception(
        "This file is intended to be used as an executable program. You cannot use it as a module."
        f"To execute this script, run the '{program}' command"
    )

DEV_BREEZE_SRC_PATH = Path("dev") / "breeze" / "src"

console = Console(
    force_terminal=True,
    color_system="standard",
    width=180,
)

env = os.environ.copy()
env['PYTHONPATH'] = str(DEV_BREEZE_SRC_PATH)
try:
    check_call(["python", DEV_BREEZE_SRC_PATH / "airflow_breeze" / "breeze.py", "--help"], env=env)
except CalledProcessError:
    console.print("[red]Breeze should only use limited dependencies when imported (see errors above).[/]\n")
    console.print(
        "[bright_yellow]Please make sure you only use local imports for new dependencies in breeze.[/]"
    )
    console.print()
    console.print("The only top-level dependencies should be `rich` and `click'")
    sys.exit(1)
