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

import json
import re
import subprocess
from pathlib import Path

from rich.console import Console

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[2].resolve()

if __name__ == "__main__":
    console = Console(width=400, color_system="standard")

    provider_dependencies = json.loads(
        (AIRFLOW_SOURCES_ROOT / "generated" / "provider_dependencies.json").read_text()
    )
    all_dependencies_to_remove = []
    for provider in provider_dependencies:
        for dependency in provider_dependencies[provider]["deps"]:
            if 'platform_machine != "aarch64"' in dependency:
                all_dependencies_to_remove.append(re.split(r"[~<>=;]", dependency)[0])
    console.print(
        "\n[bright_blue]Uninstalling ARM-incompatible libraries "
        + " ".join(all_dependencies_to_remove)
        + "\n"
    )
    subprocess.run(["pip", "uninstall", "-y"] + all_dependencies_to_remove)
    result = subprocess.run(["pytest", "--collect-only", "-qqqq", "--disable-warnings", "tests"], check=False)
    if result.returncode != 0:
        console.print("\n[red]Test collection in ARM environment failed.")
        console.print(
            "[yellow]You should wrap the failing imports in try/except/skip clauses\n"
            "See similar examples as skipped tests right above.\n"
        )
        exit(result.returncode)
