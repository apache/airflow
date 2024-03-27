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

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_precommit_utils import console, initialize_breeze_precommit

initialize_breeze_precommit(__name__, __file__)

POSSIBLE_TEST_FOLDERS = [
    "always",
    "api",
    "api_connexion",
    "api_experimental",
    "api_internal",
    "auth",
    "callbacks",
    "charts",
    "cli",
    "cluster_policies",
    "config_templates",
    "core",
    "dag_processing",
    "dags",
    "dags_corrupted",
    "dags_with_system_exit",
    "datasets",
    "decorators",
    "executors",
    "hooks",
    "integration",
    "io",
    "jobs",
    "lineage",
    "listeners",
    "macros",
    "models",
    "notifications",
    "operators",
    "plugins",
    "providers",
    "secrets",
    "security",
    "sensors",
    "serialization",
    "system",
    "task",
    "template",
    "test_utils",
    "testconfig",
    "ti_deps",
    "timetables",
    "triggers",
    "utils",
    "www",
]

EXCEPTIONS = ["tests/__init__.py", "tests/conftest.py"]

if __name__ == "__main__":
    files = sys.argv[1:]

    MATCH_TOP_LEVEL_TEST_FILES = re.compile(r"tests/[^/]+\.py")
    files = [file for file in files if file not in EXCEPTIONS]

    errors = False
    top_level_files = [file for file in files if MATCH_TOP_LEVEL_TEST_FILES.match(file)]
    if top_level_files:
        console.print("[red]There should be no test files directly in the top-level of `tests` folder:[/]")
        console.print(top_level_files)
        errors = True
    for file in files:
        if not any(file.startswith(f"tests/{folder}/") for folder in POSSIBLE_TEST_FOLDERS):
            console.print(
                "[red]The file is in a wrong folder. Make sure to move it to the right folder "
                "listed in `./script/ci/pre_commit/pre_commit_check_tests_in_right_folders.py` "
                "or create new folder and add it to the script if you know what you are doing.[/]"
            )
            console.print(file)
            errors = True
    if errors:
        console.print("[red]Some tests are in wrong folders[/]")
        sys.exit(1)
    console.print("[green]All tests are in the right folders[/]")
    sys.exit(0)
