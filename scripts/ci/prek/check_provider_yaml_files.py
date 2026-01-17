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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import (
    console,
    initialize_breeze_prek,
    run_command_via_breeze_shell,
    validate_cmd_result,
)

initialize_breeze_prek(__name__, __file__)

files_to_test = sys.argv[1:]

# First, remove dev dependencies inside the breeze container to ensure we can detect cases
# where provider code has optional cross-provider dependencies that aren't handled properly.
# See: https://github.com/apache/airflow/issues/60662
sync_result = run_command_via_breeze_shell(
    ["uv", "sync", "--no-dev", "--all-packages", "--no-python-downloads", "--no-managed-python"],
    backend="sqlite",
    warn_image_upgrade_needed=True,
)
if sync_result.returncode != 0:
    console.print("[red]Failed to remove dev dependencies with uv sync --no-dev[/]")
    sys.exit(sync_result.returncode)
console.print("[green]Dev dependencies removed successfully[/]")

cmd_result = run_command_via_breeze_shell(
    ["python3", "/opt/airflow/scripts/in_container/run_provider_yaml_files_check.py", *files_to_test],
    backend="sqlite",
    warn_image_upgrade_needed=True,
    extra_env={"PYTHONWARNINGS": "default"},
)
validate_cmd_result(cmd_result, include_ci_env_check=True)
