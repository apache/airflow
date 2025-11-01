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
import subprocess
import tempfile

list_default_config_cmd = [
    "airflow",
    "config",
    "list",
    "--default",
]
lint_config_cmd = [
    "airflow",
    "config",
    "lint",
]
expected_output = "No issues found in your airflow.cfg."

if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Write default config cmd output to a temporary file
        default_config_file = os.path.join(tmp_dir, "airflow.cfg")
        with open(default_config_file, "w") as f:
            result = subprocess.run(list_default_config_cmd, check=False, stdout=f)
        if result.returncode != 0:
            print(f"\033[0;31mERROR: when running `{' '.join(list_default_config_cmd)}`\033[0m\n")
            exit(1)
        # Run airflow config lint to check the default config
        env = os.environ.copy()
        env["AIRFLOW_HOME"] = tmp_dir
        env["AIRFLOW_CONFIG"] = default_config_file
        result = subprocess.run(lint_config_cmd, check=False, capture_output=True, env=env)

    output: str = result.stdout.decode().strip()
    if result.returncode != 0 or expected_output not in output:
        print(f"\033[0;31mERROR: when running `{' '.join(lint_config_cmd)}`\033[0m\n")
        print(output)
        exit(1)
    print(output)
    exit(0)
