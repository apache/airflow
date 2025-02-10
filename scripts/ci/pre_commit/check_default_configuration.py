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

import subprocess

cmd = [
    "airflow",
    "config",
    "lint",
]
expected_outout = "No issues found in your airflow.cfg."

if __name__ == "__main__":
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    output = result.stdout
    if result.returncode != 0 or expected_outout not in output:
        print("\033[0;31mERROR: when running `airflow config lint`\033[0m\n")
        print(output)
        exit(1)
    else:
        print(output)
        exit(0)
