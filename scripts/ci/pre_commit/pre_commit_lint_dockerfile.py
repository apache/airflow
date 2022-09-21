#!/usr/bin/env python
#
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
import sys
from pathlib import Path

AIRFLOW_SOURCES = Path(__file__).parents[3].resolve()
docker_files = [f"/root/{name}" for name in sys.argv[1:]]

print(sys.argv)
cmd = [
    "docker",
    "run",
    "-v",
    f"{AIRFLOW_SOURCES}:/root",
    "-w",
    "/root",
    "--rm",
    "hadolint/hadolint:2.10.0-beta-alpine",
    "/bin/hadolint",
    *docker_files,
]

print("Running command:")
print(" ".join(cmd))
print()

result = subprocess.run(
    cmd,
    capture_output=True,
    text=True,
    check=False,
)

output = result.stdout
if result.returncode != 0:
    print(f"\033[0;31mERROR: {result.returncode} when running hadolint\033[0m\n")
    for line in output.splitlines():
        print(line.lstrip("/root/"))
    sys.exit(result.returncode)
