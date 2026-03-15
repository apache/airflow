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
"""Update the root uv.lock file by running ``uv lock --upgrade`` inside the Breeze CI image."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

AIRFLOW_ROOT_PATH = Path(__file__).resolve().parents[3]


def main() -> int:
    print("Updating uv.lock inside the Breeze CI image ...")
    result = subprocess.run(
        [
            "breeze",
            "run",
            "uv",
            "lock",
            "--upgrade",
        ],
        cwd=AIRFLOW_ROOT_PATH,
        check=False,
    )
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
