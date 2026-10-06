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
"""
Check that the Go SDK serializes Dags the way Airflow does.

It runs ``scripts/ci/lang_sdk_serialization/compare.py`` with the ``TestSerializeConformanceDags``
test in ``go-sdk/airflow`` as the serializer of the Go SDK. compare.py also serializes the Dags of
``test_dags.yaml`` with Airflow's own serializer. It loads the Go output with Airflow's deserializer
and compares the two serializations field by field.

Run from the repo root:

    python3 scripts/ci/prek/check_go_sdk_serialization_conformance.py

It needs ``go`` and ``uv``. Exits 0 when the two serializations agree, 1 otherwise.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
COMPARE = REPO_ROOT / "scripts" / "ci" / "lang_sdk_serialization" / "compare.py"

# compare.py runs this command from the repo root and appends the paths of test_dags.yaml and of
# the file to write. The paths come after -args, so they reach the test.
SERIALIZER = [
    "go",
    "-C",
    "go-sdk",
    "test",
    "./airflow",
    "-count=1",
    "-run",
    "^TestSerializeConformanceDags$",
    "-args",
]


def main() -> int:
    command = [sys.executable, str(COMPARE), "--sdk", "go", "--", *SERIALIZER]
    return subprocess.run(command, cwd=REPO_ROOT, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
