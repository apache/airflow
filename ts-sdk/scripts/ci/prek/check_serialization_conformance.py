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
"""Check the TS SDK serializes Dags as Airflow does, with scripts/ci/lang_sdk_serialization/compare.py."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "scripts" / "ci" / "prek"))

from common_prek_utils import AIRFLOW_ROOT_PATH, run_command

# The features of test_dags.yaml that tests/conformance/serialize_typescript.ts can build. A Dag that
# requires another feature is left out until the builder gets it.
SUPPORTED_FEATURES: list[str] = []

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

if __name__ == "__main__":
    run_command(
        ["pnpm", "install", "--frozen-lockfile", "--config.confirmModulesPurge=false"],
        cwd=AIRFLOW_ROOT_PATH / "ts-sdk",
    )
    compare = AIRFLOW_ROOT_PATH / "scripts" / "ci" / "lang_sdk_serialization" / "compare.py"
    serializer = ["pnpm", "--dir", "ts-sdk", "exec", "tsx", "tests/conformance/serialize_typescript.ts"]
    command = [
        sys.executable,
        str(compare),
        "--sdk",
        "typescript",
        "--supports",
        ",".join(SUPPORTED_FEATURES),
        "--",
        *serializer,
    ]
    sys.exit(subprocess.run(command, check=False).returncode)
