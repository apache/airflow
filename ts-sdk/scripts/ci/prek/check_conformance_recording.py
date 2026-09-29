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
"""
Keep the recorded Python serialization output the conformance suite compares against honest.

``ts-sdk/tests/conformance/serialized_python.json`` is what Airflow's own serializer
produced for the fixtures in ``test_dags.json``. The TypeScript conformance suite diffs
its output against that recording, so the recording going stale would leave the suite
passing against an Airflow that no longer serializes the same way.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "scripts" / "ci" / "prek"))

from common_prek_utils import AIRFLOW_ROOT_PATH, console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

SCRIPT = Path("ts-sdk/tests/conformance/serialize_python.py")

if __name__ == "__main__":
    result = subprocess.run(
        ["uv", "run", "--project", "airflow-core", "python", str(SCRIPT), "--check"],
        cwd=AIRFLOW_ROOT_PATH,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        sys.exit(0)
    message = (
        f"{SCRIPT} --check failed, so the recorded Python output no longer matches what "
        "Airflow's serializer produces.\n"
        f"Re-record it with `uv run --project airflow-core python {SCRIPT}` and review the diff: "
        "a change there is a change in the parity contract.\n\n"
        f"{result.stdout}{result.stderr}"
    )
    if console:
        console.print(f"[yellow]{message}[/]")
    else:
        print(message, file=sys.stderr)
    raise SystemExit(result.returncode)
