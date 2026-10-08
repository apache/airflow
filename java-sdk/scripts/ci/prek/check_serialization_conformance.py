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
"""Check the Java SDK serializes Dags as Airflow does, with scripts/ci/lang_sdk_serialization/compare.py."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "scripts" / "ci" / "prek"))

from common_prek_utils import AIRFLOW_ROOT_PATH

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

if __name__ == "__main__":
    java_sdk = AIRFLOW_ROOT_PATH / "java-sdk"
    # Gradle prints its own progress on stdout, so the classpath is the last line.
    printed = subprocess.run(
        [str(java_sdk / "gradlew"), "-p", str(java_sdk), "-q", ":sdk:printConformanceClasspath"],
        check=False,
        capture_output=True,
        text=True,
    )
    lines = printed.stdout.strip().splitlines()
    if printed.returncode or not lines:
        sys.stderr.write(printed.stdout)
        sys.stderr.write(printed.stderr)
        raise SystemExit("Could not build the Java SDK conformance classpath; see the Gradle output above")
    classpath = lines[-1]
    compare = AIRFLOW_ROOT_PATH / "scripts" / "ci" / "lang_sdk_serialization" / "compare.py"
    serializer = ["java", "-cp", classpath, "org.apache.airflow.sdk.conformance.SerializeJavaKt"]
    command = [
        sys.executable,
        str(compare),
        "--sdk",
        "java",
        "--supports",
        "literal_inputs",
        "--",
        *serializer,
    ]
    sys.exit(subprocess.run(command, check=False).returncode)
