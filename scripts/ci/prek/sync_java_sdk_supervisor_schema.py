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
Keep the Java SDK's bundled Supervisor Schema in sync with ``airflowSupervisorSchemaVersion``.

The Gradle task ``:sdk:syncSupervisorSchema`` downloads a fresh ``schema.json`` when the
``api_version`` inside it differs from the version declared in ``java-sdk/gradle.properties``.
Starting Gradle for that comparison costs over a minute on a cold CI runner (wrapper download,
JVM start, plugin resolution), so this hook does the same comparison in Python first and only
hands over to Gradle when the two versions actually differ.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
JAVA_SDK_DIR = REPO_ROOT / "java-sdk"
GRADLE_PROPERTIES = JAVA_SDK_DIR / "gradle.properties"
SCHEMA_FILE = JAVA_SDK_DIR / "sdk" / "schema" / "schema.json"

VERSION_PATTERN = re.compile(r"^airflowSupervisorSchemaVersion\s*=\s*(\S+)\s*$", re.MULTILINE)


def configured_version() -> str:
    match = VERSION_PATTERN.search(GRADLE_PROPERTIES.read_text())
    if not match:
        print(f"airflowSupervisorSchemaVersion is not set in {GRADLE_PROPERTIES}", file=sys.stderr)
        sys.exit(1)
    return match.group(1)


def bundled_version() -> str | None:
    if not SCHEMA_FILE.exists():
        return None
    try:
        with SCHEMA_FILE.open() as schema:
            return json.load(schema).get("api_version")
    except json.JSONDecodeError:
        # A truncated or conflict-marked file counts as drift; Gradle overwrites it.
        return None


def main() -> int:
    expected = configured_version()
    actual = bundled_version()
    if actual == expected:
        print(f"Supervisor Schema is up-to-date (api_version={expected}).")
        return 0
    print(
        f"Supervisor Schema api_version={actual!r} differs from configured {expected!r}, syncing via Gradle."
    )
    result = subprocess.run(
        [str(JAVA_SDK_DIR / "gradlew"), "-p", str(JAVA_SDK_DIR), ":sdk:syncSupervisorSchema"],
        check=False,
    )
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
