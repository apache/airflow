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

The Gradle task ``:sdk:syncSupervisorSchema`` copies the Task SDK's snapshot when it declares the
configured ``api_version``, and otherwise downloads the published schema when the ``api_version``
inside ``schema.json`` differs from the version declared in ``java-sdk/gradle.properties``.
Starting Gradle for that comparison costs over a minute on a cold CI runner (wrapper download,
JVM start, plugin resolution), so this hook does the same comparison in Python first and only
hands over to Gradle when a download is needed.
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
MONOREPO_SCHEMA_FILE = (
    REPO_ROOT / "task-sdk" / "src" / "airflow" / "sdk" / "execution_time" / "schema" / "schema.json"
)

VERSION_PATTERN = re.compile(r"^airflowSupervisorSchemaVersion\s*=\s*(\S+)\s*$", re.MULTILINE)


def configured_version() -> str:
    match = VERSION_PATTERN.search(GRADLE_PROPERTIES.read_text())
    if not match:
        print(f"airflowSupervisorSchemaVersion is not set in {GRADLE_PROPERTIES}", file=sys.stderr)
        sys.exit(1)
    return match.group(1)


def read_api_version(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        with path.open() as schema:
            return json.load(schema).get("api_version")
    except json.JSONDecodeError:
        # A truncated or conflict-marked file counts as drift; it is overwritten.
        return None


def main() -> int:
    expected = configured_version()
    if read_api_version(MONOREPO_SCHEMA_FILE) == expected:
        snapshot = MONOREPO_SCHEMA_FILE.read_bytes()
        if SCHEMA_FILE.exists() and SCHEMA_FILE.read_bytes() == snapshot:
            print(f"Supervisor Schema matches the Task SDK snapshot (api_version={expected}).")
        else:
            print(f"Refreshing Supervisor Schema from {MONOREPO_SCHEMA_FILE.relative_to(REPO_ROOT)}.")
            SCHEMA_FILE.write_bytes(snapshot)
        return 0
    actual = read_api_version(SCHEMA_FILE)
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
