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
Copy the Task SDK supervisor schema into the Java SDK's vendored ``schema.json``.

The copy is compared by content, so any change to the source reaches it. The declared
``airflowSupervisorSchemaVersion`` is the version the Java SDK speaks, which is checked to be one
the schema bundle defines rather than the source's newest ``api_version``. Outside the monorepo,
it falls back to the Gradle download.

Exits 0 when the copy is in sync, 1 when it was refreshed or the declared version is unknown.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

GRADLE_PROPERTIES = Path("java-sdk/gradle.properties")
VENDORED_SCHEMA = Path("java-sdk/sdk/schema/schema.json")
SOURCE_SCHEMA = Path("task-sdk/src/airflow/sdk/execution_time/schema/schema.json")
SCHEMA_VERSIONS = Path("task-sdk/src/airflow/sdk/execution_time/schema/versions")

VERSION_PATTERN = re.compile(r"^airflowSupervisorSchemaVersion\s*=\s*(\S+)\s*$", re.MULTILINE)
VERSION_MODULE_PATTERN = re.compile(r"^v(\d{4})_(\d{2})_(\d{2})\.py$")


def configured_version(repo_root: Path = REPO_ROOT) -> str:
    properties = repo_root / GRADLE_PROPERTIES
    match = VERSION_PATTERN.search(properties.read_text())
    if not match:
        raise SystemExit(f"airflowSupervisorSchemaVersion is not set in {GRADLE_PROPERTIES}")
    return match.group(1)


def api_version_of(path: Path) -> str | None:
    if not path.is_file():
        return None
    try:
        with path.open() as schema:
            return json.load(schema).get("api_version")
    except json.JSONDecodeError:
        return None


def known_versions(repo_root: Path = REPO_ROOT) -> list[str]:
    """The supervisor schema versions the bundle defines, oldest first."""
    versions = [
        "-".join(match.groups())
        for path in (repo_root / SCHEMA_VERSIONS).glob("v*.py")
        if (match := VERSION_MODULE_PATTERN.match(path.name))
    ]
    return sorted(versions)


def refresh_from_source(expected: str, repo_root: Path = REPO_ROOT) -> tuple[int, str]:
    source = repo_root / SOURCE_SCHEMA
    vendored = repo_root / VENDORED_SCHEMA
    # The pin is the version the Java SDK speaks, not the newest one the bundle carries, so it only
    # has to be a version the bundle defines.
    if expected not in (versions := known_versions(repo_root)):
        return 1, (
            f"{GRADLE_PROPERTIES} declares airflowSupervisorSchemaVersion={expected!r}, which "
            f"{SCHEMA_VERSIONS} does not define.\n"
            f"Known versions: {', '.join(versions) or 'none'}."
        )
    if vendored.is_file() and vendored.read_bytes() == source.read_bytes():
        return 0, (
            f"{VENDORED_SCHEMA} matches the schema it is vendored from "
            f"(api_version={api_version_of(source)})."
        )
    vendored.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, vendored)
    return 1, (
        f"Refreshed {VENDORED_SCHEMA} from {SOURCE_SCHEMA}.\nReview the diff, then git add it and recommit."
    )


def download_via_gradle(expected: str, repo_root: Path = REPO_ROOT) -> int:
    if api_version_of(repo_root / VENDORED_SCHEMA) == expected:
        print(f"{VENDORED_SCHEMA} is up-to-date (api_version={expected}).")
        return 0
    print(f"{VENDORED_SCHEMA} differs from configured {expected!r}, syncing via Gradle.")
    java_sdk_dir = repo_root / "java-sdk"
    result = subprocess.run(
        [str(java_sdk_dir / "gradlew"), "-p", str(java_sdk_dir), ":sdk:syncSupervisorSchema"],
        check=False,
    )
    return result.returncode


def main(repo_root: Path = REPO_ROOT) -> int:
    expected = configured_version(repo_root)
    if not (repo_root / SOURCE_SCHEMA).is_file():
        return download_via_gradle(expected, repo_root)
    exit_code, report = refresh_from_source(expected, repo_root)
    print(report, file=sys.stderr if exit_code else sys.stdout)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
