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
Refresh the Java SDK's vendored copy of the supervisor schema that the Python Task SDK owns.

``java-sdk`` vendors the schema so it builds from a source distribution with no network, the
way ``go-sdk`` and ``ts-sdk`` vendor theirs. It used to refresh that copy by downloading the
version named in ``java-sdk/gradle.properties``, which has two problems: the remote only
publishes released versions, so an in-progress one cannot be fetched at all, and a schema
change that leaves ``api_version`` alone moved nothing, so the copy went stale unnoticed.

The copy is now refreshed from the schema in this repository and compared by content, so any
change to the Python source moves it in the same commit. Copying is mechanical, so this copies
for you and fails; ``git add`` the refreshed copy and recommit. ``airflowSupervisorSchemaVersion``
stays the declared version and is asserted against the source rather than used to fetch it.

Outside the monorepo the source is absent and the copy is already what the distribution
shipped, so the download is left as the fallback.

Run it from the repo root, through prek:

    prek run sync-java-sdk-supervisor-schema

Exits 0 when the copy already matched its source, 1 when it refreshed it.
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

VERSION_PATTERN = re.compile(r"^airflowSupervisorSchemaVersion\s*=\s*(\S+)\s*$", re.MULTILINE)


def configured_version(repo_root: Path = REPO_ROOT) -> str:
    """Return the version ``gradle.properties`` declares, which the schema has to agree with."""
    properties = repo_root / GRADLE_PROPERTIES
    match = VERSION_PATTERN.search(properties.read_text())
    if not match:
        raise SystemExit(f"airflowSupervisorSchemaVersion is not set in {GRADLE_PROPERTIES}")
    return match.group(1)


def api_version_of(path: Path) -> str | None:
    """Return the ``api_version`` of a schema file, or None when it is missing or unreadable."""
    if not path.is_file():
        return None
    try:
        with path.open() as schema:
            return json.load(schema).get("api_version")
    except json.JSONDecodeError:
        # A truncated or conflict-marked file counts as drift; it is overwritten.
        return None


def refresh_from_source(expected: str, repo_root: Path = REPO_ROOT) -> tuple[int, str]:
    """Copy the schema this repository owns over the vendored copy when the two differ."""
    source = repo_root / SOURCE_SCHEMA
    vendored = repo_root / VENDORED_SCHEMA
    source_version = api_version_of(source)
    if source_version != expected:
        return 1, (
            f"{GRADLE_PROPERTIES} declares airflowSupervisorSchemaVersion={expected!r} but "
            f"{SOURCE_SCHEMA} is api_version={source_version!r}.\n"
            "Move the property to the version the schema declares."
        )
    if vendored.is_file() and vendored.read_bytes() == source.read_bytes():
        return 0, f"{VENDORED_SCHEMA} matches the schema it is vendored from (api_version={expected})."
    vendored.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, vendored)
    return 1, (
        f"Refreshed {VENDORED_SCHEMA} from {SOURCE_SCHEMA}.\nReview the diff, then git add it and recommit."
    )


def download_via_gradle(expected: str, repo_root: Path = REPO_ROOT) -> int:
    """Fall back to the Gradle download, for a build that has no monorepo around it."""
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
