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
Keep the Java SDK's vendored ``schema.json`` at the pinned supervisor schema version.

``airflowSupervisorSchemaVersion`` in ``java-sdk/gradle.properties`` is the version the Java SDK
speaks and must be one the Task SDK schema bundle defines. When it is the bundle's newest version,
the vendored copy is compared by content with the Task SDK schema and refreshed from it. When it is
older, the vendored copy must already declare that version, as copied from the release that ships it.

Exits 0 when the copy is in sync, 1 when it was refreshed or needs attention.
"""

from __future__ import annotations

import json
import re
import shutil
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


def sync_vendored_schema(expected: str, repo_root: Path = REPO_ROOT) -> tuple[int, str]:
    source = repo_root / SOURCE_SCHEMA
    vendored = repo_root / VENDORED_SCHEMA
    if expected not in (versions := known_versions(repo_root)):
        return 1, (
            f"{GRADLE_PROPERTIES} declares airflowSupervisorSchemaVersion={expected!r}, which "
            f"{SCHEMA_VERSIONS} does not define.\n"
            f"Known versions: {', '.join(versions) or 'none'}."
        )
    if expected != (newest := versions[-1]):
        if (vendored_version := api_version_of(vendored)) == expected:
            return 0, f"{VENDORED_SCHEMA} is pinned to api_version={expected} (newest is {newest})."
        return 1, (
            f"{VENDORED_SCHEMA} is api_version={vendored_version!r} but {GRADLE_PROPERTIES} pins {expected!r}.\n"
            f"Copy {SOURCE_SCHEMA} from the release that ships that version, or move the pin."
        )
    if (source_version := api_version_of(source)) != newest:
        return 1, (
            f"{SOURCE_SCHEMA} is api_version={source_version!r} but {SCHEMA_VERSIONS} defines {newest!r}.\n"
            "Regenerate the Task SDK schema before syncing the Java SDK copy."
        )
    if vendored.is_file() and vendored.read_bytes() == source.read_bytes():
        return 0, f"{VENDORED_SCHEMA} matches the schema it is vendored from (api_version={expected})."
    vendored.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, vendored)
    return 1, (
        f"Refreshed {VENDORED_SCHEMA} from {SOURCE_SCHEMA}.\nReview the diff, then git add it and recommit."
    )


def main(repo_root: Path = REPO_ROOT) -> int:
    exit_code, report = sync_vendored_schema(configured_version(repo_root), repo_root)
    print(report, file=sys.stderr if exit_code else sys.stdout)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
