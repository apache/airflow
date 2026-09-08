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
Rewrite java-sdk/gradle/verification-metadata.xml from an empty component list.

Gradle only ever appends to that file, so a version bump leaves the superseded
entries behind and they stay trusted for good. Starting from an empty list drops
them, which is what turns a stale checksum into a visible diff.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys
import time
from collections.abc import Callable

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
JAVA_SDK = REPO_ROOT / "java-sdk"
METADATA = JAVA_SDK / "gradle" / "verification-metadata.xml"

GRADLE_TASKS = [
    "build",
    ":sdk:dokkaGeneratePublicationHtml",
    ":sdk:dokkaGeneratePublicationJavadoc",
    "sourceTarball",
    "checksumSourceTarball",
    "publishToMavenLocal",
]

MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 30

LICENSE_HEADER = """<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->"""

ADVISORY = """
The trust list changed. Entries that disappeared are checksums nothing resolves
any more - usually versions superseded by a dependency bump. They stayed
trusted, so a future direct or transitive dependency could have pulled that
exact version back in with nobody reviewing its checksum.

Review every entry in the diff before staging it: generating the file records
what the repositories served, it does not make those bytes trustworthy.
"""


class RegenerationFailedError(RuntimeError):
    """Gradle could not regenerate the metadata within the attempt budget."""


def build_empty_metadata(committed: str) -> str:
    """Return the committed metadata with its component list emptied."""
    kept: list[str] = []
    for line in committed.splitlines():
        if "<components>" in line:
            kept += ["   <components/>", "</verification-metadata>"]
            break
        kept.append(line)
    return "\n".join(kept) + "\n"


def insert_license_header(metadata: str) -> str:
    """Put the ASF header back after the XML declaration, unless Gradle kept one."""
    if "Licensed to the Apache Software Foundation" in metadata:
        return metadata
    declaration, *rest = metadata.splitlines()
    return "\n".join([declaration, LICENSE_HEADER, *rest]) + "\n"


def run_gradle() -> bool:
    """Both properties are needed for the run to reach the end: signing has no key
    here, and sourceTarball has no default ref."""
    result = subprocess.run(
        [
            "./gradlew",
            "--no-daemon",
            "--write-verification-metadata",
            "sha256",
            "--refresh-dependencies",
            *GRADLE_TASKS,
            "-PskipSigning=true",
            "-PgitRef=HEAD",
        ],
        cwd=JAVA_SDK,
        check=False,
    )
    return result.returncode == 0


def regenerate(
    metadata: pathlib.Path,
    gradle: Callable[[], bool],
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Rewrite the metadata in place, putting the committed file back if the run never succeeds."""
    committed = metadata.read_text()
    restore = True
    try:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            print(f"==> Regenerating verification metadata (attempt {attempt}/{MAX_ATTEMPTS})", flush=True)
            metadata.write_text(build_empty_metadata(committed))
            if gradle():
                metadata.write_text(insert_license_header(metadata.read_text()))
                restore = False
                return
            if attempt < MAX_ATTEMPTS:
                print(f"Regeneration failed, retrying in {RETRY_DELAY_SECONDS}s", file=sys.stderr, flush=True)
                sleep(RETRY_DELAY_SECONDS)
        raise RegenerationFailedError(f"Regeneration failed after {MAX_ATTEMPTS} attempts")
    finally:
        if restore:
            metadata.write_text(committed)


def metadata_changed(metadata: pathlib.Path) -> bool:
    result = subprocess.run(["git", "diff", "--quiet", "--", str(metadata)], cwd=REPO_ROOT, check=False)
    return result.returncode != 0


def main() -> int:
    try:
        regenerate(METADATA, run_gradle)
    except RegenerationFailedError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    if metadata_changed(METADATA):
        print(ADVISORY, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
