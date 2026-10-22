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
# /// script
# requires-python = ">=3.11,<3.12"
# dependencies = []
# ///
"""
Fail when the user-facing docs tell readers to install different Java SDK versions.

The version a reader installs appears in the guide, the SDK README and the Dokka landing page.
Nothing resolves it at build time, so a release that updates one and not the others leaves
instructions that do not work together. This hook compares every occurrence and reports the ones
that drifted; bumping a release means making them all read the same.

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/check_java_sdk_docs_version_in_sync.py
"""

from __future__ import annotations

import pathlib
import re
import sys

JAVA_RST = "airflow-core/docs/authoring-and-scheduling/language-sdks/java.rst"
SDK_README = "java-sdk/README.md"
DOKKA_MODULE = "java-sdk/sdk/module.md"

# Each pattern's first group is the version string, and every match of every pattern must agree.
VERSION_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    JAVA_RST: (
        re.compile(r"This guide covers version\s+``([^`]+)``", re.MULTILINE),
        re.compile(r"^\s*airflowJavaSdkVersion=(\S+)", re.MULTILINE),
        re.compile(r"<airflow\.sdk\.version>([^<]+)</airflow\.sdk\.version>"),
    ),
    SDK_README: (
        re.compile(r"^\*\*Status:\*\*\s+(\S+)", re.MULTILINE),
        re.compile(r'id\("org\.apache\.airflow\.sdk"\)\s+version\s+"([^"]+)"'),
        re.compile(r'"org\.apache\.airflow:airflow-sdk(?:-[a-z]+)?:([^"]+)"'),
    ),
    DOKKA_MODULE: (
        re.compile(r'id\("org\.apache\.airflow\.sdk"\)\s+version\s+"([^"]+)"'),
        re.compile(r'"org\.apache\.airflow:airflow-sdk(?:-[a-z]+)?:([^"]+)"'),
    ),
}


def find_versions(contents: dict[str, str]) -> dict[str, list[str]]:
    """Collect every documented version string, keyed by the file it was found in."""
    found: dict[str, list[str]] = {}
    for path, patterns in VERSION_PATTERNS.items():
        text = contents[path]
        versions = [match.group(1) for pattern in patterns for match in pattern.finditer(text)]
        found[path] = versions
    return found


def check_sync(contents: dict[str, str]) -> list[str]:
    """Return one error line per problem: a file with no version, or a version that disagrees."""
    found = find_versions(contents)
    errors = [f"{path}: no Java SDK version found" for path, versions in found.items() if not versions]
    distinct = {version for versions in found.values() for version in versions}
    if len(distinct) > 1:
        errors.append(f"Documented Java SDK versions disagree: {', '.join(sorted(distinct))}")
        errors += [f"  {path}: {', '.join(versions)}" for path, versions in found.items() if versions]
    return errors


def main() -> int:
    repo_root = pathlib.Path(__file__).resolve().parents[3]
    contents = {path: repo_root.joinpath(path).read_text() for path in VERSION_PATTERNS}
    if errors := check_sync(contents):
        print("\n".join(errors))
        print("\nMake every documented Java SDK version read the same, or adjust the patterns in")
        print(f"  {pathlib.Path(__file__).relative_to(repo_root)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
