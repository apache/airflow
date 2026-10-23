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
# dependencies = ["rich>=13.6.0"]
# ///
"""
Keep the Java SDK quick start the same wherever a reader meets it.

The quick start appears on three pages, and nothing resolves them at build time, so an edit to one
leaves the others saying something else. The guide owns the snippets; this hook copies them into
the SDK README and the Dokka landing page between their ``SHARED SNIPPET`` markers, and checks that
none of the three names a release instead of the version placeholder, since a pinned release goes
stale as soon as the next one ships.

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/sync_java_sdk_docs.py
"""

from __future__ import annotations

import re
import sys
import textwrap
from pathlib import Path

from common_prek_utils import console, insert_documentation

JAVA_RST = "airflow-core/docs/authoring-and-scheduling/language-sdks/java.rst"
SDK_README = "java-sdk/README.md"
DOKKA_MODULE = "java-sdk/sdk/module.md"

SNIPPET_NAMES = ("hello-dag", "hello-main")
GENERATED_FILES = (SDK_README, DOKKA_MODULE)

# The docs point readers at Maven Central for a release, and show this in the build snippets instead.
VERSION_PLACEHOLDER = "<version>"

# Each pattern's first group is the version a reader installs, and all of them must be the placeholder.
VERSION_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    JAVA_RST: (
        re.compile(r"^\s*airflowJavaSdkVersion=(\S+)", re.MULTILINE),
        re.compile(r"<airflow\.sdk\.version>(.+?)</airflow\.sdk\.version>"),
    ),
    SDK_README: (
        re.compile(r'id\("org\.apache\.airflow\.sdk"\)\s+version\s+"([^"]+)"'),
        re.compile(r'"org\.apache\.airflow:airflow-sdk(?:-[a-z]+)?:([^"]+)"'),
    ),
    DOKKA_MODULE: (
        re.compile(r'id\("org\.apache\.airflow\.sdk"\)\s+version\s+"([^"]+)"'),
        re.compile(r'"org\.apache\.airflow:airflow-sdk(?:-[a-z]+)?:([^"]+)"'),
    ),
}


def extract_snippet(rst: str, name: str) -> str:
    """Return the Java the guide shows between the markers named ``name``, dedented."""
    match = re.search(
        rf"^\.\. BEGIN SHARED SNIPPET {re.escape(name)}$(.*?)^\.\. END SHARED SNIPPET {re.escape(name)}$",
        rst,
        re.DOTALL | re.MULTILINE,
    )
    if match is None:
        raise ValueError(f"{JAVA_RST} has no SHARED SNIPPET markers named {name!r}")
    body = match.group(1).replace(".. code-block:: java\n", "", 1)
    return textwrap.dedent(body).strip()


def render_snippet(code: str) -> list[str]:
    """Wrap the Java in a fenced block, as the Markdown pages render it."""
    return ["\n", "```java\n", *[f"{line}\n" for line in code.splitlines()], "```\n", "\n"]


def find_versions(contents: dict[str, str]) -> dict[str, list[str]]:
    """Collect every documented version string, keyed by the file it was found in."""
    return {
        path: [match.group(1) for pattern in patterns for match in pattern.finditer(contents[path])]
        for path, patterns in VERSION_PATTERNS.items()
    }


def check_versions(contents: dict[str, str]) -> list[str]:
    """Return one error line per problem: a file with no version, or one that names a release."""
    found = find_versions(contents)
    errors = [f"{path}: no Java SDK version found" for path, versions in found.items() if not versions]
    errors += [
        f"{path}: the build snippets install {version!r}"
        for path, versions in found.items()
        for version in sorted(set(versions))
        if version != VERSION_PLACEHOLDER
    ]
    return errors


def main() -> int:
    repo_root = Path(__file__).resolve().parents[3]
    rst = repo_root.joinpath(JAVA_RST).read_text()
    try:
        snippets = {name: extract_snippet(rst, name) for name in SNIPPET_NAMES}
    except ValueError as error:
        console.print(f"[red]{error}[/]")
        return 1

    changed = [
        path
        for path in GENERATED_FILES
        for name, code in snippets.items()
        if insert_documentation(
            repo_root.joinpath(path),
            render_snippet(code),
            f"<!-- BEGIN SHARED SNIPPET {name} -->",
            f"<!-- END SHARED SNIPPET {name} -->",
            extra_information=f"the {name} quick-start snippet from {JAVA_RST}",
        )
    ]

    contents = {path: repo_root.joinpath(path).read_text() for path in VERSION_PATTERNS}
    if errors := check_versions(contents):
        console.print("[red]" + "\n".join(errors) + "[/]")
        console.print(
            f"Install {VERSION_PLACEHOLDER!r} and link to Maven Central for the release, or fix the "
            f"patterns in\n  {__file__}"
        )
        return 1
    if changed:
        console.print(f"[yellow]Copied the quick start from {JAVA_RST}; re-stage the files it changed.[/]")
        return 1
    return 0


if __name__ in ("__main__", "__mp_main__"):
    sys.exit(main())
