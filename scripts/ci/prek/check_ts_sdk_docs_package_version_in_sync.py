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
Fail if ts-sdk/package.json and ts-sdk/docs/package.json pin a shared dependency to
different version strings (compared literally, not as resolved semver ranges — so
``^3.1.2`` and ``3.1.2`` still count as drift).

Run from the repo root:

    uv run --project scripts python scripts/ci/prek/check_ts_sdk_docs_package_version_in_sync.py
"""

from __future__ import annotations

import json
import pathlib
import sys

from common_prek_utils import AIRFLOW_ROOT_PATH
from tabulate import tabulate

TS_SDK_PACKAGE_JSON = "ts-sdk/package.json"
TS_SDK_DOCS_PACKAGE_JSON = "ts-sdk/docs/package.json"

DEPENDENCY_SECTIONS = ("dependencies", "devDependencies", "peerDependencies", "optionalDependencies")


def load_dependencies(path: pathlib.Path, repo_root: pathlib.Path) -> dict[str, str] | str:
    """Flatten every dependency section into ``{name: version}``, or return an error message."""
    label = path.relative_to(repo_root) if path.is_relative_to(repo_root) else path
    if not path.exists():
        return f"{label} does not exist"
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        return f"{label} is not valid JSON: {exc}"

    seen_in: dict[str, str] = {}
    deps: dict[str, str] = {}
    for section in DEPENDENCY_SECTIONS:
        for name, version in data.get(section, {}).items():
            if not isinstance(version, str):
                return (
                    f"{label} pins {name!r} to a non-string version in its {section!r} section: {version!r}"
                )
            if name in seen_in and deps[name] != version:
                return (
                    f"{label} pins {name!r} to different versions in its own "
                    f"{seen_in[name]!r} ({deps[name]!r}) and {section!r} ({version!r}) sections"
                )
            seen_in[name] = section
            deps[name] = version
    return deps


def check_sync(repo_root: pathlib.Path) -> tuple[int, str]:
    """Compare shared dependency versions between the ts-sdk and ts-sdk/docs package.json files."""
    sdk_path = repo_root / TS_SDK_PACKAGE_JSON
    docs_path = repo_root / TS_SDK_DOCS_PACKAGE_JSON

    sdk_result = load_dependencies(sdk_path, repo_root)
    docs_result = load_dependencies(docs_path, repo_root)

    errors = [result for result in (sdk_result, docs_result) if isinstance(result, str)]
    if errors:
        return 1, "\n".join(f"ERROR: {error}" for error in errors)

    sdk_deps: dict[str, str] = sdk_result  # type: ignore[assignment]
    docs_deps: dict[str, str] = docs_result  # type: ignore[assignment]

    shared = sorted(set(sdk_deps) & set(docs_deps))
    if not shared:
        return (
            0,
            f"OK: {TS_SDK_PACKAGE_JSON} and {TS_SDK_DOCS_PACKAGE_JSON} share no dependencies to compare.",
        )

    mismatched = [name for name in shared if sdk_deps[name] != docs_deps[name]]
    if not mismatched:
        return (
            0,
            f"OK: {len(shared)} shared dependencies are pinned to the same version in "
            f"{TS_SDK_PACKAGE_JSON} and {TS_SDK_DOCS_PACKAGE_JSON}.",
        )

    table = tabulate(
        [(name, sdk_deps[name], docs_deps[name]) for name in mismatched],
        headers=["PACKAGE", TS_SDK_PACKAGE_JSON, TS_SDK_DOCS_PACKAGE_JSON],
        tablefmt="github",
    )
    lines = [
        f"ERROR: Dependency versions drifted between {TS_SDK_PACKAGE_JSON} and {TS_SDK_DOCS_PACKAGE_JSON}:",
        "",
        table,
        "",
        "Update the drifting package(s) to pin the same version in both files.",
    ]
    return 1, "\n".join(lines)


def main() -> int:
    exit_code, report = check_sync(AIRFLOW_ROOT_PATH)
    print(report)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
