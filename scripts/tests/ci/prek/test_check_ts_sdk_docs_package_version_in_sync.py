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
from __future__ import annotations

import json
from pathlib import Path

import pytest
from check_ts_sdk_docs_package_version_in_sync import check_sync, load_dependencies

LONG_VERSION = "^3.1.2-some-very-long-prerelease-tag-that-exceeds-the-header-width"


def _write_package_json(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(content))


def _write_tree(root: Path, *, sdk_package: dict, docs_package: dict) -> None:
    _write_package_json(root / "ts-sdk" / "package.json", sdk_package)
    _write_package_json(root / "ts-sdk" / "docs" / "package.json", docs_package)


def _setup_missing_docs_file(root: Path) -> None:
    _write_package_json(root / "ts-sdk" / "package.json", {"dependencies": {}})


def _setup_malformed_docs_json(root: Path) -> None:
    _write_package_json(root / "ts-sdk" / "package.json", {"dependencies": {}})
    docs_path = root / "ts-sdk" / "docs" / "package.json"
    docs_path.parent.mkdir(parents=True, exist_ok=True)
    docs_path.write_text("{not valid json")


@pytest.mark.parametrize(
    ("sdk_package", "docs_package", "expected_exit_code", "expected_substrings", "unexpected_substrings"),
    [
        pytest.param(
            {"dependencies": {"@msgpack/msgpack": "^3.1.2"}},
            {"devDependencies": {"@msgpack/msgpack": "^3.1.2"}},
            0,
            ["OK: 1 shared dependencies"],
            [],
            id="all-shared-dependencies-in-sync",
        ),
        pytest.param(
            {"dependencies": {"@msgpack/msgpack": "^3.1.2"}},
            {"devDependencies": {"@msgpack/msgpack": "^3.1.3"}},
            1,
            ["@msgpack/msgpack", "^3.1.2", "^3.1.3"],
            [],
            id="shared-dependency-drift-is-flagged",
        ),
        pytest.param(
            {"dependencies": {"@msgpack/msgpack": LONG_VERSION}},
            {"devDependencies": {"@msgpack/msgpack": "^3.1.3"}},
            1,
            [f"{LONG_VERSION}  ^3.1.3"],
            [],
            id="drift-report-column-widens-for-long-version-strings",
        ),
        pytest.param(
            {"dependencies": {"@msgpack/msgpack": {"nested": "object"}}},
            {"devDependencies": {"@msgpack/msgpack": "^3.1.2"}},
            1,
            ["@msgpack/msgpack", "non-string version"],
            [],
            id="non-string-version-is-reported-instead-of-crashing",
        ),
        pytest.param(
            # ^3.1.2 and 3.1.2 resolve to the same semver but are still flagged: the check
            # compares pinned strings, not resolved ranges.
            {"dependencies": {"@msgpack/msgpack": "^3.1.2"}},
            {"devDependencies": {"@msgpack/msgpack": "3.1.2"}},
            1,
            ["@msgpack/msgpack"],
            [],
            id="semver-compatible-but-different-string-is-still-flagged",
        ),
        pytest.param(
            {"devDependencies": {"typescript": "^6.0.2", "vitest": "^4.1.7"}},
            {"devDependencies": {"typescript": "^6.0.2"}},
            0,
            [],
            ["vitest"],
            id="dependency-present-in-only-one-file-is-ignored",
        ),
        pytest.param(
            {"devDependencies": {"vitest": "^4.1.7"}},
            {"devDependencies": {"typedoc": "^0.28.20"}},
            0,
            ["share no dependencies"],
            [],
            id="no-shared-dependencies-passes",
        ),
    ],
)
def test_check_sync(
    tmp_path: Path,
    sdk_package: dict,
    docs_package: dict,
    expected_exit_code: int,
    expected_substrings: list[str],
    unexpected_substrings: list[str],
):
    _write_tree(tmp_path, sdk_package=sdk_package, docs_package=docs_package)
    exit_code, report = check_sync(tmp_path)
    assert exit_code == expected_exit_code
    for substring in expected_substrings:
        assert substring in report
    for substring in unexpected_substrings:
        assert substring not in report


@pytest.mark.parametrize(
    ("setup", "expected_substrings"),
    [
        pytest.param(
            _setup_missing_docs_file, ["ts-sdk/docs/package.json", "does not exist"], id="missing-file"
        ),
        pytest.param(
            _setup_malformed_docs_json, ["ts-sdk/docs/package.json", "not valid JSON"], id="malformed-json"
        ),
    ],
)
def test_check_sync_error_scenarios(tmp_path: Path, setup, expected_substrings: list[str]):
    setup(tmp_path)
    exit_code, report = check_sync(tmp_path)
    assert exit_code == 1
    for substring in expected_substrings:
        assert substring in report


@pytest.mark.parametrize(
    ("dev_version", "peer_version", "expect_conflict"),
    [
        pytest.param("^0.28.1", "^0.29.0", True, id="different-versions-across-sections-conflict"),
        pytest.param("^0.28.1", "^0.28.1", False, id="same-version-across-sections-is-not-a-conflict"),
    ],
)
def test_same_file_cross_section_versions(
    tmp_path: Path, dev_version: str, peer_version: str, expect_conflict: bool
):
    path = tmp_path / "package.json"
    _write_package_json(
        path,
        {"devDependencies": {"esbuild": dev_version}, "peerDependencies": {"esbuild": peer_version}},
    )
    result = load_dependencies(path, tmp_path)
    if expect_conflict:
        assert isinstance(result, str)
        assert "esbuild" in result
        assert "devDependencies" in result
        assert "peerDependencies" in result
    else:
        assert result == {"esbuild": dev_version}
