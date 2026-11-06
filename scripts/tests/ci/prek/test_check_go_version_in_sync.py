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

from pathlib import Path

import pytest
from check_go_version_in_sync import build_sites, check_sync, major_minor

# Default (all-consistent) version at every pin site. Individual tests override
# one entry to simulate drift. go.mod carries the patch suffix; the workflow and
# image sites carry only major.minor — the check must treat these as equal.
DEFAULT_VERSIONS = {
    "go_sdk_mod": "1.25.0",
    "go_example_mod": "1.25.0",
    "ci_amd": "1.25",
    "ci_arm": "1.25",
    "constants": "1.25",
    "kubernetes_commands": "1.25",
    "go_sdk_prek": "1.25.0",
    "root_prek": "1.25.0",
}


def _write_tree(root: Path, versions: dict[str, str]) -> None:
    """Materialise a minimal repo tree with a Go version pin at each of the eight sites."""

    def write(rel: str, content: str) -> None:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    write("go-sdk/go.mod", f"module github.com/apache/airflow/go-sdk\n\ngo {versions['go_sdk_mod']}\n")
    write(
        "kubernetes-tests/lang_sdk/go_example/go.mod",
        f"module github.com/apache/airflow/kubernetes-tests/lang_sdk/go_example\n\n"
        f"go {versions['go_example_mod']}\n",
    )
    for site, rel in (("ci_amd", "ci-amd.yml"), ("ci_arm", "ci-arm.yml")):
        write(
            f".github/workflows/{rel}",
            f"      - name: Setup Go\n        with:\n          go-version: {versions[site]}\n",
        )
    write(
        "airflow-e2e-tests/tests/airflow_e2e_tests/constants.py",
        f'GO_BUILDER_IMAGE = os.environ.get("GO_BUILDER_IMAGE", "golang:{versions["constants"]}-alpine")\n',
    )
    write(
        "dev/breeze/src/airflow_breeze/commands/kubernetes_commands.py",
        f'LANG_SDK_GO_BUILDER_IMAGE = os.environ.get("GO_BUILDER_IMAGE", '
        f'"golang:{versions["kubernetes_commands"]}-alpine")\n',
    )
    for site, rel in (
        ("go_sdk_prek", "go-sdk/.pre-commit-config.yaml"),
        ("root_prek", ".pre-commit-config.yaml"),
    ):
        write(
            rel,
            f"default_language_version:\n  python: python3\n  node: 22.19.0\n  golang: {versions[site]}\n",
        )


@pytest.mark.parametrize(
    ("version", "expected"),
    [("1.25", "1.25"), ("1.25.0", "1.25"), ("1.24.6", "1.24")],
)
def test_major_minor(version: str, expected: str):
    assert major_minor(version) == expected


def test_all_sites_consistent(tmp_path: Path):
    _write_tree(tmp_path, DEFAULT_VERSIONS)
    exit_code, report = check_sync(build_sites(tmp_path), tmp_path)
    assert exit_code == 0
    assert "consistently 1.25" in report


def test_go_mod_patch_suffix_matches_major_minor_pins(tmp_path: Path):
    """go.mod may pin 1.25.0 while the workflow/image sites pin 1.25 — that is in sync."""
    _write_tree(tmp_path, {**DEFAULT_VERSIONS, "go_sdk_mod": "1.25.0", "ci_amd": "1.25"})
    exit_code, _ = check_sync(build_sites(tmp_path), tmp_path)
    assert exit_code == 0


@pytest.mark.parametrize(
    "drifted_site",
    ["go_example_mod", "ci_amd", "ci_arm", "constants", "kubernetes_commands", "go_sdk_prek", "root_prek"],
)
def test_drift_in_any_derived_site_is_flagged(tmp_path: Path, drifted_site: str):
    """A single derived site left on the old minor version must fail the check."""
    _write_tree(
        tmp_path, {**DEFAULT_VERSIONS, drifted_site: "1.24" if "mod" not in drifted_site else "1.24.6"}
    )
    exit_code, report = check_sync(build_sites(tmp_path), tmp_path)
    assert exit_code == 1
    assert "drifted from the source of truth" in report
    assert "<- DRIFT" in report


def test_source_of_truth_bump_flags_all_stale_sites(tmp_path: Path):
    """Bumping only go-sdk/go.mod (the source of truth) flags every other site — the #69214 scenario."""
    _write_tree(tmp_path, {**DEFAULT_VERSIONS, "go_sdk_mod": "1.26.0"})
    exit_code, report = check_sync(build_sites(tmp_path), tmp_path)
    assert exit_code == 1
    # All seven non-source sites are still on the old version -> all flagged.
    assert report.count("<- DRIFT") == 7
    assert "<- source of truth" in report


def test_missing_pin_is_reported(tmp_path: Path):
    _write_tree(tmp_path, DEFAULT_VERSIONS)
    (tmp_path / "go-sdk" / "go.mod").write_text("module github.com/apache/airflow/go-sdk\n")
    exit_code, report = check_sync(build_sites(tmp_path), tmp_path)
    assert exit_code == 1
    assert "Go version pin not found" in report
    assert "go-sdk/go.mod" in report
