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

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[3] / "scripts" / "ci" / "cleanup_old_image_versions.py"
CUTOFF = datetime(2026, 10, 16, tzinfo=UTC)
OLD = "2026-09-01T00:00:00Z"
RECENT = "2026-11-01T00:00:00Z"


@pytest.fixture
def cleanup():
    module_name = "test_cleanup_old_image_versions_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _version(digest: str, created_at: str, tags: list[str] | None = None) -> dict:
    return {"name": digest, "created_at": created_at, "metadata": {"container": {"tags": tags or []}}}


def _page(*middle: dict) -> list[dict]:
    """A page, newest first, whose edge versions are far from the versions under test."""
    return [
        _version("sha256:newest-edge", "2026-09-20T00:00:00Z"),
        *middle,
        _version("sha256:oldest-edge", "2026-08-01T00:00:00Z"),
    ]


@pytest.mark.parametrize(
    ("version", "deletable"),
    [
        pytest.param(_version("sha256:old", OLD), True, id="old-untagged"),
        pytest.param(_version("sha256:recent", RECENT), False, id="recent-untagged"),
        pytest.param(_version("sha256:tagged", OLD, ["latest"]), False, id="old-tagged"),
        pytest.param(_version("sha256:kept", OLD), False, id="old-referenced-by-a-long-lived-tag"),
    ],
)
def test_find_deletable_versions(cleanup, version, deletable):
    deletable_names = {
        v["name"] for v in cleanup.find_deletable_versions(_page(version), {"sha256:kept"}, CUTOFF)
    }

    assert (version["name"] in deletable_names) is deletable


def test_find_deletable_versions_keeps_untagged_pushed_with_a_tagged_version(cleanup):
    page = _page(
        _version("sha256:commit-image", "2026-09-01T00:00:30Z", ["0123456789abcdef0123456789abcdef01234567"]),
        _version("sha256:its-platform-manifest", "2026-09-01T00:00:10Z"),
        _version("sha256:unrelated", "2026-09-01T01:00:00Z"),
    )

    assert [v["name"] for v in cleanup.find_deletable_versions(page, set(), CUTOFF)] == ["sha256:unrelated"]


def test_find_deletable_versions_keeps_versions_near_the_page_edges(cleanup):
    page = [
        _version("sha256:edge", OLD),
        _version("sha256:near-edge", "2026-09-01T00:01:00Z"),
        _version("sha256:middle", "2026-08-20T00:00:00Z"),
        _version("sha256:last-edge", "2026-08-01T00:00:00Z"),
    ]

    assert [v["name"] for v in cleanup.find_deletable_versions(page, set(), CUTOFF)] == ["sha256:middle"]


def test_get_referenced_digests_lists_index_children(cleanup):
    index = {
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{"digest": "sha256:amd64"}, {"digest": "sha256:attestation"}],
    }

    assert cleanup.get_referenced_digests(index) == {"sha256:amd64", "sha256:attestation"}


def test_get_referenced_digests_of_single_manifest_is_empty(cleanup):
    assert cleanup.get_referenced_digests({"layers": [{"digest": "sha256:layer"}]}) == set()


@pytest.mark.parametrize(
    ("package", "matches"),
    [
        pytest.param("airflow/main/ci/python3.11", True, id="ci"),
        pytest.param("airflow/v2-11-test/prod/python3.10", True, id="prod"),
        pytest.param("airflow/main/ci/python3.11/extra", False, id="nested"),
        pytest.param("airflow-site/main/ci/python3.11", False, id="other-repo"),
    ],
)
def test_image_package_pattern(cleanup, package, matches):
    assert bool(cleanup.get_image_package_pattern("airflow").match(package)) is matches
