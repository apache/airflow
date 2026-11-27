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

MODULE_PATH = Path(__file__).resolve().parents[3] / "scripts" / "ci" / "cleanup_keyed_image_cache.py"
CUTOFF = datetime(2026, 10, 16, tzinfo=UTC)
OLD = "2026-09-01T00:00:00Z"
RECENT = "2026-11-01T00:00:00Z"


@pytest.fixture
def cleanup():
    module_name = "test_cleanup_keyed_image_cache_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _version(tags: list[str], created_at: str, updated_at: str | None = None) -> dict:
    return {
        "id": 1,
        "created_at": created_at,
        "updated_at": updated_at or created_at,
        "metadata": {"container": {"tags": tags}},
    }


@pytest.mark.parametrize(
    ("tags", "updated_at", "expired"),
    [
        pytest.param(["cache-linux-amd64-0123456789abcdef"], OLD, True, id="old-keyed"),
        pytest.param(["cache-linux-arm64-0123456789abcdef"], OLD, True, id="old-keyed-arm"),
        pytest.param(["cache-linux-amd64-0123456789abcdef"], RECENT, False, id="recent-keyed"),
        pytest.param(["cache-linux-amd64"], OLD, False, id="shared-cache"),
        pytest.param(["latest"], OLD, False, id="latest"),
        pytest.param(
            ["cache-linux-amd64-0123456789abcdef", "cache-linux-amd64"], OLD, False, id="keyed-and-shared"
        ),
        pytest.param([], OLD, False, id="untagged"),
        pytest.param(["cache-linux-amd64-not-a-key"], OLD, False, id="not-a-key"),
    ],
)
def test_is_expired_keyed_cache(cleanup, tags, updated_at, expired):
    assert cleanup.is_expired_keyed_cache(_version(tags, updated_at), CUTOFF) is expired


def test_get_versions_created_since_stops_at_the_first_page_past_since(cleanup):
    def created(day: int) -> dict:
        return {"created_at": f"2026-10-{day:02d}T00:00:00Z"}

    pages = {1: [created(30), created(20)], 2: [created(16), created(10)], 3: [created(5)]}
    fetched: list[int] = []

    def fetch_page(page: int) -> list[dict]:
        fetched.append(page)
        return pages.get(page, [])

    versions = cleanup.get_versions_created_since(fetch_page, datetime(2026, 10, 15, tzinfo=UTC))

    assert [v["created_at"][:10] for v in versions] == ["2026-10-30", "2026-10-20", "2026-10-16"]
    assert fetched == [1, 2]


def test_get_versions_created_since_stops_on_empty_page(cleanup):
    fetched: list[int] = []

    def fetch_page(page: int) -> list[dict]:
        fetched.append(page)
        return [{"created_at": "2026-11-01T00:00:00Z"}] if page == 1 else []

    assert len(cleanup.get_versions_created_since(fetch_page, CUTOFF)) == 1
    assert fetched == [1, 2]


def test_keyed_cache_expires_by_creation_even_when_pushed_again(cleanup):
    version = _version(["cache-linux-amd64-0123456789abcdef"], created_at=OLD, updated_at=RECENT)

    assert cleanup.is_expired_keyed_cache(version, CUTOFF) is True


@pytest.mark.parametrize(
    ("package", "matches"),
    [
        pytest.param("airflow/main/ci/python3.11", True, id="main-ci"),
        pytest.param("airflow/v3-4-test/ci/python3.14", True, id="release-branch-ci"),
        pytest.param("airflow/main/prod/python3.11", False, id="prod"),
        pytest.param("other/main/ci/python3.11", False, id="other-repo"),
    ],
)
def test_ci_image_package_pattern(cleanup, package, matches):
    assert bool(cleanup.get_ci_image_package_pattern("airflow").match(package)) is matches
