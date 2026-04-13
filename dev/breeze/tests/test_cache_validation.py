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

from unittest import mock

import pytest

from airflow_breeze.utils.pr_cache import (
    invalidate_stale_caches,
    review_cache,
    save_review_cache,
    scan_cached_pr_numbers,
    triage_cache,
)


@pytest.fixture
def _fake_cache_dirs(tmp_path):
    """Each CacheStore gets its own subdir under tmp_path."""

    def _make_dir(self, github_repository):
        safe = github_repository.replace("/", "_")
        d = tmp_path / self._cache_name / safe
        d.mkdir(parents=True, exist_ok=True)
        return d

    with mock.patch(
        "airflow_breeze.utils.pr_cache.CacheStore.cache_dir",
        _make_dir,
    ):
        yield tmp_path


class TestScanCachedPrNumbers:
    def test_scans_across_caches(self, _fake_cache_dirs):
        save_review_cache("apache/airflow", 100, "sha_aaa", {"summary": "ok"})
        save_review_cache("apache/airflow", 200, "sha_bbb", {"summary": "ok"})

        result = scan_cached_pr_numbers("apache/airflow")
        assert 100 in result
        assert 200 in result
        assert result[100]["review_cache"] == "sha_aaa"

    def test_empty_cache(self, _fake_cache_dirs):
        result = scan_cached_pr_numbers("apache/airflow")
        assert result == {}

    def test_ignores_non_pr_files(self, _fake_cache_dirs):
        # Create a file that doesn't match pr_<number>.json
        cache_dir = review_cache.cache_dir("apache/airflow")
        (cache_dir / "other_data.json").write_text('{"key": "value"}')

        result = scan_cached_pr_numbers("apache/airflow")
        assert result == {}

    def test_handles_corrupt_json(self, _fake_cache_dirs):
        cache_dir = review_cache.cache_dir("apache/airflow")
        (cache_dir / "pr_999.json").write_text("not json{{{")

        result = scan_cached_pr_numbers("apache/airflow")
        assert 999 not in result


class TestInvalidateStaleCaches:
    def test_removes_stale_entries(self, _fake_cache_dirs):
        save_review_cache("apache/airflow", 100, "old_sha", {"summary": "ok"})

        removed = invalidate_stale_caches("apache/airflow", {100: "new_sha"})
        assert removed == 1

        # Cache file should be gone
        assert not (review_cache.cache_dir("apache/airflow") / "pr_100.json").exists()

    def test_keeps_fresh_entries(self, _fake_cache_dirs):
        save_review_cache("apache/airflow", 100, "same_sha", {"summary": "ok"})

        removed = invalidate_stale_caches("apache/airflow", {100: "same_sha"})
        assert removed == 0

        # Cache file should still exist
        assert (review_cache.cache_dir("apache/airflow") / "pr_100.json").exists()

    def test_handles_missing_pr(self, _fake_cache_dirs):
        # PR 999 has no cache entry
        removed = invalidate_stale_caches("apache/airflow", {999: "any_sha"})
        assert removed == 0

    def test_removes_corrupt_files(self, _fake_cache_dirs):
        cache_dir = review_cache.cache_dir("apache/airflow")
        (cache_dir / "pr_100.json").write_text("corrupt{{{")

        removed = invalidate_stale_caches("apache/airflow", {100: "any_sha"})
        assert removed == 1
        assert not (cache_dir / "pr_100.json").exists()

    def test_removes_across_multiple_caches(self, _fake_cache_dirs):
        save_review_cache("apache/airflow", 100, "old_sha", {"summary": "ok"})
        # Also save to triage cache
        triage_cache.save("apache/airflow", "pr_100", {"head_sha": "old_sha", "assessment": {}})

        removed = invalidate_stale_caches("apache/airflow", {100: "new_sha"})
        assert removed == 2
