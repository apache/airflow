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
"""Tests for PR cache utilities."""

from __future__ import annotations

import json
import time
from unittest.mock import patch

import pytest

from airflow_breeze.utils.pr_cache import CacheStore


@pytest.fixture
def tmp_cache(tmp_path):
    """Create a CacheStore that uses a temporary directory."""
    store = CacheStore("test_cache")
    with patch("airflow_breeze.utils.pr_cache.CacheStore.cache_dir", return_value=tmp_path):
        yield store


@pytest.fixture
def tmp_ttl_cache(tmp_path):
    """Create a CacheStore with TTL that uses a temporary directory."""
    store = CacheStore("test_ttl_cache", ttl_seconds=3600)
    with patch("airflow_breeze.utils.pr_cache.CacheStore.cache_dir", return_value=tmp_path):
        yield store


class TestCacheStore:
    def test_get_missing(self, tmp_cache):
        assert tmp_cache.get("org/repo", "missing_key") is None

    def test_save_and_get(self, tmp_cache):
        tmp_cache.save("org/repo", "key1", {"value": 42})
        result = tmp_cache.get("org/repo", "key1")
        assert result is not None
        assert result["value"] == 42

    def test_get_with_match_success(self, tmp_cache):
        tmp_cache.save("org/repo", "pr_1", {"head_sha": "abc123", "data": "test"})
        result = tmp_cache.get("org/repo", "pr_1", match={"head_sha": "abc123"})
        assert result is not None
        assert result["data"] == "test"

    def test_get_with_match_mismatch(self, tmp_cache):
        tmp_cache.save("org/repo", "pr_1", {"head_sha": "abc123", "data": "test"})
        result = tmp_cache.get("org/repo", "pr_1", match={"head_sha": "different"})
        assert result is None

    def test_ttl_fresh(self, tmp_ttl_cache):
        tmp_ttl_cache.save("org/repo", "key1", {"value": "fresh"})
        result = tmp_ttl_cache.get("org/repo", "key1")
        assert result is not None
        assert result["value"] == "fresh"

    def test_ttl_expired(self, tmp_ttl_cache, tmp_path):
        # Write directly with an old timestamp
        data = {"value": "stale", "cached_at": time.time() - 7200}
        (tmp_path / "key1.json").write_text(json.dumps(data))
        result = tmp_ttl_cache.get("org/repo", "key1")
        assert result is None

    def test_corrupt_json(self, tmp_cache, tmp_path):
        (tmp_path / "bad.json").write_text("not valid json{{{")
        assert tmp_cache.get("org/repo", "bad") is None

    def test_overwrite(self, tmp_cache):
        tmp_cache.save("org/repo", "key1", {"v": 1})
        tmp_cache.save("org/repo", "key1", {"v": 2})
        result = tmp_cache.get("org/repo", "key1")
        assert result["v"] == 2
