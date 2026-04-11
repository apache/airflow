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
import time
from unittest import mock

import pytest

from airflow_breeze.utils.pr_cache import (
    author_cache,
    get_cached_author_profile,
    save_author_profile,
)


@pytest.fixture
def _fake_cache_dir(tmp_path):
    """Redirect CacheStore to a temporary directory."""
    with mock.patch(
        "airflow_breeze.utils.pr_cache.CacheStore.cache_dir",
        return_value=tmp_path,
    ):
        yield tmp_path


class TestAuthorProfilePersistence:
    def test_save_and_load(self, _fake_cache_dir):
        profile = {
            "login": "testuser",
            "account_age": "2 years",
            "repo_total_prs": 10,
            "repo_merged_prs": 8,
        }
        save_author_profile("apache/airflow", "testuser", profile)
        loaded = get_cached_author_profile("apache/airflow", "testuser")
        assert loaded is not None
        assert loaded["login"] == "testuser"
        assert loaded["repo_total_prs"] == 10

    def test_returns_none_when_missing(self, _fake_cache_dir):
        assert get_cached_author_profile("apache/airflow", "nobody") is None

    def test_ttl_expiration(self, _fake_cache_dir):
        profile = {"login": "olduser", "repo_total_prs": 5}
        save_author_profile("apache/airflow", "olduser", profile)

        # Manually backdate the cached_at timestamp
        cache_file = _fake_cache_dir / "author_olduser.json"
        data = json.loads(cache_file.read_text())
        data["cached_at"] = time.time() - (8 * 24 * 3600)  # 8 days ago
        cache_file.write_text(json.dumps(data))

        assert get_cached_author_profile("apache/airflow", "olduser") is None

    def test_fresh_cache_not_expired(self, _fake_cache_dir):
        profile = {"login": "freshuser", "repo_total_prs": 3}
        save_author_profile("apache/airflow", "freshuser", profile)

        loaded = get_cached_author_profile("apache/airflow", "freshuser")
        assert loaded is not None
        assert loaded["login"] == "freshuser"

    def test_ttl_is_7_days(self):
        assert author_cache._ttl_seconds == 7 * 24 * 3600
