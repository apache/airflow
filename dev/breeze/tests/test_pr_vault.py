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
from dataclasses import dataclass
from unittest import mock

import pytest

from airflow_breeze.utils.pr_vault import load_pr, save_pr, save_prs_batch


@dataclass
class _FakePR:
    """Minimal PR-like object for testing vault serialization."""

    number: int = 12345
    title: str = "Fix something"
    body: str = "Description"
    url: str = "https://github.com/apache/airflow/pull/12345"
    created_at: str = "2026-03-01T00:00:00Z"
    updated_at: str = "2026-03-02T00:00:00Z"
    node_id: str = "PR_abc"
    author_login: str = "testuser"
    author_association: str = "CONTRIBUTOR"
    head_sha: str = "sha123"
    base_ref: str = "main"
    check_summary: str = "3 checks: 2 success, 1 failure"
    checks_state: str = "FAILURE"
    failed_checks: list = None
    commits_behind: int = 5
    is_draft: bool = False
    mergeable: str = "MERGEABLE"
    labels: list = None

    def __post_init__(self):
        if self.failed_checks is None:
            self.failed_checks = ["mypy"]
        if self.labels is None:
            self.labels = ["area:core"]


@pytest.fixture
def _fake_cache_dir(tmp_path):
    with mock.patch(
        "airflow_breeze.utils.pr_cache.CacheStore.cache_dir",
        return_value=tmp_path,
    ):
        yield tmp_path


class TestPRVault:
    def test_save_and_load(self, _fake_cache_dir):
        pr = _FakePR()
        save_pr("apache/airflow", pr)
        loaded = load_pr("apache/airflow", 12345)
        assert loaded is not None
        assert loaded["number"] == 12345
        assert loaded["title"] == "Fix something"
        assert loaded["head_sha"] == "sha123"
        assert loaded["labels"] == ["area:core"]

    def test_load_missing(self, _fake_cache_dir):
        assert load_pr("apache/airflow", 99999) is None

    def test_load_with_matching_sha(self, _fake_cache_dir):
        pr = _FakePR()
        save_pr("apache/airflow", pr)
        loaded = load_pr("apache/airflow", 12345, head_sha="sha123")
        assert loaded is not None
        assert loaded["number"] == 12345

    def test_load_with_stale_sha(self, _fake_cache_dir):
        pr = _FakePR()
        save_pr("apache/airflow", pr)
        loaded = load_pr("apache/airflow", 12345, head_sha="different_sha")
        assert loaded is None

    def test_ttl_expiration(self, _fake_cache_dir):
        pr = _FakePR()
        save_pr("apache/airflow", pr)

        cache_file = _fake_cache_dir / "pr_12345.json"
        data = json.loads(cache_file.read_text())
        data["cached_at"] = time.time() - (5 * 3600)  # 5 hours ago, past 4h TTL
        cache_file.write_text(json.dumps(data))

        assert load_pr("apache/airflow", 12345) is None

    def test_fresh_not_expired(self, _fake_cache_dir):
        pr = _FakePR()
        save_pr("apache/airflow", pr)
        assert load_pr("apache/airflow", 12345) is not None

    def test_save_batch(self, _fake_cache_dir):
        prs = [_FakePR(number=100), _FakePR(number=200), _FakePR(number=300)]
        count = save_prs_batch("apache/airflow", prs)
        assert count == 3
        assert load_pr("apache/airflow", 100) is not None
        assert load_pr("apache/airflow", 200) is not None
        assert load_pr("apache/airflow", 300) is not None

    def test_does_not_serialize_unresolved_threads(self, _fake_cache_dir):
        pr = _FakePR()
        save_pr("apache/airflow", pr)
        loaded = load_pr("apache/airflow", 12345)
        assert "unresolved_threads" not in loaded
