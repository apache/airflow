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
"""Tests for PR display utilities."""

from __future__ import annotations

from airflow_breeze.utils.pr_display import fmt_duration, human_readable_age, linkify_commit_hashes


class TestFmtDuration:
    def test_seconds_only(self):
        assert fmt_duration(5.3) == "5.3s"

    def test_minutes_and_seconds(self):
        assert fmt_duration(90) == "1m 30s"

    def test_hours(self):
        assert fmt_duration(3661) == "1h 01m 01s"

    def test_zero(self):
        assert fmt_duration(0) == "0.0s"

    def test_sub_second(self):
        assert fmt_duration(0.5) == "0.5s"


class TestHumanReadableAge:
    def test_today(self):
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        assert human_readable_age(now) == "today"

    def test_invalid_date(self):
        assert human_readable_age("not-a-date") == "unknown"

    def test_old_date(self):
        result = human_readable_age("2020-01-01T00:00:00Z")
        assert "year" in result
        assert "ago" in result


class TestLinkifyCommitHashes:
    def test_linkifies_40_char_hex(self):
        sha = "a" * 40
        result = linkify_commit_hashes(f"commit {sha} done")
        assert f"[{sha[:10]}]" in result
        assert f"/commit/{sha})" in result

    def test_ignores_short_hex(self):
        text = "short abc123 string"
        assert linkify_commit_hashes(text) == text

    def test_ignores_sha_after_slash(self):
        sha = "b" * 40
        text = f"url/path/{sha}"
        result = linkify_commit_hashes(text)
        # Should not linkify because preceded by /
        assert f"[{sha[:10]}]" not in result

    def test_custom_repository(self):
        sha = "c" * 40
        result = linkify_commit_hashes(f"see {sha}", github_repository="org/repo")
        assert "org/repo/commit" in result
