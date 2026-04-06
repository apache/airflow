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
"""Tests for PR data models."""

from __future__ import annotations

from airflow_breeze.utils.pr_models import (
    CHECK_FAILURE_GRACE_PERIOD_HOURS,
    CHECK_FAILURE_GRACE_PERIOD_WITH_ENGAGEMENT_HOURS,
    PRData,
    UnresolvedThread,
)


def _make_pr(**kwargs) -> PRData:
    defaults = {
        "number": 1,
        "title": "Test PR",
        "body": "",
        "url": "https://github.com/test/repo/pull/1",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "node_id": "PR_1",
        "author_login": "author",
        "author_association": "NONE",
        "head_sha": "abc123",
        "base_ref": "main",
        "check_summary": "",
        "checks_state": "SUCCESS",
        "failed_checks": [],
        "commits_behind": 0,
        "is_draft": False,
        "mergeable": "MERGEABLE",
        "labels": [],
        "unresolved_threads": [],
    }
    defaults.update(kwargs)
    return PRData(**defaults)


class TestUnresolvedThread:
    def test_reviewer_display(self):
        t = UnresolvedThread(
            reviewer_login="alice",
            reviewer_association="MEMBER",
            comment_body="Fix this",
            comment_url="https://example.com",
            author_last_reply="",
        )
        assert t.reviewer_display == "alice (MEMBER)"


class TestPRData:
    def test_unresolved_review_comments_empty(self):
        pr = _make_pr()
        assert pr.unresolved_review_comments == 0

    def test_unresolved_review_comments_count(self):
        threads = [
            UnresolvedThread("r1", "MEMBER", "fix", "url1", ""),
            UnresolvedThread("r2", "OWNER", "fix2", "url2", "done"),
        ]
        pr = _make_pr(unresolved_threads=threads)
        assert pr.unresolved_review_comments == 2

    def test_grace_period_default(self):
        pr = _make_pr()
        assert pr.ci_grace_period_hours == CHECK_FAILURE_GRACE_PERIOD_HOURS

    def test_grace_period_with_engagement(self):
        pr = _make_pr(has_collaborator_review=True)
        assert pr.ci_grace_period_hours == CHECK_FAILURE_GRACE_PERIOD_WITH_ENGAGEMENT_HOURS
