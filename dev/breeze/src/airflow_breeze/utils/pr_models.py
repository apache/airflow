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
"""Data models shared across PR triage modules."""

from __future__ import annotations

from dataclasses import dataclass

# Number of hours after which a CI failure is considered "stale" and should be flagged.
# Default grace period for new PRs without collaborator engagement.
CHECK_FAILURE_GRACE_PERIOD_HOURS = 24
# Extended grace period (in hours) when a collaborator/member/owner has left
# a review or comment on the PR — gives contributors more time to respond.
CHECK_FAILURE_GRACE_PERIOD_WITH_ENGAGEMENT_HOURS = 96


@dataclass
class UnresolvedThread:
    """Detail about a single unresolved review thread from a maintainer."""

    reviewer_login: str
    reviewer_association: str  # COLLABORATOR, MEMBER, OWNER
    comment_body: str  # the reviewer's original comment (first in thread)
    comment_url: str  # permalink to the thread
    author_last_reply: str  # the PR author's most recent reply in that thread (empty if none)

    @property
    def reviewer_display(self) -> str:
        """Human-readable reviewer identifier like 'login (MEMBER)'."""
        return f"{self.reviewer_login} ({self.reviewer_association})"


@dataclass
class PRData:
    """PR data fetched from GraphQL."""

    number: int
    title: str
    body: str
    url: str
    created_at: str
    updated_at: str
    node_id: str
    author_login: str
    author_association: str
    head_sha: str
    base_ref: str
    check_summary: str
    checks_state: str
    failed_checks: list[str]
    commits_behind: int
    is_draft: bool
    mergeable: str
    labels: list[str]
    unresolved_threads: list[UnresolvedThread]
    has_collaborator_review: bool = False

    @property
    def unresolved_review_comments(self) -> int:
        """Count of unresolved review threads from maintainers."""
        return len(self.unresolved_threads)

    @property
    def ci_grace_period_hours(self) -> int:
        """Return the appropriate CI failure grace period based on collaborator engagement."""
        if self.has_collaborator_review:
            return CHECK_FAILURE_GRACE_PERIOD_WITH_ENGAGEMENT_HOURS
        return CHECK_FAILURE_GRACE_PERIOD_HOURS
