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
"""Vault storage for PR metadata — persist PR data across triage sessions."""

from __future__ import annotations

from airflow_breeze.utils.pr_cache import CacheStore

# 4-hour TTL: PR metadata can change (labels, checks, mergeable status)
# but re-fetching every run is wasteful for PRs that haven't been updated.
_pr_vault = CacheStore("pr_vault", ttl_seconds=4 * 3600)

# Fields from PRData that are safe to serialize to JSON.
# Excludes unresolved_threads (fetched separately) and has_collaborator_review
# (computed per-session).
_VAULT_FIELDS = (
    "number",
    "title",
    "body",
    "url",
    "created_at",
    "updated_at",
    "node_id",
    "author_login",
    "author_association",
    "head_sha",
    "base_ref",
    "check_summary",
    "checks_state",
    "failed_checks",
    "commits_behind",
    "is_draft",
    "mergeable",
    "labels",
)


def save_pr(github_repository: str, pr) -> None:
    """Persist a PRData instance to the vault."""
    data = {field: getattr(pr, field) for field in _VAULT_FIELDS}
    _pr_vault.save(github_repository, f"pr_{pr.number}", data)


def load_pr(github_repository: str, pr_number: int, *, head_sha: str | None = None) -> dict | None:
    """Load a PR from the vault.

    If *head_sha* is given, only returns data when the stored SHA matches
    (ensures we don't serve stale metadata for a PR that received new commits).
    Returns None when not cached, expired (4h TTL), or SHA mismatch.
    """
    match = {"head_sha": head_sha} if head_sha else None
    return _pr_vault.get(github_repository, f"pr_{pr_number}", match=match)


def save_prs_batch(github_repository: str, prs) -> int:
    """Persist a batch of PRData instances. Returns count saved."""
    for pr in prs:
        save_pr(github_repository, pr)
    return len(prs)
