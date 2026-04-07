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
"""Author profiling and filtering utilities for PR triage.

Provides the AuthorFilter enum, author profile fetching via GraphQL,
contributor scoring / risk assessment, and an in-memory profile cache.
"""
from __future__ import annotations

from enum import Enum

from airflow_breeze.utils.pr_display import human_readable_age
from airflow_breeze.utils.pr_github import COLLABORATOR_ASSOCIATIONS, graphql_request


class AuthorFilter(Enum):
    """Which PR authors to include in triage."""

    CONTRIBUTORS = "contributors"  # non-collaborators only (default)
    COLLABORATORS = "collaborators"  # collaborators/members/owners only
    ALL = "all"  # everyone

    def should_include(self, author_association: str) -> bool:
        """Return True if a PR with this author_association should be included."""
        is_collab = author_association in COLLABORATOR_ASSOCIATIONS
        if self == AuthorFilter.CONTRIBUTORS:
            return not is_collab
        if self == AuthorFilter.COLLABORATORS:
            return is_collab
        return True  # ALL


AUTHOR_PROFILE_QUERY = """
query(
  $login: String!,
  $repoAll: String!, $repoMerged: String!, $repoClosed: String!,
  $globalAll: String!, $globalMerged: String!, $globalClosed: String!
) {
  user(login: $login) {
    createdAt
    repositoriesContributedTo(
      first: 10,
      contributionTypes: [COMMIT, PULL_REQUEST],
      orderBy: {field: STARGAZERS, direction: DESC}
    ) {
      totalCount
      nodes {
        nameWithOwner
        url
        stargazerCount
        isPrivate
      }
    }
  }
  repoAll: search(query: $repoAll, type: ISSUE) { issueCount }
  repoMerged: search(query: $repoMerged, type: ISSUE) { issueCount }
  repoClosed: search(query: $repoClosed, type: ISSUE) { issueCount }
  globalAll: search(query: $globalAll, type: ISSUE) { issueCount }
  globalMerged: search(query: $globalMerged, type: ISSUE) { issueCount }
  globalClosed: search(query: $globalClosed, type: ISSUE) { issueCount }
}
"""

_author_profile_cache: dict[str, dict] = {}


def compute_author_scoring(
    repo_total: int,
    repo_merged: int,
    repo_closed: int,
    global_total: int,
    global_merged: int,
    global_closed: int,
    created_at: str,
    contributed_repos_total: int,
) -> dict:
    """Derive scoring fields from raw PR counts.

    Returns a dict with merge rates, contributor tier, and risk level
    that gets merged into the author profile.
    """
    from datetime import datetime, timezone

    repo_merge_rate = repo_merged / repo_total if repo_total > 0 else 0.0
    global_merge_rate = global_merged / global_total if global_total > 0 else 0.0

    # Account age in days
    account_age_days = 0
    if created_at and created_at != "unknown":
        try:
            created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            account_age_days = (datetime.now(timezone.utc) - created_dt).days
        except (ValueError, TypeError):
            pass

    # Contributor tier based on repo history
    if repo_merged >= 10:
        tier = "established"
    elif repo_merged >= 3:
        tier = "regular"
    elif repo_merged >= 1:
        tier = "occasional"
    elif repo_total > 0:
        tier = "attempted"
    else:
        tier = "new"

    # Risk level for triage prioritization
    risk_signals = 0
    if account_age_days < 30:
        risk_signals += 2
    elif account_age_days < 90:
        risk_signals += 1
    if repo_total > 0 and repo_merge_rate < 0.3:
        risk_signals += 2
    if repo_total == 0 and global_total > 5 and global_merge_rate < 0.2:
        risk_signals += 1
    if contributed_repos_total == 0:
        risk_signals += 1

    if risk_signals >= 3:
        risk = "high"
    elif risk_signals >= 1:
        risk = "medium"
    else:
        risk = "low"

    return {
        "repo_merge_rate": round(repo_merge_rate, 2),
        "global_merge_rate": round(global_merge_rate, 2),
        "account_age_days": account_age_days,
        "contributor_tier": tier,
        "risk_level": risk,
    }


def fetch_author_profile(token: str, login: str, github_repository: str) -> dict:
    """Fetch author profile info via GraphQL: account age, PR counts, contributed repos.

    Results are cached per login so the same author is only queried once.
    """
    if login in _author_profile_cache:
        return _author_profile_cache[login]

    repo_prefix = f"repo:{github_repository} type:pr author:{login}"
    global_prefix = f"type:pr author:{login}"
    try:
        data = graphql_request(
            token,
            AUTHOR_PROFILE_QUERY,
            {
                "login": login,
                "repoAll": repo_prefix,
                "repoMerged": f"{repo_prefix} is:merged",
                "repoClosed": f"{repo_prefix} is:closed is:unmerged",
                "globalAll": global_prefix,
                "globalMerged": f"{global_prefix} is:merged",
                "globalClosed": f"{global_prefix} is:closed is:unmerged",
            },
        )
    except SystemExit:
        # Bot accounts (e.g. dependabot) cannot be resolved as Users
        profile = {
            "login": login,
            "account_age": "unknown (bot account)",
            "repo_total_prs": 0,
            "repo_merged_prs": 0,
            "repo_closed_prs": 0,
            "global_total_prs": 0,
            "global_merged_prs": 0,
            "global_closed_prs": 0,
            "contributed_repos": [],
            "contributed_repos_total": 0,
        }
        _author_profile_cache[login] = profile
        return profile
    user_data = data.get("user") or {}
    created_at = user_data.get("createdAt", "unknown")
    account_age = human_readable_age(created_at) if created_at != "unknown" else "unknown"

    # Extract contributed repos (public only)
    contrib_data = user_data.get("repositoriesContributedTo", {})
    contrib_total = contrib_data.get("totalCount", 0)
    contributed_repos = []
    for repo_node in contrib_data.get("nodes", []):
        if repo_node and not repo_node.get("isPrivate", False):
            contributed_repos.append(
                {
                    "name": repo_node["nameWithOwner"],
                    "url": repo_node["url"],
                    "stars": repo_node.get("stargazerCount", 0),
                }
            )

    repo_total = data.get("repoAll", {}).get("issueCount", 0)
    repo_merged = data.get("repoMerged", {}).get("issueCount", 0)
    repo_closed = data.get("repoClosed", {}).get("issueCount", 0)
    global_total = data.get("globalAll", {}).get("issueCount", 0)
    global_merged = data.get("globalMerged", {}).get("issueCount", 0)
    global_closed = data.get("globalClosed", {}).get("issueCount", 0)

    profile = {
        "login": login,
        "account_age": account_age,
        "created_at": created_at,
        "repo_total_prs": repo_total,
        "repo_merged_prs": repo_merged,
        "repo_closed_prs": repo_closed,
        "global_total_prs": global_total,
        "global_merged_prs": global_merged,
        "global_closed_prs": global_closed,
        "contributed_repos": contributed_repos,
        "contributed_repos_total": contrib_total,
        **compute_author_scoring(
            repo_total,
            repo_merged,
            repo_closed,
            global_total,
            global_merged,
            global_closed,
            created_at,
            contrib_total,
        ),
    }
    _author_profile_cache[login] = profile
    return profile
