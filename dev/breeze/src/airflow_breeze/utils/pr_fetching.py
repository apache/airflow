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
"""GraphQL PR data fetching for PR triage.

Provides functions to fetch PR data, review comments, commits behind,
and other PR metadata via GitHub GraphQL and REST APIs.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from airflow_breeze.utils.console import console_print, get_console
from airflow_breeze.utils.pr_checks import extract_basic_check_info, extract_review_decisions
from airflow_breeze.utils.pr_github import COLLABORATOR_ASSOCIATIONS, graphql_request
from airflow_breeze.utils.pr_models import PRData, ReviewDecision, UnresolvedThread

# ---------------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------------

SEARCH_PRS_QUERY = """
query($query: String!, $first: Int!, $after: String) {
  search(query: $query, type: ISSUE, first: $first, after: $after) {
    issueCount
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on PullRequest {
        number title body url createdAt updatedAt id
        author { login } authorAssociation baseRefName isDraft mergeable
        labels(first: 20) { nodes { name } }
        reviews(last: 20) { nodes { author { login } authorAssociation state } }
        commits(last: 1) { nodes { commit { oid statusCheckRollup { state } } } }
      }
    }
  }
}
"""

FETCH_SINGLE_PR_QUERY = """
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      number title body url createdAt updatedAt id
      author { login } authorAssociation baseRefName isDraft mergeable
      labels(first: 20) { nodes { name } }
      reviews(last: 20) { nodes { author { login } authorAssociation state } }
      commits(last: 1) { nodes { commit { oid statusCheckRollup { state } } } }
    }
  }
}
"""

REVIEW_THREADS_BATCH_SIZE = 10
COMMITS_BEHIND_BATCH_SIZE = 20


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ExistingComment:
    """An existing review comment already posted on a PR."""
    path: str
    line: int | None
    body: str
    user_login: str


@dataclass
class FetchResult:
    """Result from initial PR fetch."""
    all_prs: list[PRData]
    has_next_page: bool
    next_cursor: str | None
    total_matching_prs: int
    reviewed_by_prs: set[int] = field(default_factory=set)


# ---------------------------------------------------------------------------
# Core fetching functions
# ---------------------------------------------------------------------------


def _pr_node_to_prdata(node: dict) -> PRData:
    """Convert a GraphQL PR node to PRData."""
    head_sha, rollup_state = extract_basic_check_info(node)
    review_decisions = extract_review_decisions(node)
    return PRData(
        number=node.get("number", 0),
        title=node.get("title", ""),
        body=node.get("body", ""),
        url=node.get("url", ""),
        created_at=node.get("createdAt", ""),
        updated_at=node.get("updatedAt", ""),
        node_id=node.get("id", ""),
        author_login=(node.get("author") or {}).get("login", "ghost"),
        author_association=node.get("authorAssociation", "NONE"),
        head_sha=head_sha,
        base_ref=node.get("baseRefName", "main"),
        check_summary="",
        checks_state=rollup_state,
        failed_checks=[],
        commits_behind=0,
        is_draft=node.get("isDraft", False),
        mergeable=node.get("mergeable", "UNKNOWN"),
        labels=[lbl["name"] for lbl in (node.get("labels") or {}).get("nodes", [])],
        unresolved_threads=[],
        review_decisions=review_decisions,
    )


def fetch_prs_graphql(
    token: str,
    query_string: str,
    *,
    batch_size: int = 50,
    cursor: str | None = None,
) -> tuple[list[PRData], bool, str | None, int]:
    """Fetch PRs via GitHub GraphQL search. Returns (prs, has_next, cursor, total)."""
    variables = {"query": query_string, "first": batch_size}
    if cursor:
        variables["after"] = cursor
    data = graphql_request(token, SEARCH_PRS_QUERY, variables)
    search = data.get("search", {})
    total = search.get("issueCount", 0)
    page_info = search.get("pageInfo", {})
    has_next = page_info.get("hasNextPage", False)
    end_cursor = page_info.get("endCursor")
    prs = [_pr_node_to_prdata(node) for node in search.get("nodes", []) if node.get("number")]
    return prs, has_next, end_cursor, total


def fetch_single_pr_graphql(token: str, github_repository: str, pr_number: int) -> PRData:
    """Fetch a single PR by number via GraphQL."""
    owner, repo = github_repository.split("/", 1)
    data = graphql_request(token, FETCH_SINGLE_PR_QUERY, {"owner": owner, "repo": repo, "number": pr_number})
    node = data.get("repository", {}).get("pullRequest", {})
    return _pr_node_to_prdata(node)


def fetch_unresolved_comments_batch(
    token: str, github_repository: str, prs: list[PRData],
    on_progress: Callable[[int, int], None] | None = None,
) -> None:
    """Fetch unresolved review thread details for PRs in chunked GraphQL queries."""
    owner, repo = github_repository.split("/", 1)
    if not prs:
        return
    for chunk_start in range(0, len(prs), REVIEW_THREADS_BATCH_SIZE):
        chunk = prs[chunk_start : chunk_start + REVIEW_THREADS_BATCH_SIZE]
        pr_fields = []
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_fields.append(
                f"    {alias}: pullRequest(number: {pr.number}) {{\n"
                f"      reviewThreads(first: 100) {{\n"
                f"        nodes {{\n"
                f"          isResolved\n"
                f"          comments(first: 20) {{\n"
                f"            nodes {{ author {{ login }} authorAssociation body url }}\n"
                f"          }}\n"
                f"        }}\n"
                f"      }}\n"
                f"      latestReviews(first: 20) {{\n"
                f"        nodes {{ author {{ login }} authorAssociation state }}\n"
                f"      }}\n"
                f"    }}"
            )
        query = (
            f'query {{\n  repository(owner: "{owner}", name: "{repo}") {{\n'
            + "\n".join(pr_fields) + "\n  }\n}"
        )
        try:
            data = graphql_request(token, query, {})
        except SystemExit:
            continue
        repo_data = data.get("repository", {})
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_data = repo_data.get(alias) or {}
            threads = pr_data.get("reviewThreads", {}).get("nodes", [])
            unresolved: list[UnresolvedThread] = []
            for thread in threads:
                if thread.get("isResolved"):
                    continue
                comments = thread.get("comments", {}).get("nodes", [])
                if not comments:
                    continue
                first = comments[0]
                assoc = first.get("authorAssociation", "NONE")
                if assoc not in COLLABORATOR_ASSOCIATIONS:
                    continue
                reviewer_login = (first.get("author") or {}).get("login", "unknown")
                comment_body = first.get("body", "")
                comment_url = first.get("url", "")
                author_last_reply = ""
                for c in reversed(comments[1:]):
                    c_login = (c.get("author") or {}).get("login", "")
                    if c_login == pr.author_login:
                        author_last_reply = c.get("body", "")
                        break
                unresolved.append(UnresolvedThread(
                    reviewer_login=reviewer_login, reviewer_association=assoc,
                    comment_body=comment_body, comment_url=comment_url,
                    author_last_reply=author_last_reply,
                ))
            pr.unresolved_threads = unresolved
            reviews = pr_data.get("latestReviews", {}).get("nodes", [])
            assoc_by_login: dict[str, str] = {}
            for review in reviews:
                assoc = review.get("authorAssociation", "NONE")
                login = (review.get("author") or {}).get("login", "")
                if login:
                    assoc_by_login[login] = assoc
                if assoc in COLLABORATOR_ASSOCIATIONS:
                    pr.has_collaborator_review = True
            for rd in pr.review_decisions:
                if not rd.reviewer_association and rd.reviewer_login in assoc_by_login:
                    rd.reviewer_association = assoc_by_login[rd.reviewer_login]
            if unresolved:
                pr.has_collaborator_review = True
        if on_progress:
            on_progress(min(chunk_start + len(chunk), len(prs)), len(prs))


def fetch_commits_behind_batch(
    token: str, github_repository: str, prs: list[PRData],
    on_progress: Callable[[int, int], None] | None = None,
) -> None:
    """Fetch how many commits each PR is behind its base branch."""
    owner, repo = github_repository.split("/", 1)
    if not prs:
        return
    for chunk_start in range(0, len(prs), COMMITS_BEHIND_BATCH_SIZE):
        chunk = prs[chunk_start : chunk_start + COMMITS_BEHIND_BATCH_SIZE]
        pr_fields = []
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_fields.append(
                f"    {alias}: pullRequest(number: {pr.number}) {{\n"
                f"      baseRef {{ target {{ ... on Commit {{ history(first: 0) {{ totalCount }} }} }} }}\n"
                f"      headRef {{ target {{ ... on Commit {{ history(first: 0) {{ totalCount }} }} }} }}\n"
                f"      commits {{ totalCount }}\n"
                f"    }}"
            )
        query = (
            f'query {{\n  repository(owner: "{owner}", name: "{repo}") {{\n'
            + "\n".join(pr_fields) + "\n  }\n}"
        )
        try:
            data = graphql_request(token, query, {})
        except SystemExit:
            continue
        repo_data = data.get("repository", {})
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_data = repo_data.get(alias) or {}
            base_count = ((pr_data.get("baseRef") or {}).get("target") or {}).get("history", {}).get("totalCount", 0)
            head_count = ((pr_data.get("headRef") or {}).get("target") or {}).get("history", {}).get("totalCount", 0)
            pr_commits = pr_data.get("commits", {}).get("totalCount", 0)
            if base_count and head_count and pr_commits:
                pr.commits_behind = max(0, base_count - (head_count - pr_commits))
        if on_progress:
            on_progress(min(chunk_start + len(chunk), len(prs)), len(prs))


def fetch_existing_review_comments(
    token: str, github_repository: str, pr_number: int
) -> list[ExistingComment]:
    """Fetch all existing review comments on a PR via REST API."""
    import requests
    comments: list[ExistingComment] = []
    url = f"https://api.github.com/repos/{github_repository}/pulls/{pr_number}/comments"
    page = 1
    while True:
        try:
            response = requests.get(
                url, params={"per_page": 100, "page": page},
                headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
                timeout=(10, 20),
            )
        except (requests.ConnectionError, requests.Timeout, OSError):
            break
        if response.status_code != 200:
            break
        items = response.json()
        if not items:
            break
        for item in items:
            comments.append(ExistingComment(
                path=item.get("path", ""), line=item.get("line") or item.get("original_line"),
                body=item.get("body", ""), user_login=item.get("user", {}).get("login", ""),
            ))
        page += 1
    return comments


def fetch_pr_diff(token: str, github_repository: str, pr_number: int) -> str | None:
    """Fetch the diff for a PR via GitHub REST API."""
    import requests
    url = f"https://api.github.com/repos/{github_repository}/pulls/{pr_number}"
    try:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github.v3.diff"},
            timeout=60,
        )
    except requests.RequestException:
        return None
    if response.status_code == 200:
        return response.text
    return None
