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

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import click
from rich.panel import Panel
from rich.table import Table

from airflow_breeze.commands.common_options import (
    option_dry_run,
    option_github_repository,
    option_github_token,
    option_llm_model,
    option_verbose,
)
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.confirm import Answer, TriageAction, prompt_triage_action, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.custom_param_types import NotVerifiedBetterChoice
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose

QUALITY_CRITERIA_LINK = (
    "[Pull Request quality criteria](https://github.com/apache/airflow/blob/main/"
    "contributing-docs/05_pull_requests.rst#pull-request-quality-criteria)"
)

# authorAssociation values that indicate the author has write access
_COLLABORATOR_ASSOCIATIONS = {"COLLABORATOR", "MEMBER", "OWNER"}

# Label applied when a maintainer marks a flagged PR as ready for review
_READY_FOR_REVIEW_LABEL = "ready for maintainer review"

# Label applied when a PR is closed due to multiple quality violations
_CLOSED_QUALITY_LABEL = "closed because of multiple quality violations"

# Label applied when a PR is closed due to suspicious changes
_SUSPICIOUS_CHANGES_LABEL = "suspicious changes detected"

# GitHub accounts that should be auto-skipped during triage
_BOT_ACCOUNT_LOGINS = {"dependabot", "dependabot[bot]", "renovate[bot]", "github-actions[bot]"}

_SEARCH_PRS_QUERY = """
query($query: String!, $first: Int!, $after: String) {
  search(query: $query, type: ISSUE, first: $first, after: $after) {
    issueCount
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      ... on PullRequest {
        number
        title
        body
        url
        createdAt
        updatedAt
        id
        author { login }
        authorAssociation
        baseRefName
        mergeable
        labels(first: 20) {
          nodes { name }
        }
        commits(last: 1) {
          nodes {
            commit {
              oid
              statusCheckRollup {
                state
              }
            }
          }
        }
      }
    }
  }
}
"""

_FETCH_SINGLE_PR_QUERY = """
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      number
      title
      body
      url
      createdAt
      updatedAt
      id
      author { login }
      authorAssociation
      baseRefName
      mergeable
      labels(first: 20) {
        nodes { name }
      }
      commits(last: 1) {
        nodes {
          commit {
            oid
            statusCheckRollup {
              state
            }
          }
        }
      }
    }
  }
}
"""

_CHECK_CONTEXTS_QUERY = """
query($owner: String!, $repo: String!, $oid: GitObjectID!, $first: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    object(oid: $oid) {
      ... on Commit {
        statusCheckRollup {
          contexts(first: $first, after: $after) {
            totalCount
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              ... on CheckRun {
                __typename
                name
                conclusion
                status
              }
              ... on StatusContext {
                __typename
                context
                state
              }
            }
          }
        }
      }
    }
  }
}
"""

_AUTHOR_PROFILE_QUERY = """
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

_CONVERT_TO_DRAFT_MUTATION = """
mutation($prId: ID!) {
  convertPullRequestToDraft(input: {pullRequestId: $prId}) {
    pullRequest { id }
  }
}
"""

_ADD_COMMENT_MUTATION = """
mutation($subjectId: ID!, $body: String!) {
  addComment(input: {subjectId: $subjectId, body: $body}) {
    commentEdge { node { id } }
  }
}
"""

_ADD_LABELS_MUTATION = """
mutation($labelableId: ID!, $labelIds: [ID!]!) {
  addLabelsToLabelable(input: {labelableId: $labelableId, labelIds: $labelIds}) {
    labelable { ... on PullRequest { id } }
  }
}
"""

_GET_LABEL_ID_QUERY = """
query($owner: String!, $repo: String!, $name: String!) {
  repository(owner: $owner, name: $repo) {
    label(name: $name) { id }
  }
}
"""

_CLOSE_PR_MUTATION = """
mutation($prId: ID!) {
  closePullRequest(input: {pullRequestId: $prId}) {
    pullRequest { id }
  }
}
"""


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
    base_ref: str  # e.g. "main"
    check_summary: str
    checks_state: str  # statusCheckRollup.state: SUCCESS, FAILURE, PENDING, etc.
    failed_checks: list[str]  # best-effort list of individual failing check names
    commits_behind: int  # how many commits behind the base branch
    mergeable: str  # MERGEABLE, CONFLICTING, or UNKNOWN
    labels: list[str]  # label names attached to this PR
    unresolved_review_comments: int  # count of unresolved review threads from maintainers


@click.group(cls=BreezeGroup, name="pr", help="Tools for managing GitHub pull requests.")
def pr_group():
    pass


def _resolve_github_token(github_token: str | None) -> str | None:
    """Resolve GitHub token from option, environment, or gh CLI."""
    if github_token:
        return github_token
    gh_token_result = run_command(
        ["gh", "auth", "token"],
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    if gh_token_result.returncode == 0:
        return gh_token_result.stdout.strip()
    return None


def _graphql_request(token: str, query: str, variables: dict) -> dict:
    """Execute a GitHub GraphQL request. Returns the 'data' dict or exits on error."""
    import requests

    response = requests.post(
        "https://api.github.com/graphql",
        json={"query": query, "variables": variables},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=60,
    )
    if response.status_code != 200:
        get_console().print(f"[error]GraphQL request failed: {response.status_code} {response.text}[/]")
        sys.exit(1)
    result = response.json()
    if "errors" in result:
        get_console().print(f"[error]GraphQL errors: {result['errors']}[/]")
        sys.exit(1)
    return result["data"]


_CHECK_FAILURE_CONCLUSIONS = {"FAILURE", "TIMED_OUT", "ACTION_REQUIRED"}
_STATUS_FAILURE_STATES = {"FAILURE", "ERROR"}

# Batch sizes for chunked GraphQL queries to avoid GitHub timeout errors
_CHECK_DETAIL_BATCH_SIZE = 10
_COMMITS_BEHIND_BATCH_SIZE = 20

# Substrings that indicate a check is from a CI test workflow (not just labelers/bots)
_TEST_WORKFLOW_PATTERNS = [
    "test",
    "static check",
    "build",
    "ci image",
    "prod image",
    "helm",
    "k8s",
    "basic",
    "unit",
    "integration",
    "provider",
    "mypy",
    "pre-commit",
    "docs",
]


def _is_test_check(name: str) -> bool:
    """Return True if the check name looks like a CI test workflow (not just a bot/labeler)."""
    lower = name.lower()
    return any(p in lower for p in _TEST_WORKFLOW_PATTERNS)


def _extract_basic_check_info(pr_node: dict) -> tuple[str, str]:
    """Extract basic check info from a lightweight GraphQL PR node (no contexts).

    Returns (head_sha, rollup_state).
    """
    commits = pr_node.get("commits", {}).get("nodes", [])
    if not commits:
        return "", "UNKNOWN"
    commit = commits[0].get("commit", {})
    head_sha = commit.get("oid", "")
    rollup = commit.get("statusCheckRollup")
    if not rollup:
        return head_sha, "UNKNOWN"
    return head_sha, rollup.get("state", "UNKNOWN")


def _process_check_contexts(contexts: list[dict], total_count: int) -> tuple[str, list[str], bool]:
    """Process check context nodes into summary text, failed names, and test-check presence.

    Returns (summary_text, failed_check_names, has_test_checks).
    """
    lines: list[str] = []
    failed: list[str] = []
    has_test_checks = False
    for ctx in contexts:
        typename = ctx.get("__typename")
        if typename == "CheckRun":
            name = ctx.get("name", "unknown")
            conclusion = ctx.get("conclusion") or ctx.get("status") or "unknown"
            lines.append(f"  {name}: {conclusion}")
            if _is_test_check(name):
                has_test_checks = True
            if conclusion.upper() in _CHECK_FAILURE_CONCLUSIONS:
                failed.append(name)
        elif typename == "StatusContext":
            name = ctx.get("context", "unknown")
            state = ctx.get("state", "unknown")
            lines.append(f"  {name}: {state}")
            if _is_test_check(name):
                has_test_checks = True
            if state.upper() in _STATUS_FAILURE_STATES:
                failed.append(name)
    if total_count > len(contexts):
        extra = total_count - len(contexts)
        lines.append(f"  ... ({extra} more {'checks' if extra != 1 else 'check'} not shown)")
    summary = "\n".join(lines) if lines else "No check runs found."
    return summary, failed, has_test_checks


def _fetch_failed_checks(token: str, github_repository: str, head_sha: str) -> list[str]:
    """Fetch all failing check names for a commit by paginating through check contexts."""
    owner, repo = github_repository.split("/", 1)
    failed: list[str] = []
    cursor: str | None = None

    while True:
        variables: dict = {"owner": owner, "repo": repo, "oid": head_sha, "first": 100}
        if cursor:
            variables["after"] = cursor

        data = _graphql_request(token, _CHECK_CONTEXTS_QUERY, variables)
        rollup = (data.get("repository", {}).get("object", {}) or {}).get("statusCheckRollup")
        if not rollup:
            break

        contexts_data = rollup.get("contexts", {})
        for ctx in contexts_data.get("nodes", []):
            typename = ctx.get("__typename")
            if typename == "CheckRun":
                conclusion = ctx.get("conclusion") or ctx.get("status") or "unknown"
                if conclusion.upper() in _CHECK_FAILURE_CONCLUSIONS:
                    failed.append(ctx.get("name", "unknown"))
            elif typename == "StatusContext":
                state = ctx.get("state", "unknown")
                if state.upper() in _STATUS_FAILURE_STATES:
                    failed.append(ctx.get("context", "unknown"))

        page_info = contexts_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    return failed


def _fetch_check_details_batch(token: str, github_repository: str, prs: list[PRData]) -> None:
    """Fetch detailed check contexts for PRs in chunked GraphQL queries.

    Updates each PR's check_summary, checks_state, and failed_checks in-place.
    Processes in chunks of _CHECK_DETAIL_BATCH_SIZE to avoid GitHub timeout errors.
    """
    owner, repo = github_repository.split("/", 1)
    eligible = [pr for pr in prs if pr.head_sha]
    if not eligible:
        return

    for chunk_start in range(0, len(eligible), _CHECK_DETAIL_BATCH_SIZE):
        chunk = eligible[chunk_start : chunk_start + _CHECK_DETAIL_BATCH_SIZE]

        object_fields = []
        for pr in chunk:
            alias = f"pr{pr.number}"
            object_fields.append(
                f'    {alias}: object(oid: "{pr.head_sha}") {{\n'
                f"      ... on Commit {{\n"
                f"        statusCheckRollup {{\n"
                f"          state\n"
                f"          contexts(first: 100) {{\n"
                f"            totalCount\n"
                f"            nodes {{\n"
                f"              ... on CheckRun {{ __typename name conclusion status }}\n"
                f"              ... on StatusContext {{ __typename context state }}\n"
                f"            }}\n"
                f"          }}\n"
                f"        }}\n"
                f"      }}\n"
                f"    }}"
            )

        query = (
            f'query {{\n  repository(owner: "{owner}", name: "{repo}") {{\n'
            + "\n".join(object_fields)
            + "\n  }\n}"
        )

        try:
            data = _graphql_request(token, query, {})
        except SystemExit:
            continue

        repo_data = data.get("repository", {})
        for pr in chunk:
            alias = f"pr{pr.number}"
            commit_data = repo_data.get(alias) or {}
            rollup = commit_data.get("statusCheckRollup")
            if not rollup:
                continue

            rollup_state = rollup.get("state", "UNKNOWN")
            contexts_data = rollup.get("contexts", {})
            total_count = contexts_data.get("totalCount", 0)
            contexts = contexts_data.get("nodes", [])

            summary, failed, has_test_checks = _process_check_contexts(contexts, total_count)

            if contexts and not has_test_checks:
                rollup_state = "NOT_RUN"

            pr.checks_state = rollup_state
            pr.check_summary = summary
            pr.failed_checks = failed


_REVIEW_THREADS_BATCH_SIZE = 10


def _fetch_unresolved_comments_batch(token: str, github_repository: str, prs: list[PRData]) -> None:
    """Fetch unresolved review thread counts for PRs in chunked GraphQL queries.

    Counts only threads started by collaborators/members/owners (i.e. maintainers).
    Updates each PR's unresolved_review_comments in-place.
    """
    owner, repo = github_repository.split("/", 1)
    if not prs:
        return

    for chunk_start in range(0, len(prs), _REVIEW_THREADS_BATCH_SIZE):
        chunk = prs[chunk_start : chunk_start + _REVIEW_THREADS_BATCH_SIZE]

        pr_fields = []
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_fields.append(
                f"    {alias}: pullRequest(number: {pr.number}) {{\n"
                f"      reviewThreads(first: 100) {{\n"
                f"        nodes {{\n"
                f"          isResolved\n"
                f"          comments(first: 1) {{\n"
                f"            nodes {{\n"
                f"              author {{ login }}\n"
                f"              authorAssociation\n"
                f"            }}\n"
                f"          }}\n"
                f"        }}\n"
                f"      }}\n"
                f"    }}"
            )

        query = (
            f'query {{\n  repository(owner: "{owner}", name: "{repo}") {{\n'
            + "\n".join(pr_fields)
            + "\n  }\n}"
        )

        try:
            data = _graphql_request(token, query, {})
        except SystemExit:
            continue

        repo_data = data.get("repository", {})
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_data = repo_data.get(alias) or {}
            threads = pr_data.get("reviewThreads", {}).get("nodes", [])
            unresolved = 0
            for thread in threads:
                if thread.get("isResolved"):
                    continue
                # Only count threads started by maintainers (collaborators/members/owners)
                comments = thread.get("comments", {}).get("nodes", [])
                if comments:
                    assoc = comments[0].get("authorAssociation", "NONE")
                    if assoc in _COLLABORATOR_ASSOCIATIONS:
                        unresolved += 1
            pr.unresolved_review_comments = unresolved


def _fetch_commits_behind_batch(token: str, github_repository: str, prs: list[PRData]) -> dict[int, int]:
    """Fetch how many commits each PR is behind its base branch in chunked GraphQL queries.

    Uses aliased ref.compare fields batched into chunks of _COMMITS_BEHIND_BATCH_SIZE
    to avoid GitHub timeout errors.
    Returns a dict mapping PR number to commits behind count.
    """
    owner, repo = github_repository.split("/", 1)
    eligible = [(i, pr) for i, pr in enumerate(prs) if pr.head_sha]
    if not eligible:
        return {}

    result: dict[int, int] = {}
    for chunk_start in range(0, len(eligible), _COMMITS_BEHIND_BATCH_SIZE):
        chunk = eligible[chunk_start : chunk_start + _COMMITS_BEHIND_BATCH_SIZE]

        compare_fields = []
        for _i, pr in chunk:
            alias = f"pr{pr.number}"
            compare_fields.append(
                f'    {alias}: ref(qualifiedName: "refs/heads/{pr.base_ref}") {{\n'
                f'      compare(headRef: "{pr.head_sha}") {{ behindBy }}\n'
                f"    }}"
            )

        query = (
            f'query {{\n  repository(owner: "{owner}", name: "{repo}") {{\n'
            + "\n".join(compare_fields)
            + "\n  }\n}"
        )

        try:
            data = _graphql_request(token, query, {})
        except SystemExit:
            continue

        repo_data = data.get("repository", {})
        for _i, pr in chunk:
            alias = f"pr{pr.number}"
            ref_data = repo_data.get(alias) or {}
            compare = ref_data.get("compare") or {}
            result[pr.number] = compare.get("behindBy", 0)

    return result


def _fetch_prs_graphql(
    token: str,
    github_repository: str,
    labels: tuple[str, ...],
    exclude_labels: tuple[str, ...],
    filter_user: str | None,
    sort: str,
    batch_size: int,
    created_after: str | None = None,
    created_before: str | None = None,
    updated_after: str | None = None,
    updated_before: str | None = None,
) -> list[PRData]:
    """Fetch a single batch of matching PRs via GraphQL."""
    query_parts = [f"repo:{github_repository}", "type:pr", "is:open", "draft:false"]
    if filter_user:
        query_parts.append(f"author:{filter_user}")
    for label in labels:
        query_parts.append(f'label:"{label}"')
    for label in exclude_labels:
        query_parts.append(f'-label:"{label}"')
    # Date range filters — GitHub search supports created: and updated: qualifiers
    if created_after and created_before:
        query_parts.append(f"created:{created_after}..{created_before}")
    elif created_after:
        query_parts.append(f"created:>={created_after}")
    elif created_before:
        query_parts.append(f"created:<={created_before}")
    if updated_after and updated_before:
        query_parts.append(f"updated:{updated_after}..{updated_before}")
    elif updated_after:
        query_parts.append(f"updated:>={updated_after}")
    elif updated_before:
        query_parts.append(f"updated:<={updated_before}")
    search_query = " ".join(query_parts)

    sort_field, sort_direction = sort.rsplit("-", 1)
    search_query += f" sort:{sort_field}-{sort_direction}"

    get_console().print(f"[info]Searching PRs: {search_query}[/]")

    data = _graphql_request(token, _SEARCH_PRS_QUERY, {"query": search_query, "first": batch_size})
    search_data = data["search"]

    get_console().print(
        f"[info]Found {search_data['issueCount']} matching "
        f"{'PRs' if search_data['issueCount'] != 1 else 'PR'}, "
        f"fetched {len(search_data['nodes'])}.[/]"
    )

    prs: list[PRData] = []
    for node in search_data["nodes"]:
        if not node:
            continue
        author = node.get("author") or {}
        head_sha, checks_state = _extract_basic_check_info(node)
        prs.append(
            PRData(
                number=node["number"],
                title=node["title"],
                body=node.get("body") or "",
                url=node["url"],
                created_at=node["createdAt"],
                updated_at=node["updatedAt"],
                node_id=node["id"],
                author_login=author.get("login", "ghost"),
                author_association=node.get("authorAssociation", "NONE"),
                head_sha=head_sha,
                base_ref=node.get("baseRefName", "main"),
                check_summary="",
                checks_state=checks_state,
                failed_checks=[],
                commits_behind=0,
                mergeable=node.get("mergeable", "UNKNOWN"),
                labels=[lbl["name"] for lbl in (node.get("labels") or {}).get("nodes", []) if lbl],
                unresolved_review_comments=0,
            )
        )

    return prs


def _fetch_single_pr_graphql(token: str, github_repository: str, pr_number: int) -> PRData:
    """Fetch a single PR by number via GraphQL."""
    owner, repo = github_repository.split("/", 1)
    data = _graphql_request(
        token, _FETCH_SINGLE_PR_QUERY, {"owner": owner, "repo": repo, "number": pr_number}
    )
    node = data.get("repository", {}).get("pullRequest")
    if not node:
        get_console().print(f"[error]PR #{pr_number} not found in {github_repository}.[/]")
        sys.exit(1)

    author = node.get("author") or {}
    head_sha, checks_state = _extract_basic_check_info(node)
    return PRData(
        number=node["number"],
        title=node["title"],
        body=node.get("body") or "",
        url=node["url"],
        created_at=node["createdAt"],
        updated_at=node["updatedAt"],
        node_id=node["id"],
        author_login=author.get("login", "ghost"),
        author_association=node.get("authorAssociation", "NONE"),
        head_sha=head_sha,
        base_ref=node.get("baseRefName", "main"),
        check_summary="",
        checks_state=checks_state,
        failed_checks=[],
        commits_behind=0,
        mergeable=node.get("mergeable", "UNKNOWN"),
        labels=[lbl["name"] for lbl in (node.get("labels") or {}).get("nodes", []) if lbl],
        unresolved_review_comments=0,
    )


def _human_readable_age(iso_date: str) -> str:
    """Convert an ISO date string to a human-readable relative age (e.g. '2 years, 3 months ago')."""
    from datetime import datetime, timezone

    try:
        created = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - created
        total_days = delta.days

        years = total_days // 365
        remaining_days = total_days % 365
        months = remaining_days // 30

        parts: list[str] = []
        if years:
            parts.append(f"{years} year{'s' if years != 1 else ''}")
        if months:
            parts.append(f"{months} month{'s' if months != 1 else ''}")
        if not parts:
            if total_days > 0:
                parts.append(f"{total_days} day{'s' if total_days != 1 else ''}")
            else:
                return "today"
        return ", ".join(parts) + " ago"
    except (ValueError, TypeError):
        return "unknown"


_author_profile_cache: dict[str, dict] = {}


def _fetch_author_profile(token: str, login: str, github_repository: str) -> dict:
    """Fetch author profile info via GraphQL: account age, PR counts, contributed repos.

    Results are cached per login so the same author is only queried once.
    """
    if login in _author_profile_cache:
        return _author_profile_cache[login]

    repo_prefix = f"repo:{github_repository} type:pr author:{login}"
    global_prefix = f"type:pr author:{login}"
    try:
        data = _graphql_request(
            token,
            _AUTHOR_PROFILE_QUERY,
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
    account_age = _human_readable_age(created_at) if created_at != "unknown" else "unknown"

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

    profile = {
        "login": login,
        "account_age": account_age,
        "repo_total_prs": data.get("repoAll", {}).get("issueCount", 0),
        "repo_merged_prs": data.get("repoMerged", {}).get("issueCount", 0),
        "repo_closed_prs": data.get("repoClosed", {}).get("issueCount", 0),
        "global_total_prs": data.get("globalAll", {}).get("issueCount", 0),
        "global_merged_prs": data.get("globalMerged", {}).get("issueCount", 0),
        "global_closed_prs": data.get("globalClosed", {}).get("issueCount", 0),
        "contributed_repos": contributed_repos,
        "contributed_repos_total": contrib_total,
    }
    _author_profile_cache[login] = profile
    return profile


def _is_bot_account(login: str) -> bool:
    """Check if a GitHub login belongs to a bot account."""
    return login.lower() in _BOT_ACCOUNT_LOGINS or login.endswith("[bot]")


def _convert_pr_to_draft(token: str, node_id: str) -> bool:
    """Convert a PR to draft using GitHub GraphQL API."""
    try:
        _graphql_request(token, _CONVERT_TO_DRAFT_MUTATION, {"prId": node_id})
        return True
    except SystemExit:
        return False


def _close_pr(token: str, node_id: str) -> bool:
    """Close a PR using GitHub GraphQL API."""
    try:
        _graphql_request(token, _CLOSE_PR_MUTATION, {"prId": node_id})
        return True
    except SystemExit:
        return False


def _post_comment(token: str, node_id: str, body: str) -> bool:
    """Post a comment on a PR using GitHub GraphQL API."""
    try:
        _graphql_request(token, _ADD_COMMENT_MUTATION, {"subjectId": node_id, "body": body})
        return True
    except SystemExit:
        return False


_label_id_cache: dict[str, str | None] = {}


def _resolve_label_id(token: str, github_repository: str, label_name: str) -> str | None:
    """Resolve a label name to its node ID via GraphQL. Cached per label name."""
    if label_name in _label_id_cache:
        return _label_id_cache[label_name]
    owner, repo = github_repository.split("/", 1)
    try:
        data = _graphql_request(
            token, _GET_LABEL_ID_QUERY, {"owner": owner, "repo": repo, "name": label_name}
        )
        label_data = (data.get("repository") or {}).get("label")
        label_id = label_data["id"] if label_data else None
    except (SystemExit, KeyError):
        label_id = None
    _label_id_cache[label_name] = label_id
    return label_id


def _add_label(token: str, github_repository: str, pr_node_id: str, label_name: str) -> bool:
    """Add a label to a PR. Returns True on success."""
    label_id = _resolve_label_id(token, github_repository, label_name)
    if not label_id:
        get_console().print(
            f"[warning]Label '{label_name}' not found in {github_repository}. Skipping label.[/]"
        )
        return False
    try:
        _graphql_request(token, _ADD_LABELS_MUTATION, {"labelableId": pr_node_id, "labelIds": [label_id]})
        return True
    except SystemExit:
        return False


def _load_labels_from_boring_cyborg() -> list[str]:
    """Read labels from .github/boring-cyborg.yml."""
    from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

    boring_cyborg_path = AIRFLOW_ROOT_PATH / ".github" / "boring-cyborg.yml"
    if not boring_cyborg_path.exists():
        get_console().print("[warning]boring-cyborg.yml not found, label validation disabled.[/]")
        return []

    import yaml

    with open(boring_cyborg_path) as f:
        data = yaml.safe_load(f)

    labels: list[str] = []
    label_pr_section = data.get("labelPRBasedOnFilePath", {})
    for label_name in label_pr_section:
        labels.append(label_name)
    return sorted(labels)


def _load_what_to_do_next() -> str:
    """Load 'what to do next' instructions from contributing docs."""
    from airflow_breeze.utils.llm_utils import _read_file_section
    from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

    # Read the "What happens when a PR is converted to draft?" section
    section = _read_file_section(
        AIRFLOW_ROOT_PATH,
        "contributing-docs/05_pull_requests.rst",
        "What happens when a PR is converted to draft",
        "Converting a PR to draft is",
    )
    if section:
        # Convert RST list items to markdown
        lines = []
        for line in section.splitlines():
            stripped = line.strip()
            if stripped.startswith("-"):
                lines.append(stripped.replace("-  ", "- ", 1))
            elif stripped:
                lines.append(stripped)
        return "\n".join(lines)
    return (
        "- Fix each issue listed above.\n"
        "- Make sure static checks pass locally (`prek run --from-ref main --stage pre-commit`).\n"
        '- Mark the PR as "Ready for review" when you\'re done.'
    )


def _build_comment(
    pr_author: str,
    violations: list,
    pr_number: int,
    commits_behind: int,
    base_ref: str,
    comment_only: bool = False,
) -> str:
    """Build the comment to post on a flagged PR.

    When comment_only is True, the comment just lists findings without
    mentioning draft conversion.
    """
    violation_lines = []
    for v in violations:
        icon = "x" if v.severity == "error" else "warning"
        line = f"- :{icon}: **{v.category}**: {v.explanation}"
        if v.details:
            line += f" {v.details}"
        violation_lines.append(line)
    violations_text = "\n".join(violation_lines)

    what_to_do = _load_what_to_do_next()

    rebase_note = ""
    if commits_behind > 0:
        rebase_note = (
            f"\n\n> **Note:** Your branch is **{commits_behind} "
            f"commit{'s' if commits_behind != 1 else ''} behind `{base_ref}`**. "
            "Some check failures may be caused by changes in the base branch rather than by your PR. "
            "Please rebase your branch and push again to get up-to-date CI results."
        )

    if comment_only:
        return (
            f"@{pr_author} This PR has a few issues that need to be addressed before it can be "
            f"reviewed — please see our {QUALITY_CRITERIA_LINK}.\n\n"
            f"**Issues found:**\n{violations_text}{rebase_note}\n\n"
            f"**What to do next:**\n{what_to_do}\n\n"
            "Please address the issues above and push again. "
            "If you have questions, feel free to ask on the "
            "[Airflow Slack](https://s.apache.org/airflow-slack)."
        )

    return (
        f"@{pr_author} This PR has been converted to **draft** because it does not yet meet "
        f"our {QUALITY_CRITERIA_LINK}.\n\n"
        f"**Issues found:**\n{violations_text}{rebase_note}\n\n"
        f"**What to do next:**\n{what_to_do}\n\n"
        "Converting a PR to draft is **not** a rejection — it is an invitation to bring the PR up to "
        "the project's standards so that maintainer review time is spent productively. "
        "If you have questions, feel free to ask on the "
        "[Airflow Slack](https://s.apache.org/airflow-slack)."
    )


def _build_close_comment(pr_author: str, violations: list, pr_number: int, author_flagged_count: int) -> str:
    """Build the comment to post on a PR being closed due to quality issues."""
    violation_lines = []
    for v in violations:
        icon = "x" if v.severity == "error" else "warning"
        line = f"- :{icon}: **{v.category}**: {v.explanation}"
        if v.details:
            line += f" {v.details}"
        violation_lines.append(line)
    if author_flagged_count > 3:
        violation_lines.append(
            f"- :x: **Multiple flagged PRs**: You currently have **{author_flagged_count} "
            f"{'PRs' if author_flagged_count != 1 else 'PR'}** "
            "flagged for quality issues in this repository. We recommend focusing on improving "
            f"your existing {'PRs' if author_flagged_count != 1 else 'PR'} before opening new ones."
        )
    violations_text = "\n".join(violation_lines)

    return (
        f"@{pr_author} This PR has been **closed because of multiple quality violations** "
        f"— it does not meet our {QUALITY_CRITERIA_LINK}.\n\n"
        f"**Issues found:**\n{violations_text}\n\n"
        "You are welcome to open a new PR that addresses the issues listed above. "
        "If you have questions, feel free to ask on the "
        "[Airflow Slack](https://s.apache.org/airflow-slack)."
    )


def _compute_default_action(
    pr: PRData, assessment, author_flagged_count: dict[str, int]
) -> tuple[TriageAction, str]:
    """Compute the suggested default triage action and reason for a flagged PR."""
    reason_parts: list[str] = []

    has_conflicts = pr.mergeable == "CONFLICTING"
    if has_conflicts:
        reason_parts.append("has merge conflicts")

    failed_count = len(pr.failed_checks)
    has_ci_failures = failed_count > 0
    if has_ci_failures:
        reason_parts.append(f"{failed_count} CI failure{'s' if failed_count != 1 else ''}")

    has_unresolved_comments = pr.unresolved_review_comments > 0
    if has_unresolved_comments and not any("unresolved" in p for p in reason_parts):
        reason_parts.append(
            f"{pr.unresolved_review_comments} unresolved review comment{'s' if pr.unresolved_review_comments != 1 else ''}"
        )

    if assessment.summary:
        reason_parts.append(assessment.summary.lower())

    count = author_flagged_count.get(pr.author_login, 0)
    if count > 3:
        reason_parts.append(f"author has {count} flagged {'PRs' if count != 1 else 'PR'}")
        action = TriageAction.CLOSE
    elif not has_ci_failures and (has_conflicts or has_unresolved_comments):
        # CI passes, no LLM issues — only conflicts or unresolved comments; just add a comment
        action = TriageAction.COMMENT
    else:
        action = TriageAction.DRAFT

    reason = "; ".join(reason_parts) if reason_parts else "flagged for quality issues"
    reason = reason[0].upper() + reason[1:]
    action_label = {
        TriageAction.DRAFT: "draft",
        TriageAction.COMMENT: "add comment",
        TriageAction.CLOSE: "close",
    }[action]
    return action, f"{reason} — suggesting {action_label}"


def _pr_link(pr: PRData) -> str:
    """Return a Rich-markup clickable link for a PR: [link=url]#number[/link]."""
    return f"[link={pr.url}]#{pr.number}[/link]"


def _display_pr_info_panels(pr: PRData, author_profile: dict | None):
    """Display PR info and author panels (shared by flagged-PR and workflow-approval flows)."""
    console = get_console()
    console.print()
    console.rule(style="dim")
    console.print()

    status_info = ""
    if pr.commits_behind > 0:
        status_info += (
            f"\n[yellow]{pr.commits_behind} commit{'s' if pr.commits_behind != 1 else ''} "
            f"behind {pr.base_ref}[/]"
        )
    if pr.mergeable == "CONFLICTING":
        status_info += "\n[red]Has merge conflicts[/]"
    pr_info = (
        f"[link={pr.url}][cyan]#{pr.number}[/][/link] {pr.title}\n"
        f"Author: [link=https://github.com/{pr.author_login}][bold]{pr.author_login}[/][/link]  |  "
        f"Created: {_human_readable_age(pr.created_at)}  |  "
        f"Updated: {_human_readable_age(pr.updated_at)}"
        f"{status_info}"
    )
    console.print(Panel(pr_info, title="Pull Request", border_style="cyan"))

    if author_profile:
        login = author_profile["login"]
        lines = [
            f"Account created: {author_profile['account_age']}",
            (
                f"PRs in this repo: {author_profile['repo_total_prs']} total, "
                f"{author_profile['repo_merged_prs']} merged, "
                f"{author_profile['repo_closed_prs']} closed (unmerged)"
            ),
            (
                f"PRs across GitHub: {author_profile['global_total_prs']} total, "
                f"{author_profile['global_merged_prs']} merged, "
                f"{author_profile['global_closed_prs']} closed (unmerged)"
            ),
        ]

        contributed_repos = author_profile.get("contributed_repos", [])
        contrib_total = author_profile.get("contributed_repos_total", 0)
        if contributed_repos:
            repo_items = []
            for repo in contributed_repos:
                repo_link = f"[link={repo['url']}/pulls?q=author%3A{login}]{repo['name']}[/link]"
                repo_items.append(repo_link)
            repos_text = ", ".join(repo_items)
            if contrib_total > len(contributed_repos):
                repos_text += f" (+{contrib_total - len(contributed_repos)} more)"
            lines.append(f"Contributed to: {repos_text}")
        elif contrib_total > 0:
            lines.append(f"Contributed to: {contrib_total} repos (private or not shown)")
        else:
            lines.append("Contributed to: no public repos found")

        author_link = f"[link=https://github.com/{login}]{login}[/link]"
        console.print(Panel("\n".join(lines), title=f"Author: {author_link}", border_style="yellow"))


def _display_pr_panel(pr: PRData, author_profile: dict | None, assessment, comment: str):
    """Display Rich panels with PR details, author info, violations, and proposed comment."""
    console = get_console()
    _display_pr_info_panels(pr, author_profile)

    violation_lines = []
    for v in assessment.violations:
        color = "red" if v.severity == "error" else "yellow"
        violation_lines.append(f"[{color}][{v.severity.upper()}][/{color}] {v.category}: {v.explanation}")
    console.print(
        Panel("\n".join(violation_lines), title=f"Assessment: {assessment.summary}", border_style="red")
    )

    console.print(Panel(comment, title="Proposed comment", border_style="green"))


def _display_workflow_approval_panel(pr: PRData, author_profile: dict | None, pending_runs: list[dict]):
    """Display Rich panels for a PR needing workflow approval."""
    console = get_console()
    _display_pr_info_panels(pr, author_profile)

    run_lines = []
    if pending_runs:
        for run in pending_runs:
            name = run.get("name", "unknown")
            run_url = run.get("html_url", "")
            if run_url:
                run_lines.append(f"  [link={run_url}]{name}[/link]")
            else:
                run_lines.append(f"  {name}")
        runs_text = "\n".join(run_lines)
        info_text = (
            f"[bright_cyan]{len(pending_runs)} workflow "
            f"{'runs' if len(pending_runs) != 1 else 'run'} awaiting approval:[/]\n{runs_text}\n\n"
        )
    else:
        info_text = "[bright_cyan]No test workflows have run on this PR.[/]\n\n"

    info_text += (
        "Please review the PR changes before approving:\n"
        f"  [link={pr.url}/files]View changes on GitHub[/link]"
    )
    console.print(Panel(info_text, title="Workflow Approval Needed", border_style="bright_cyan"))


def _fetch_pr_diff(token: str, github_repository: str, pr_number: int) -> str | None:
    """Fetch the diff for a PR via GitHub REST API. Returns the diff text or None on failure."""
    import requests

    url = f"https://api.github.com/repos/{github_repository}/pulls/{pr_number}"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github.v3.diff"},
        timeout=60,
    )
    if response.status_code != 200:
        return None
    return response.text


def _fetch_author_open_prs(token: str, github_repository: str, author_login: str) -> list[dict]:
    """Fetch all open PRs by a given author. Returns list of dicts with number, url, title, node_id."""
    search_query = f"repo:{github_repository} type:pr is:open author:{author_login}"
    data = _graphql_request(token, _SEARCH_PRS_QUERY, {"query": search_query, "first": 100})
    results = []
    for node in data["search"]["nodes"]:
        if not node:
            continue
        results.append(
            {
                "number": node["number"],
                "url": node["url"],
                "title": node["title"],
                "node_id": node["id"],
            }
        )
    return results


def _close_suspicious_prs(
    token: str,
    github_repository: str,
    author_prs: list[dict],
    flagged_pr_number: int,
) -> tuple[int, int]:
    """Close PRs flagged as suspicious, add label and comment. Returns (closed, commented) counts."""
    closed = 0
    commented = 0
    for pr_info in author_prs:
        pr_num = pr_info["number"]
        node_id = pr_info["node_id"]

        if _close_pr(token, node_id):
            get_console().print(f"  [success]PR [link={pr_info['url']}]#{pr_num}[/link] closed.[/]")
            closed += 1
        else:
            get_console().print(f"  [error]Failed to close PR [link={pr_info['url']}]#{pr_num}[/link].[/]")
            continue

        _add_label(token, github_repository, node_id, _SUSPICIOUS_CHANGES_LABEL)

        comment = (
            f"This PR has been **closed** because suspicious changes were detected "
            f"in [PR #{flagged_pr_number}]"
            f"(https://github.com/{github_repository}/pull/{flagged_pr_number}) "
            f"by the same author.\n\n"
            "All open PRs by this author have been closed as a precaution. "
            "If you believe this was done in error, please reach out on the "
            "[Airflow Slack](https://s.apache.org/airflow-slack)."
        )
        if _post_comment(token, node_id, comment):
            commented += 1

    return closed, commented


def _find_pending_workflow_runs(token: str, github_repository: str, head_sha: str) -> list[dict]:
    """Find workflow runs awaiting approval for a given commit SHA."""
    import requests

    url = f"https://api.github.com/repos/{github_repository}/actions/runs"
    response = requests.get(
        url,
        params={"head_sha": head_sha, "status": "action_required", "per_page": "50"},
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
    )
    if response.status_code != 200:
        return []
    return response.json().get("workflow_runs", [])


def _approve_workflow_runs(token: str, github_repository: str, pending_runs: list[dict]) -> int:
    """Approve pending workflow runs. Returns number successfully approved."""
    import requests

    approved = 0
    for run in pending_runs:
        run_id = run["id"]
        url = f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/approve"
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=30,
        )
        if response.status_code in (201, 204):
            approved += 1
        else:
            get_console().print(
                f"  [warning]Failed to approve run {run.get('name', run_id)}: {response.status_code}[/]"
            )
    return approved


@pr_group.command(
    name="auto-triage",
    help="Find open PRs from non-collaborators that don't meet quality criteria and convert to draft.",
)
@option_github_token
@option_github_repository
# --- Target selection ---
@click.option(
    "--pr",
    "pr_number",
    type=int,
    default=None,
    help="Triage a specific PR by number instead of searching.",
)
# --- Filter options ---
@click.option(
    "--label",
    "labels",
    type=NotVerifiedBetterChoice(_load_labels_from_boring_cyborg()),
    multiple=True,
    help="Filter PRs by label. Supports wildcards (e.g. 'area:*', 'provider:amazon*'). Can be repeated.",
)
@click.option(
    "--exclude-label",
    "exclude_labels",
    type=str,
    multiple=True,
    help="Exclude PRs with this label. Supports wildcards. Can be repeated.",
)
@click.option(
    "--author",
    "filter_user",
    default=None,
    help="Filter PRs to a specific author.",
)
@click.option(
    "--created-after",
    default=None,
    help="Only PRs created on or after this date (YYYY-MM-DD).",
)
@click.option(
    "--created-before",
    default=None,
    help="Only PRs created on or before this date (YYYY-MM-DD).",
)
@click.option(
    "--updated-after",
    default=None,
    help="Only PRs updated on or after this date (YYYY-MM-DD).",
)
@click.option(
    "--updated-before",
    default=None,
    help="Only PRs updated on or before this date (YYYY-MM-DD).",
)
@click.option(
    "--include-collaborators",
    is_flag=True,
    default=False,
    help="Include PRs from collaborators/members/owners (normally skipped).",
)
@click.option(
    "--pending-approval-only",
    is_flag=True,
    default=False,
    help="Only show PRs with workflow runs awaiting approval.",
)
@click.option(
    "--checks-state",
    type=click.Choice(["failure", "success", "pending", "any"]),
    default="any",
    show_default=True,
    help="Only assess PRs with this CI checks state.",
)
@click.option(
    "--min-commits-behind",
    type=int,
    default=0,
    show_default=True,
    help="Only assess PRs that are at least this many commits behind base branch.",
)
# --- Pagination and sorting ---
@click.option(
    "--batch-size",
    type=int,
    default=50,
    show_default=True,
    help="Number of PRs to fetch per GraphQL page.",
)
@click.option(
    "--max-num",
    type=int,
    default=0,
    show_default=True,
    help="Maximum number of non-collaborator PRs to assess (0 = no limit).",
)
@click.option(
    "--sort",
    type=click.Choice(["created-asc", "created-desc", "updated-asc", "updated-desc"]),
    default="created-desc",
    show_default=True,
    help="Sort order for PR search results.",
)
# --- Assessment options ---
@click.option(
    "--check-mode",
    type=click.Choice(["both", "ci", "llm"]),
    default="both",
    show_default=True,
    help="Which checks to run: 'both' (CI + LLM), 'ci' (deterministic only), 'llm' (LLM only).",
)
@click.option(
    "--llm-concurrency",
    type=int,
    default=4,
    show_default=True,
    help="Number of concurrent LLM assessment calls.",
)
@option_llm_model
# --- Action options ---
@click.option(
    "--answer-triage",
    type=click.Choice(["d", "c", "r", "s", "q", "y", "n"], case_sensitive=False),
    default=None,
    help="Force answer to triage prompts: [d]raft, [c]lose, [r]eady, [s]kip, [q]uit, [y]es, [n]o.",
)
@option_dry_run
@option_verbose
def auto_triage(
    github_token: str | None,
    github_repository: str,
    pr_number: int | None,
    labels: tuple[str, ...],
    exclude_labels: tuple[str, ...],
    batch_size: int,
    max_num: int,
    sort: str,
    filter_user: str | None,
    created_after: str | None,
    created_before: str | None,
    updated_after: str | None,
    updated_before: str | None,
    include_collaborators: bool,
    pending_approval_only: bool,
    checks_state: str,
    min_commits_behind: int,
    check_mode: str,
    llm_concurrency: int,
    llm_model: str,
    answer_triage: str | None,
):
    from airflow_breeze.utils.github import (
        PRAssessment,
        assess_pr_checks,
        assess_pr_conflicts,
        assess_pr_unresolved_comments,
    )
    from airflow_breeze.utils.llm_utils import (
        _check_cli_available,
        _resolve_cli_provider,
        assess_pr,
    )

    token = _resolve_github_token(github_token)
    if not token:
        get_console().print(
            "[error]GitHub token not found. Provide --github-token, "
            "set GITHUB_TOKEN, or authenticate with `gh auth login`.[/]"
        )
        sys.exit(1)

    run_ci = check_mode in ("both", "ci")
    run_llm = check_mode in ("both", "llm")

    # Validate CLI tool is available early (only when LLM checks are enabled)
    if run_llm:
        provider, _model = _resolve_cli_provider(llm_model)
        _check_cli_available(provider)

    dry_run = get_dry_run()

    # Split labels into exact (for GitHub search) and wildcard (for client-side filtering)
    from fnmatch import fnmatch

    exact_labels = tuple(lbl for lbl in labels if "*" not in lbl and "?" not in lbl)
    wildcard_labels = [lbl for lbl in labels if "*" in lbl or "?" in lbl]
    exact_exclude_labels = tuple(lbl for lbl in exclude_labels if "*" not in lbl and "?" not in lbl)
    wildcard_exclude_labels = [lbl for lbl in exclude_labels if "*" in lbl or "?" in lbl]

    # Phase 1: Lightweight fetch of PRs via GraphQL (no check contexts — fast)
    if pr_number:
        get_console().print(f"[info]Fetching PR #{pr_number} via GraphQL...[/]")
        all_prs = [_fetch_single_pr_graphql(token, github_repository, pr_number)]
    else:
        get_console().print("[info]Fetching PRs via GraphQL...[/]")
        all_prs = _fetch_prs_graphql(
            token,
            github_repository,
            labels=exact_labels,
            exclude_labels=exact_exclude_labels,
            filter_user=filter_user,
            sort=sort,
            batch_size=batch_size,
            created_after=created_after,
            created_before=created_before,
            updated_after=updated_after,
            updated_before=updated_before,
        )

    # Apply wildcard label filters client-side
    if wildcard_labels:
        all_prs = [
            pr for pr in all_prs if any(fnmatch(lbl, pat) for pat in wildcard_labels for lbl in pr.labels)
        ]
    if wildcard_exclude_labels:
        all_prs = [
            pr
            for pr in all_prs
            if not any(fnmatch(lbl, pat) for pat in wildcard_exclude_labels for lbl in pr.labels)
        ]

    # Resolve how far behind base branch each PR is (chunked GraphQL)
    get_console().print("[info]Checking how far behind base branch each PR is...[/]")
    behind_map = _fetch_commits_behind_batch(token, github_repository, all_prs)
    for pr in all_prs:
        pr.commits_behind = behind_map.get(pr.number, 0)

    # Display fetched PRs overview (skip collaborator PRs — they'll be summarised below)
    non_collab_prs = [pr for pr in all_prs if pr.author_association not in _COLLABORATOR_ASSOCIATIONS]
    collab_count = len(all_prs) - len(non_collab_prs)
    pr_table = Table(title=f"Fetched PRs ({len(non_collab_prs)} non-collaborator)")
    pr_table.add_column("PR", style="cyan", no_wrap=True)
    pr_table.add_column("Title", max_width=50)
    pr_table.add_column("Author")
    pr_table.add_column("Status")
    pr_table.add_column("Behind", justify="right")
    pr_table.add_column("Conflicts")
    pr_table.add_column("CI Status")
    for pr in non_collab_prs:
        if pr.checks_state == "FAILURE":
            ci_status = "[red]Failing[/]"
        elif pr.checks_state == "PENDING":
            ci_status = "[yellow]Pending[/]"
        elif pr.checks_state == "UNKNOWN":
            ci_status = "[dim]No checks[/]"
        else:
            ci_status = f"[green]{pr.checks_state.capitalize()}[/]"
        if pr.commits_behind > 0:
            behind_text = f"[yellow]{pr.commits_behind}[/]"
        else:
            behind_text = "[green]0[/]"
        if pr.mergeable == "CONFLICTING":
            conflicts_text = "[red]Yes[/]"
        elif pr.mergeable == "UNKNOWN":
            conflicts_text = "[dim]?[/]"
        else:
            conflicts_text = "[green]No[/]"

        has_issues = pr.checks_state == "FAILURE" or pr.mergeable == "CONFLICTING"
        overall = "[red]Flag[/]" if has_issues else "[green]OK[/]"

        pr_table.add_row(
            _pr_link(pr),
            pr.title[:50],
            pr.author_login,
            overall,
            behind_text,
            conflicts_text,
            ci_status,
        )
    get_console().print(pr_table)
    if collab_count:
        get_console().print(
            f"  [dim]({collab_count} collaborator/member {'PRs' if collab_count != 1 else 'PR'} not shown)[/]"
        )
    get_console().print()

    # Phase 2: Filter out collaborators, bots, and ready-for-review PRs, then apply post-fetch filters
    candidate_prs: list[PRData] = []
    total_skipped_collaborator = 0
    total_skipped_bot = 0
    total_skipped_accepted = 0
    total_skipped_checks_state = 0
    total_skipped_commits_behind = 0
    verbose = get_verbose()
    for pr in all_prs:
        if not include_collaborators and pr.author_association in _COLLABORATOR_ASSOCIATIONS:
            total_skipped_collaborator += 1
            if verbose:
                get_console().print(
                    f"  [dim]Skipping PR {_pr_link(pr)} by "
                    f"{pr.author_association.lower()} {pr.author_login}[/]"
                )
        elif _is_bot_account(pr.author_login):
            total_skipped_bot += 1
            if verbose:
                get_console().print(f"  [dim]Skipping PR {_pr_link(pr)} — bot account {pr.author_login}[/]")
        elif _READY_FOR_REVIEW_LABEL in pr.labels:
            total_skipped_accepted += 1
            if verbose:
                get_console().print(
                    f"  [dim]Skipping PR {_pr_link(pr)} — already has '{_READY_FOR_REVIEW_LABEL}' label[/]"
                )
        elif checks_state != "any" and pr.checks_state.lower() != checks_state:
            total_skipped_checks_state += 1
            if verbose:
                get_console().print(
                    f"  [dim]Skipping PR {_pr_link(pr)} — checks state {pr.checks_state} != {checks_state}[/]"
                )
        elif min_commits_behind > 0 and pr.commits_behind < min_commits_behind:
            total_skipped_commits_behind += 1
            if verbose:
                get_console().print(
                    f"  [dim]Skipping PR {_pr_link(pr)} — only "
                    f"{pr.commits_behind} commits behind (min: {min_commits_behind})[/]"
                )
        else:
            candidate_prs.append(pr)

    if max_num and len(candidate_prs) > max_num:
        candidate_prs = candidate_prs[:max_num]

    skipped_parts: list[str] = []
    if total_skipped_collaborator:
        skipped_parts.append(
            f"{total_skipped_collaborator} "
            f"{'collaborators' if total_skipped_collaborator != 1 else 'collaborator'}"
        )
    if total_skipped_bot:
        skipped_parts.append(f"{total_skipped_bot} {'bots' if total_skipped_bot != 1 else 'bot'}")
    if total_skipped_accepted:
        skipped_parts.append(f"{total_skipped_accepted} ready-for-review")
    if total_skipped_checks_state:
        skipped_parts.append(f"{total_skipped_checks_state} checks-state mismatch")
    if total_skipped_commits_behind:
        skipped_parts.append(f"{total_skipped_commits_behind} below min-commits-behind")
    skipped_text = f"skipped {', '.join(skipped_parts)}, " if skipped_parts else ""
    get_console().print(
        f"\n[info]Fetched {len(all_prs)} {'PRs' if len(all_prs) != 1 else 'PR'}, "
        f"{skipped_text}"
        f"assessing {len(candidate_prs)} {'PRs' if len(candidate_prs) != 1 else 'PR'}"
        f"{f' (capped at {max_num})' if max_num else ''}...[/]\n"
    )

    # Phase 2b: Fetch detailed check contexts only for candidate PRs (chunked to avoid timeouts)
    if candidate_prs:
        get_console().print(
            f"[info]Fetching check details for {len(candidate_prs)} "
            f"candidate {'PRs' if len(candidate_prs) != 1 else 'PR'}...[/]"
        )
        _fetch_check_details_batch(token, github_repository, candidate_prs)

        # For PRs with >100 checks where failures weren't found, paginate individually
        for pr in candidate_prs:
            if pr.checks_state == "FAILURE" and not pr.failed_checks and pr.head_sha:
                get_console().print(
                    f"  [dim]Fetching full check details for PR {_pr_link(pr)} "
                    f"(failures beyond first 100 checks)...[/]"
                )
                pr.failed_checks = _fetch_failed_checks(token, github_repository, pr.head_sha)

    # Phase 2c: Fetch unresolved review comment counts for candidate PRs
    if candidate_prs and run_ci:
        get_console().print(
            f"[info]Fetching review thread details for {len(candidate_prs)} "
            f"candidate {'PRs' if len(candidate_prs) != 1 else 'PR'}...[/]"
        )
        _fetch_unresolved_comments_batch(token, github_repository, candidate_prs)

    # Phase 3: Deterministic checks (CI failures + merge conflicts + unresolved comments),
    # then LLM for the rest
    # PRs with NOT_RUN checks are separated for workflow approval instead of LLM assessment.
    assessments: dict[int, PRAssessment] = {}
    llm_candidates: list[PRData] = []
    passing_prs: list[PRData] = []
    pending_approval: list[PRData] = []
    total_deterministic_flags = 0

    if run_ci:
        for pr in candidate_prs:
            ci_assessment = assess_pr_checks(pr.number, pr.checks_state, pr.failed_checks)
            conflict_assessment = assess_pr_conflicts(pr.number, pr.mergeable, pr.base_ref, pr.commits_behind)
            comments_assessment = assess_pr_unresolved_comments(pr.number, pr.unresolved_review_comments)

            # Merge violations from all deterministic checks
            if ci_assessment or conflict_assessment or comments_assessment:
                total_deterministic_flags += 1
                violations = []
                summaries = []
                if conflict_assessment:
                    violations.extend(conflict_assessment.violations)
                    summaries.append(conflict_assessment.summary)
                if ci_assessment:
                    violations.extend(ci_assessment.violations)
                    summaries.append(ci_assessment.summary)
                if comments_assessment:
                    violations.extend(comments_assessment.violations)
                    summaries.append(comments_assessment.summary)
                assessments[pr.number] = PRAssessment(
                    should_flag=True,
                    violations=violations,
                    summary=" ".join(summaries),
                )
            elif pr.checks_state == "NOT_RUN":
                pending_approval.append(pr)
            else:
                llm_candidates.append(pr)
    else:
        for pr in candidate_prs:
            if pr.checks_state == "NOT_RUN":
                pending_approval.append(pr)
            else:
                llm_candidates.append(pr)

    # If --pending-approval-only, discard all assessment results and only keep pending_approval
    if pending_approval_only:
        assessments = {}
        llm_candidates = []
        total_deterministic_flags = 0
        if not pending_approval:
            get_console().print("[success]No PRs with pending workflow approvals found.[/]")
            sys.exit(0)
        get_console().print(
            f"[info]--pending-approval-only: found {len(pending_approval)} "
            f"{'PRs' if len(pending_approval) != 1 else 'PR'} awaiting workflow approval.[/]\n"
        )

    # Phase 4: Run LLM assessments concurrently for PRs without CI failures
    total_llm_errors = 0

    if not run_llm:
        if llm_candidates:
            get_console().print(
                f"\n[info]--check-mode=ci: skipping LLM assessment for {len(llm_candidates)} "
                f"{'PRs' if len(llm_candidates) != 1 else 'PR'}.[/]\n"
            )
            passing_prs.extend(llm_candidates)
    elif llm_candidates:
        skipped_detail = f"{total_deterministic_flags} CI/conflicts/comments"
        if pending_approval:
            skipped_detail += f", {len(pending_approval)} awaiting workflow approval"
        get_console().print(
            f"\n[info]Running LLM assessment for {len(llm_candidates)} "
            f"{'PRs' if len(llm_candidates) != 1 else 'PR'} (skipped {skipped_detail})...[/]\n"
        )
        with ThreadPoolExecutor(max_workers=llm_concurrency) as executor:
            future_to_pr = {
                executor.submit(
                    assess_pr,
                    pr_number=pr.number,
                    pr_title=pr.title,
                    pr_body=pr.body,
                    check_status_summary=pr.check_summary,
                    llm_model=llm_model,
                ): pr
                for pr in llm_candidates
            }
            for future in as_completed(future_to_pr):
                pr = future_to_pr[future]
                assessment = future.result()
                if assessment.error:
                    total_llm_errors += 1
                    continue
                if not assessment.should_flag:
                    get_console().print(f"  [success]PR {_pr_link(pr)} passes quality check.[/]")
                    passing_prs.append(pr)
                    continue
                assessments[pr.number] = assessment

    total_flagged = len(assessments)
    summary_parts = [
        f"{total_deterministic_flags} CI/conflicts/comments",
        f"{total_flagged - total_deterministic_flags} LLM-flagged",
    ]
    if pending_approval:
        summary_parts.append(f"{len(pending_approval)} awaiting workflow approval")
    if total_llm_errors:
        summary_parts.append(f"{total_llm_errors} LLM errors")
    get_console().print(
        f"\n[info]Assessment complete: {total_flagged} {'PRs' if total_flagged != 1 else 'PR'} "
        f"flagged ({', '.join(summary_parts)}).[/]\n"
    )

    # Phase 5: Present flagged PRs interactively, grouped by author
    total_converted = 0
    total_commented = 0
    total_closed = 0
    total_ready = 0
    total_skipped_action = 0
    quit_early = False

    # Build sorted list of flagged PRs grouped by author
    flagged_prs = [(pr, assessments[pr.number]) for pr in candidate_prs if pr.number in assessments]
    flagged_prs.sort(key=lambda pair: (pair[0].author_login.lower(), pair[0].number))
    from collections import Counter

    author_flagged_count: dict[str, int] = dict(Counter(pr.author_login for pr, _ in flagged_prs))

    current_author: str | None = None
    for pr, assessment in flagged_prs:
        if pr.author_login != current_author:
            current_author = pr.author_login
            count = author_flagged_count[current_author]
            get_console().print()
            get_console().rule(
                f"[bold]Author: {current_author}[/] ({count} flagged PR{'s' if count != 1 else ''})",
                style="cyan",
            )

        # Fetch author profile for context (only for flagged PRs)
        author_profile = _fetch_author_profile(token, pr.author_login, github_repository)

        comment = _build_comment(
            pr.author_login, assessment.violations, pr.number, pr.commits_behind, pr.base_ref
        )
        comment_only = _build_comment(
            pr.author_login,
            assessment.violations,
            pr.number,
            pr.commits_behind,
            pr.base_ref,
            comment_only=True,
        )
        close_comment = _build_close_comment(
            pr.author_login,
            assessment.violations,
            pr.number,
            author_flagged_count.get(pr.author_login, 0),
        )
        _display_pr_panel(pr, author_profile, assessment, comment)

        default_action, reason = _compute_default_action(pr, assessment, author_flagged_count)
        if default_action == TriageAction.CLOSE:
            get_console().print(Panel(close_comment, title="Proposed close comment", border_style="red"))
        get_console().print(f"  [bold]{reason}[/]")

        if dry_run:
            action_label = {
                TriageAction.DRAFT: "draft",
                TriageAction.COMMENT: "add comment",
                TriageAction.CLOSE: "close",
                TriageAction.READY: "ready",
                TriageAction.SKIP: "skip",
            }[default_action]
            get_console().print(f"[warning]Dry run — would default to: {action_label}[/]")
            continue

        action = prompt_triage_action(
            f"Action for PR {_pr_link(pr)}?",
            default=default_action,
            forced_answer=answer_triage,
        )

        if action == TriageAction.QUIT:
            get_console().print("[warning]Quitting.[/]")
            quit_early = True
            break

        if action == TriageAction.SKIP:
            get_console().print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
            total_skipped_action += 1
            continue

        if action == TriageAction.READY:
            get_console().print(
                f"  [info]Marking PR {_pr_link(pr)} as ready — adding '{_READY_FOR_REVIEW_LABEL}' label.[/]"
            )
            if _add_label(token, github_repository, pr.node_id, _READY_FOR_REVIEW_LABEL):
                get_console().print(
                    f"  [success]Label '{_READY_FOR_REVIEW_LABEL}' added to PR {_pr_link(pr)}.[/]"
                )
                total_ready += 1
            else:
                get_console().print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")
            continue

        if action == TriageAction.COMMENT:
            get_console().print(f"  Posting comment on PR {_pr_link(pr)}...")
            if _post_comment(token, pr.node_id, comment_only):
                get_console().print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
                total_commented += 1
            else:
                get_console().print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")
            continue

        if action == TriageAction.DRAFT:
            get_console().print(f"  Converting PR {_pr_link(pr)} to draft...")
            if _convert_pr_to_draft(token, pr.node_id):
                get_console().print(f"  [success]PR {_pr_link(pr)} converted to draft.[/]")
            else:
                get_console().print(f"  [error]Failed to convert PR {_pr_link(pr)} to draft.[/]")
                continue

            get_console().print(f"  Posting comment on PR {_pr_link(pr)}...")
            if _post_comment(token, pr.node_id, comment):
                get_console().print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
                total_converted += 1
            else:
                get_console().print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")
            continue

        if action == TriageAction.CLOSE:
            get_console().print(f"  Closing PR {_pr_link(pr)}...")
            if _close_pr(token, pr.node_id):
                get_console().print(f"  [success]PR {_pr_link(pr)} closed.[/]")
            else:
                get_console().print(f"  [error]Failed to close PR {_pr_link(pr)}.[/]")
                continue

            if _add_label(token, github_repository, pr.node_id, _CLOSED_QUALITY_LABEL):
                get_console().print(
                    f"  [success]Label '{_CLOSED_QUALITY_LABEL}' added to PR {_pr_link(pr)}.[/]"
                )
            else:
                get_console().print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")

            get_console().print(f"  Posting comment on PR {_pr_link(pr)}...")
            if _post_comment(token, pr.node_id, close_comment):
                get_console().print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
                total_closed += 1
            else:
                get_console().print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")

    # Phase 5b: Present passing PRs for optional ready-for-review marking
    if not quit_early and passing_prs:
        passing_prs.sort(key=lambda p: (p.author_login.lower(), p.number))
        get_console().print(
            f"\n[info]{len(passing_prs)} {'PRs pass' if len(passing_prs) != 1 else 'PR passes'} "
            f"all checks — review to mark as ready:[/]\n"
        )
        for pr in passing_prs:
            author_profile = _fetch_author_profile(token, pr.author_login, github_repository)
            _display_pr_info_panels(pr, author_profile)

            if dry_run:
                get_console().print("[warning]Dry run — skipping.[/]")
                continue

            action = prompt_triage_action(
                f"Action for PR {_pr_link(pr)}?",
                default=TriageAction.SKIP,
                forced_answer=answer_triage,
            )

            if action == TriageAction.QUIT:
                get_console().print("[warning]Quitting.[/]")
                quit_early = True
                break

            if action == TriageAction.READY:
                get_console().print(
                    f"  [info]Marking PR {_pr_link(pr)} as ready "
                    f"— adding '{_READY_FOR_REVIEW_LABEL}' label.[/]"
                )
                if _add_label(token, github_repository, pr.node_id, _READY_FOR_REVIEW_LABEL):
                    get_console().print(
                        f"  [success]Label '{_READY_FOR_REVIEW_LABEL}' added to PR {_pr_link(pr)}.[/]"
                    )
                    total_ready += 1
                else:
                    get_console().print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")
            else:
                get_console().print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
                total_skipped_action += 1

    # Phase 6: Present NOT_RUN PRs for workflow approval
    total_workflows_approved = 0
    if not quit_early and pending_approval:
        pending_approval.sort(key=lambda p: (p.author_login.lower(), p.number))
        get_console().print(
            f"\n[info]{len(pending_approval)} {'PRs have' if len(pending_approval) != 1 else 'PR has'} "
            f"no test workflows run — review and approve workflow runs:[/]\n"
        )
        for pr in pending_approval:
            author_profile = _fetch_author_profile(token, pr.author_login, github_repository)
            pending_runs = _find_pending_workflow_runs(token, github_repository, pr.head_sha)
            _display_workflow_approval_panel(pr, author_profile, pending_runs)

            # If author exceeds the close threshold, suggest closing instead of approving
            author_count = author_flagged_count.get(pr.author_login, 0)
            if author_count > 3:
                get_console().print(
                    f"  [bold red]Author {pr.author_login} has {author_count} flagged "
                    f"{'PRs' if author_count != 1 else 'PR'} "
                    f"— suggesting close instead of workflow approval.[/]"
                )
                close_comment = _build_close_comment(pr.author_login, [], pr.number, author_count)
                get_console().print(Panel(close_comment, title="Proposed close comment", border_style="red"))

                if dry_run:
                    get_console().print("[warning]Dry run — would default to: close[/]")
                    continue

                action = prompt_triage_action(
                    f"Action for PR {_pr_link(pr)}?",
                    default=TriageAction.CLOSE,
                    forced_answer=answer_triage,
                )
                if action == TriageAction.QUIT:
                    get_console().print("[warning]Quitting.[/]")
                    quit_early = True
                    break
                if action == TriageAction.SKIP:
                    get_console().print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
                    continue
                if action == TriageAction.CLOSE:
                    get_console().print(f"  Closing PR {_pr_link(pr)}...")
                    if _close_pr(token, pr.node_id):
                        get_console().print(f"  [success]PR {_pr_link(pr)} closed.[/]")
                    else:
                        get_console().print(f"  [error]Failed to close PR {_pr_link(pr)}.[/]")
                        continue
                    if _add_label(token, github_repository, pr.node_id, _CLOSED_QUALITY_LABEL):
                        get_console().print(
                            f"  [success]Label '{_CLOSED_QUALITY_LABEL}' added to PR {_pr_link(pr)}.[/]"
                        )
                    else:
                        get_console().print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")
                    get_console().print(f"  Posting comment on PR {_pr_link(pr)}...")
                    if _post_comment(token, pr.node_id, close_comment):
                        get_console().print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
                        total_closed += 1
                    else:
                        get_console().print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")
                    continue
                # For DRAFT or READY, fall through to normal workflow approval
                # (approve workflows first, then triage later)

            if dry_run:
                get_console().print("[warning]Dry run — skipping workflow approval.[/]")
                continue

            if not pending_runs:
                get_console().print(
                    f"  [dim]No pending workflow runs found for PR {_pr_link(pr)}. "
                    f"Workflows may need to be triggered manually.[/]"
                )
                continue

            answer = user_confirm(
                f"Review diff for PR {_pr_link(pr)} before approving workflows?",
                forced_answer=answer_triage,
            )
            if answer == Answer.QUIT:
                get_console().print("[warning]Quitting.[/]")
                quit_early = True
                break
            if answer == Answer.NO:
                get_console().print(f"  [info]Skipping workflow approval for PR {_pr_link(pr)}.[/]")
                continue

            get_console().print(f"  Fetching diff for PR {_pr_link(pr)}...")
            diff_text = _fetch_pr_diff(token, github_repository, pr.number)
            if diff_text:
                from rich.syntax import Syntax

                get_console().print(
                    Panel(
                        Syntax(diff_text, "diff", theme="monokai", word_wrap=True),
                        title=f"Diff for PR {_pr_link(pr)}",
                        border_style="bright_cyan",
                    )
                )
            else:
                get_console().print(
                    f"  [warning]Could not fetch diff for PR {_pr_link(pr)}. "
                    f"Review manually at: {pr.url}/files[/]"
                )

            answer = user_confirm(
                f"No suspicious changes found in PR {_pr_link(pr)}? "
                f"Approve {len(pending_runs)} workflow {'runs' if len(pending_runs) != 1 else 'run'}?",
                forced_answer=answer_triage,
            )
            if answer == Answer.QUIT:
                get_console().print("[warning]Quitting.[/]")
                quit_early = True
                break
            if answer == Answer.NO:
                get_console().print(
                    f"\n  [bold red]Suspicious changes detected in PR {_pr_link(pr)} by {pr.author_login}.[/]"
                )
                get_console().print(f"  Fetching all open PRs by {pr.author_login}...")
                author_prs = _fetch_author_open_prs(token, github_repository, pr.author_login)
                if not author_prs:
                    get_console().print(f"  [dim]No open PRs found for {pr.author_login}.[/]")
                    continue

                get_console().print()
                get_console().print(
                    f"  [bold red]The following {len(author_prs)} "
                    f"{'PRs' if len(author_prs) != 1 else 'PR'} by "
                    f"{pr.author_login} will be closed, labeled "
                    f"'{_SUSPICIOUS_CHANGES_LABEL}', and commented:[/]"
                )
                for pr_info in author_prs:
                    get_console().print(
                        f"    - [link={pr_info['url']}]#{pr_info['number']}[/link] {pr_info['title']}"
                    )
                get_console().print()

                confirm = user_confirm(
                    f"Close all {len(author_prs)} {'PRs' if len(author_prs) != 1 else 'PR'} "
                    f"by {pr.author_login} and label as suspicious?",
                    forced_answer=answer_triage,
                )
                if confirm == Answer.QUIT:
                    get_console().print("[warning]Quitting.[/]")
                    quit_early = True
                    break
                if confirm == Answer.NO:
                    get_console().print(f"  [info]Skipping — no PRs closed for {pr.author_login}.[/]")
                    continue

                closed, commented = _close_suspicious_prs(token, github_repository, author_prs, pr.number)
                get_console().print(
                    f"  [success]Closed {closed}/{len(author_prs)} "
                    f"{'PRs' if len(author_prs) != 1 else 'PR'}, commented on {commented}.[/]"
                )
                total_closed += closed
                continue

            approved = _approve_workflow_runs(token, github_repository, pending_runs)
            if approved:
                get_console().print(
                    f"  [success]Approved {approved}/{len(pending_runs)} workflow "
                    f"{'runs' if len(pending_runs) != 1 else 'run'} for PR "
                    f"{_pr_link(pr)}.[/]"
                )
                total_workflows_approved += 1
            else:
                get_console().print(f"  [error]Failed to approve workflow runs for PR {_pr_link(pr)}.[/]")

    # Summary
    get_console().print()
    summary_table = Table(title="Summary")
    summary_table.add_column("Metric", style="bold")
    summary_table.add_column("Count", justify="right")
    total_skipped = total_skipped_collaborator + total_skipped_bot + total_skipped_accepted
    summary_table.add_row("PRs fetched", str(len(all_prs)))
    if verbose:
        summary_table.add_row("Collaborators skipped", str(total_skipped_collaborator))
        summary_table.add_row("Bots skipped", str(total_skipped_bot))
        summary_table.add_row("Ready-for-review skipped", str(total_skipped_accepted))
    summary_table.add_row("PRs skipped (filtered)", str(total_skipped))
    summary_table.add_row("PRs assessed", str(len(candidate_prs)))
    summary_table.add_row("Flagged by CI/conflicts/comments", str(total_deterministic_flags))
    summary_table.add_row("Flagged by LLM", str(total_flagged - total_deterministic_flags))
    summary_table.add_row("LLM errors (skipped)", str(total_llm_errors))
    summary_table.add_row("Total flagged", str(total_flagged))
    summary_table.add_row("PRs passing all checks", str(len(passing_prs)))
    summary_table.add_row("PRs converted to draft", str(total_converted))
    summary_table.add_row("PRs commented (not drafted)", str(total_commented))
    summary_table.add_row("PRs closed", str(total_closed))
    summary_table.add_row("PRs marked ready for review", str(total_ready))
    summary_table.add_row("PRs skipped (no action)", str(total_skipped_action))
    summary_table.add_row("Awaiting workflow approval", str(len(pending_approval)))
    summary_table.add_row("PRs with workflows approved", str(total_workflows_approved))
    get_console().print(summary_table)
