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
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import click
from rich.panel import Panel
from rich.table import Table

from airflow_breeze.commands.common_options import (
    option_github_repository,
    option_github_token,
)
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.console import console_print, get_console
from airflow_breeze.utils.pr_cache import (
    get_cached_classification as _get_cached_classification,
    save_classification_cache as _save_classification_cache,
)
from airflow_breeze.utils.pr_models import (
    PRData,
    ReviewDecision,
)
from airflow_breeze.utils.run_utils import run_command

QUALITY_CRITERIA_LINK = (
    "[Pull Request quality criteria](https://github.com/apache/airflow/blob/main/"
    "contributing-docs/05_pull_requests.rst#pull-request-quality-criteria)"
)

# authorAssociation values that indicate the author has write access


_COLLABORATOR_ASSOCIATIONS = {"COLLABORATOR", "MEMBER", "OWNER"}


_READY_FOR_REVIEW_LABEL = "ready for maintainer review"


_TRIAGE_COMMENT_MARKER = "Pull Request quality criteria"


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
        isDraft
        mergeable
        labels(first: 20) {
          nodes { name }
        }
        reviews(last: 20) {
          nodes { author { login } authorAssociation state }
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


_VIEWER_QUERY = """
query { viewer { login } }
"""


def _resolve_viewer_login(token: str) -> str:
    """Resolve the GitHub login of the authenticated user via the viewer query."""
    data = _graphql_request(token, _VIEWER_QUERY, {})
    return data["viewer"]["login"]


def _graphql_request(token: str, query: str, variables: dict, *, max_retries: int = 3) -> dict:
    """Execute a GitHub GraphQL request with automatic retry on transient errors.

    Returns the 'data' dict or exits on persistent error.
    """
    import requests

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                "https://api.github.com/graphql",
                json={"query": query, "variables": variables},
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                timeout=(10, 30),
            )
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            if attempt < max_retries:
                time.sleep(2 * attempt)
                continue
            console_print(f"[error]GraphQL request failed after {max_retries} attempts: {exc}[/]")
            sys.exit(1)

        if response.status_code == 502 or response.status_code == 503:
            if attempt < max_retries:
                time.sleep(2 * attempt)
                continue
        if response.status_code != 200:
            console_print(f"[error]GraphQL request failed: {response.status_code} {response.text}[/]")
            sys.exit(1)
        result = response.json()
        if "errors" in result:
            console_print(f"[error]GraphQL errors: {result['errors']}[/]")
            sys.exit(1)
        return result["data"]

    # Should not reach here, but just in case
    console_print(f"[error]GraphQL request failed after {max_retries} attempts: {last_exc}[/]")
    sys.exit(1)


_COMMITS_BEHIND_BATCH_SIZE = 20


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


def _extract_review_decisions(pr_node: dict) -> list[ReviewDecision]:
    """Extract the latest review decision per reviewer from a GraphQL PR node.

    Keeps only the most recent review per author (last wins).
    Excludes COMMENTED and DISMISSED — only APPROVED and CHANGES_REQUESTED are meaningful.
    """
    reviews_nodes = (pr_node.get("reviews") or {}).get("nodes", [])
    latest: dict[str, tuple[str, str]] = {}  # login -> (state, association)
    for review in reviews_nodes:
        author = (review.get("author") or {}).get("login", "")
        state = review.get("state", "")
        association = review.get("authorAssociation", "")
        if author and state in ("APPROVED", "CHANGES_REQUESTED"):
            latest[author] = (state, association)
    return [
        ReviewDecision(reviewer_login=login, state=state, reviewer_association=association)
        for login, (state, association) in latest.items()
    ]


def _classify_already_triaged_prs(
    token: str,
    github_repository: str,
    prs: list[PRData],
    viewer_login: str,
    *,
    require_marker: bool = True,
    on_progress: Callable[[int, int], None] | None = None,
) -> dict[str, set[int]]:
    """Classify already-triaged PRs into waiting vs responded vs stale_draft.

    Returns a dict with keys:
    - "waiting": PR numbers where we commented but author has not responded
    - "responded": PR numbers where author responded after our triage comment
    - "stale_draft": PR numbers that are drafts, triaged >7 days ago, with no author response

    :param require_marker: if True, only match comments containing _TRIAGE_COMMENT_MARKER.
        If False, match any comment from the viewer (useful for workflow approval PRs
        where a rebase comment may have been posted instead of a full triage comment).
    """
    result: dict[str, set[int]] = {"waiting": set(), "responded": set(), "stale_draft": set()}
    if not prs:
        return result

    owner, repo = github_repository.split("/", 1)

    # Check cache first — only fetch uncached PRs
    uncached_prs: list[PRData] = []
    for pr in prs:
        if pr.head_sha and require_marker:
            cached_cls, _cached_act = _get_cached_classification(
                github_repository, pr.number, pr.head_sha, viewer_login
            )
            if cached_cls in ("waiting", "responded", "acted"):
                result[cached_cls if cached_cls != "acted" else "waiting"].add(pr.number)
                continue
            if cached_cls == "none":
                continue  # Known not-triaged, skip API call
        uncached_prs.append(pr)

    if on_progress and uncached_prs:
        # Report cache hits as progress
        cache_hits = len(prs) - len(uncached_prs)
        if cache_hits:
            on_progress(cache_hits, len(prs))

    # Batch fetch last 10 comments + last commit date for uncached PRs
    for chunk_start in range(0, len(uncached_prs), _COMMITS_BEHIND_BATCH_SIZE):
        chunk = uncached_prs[chunk_start : chunk_start + _COMMITS_BEHIND_BATCH_SIZE]

        pr_fields = []
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_fields.append(
                f"    {alias}: pullRequest(number: {pr.number}) {{\n"
                f"      comments(last: 10) {{\n"
                f"        nodes {{ author {{ login }} body createdAt }}\n"
                f"      }}\n"
                f"      commits(last: 1) {{\n"
                f"        nodes {{ commit {{ committedDate }} }}\n"
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

            # Get last commit date
            commits = pr_data.get("commits", {}).get("nodes", [])
            last_commit_date = ""
            if commits:
                last_commit_date = commits[0].get("commit", {}).get("committedDate", "")

            # Check if any comment is from the viewer and contains the triage marker
            comments = pr_data.get("comments", {}).get("nodes", [])
            triage_comment_date = ""
            for comment in reversed(comments):
                author = (comment.get("author") or {}).get("login", "")
                body = comment.get("body", "")
                comment_date = comment.get("createdAt", "")

                if (
                    author == viewer_login
                    and (not require_marker or _TRIAGE_COMMENT_MARKER in body)
                    and comment_date >= last_commit_date
                ):
                    triage_comment_date = comment_date
                    break

            if not triage_comment_date:
                # Not triaged — cache this negative result too (avoids re-fetching)
                if pr.head_sha and require_marker:
                    _save_classification_cache(
                        github_repository, pr.number, pr.head_sha, viewer_login, "none"
                    )
                continue

            # Check if the PR author responded after our triage comment
            author_responded = False
            for comment in comments:
                comment_author = (comment.get("author") or {}).get("login", "")
                comment_date = comment.get("createdAt", "")
                if comment_author == pr.author_login and comment_date > triage_comment_date:
                    author_responded = True
                    break

            if author_responded:
                classification = "responded"
            elif pr.is_draft and triage_comment_date:
                # Check if triage comment is older than 7 days — stale draft
                from datetime import datetime, timezone

                try:
                    triage_dt = datetime.fromisoformat(triage_comment_date.replace("Z", "+00:00"))
                    days_since_triage = (datetime.now(timezone.utc) - triage_dt).days
                    classification = "stale_draft" if days_since_triage >= 7 else "waiting"
                except (ValueError, TypeError):
                    classification = "waiting"
            else:
                classification = "waiting"
            result[classification].add(pr.number)
            # Cache the classification result (keyed by head_sha — invalidated on new commits)
            if pr.head_sha and require_marker:
                _save_classification_cache(
                    github_repository, pr.number, pr.head_sha, viewer_login, classification
                )
        if on_progress:
            cache_hits = len(prs) - len(uncached_prs)
            on_progress(min(cache_hits + chunk_start + len(chunk), len(prs)), len(prs))

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
    review_requested: str | None = None,
    reviewed_by: str | None = None,
    after_cursor: str | None = None,
    quiet: bool = False,
) -> tuple[list[PRData], bool, str | None, int]:
    """Fetch a single batch of matching PRs via GraphQL.

    Returns (prs, has_next_page, end_cursor, total_issue_count).
    """
    query_parts = [f"repo:{github_repository}", "type:pr", "is:open"]
    if filter_user:
        query_parts.append(f"author:{filter_user}")
    if review_requested:
        query_parts.append(f"review-requested:{review_requested}")
    if reviewed_by:
        query_parts.append(f"reviewed-by:{reviewed_by}")
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

    if not after_cursor and not quiet:
        console_print(f"[info]Searching PRs: {search_query}[/]")

    variables: dict = {"query": search_query, "first": batch_size}
    if after_cursor:
        variables["after"] = after_cursor

    data = _graphql_request(token, _SEARCH_PRS_QUERY, variables)
    search_data = data["search"]
    page_info = search_data.get("pageInfo", {})
    has_next_page = page_info.get("hasNextPage", False)
    end_cursor = page_info.get("endCursor")

    if not quiet:
        console_print(
            f"[info]Found {search_data['issueCount']} matching "
            f"{'PRs' if search_data['issueCount'] != 1 else 'PR'}, "
            f"fetched {len(search_data['nodes'])}"
            f"{' (more available)' if has_next_page else ''}.[/]"
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
                is_draft=bool(node.get("isDraft", False)),
                mergeable=node.get("mergeable", "UNKNOWN"),
                labels=[lbl["name"] for lbl in (node.get("labels") or {}).get("nodes", []) if lbl],
                unresolved_threads=[],
                review_decisions=_extract_review_decisions(node),
            )
        )

    # Persist fetched PRs to vault for reuse across sessions
    from airflow_breeze.utils.pr_vault import save_prs_batch

    save_prs_batch(github_repository, prs)

    return prs, has_next_page, end_cursor, search_data["issueCount"]


_AGE_BUCKET_LABELS = ["<1w", "1-2w", "2-4w", ">1m"]


_AGE_BUCKET_THRESHOLDS: list[tuple[int, str]] = [
    (7, "<1w"),
    (14, "1-2w"),
    (28, "2-4w"),
]


_DRAFT_AGE_BUCKET_LABELS = ["<1w", "1-2w", "2-4w", ">1m"]


@dataclass
class _AreaStats:
    """Aggregated statistics for a single area label."""

    total: int = 0
    drafts: int = 0
    non_drafts: int = 0
    contributors: int = 0  # non-collaborator authors
    triaged_waiting: int = 0
    triaged_responded: int = 0
    ready_for_review: int = 0
    triager_drafted: int = 0
    draft_age_buckets: dict[str, int] = field(
        default_factory=lambda: {b: 0 for b in _DRAFT_AGE_BUCKET_LABELS}
    )
    age_buckets: dict[str, int] = field(default_factory=lambda: {b: 0 for b in _AGE_BUCKET_LABELS})


def _compute_age_bucket(last_interaction_iso: str) -> str:
    """Map an ISO-8601 datetime string to an age bucket label."""
    from datetime import datetime, timezone

    try:
        dt = datetime.fromisoformat(last_interaction_iso.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return ">1m"
    delta = datetime.now(tz=timezone.utc) - dt
    days = delta.total_seconds() / 86400
    for threshold_days, label in _AGE_BUCKET_THRESHOLDS:
        if days < threshold_days:
            return label
    return ">1m"


def _fetch_all_open_prs(token: str, github_repository: str, batch_size: int = 100) -> list[PRData]:
    """Fetch all open PRs from the repository, paginating through all results."""
    console = get_console()
    all_prs: list[PRData] = []
    after_cursor: str | None = None
    page = 0

    while True:
        page += 1
        prs, has_next_page, end_cursor, total_count = _fetch_prs_graphql(
            token,
            github_repository,
            labels=(),
            exclude_labels=(),
            filter_user=None,
            sort="created-desc",
            batch_size=batch_size,
            after_cursor=after_cursor,
            quiet=page > 1,
        )
        all_prs.extend(prs)
        if page == 1:
            console.print(f"[info]Total open PRs: {total_count}[/]")
        else:
            console.print(f"[info]  Fetched page {page}: {len(all_prs)}/{total_count} PRs[/]")

        if not has_next_page:
            break
        after_cursor = end_cursor

    return all_prs


@dataclass
class _PRInteractionData:
    """Interaction data for a single PR."""

    last_interaction: str  # ISO-8601 datetime of last author interaction
    drafted_by_triager_at: str  # ISO-8601 datetime when triager converted to draft, or ""


def _fetch_pr_interaction_data(
    token: str, github_repository: str, prs: list[PRData], viewer_login: str
) -> dict[int, _PRInteractionData]:
    """Fetch interaction data for each PR: last author activity and triager draft conversion.

    Uses a file-based cache keyed by PR number and ``updated_at`` — if a PR hasn't
    been updated since the last run, the cached data is reused.
    """
    from airflow_breeze.utils.pr_cache import stats_interaction_cache

    if not prs:
        return {}

    owner, repo = github_repository.split("/", 1)
    result: dict[int, _PRInteractionData] = {}

    # Check cache — only fetch PRs whose updated_at changed since last cache write
    uncached_prs: list[PRData] = []
    for pr in prs:
        cached = stats_interaction_cache.get(
            github_repository, f"pr_{pr.number}", match={"updated_at": pr.updated_at}
        )
        if cached and "last_interaction" in cached:
            result[pr.number] = _PRInteractionData(
                last_interaction=cached["last_interaction"],
                drafted_by_triager_at=cached.get("drafted_by_triager_at", ""),
            )
        else:
            uncached_prs.append(pr)

    console = get_console()
    cache_hits = len(prs) - len(uncached_prs)
    console.print(
        f"[info]Fetching PR interaction data: {cache_hits}/{len(prs)} cached, "
        f"{len(uncached_prs)} to fetch...[/]"
    )

    if not uncached_prs:
        return result

    chunk_size = _COMMITS_BEHIND_BATCH_SIZE  # reuse existing batch constant (20)

    for chunk_start in range(0, len(uncached_prs), chunk_size):
        chunk = uncached_prs[chunk_start : chunk_start + chunk_size]

        pr_fields = []
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_fields.append(
                f"    {alias}: pullRequest(number: {pr.number}) {{\n"
                f"      comments(last: 10) {{\n"
                f"        nodes {{ author {{ login }} createdAt }}\n"
                f"      }}\n"
                f"      commits(last: 1) {{\n"
                f"        nodes {{ commit {{ committedDate }} }}\n"
                f"      }}\n"
                f"      timelineItems(last: 10, itemTypes: CONVERT_TO_DRAFT_EVENT) {{\n"
                f"        nodes {{\n"
                f"          ... on ConvertToDraftEvent {{\n"
                f"            createdAt\n"
                f"            actor {{ login }}\n"
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
            # On API failure, fall back to updated_at for this chunk
            for pr in chunk:
                result[pr.number] = _PRInteractionData(
                    last_interaction=pr.updated_at or pr.created_at,
                    drafted_by_triager_at="",
                )
            continue

        repo_data = data.get("repository", {})
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_data = repo_data.get(alias) or {}

            # Start with PR creation as the baseline
            latest = pr.created_at

            # Check last commit date
            commits = pr_data.get("commits", {}).get("nodes", [])
            if commits:
                commit_date = commits[0].get("commit", {}).get("committedDate", "")
                if commit_date and commit_date > latest:
                    latest = commit_date

            # Check last comment from the PR author
            comments = pr_data.get("comments", {}).get("nodes", [])
            for comment in reversed(comments):
                comment_author = (comment.get("author") or {}).get("login", "")
                if comment_author == pr.author_login:
                    comment_date = comment.get("createdAt", "")
                    if comment_date and comment_date > latest:
                        latest = comment_date
                    break

            # Check if the triager (viewer) converted this PR to draft
            drafted_by_triager_at = ""
            timeline_nodes = pr_data.get("timelineItems", {}).get("nodes", [])
            for event in reversed(timeline_nodes):
                actor = (event.get("actor") or {}).get("login", "")
                if actor == viewer_login:
                    drafted_by_triager_at = event.get("createdAt", "")
                    break

            result[pr.number] = _PRInteractionData(
                last_interaction=latest,
                drafted_by_triager_at=drafted_by_triager_at,
            )
            # Cache the result keyed by updated_at so it invalidates on any PR change
            stats_interaction_cache.save(
                github_repository,
                f"pr_{pr.number}",
                {
                    "updated_at": pr.updated_at,
                    "last_interaction": latest,
                    "drafted_by_triager_at": drafted_by_triager_at,
                },
            )

        if chunk_start + chunk_size < len(uncached_prs):
            console.print(
                f"[info]  Fetched interactions: "
                f"{min(chunk_start + chunk_size, len(uncached_prs))}/{len(uncached_prs)}[/]"
            )

    return result


def _aggregate_stats_by_area(
    prs: list[PRData],
    triage_classification: dict[str, set[int]],
    interaction_data: dict[int, _PRInteractionData],
) -> dict[str, _AreaStats]:
    """Aggregate PR statistics grouped by area: labels."""
    waiting = triage_classification.get("waiting", set())
    responded = triage_classification.get("responded", set())
    area_stats: dict[str, _AreaStats] = {}

    for pr in prs:
        # Extract area labels
        areas = [label.removeprefix("area:") for label in pr.labels if label.startswith("area:")]
        if not areas:
            areas = ["(no area)"]

        pr_data = interaction_data.get(pr.number)
        last_interaction = pr_data.last_interaction if pr_data else pr.created_at
        drafted_at = pr_data.drafted_by_triager_at if pr_data else ""

        age_bucket = _compute_age_bucket(last_interaction)
        is_ready = _READY_FOR_REVIEW_LABEL in pr.labels
        is_triaged_waiting = pr.number in waiting
        is_triaged_responded = pr.number in responded

        is_contributor = pr.author_association not in _COLLABORATOR_ASSOCIATIONS

        for area in areas:
            stats = area_stats.setdefault(area, _AreaStats())
            stats.total += 1
            if pr.is_draft:
                stats.drafts += 1
            else:
                stats.non_drafts += 1
            if is_contributor:
                stats.contributors += 1
            if is_triaged_waiting:
                stats.triaged_waiting += 1
            if is_triaged_responded:
                stats.triaged_responded += 1
            if is_ready:
                stats.ready_for_review += 1
            stats.age_buckets[age_bucket] += 1
            if drafted_at:
                stats.triager_drafted += 1
                draft_bucket = _compute_age_bucket(drafted_at)
                stats.draft_age_buckets[draft_bucket] += 1

    return area_stats


def _compute_totals(
    prs: list[PRData],
    triage_classification: dict[str, set[int]],
    interaction_data: dict[int, _PRInteractionData],
) -> _AreaStats:
    """Compute totals across all PRs (unique counts, not sum of per-area)."""
    waiting = triage_classification.get("waiting", set())
    responded = triage_classification.get("responded", set())
    totals = _AreaStats()
    for pr in prs:
        totals.total += 1
        if pr.is_draft:
            totals.drafts += 1
        else:
            totals.non_drafts += 1
        if pr.author_association not in _COLLABORATOR_ASSOCIATIONS:
            totals.contributors += 1
        if pr.number in waiting:
            totals.triaged_waiting += 1
        if pr.number in responded:
            totals.triaged_responded += 1
        if _READY_FOR_REVIEW_LABEL in pr.labels:
            totals.ready_for_review += 1
        pr_data = interaction_data.get(pr.number)
        last_interaction = pr_data.last_interaction if pr_data else pr.created_at
        drafted_at = pr_data.drafted_by_triager_at if pr_data else ""
        totals.age_buckets[_compute_age_bucket(last_interaction)] += 1
        if drafted_at:
            totals.triager_drafted += 1
            totals.draft_age_buckets[_compute_age_bucket(drafted_at)] += 1
    return totals


def _pct(numerator: int, denominator: int) -> str:
    """Format a percentage, returning '-' when the denominator is zero."""
    return f"{100 * numerator / denominator:.0f}%" if denominator else "-"


def _render_stats_tables(
    area_stats: dict[str, _AreaStats],
    totals: _AreaStats,
    github_repository: str,
    closed_stats: dict[str, _ClosedTriagedAreaStats],
    closed_since_date: str,
) -> None:
    """Render two tables: triaged final-state and triaged still-open."""
    console = get_console()
    closed_totals = closed_stats.get("__total__", _ClosedTriagedAreaStats())

    # Collect all area names across both open and closed stats
    all_areas = set(area_stats.keys()) | {k for k in closed_stats if k != "__total__"}
    sorted_area_names = sorted(
        all_areas, key=lambda a: (a == "(no area)", -(area_stats.get(a, _AreaStats()).total))
    )

    # ── Table 1: Triaged PRs — Final State (closed/merged) ──────────
    t1 = Table(
        title=f"Triaged PRs — Final State since {closed_since_date} ({github_repository})",
        title_style="bold",
        show_lines=True,
        show_footer=True,
    )
    t1.add_column("Area", style="bold cyan", min_width=12, footer="Area")
    t1.add_column("Triaged\nTotal", justify="right", style="yellow", footer="Triaged\nTotal")
    t1.add_column("Closed", justify="right", style="red", footer="Closed")
    t1.add_column("%Closed", justify="right", footer="%Closed")
    t1.add_column("Merged", justify="right", style="green", footer="Merged")
    t1.add_column("%Merged", justify="right", footer="%Merged")
    t1.add_column("Responded", justify="right", footer="Responded")
    t1.add_column("%Responded", justify="right", footer="%Responded")

    for area in sorted_area_names:
        cs = closed_stats.get(area, _ClosedTriagedAreaStats())
        if cs.total == 0:
            continue
        t1.add_row(
            area,
            str(cs.total),
            str(cs.closed),
            cs.pct_closed,
            str(cs.merged),
            cs.pct_merged,
            str(cs.responded_before_close),
            cs.pct_responded,
        )

    t1.add_row(
        "[bold white]TOTAL[/]",
        f"[bold white]{closed_totals.total}[/]",
        f"[bold white]{closed_totals.closed}[/]",
        f"[bold white]{closed_totals.pct_closed}[/]",
        f"[bold white]{closed_totals.merged}[/]",
        f"[bold white]{closed_totals.pct_merged}[/]",
        f"[bold white]{closed_totals.responded_before_close}[/]",
        f"[bold white]{closed_totals.pct_responded}[/]",
        style="on grey7",
        end_section=True,
    )

    console.print()
    console.print(t1)

    # ── Table 2: Triaged PRs — Still Open ────────────────────────────
    t2 = Table(
        title=f"Triaged PRs — Still Open ({github_repository})",
        title_style="bold",
        show_lines=True,
        show_footer=True,
    )
    t2.add_column("Area", style="bold cyan", min_width=12, footer="Area")
    t2.add_column("Total", justify="right", footer="Total")
    t2.add_column("Draft", justify="right", footer="Draft")
    t2.add_column("%Draft", justify="right", footer="%Draft")
    t2.add_column("Non-Draft", justify="right", footer="Non-Draft")
    t2.add_column("Contrib.", justify="right", footer="Contrib.")
    t2.add_column("%Contrib.", justify="right", footer="%Contrib.")
    t2.add_column("Triaged", justify="right", style="yellow", footer="Triaged")
    t2.add_column("Responded", justify="right", style="green", footer="Responded")
    t2.add_column("%Responded", justify="right", footer="%Responded")
    t2.add_column("Ready", justify="right", style="bold green", footer="Ready")
    t2.add_column("%Ready", justify="right", footer="%Ready")
    t2.add_column("Drafted\nby triager", justify="right", style="magenta", footer="Drafted\nby triager")
    for bucket in _DRAFT_AGE_BUCKET_LABELS:
        t2.add_column(f"Drafted\n{bucket}", justify="right", style="magenta dim", footer=f"Drafted\n{bucket}")
    for bucket in _AGE_BUCKET_LABELS:
        t2.add_column(f"Author resp\n{bucket}", justify="right", style="dim", footer=f"Author resp\n{bucket}")

    for area in sorted_area_names:
        s = area_stats.get(area)
        if not s or s.total == 0:
            continue
        triaged = s.triaged_waiting + s.triaged_responded
        t2.add_row(
            area,
            str(s.total),
            str(s.drafts),
            _pct(s.drafts, s.total),
            str(s.non_drafts),
            str(s.contributors),
            _pct(s.contributors, s.total),
            str(triaged),
            str(s.triaged_responded),
            _pct(s.triaged_responded, triaged),
            str(s.ready_for_review),
            _pct(s.ready_for_review, s.total),
            str(s.triager_drafted),
            *[str(s.draft_age_buckets.get(b, 0)) for b in _DRAFT_AGE_BUCKET_LABELS],
            *[str(s.age_buckets.get(b, 0)) for b in _AGE_BUCKET_LABELS],
        )

    total_triaged = totals.triaged_waiting + totals.triaged_responded
    t2.add_row(
        "[bold white]TOTAL[/]",
        f"[bold white]{totals.total}[/]",
        f"[bold white]{totals.drafts}[/]",
        f"[bold white]{_pct(totals.drafts, totals.total)}[/]",
        f"[bold white]{totals.non_drafts}[/]",
        f"[bold white]{totals.contributors}[/]",
        f"[bold white]{_pct(totals.contributors, totals.total)}[/]",
        f"[bold white]{total_triaged}[/]",
        f"[bold white]{totals.triaged_responded}[/]",
        f"[bold white]{_pct(totals.triaged_responded, total_triaged)}[/]",
        f"[bold white]{totals.ready_for_review}[/]",
        f"[bold white]{_pct(totals.ready_for_review, totals.total)}[/]",
        f"[bold white]{totals.triager_drafted}[/]",
        *[f"[bold white]{totals.draft_age_buckets.get(b, 0)}[/]" for b in _DRAFT_AGE_BUCKET_LABELS],
        *[f"[bold white]{totals.age_buckets.get(b, 0)}[/]" for b in _AGE_BUCKET_LABELS],
        style="on grey7",
        end_section=True,
    )

    console.print()
    console.print(t2)

    # ── Legend ────────────────────────────────────────────────────────
    legend_lines = [
        "[bold]Column legend:[/]",
        "  [bold]Contrib.[/]      = PRs by non-collaborator contributors",
        "  [yellow]Triaged[/]       = PRs where a triage comment was posted",
        "  [green]Responded[/]     = author replied after the triage comment",
        "  [bold green]Ready[/]         = PRs with the [bold]'ready for maintainer review'[/] label",
        "  [magenta]Drafted by triager[/] = PRs converted to draft by the triager",
        "",
        "[bold]Author resp[/] columns show time since the PR author's last interaction "
        "(comment, commit, or PR creation).",
        "[bold magenta]Drafted[/] columns show time since the triager converted the PR to draft.",
    ]
    console.print(Panel("\n".join(legend_lines), border_style="dim", expand=False))
    console.print()


_CLOSED_TRIAGED_SINCE = "2026-03-11"


_CLOSED_PRS_SEARCH_QUERY = """
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
        author { login }
        authorAssociation
        state
        mergedAt
        closedAt
        labels(first: 20) {
          nodes { name }
        }
        comments(last: 20) {
          nodes { author { login } body createdAt }
        }
      }
    }
  }
}
"""


@dataclass
class _ClosedTriagedAreaStats:
    """Per-area stats for triaged PRs that reached a final state."""

    closed: int = 0
    merged: int = 0
    responded_before_close: int = 0
    contributors: int = 0  # non-collaborator authors

    @property
    def total(self) -> int:
        return self.closed + self.merged

    @property
    def pct_closed(self) -> str:
        return f"{100 * self.closed / self.total:.0f}%" if self.total else "-"

    @property
    def pct_merged(self) -> str:
        return f"{100 * self.merged / self.total:.0f}%" if self.total else "-"

    @property
    def pct_responded(self) -> str:
        return f"{100 * self.responded_before_close / self.total:.0f}%" if self.total else "-"

    @property
    def pct_contributors(self) -> str:
        return f"{100 * self.contributors / self.total:.0f}%" if self.total else "-"


def _fetch_closed_triaged_prs(
    token: str, github_repository: str, viewer_login: str, since: str
) -> dict[str, _ClosedTriagedAreaStats]:
    """Fetch closed/merged PRs since *since* that had a triage comment.

    Returns a dict mapping area label (or "(no area)") to stats.
    Also includes a "__total__" key with aggregate counts.
    """
    console = get_console()
    console.print(f"[info]Fetching closed/merged triaged PRs since {since}...[/]")

    search_query = (
        f"repo:{github_repository} type:pr is:closed closed:>={since} "
        f"commenter:{viewer_login} sort:updated-desc"
    )

    area_stats: dict[str, _ClosedTriagedAreaStats] = {}
    total_stats = _ClosedTriagedAreaStats()
    after_cursor: str | None = None

    while True:
        variables: dict = {"query": search_query, "first": 100}
        if after_cursor:
            variables["after"] = after_cursor

        try:
            data = _graphql_request(token, _CLOSED_PRS_SEARCH_QUERY, variables)
        except SystemExit:
            break

        search_data = data["search"]
        for node in search_data.get("nodes") or []:
            if not node:
                continue
            comments = (node.get("comments") or {}).get("nodes") or []
            author_login = (node.get("author") or {}).get("login", "")

            # Check if any comment from viewer contains the triage marker
            triage_comment_date = ""
            for comment in reversed(comments):
                commenter = (comment.get("author") or {}).get("login", "")
                body = comment.get("body", "")
                if commenter == viewer_login and _TRIAGE_COMMENT_MARKER in body:
                    triage_comment_date = comment.get("createdAt", "")
                    break

            if not triage_comment_date:
                continue

            # Determine state: MERGED or CLOSED
            is_merged = node.get("state") == "MERGED" or bool(node.get("mergedAt"))
            is_contributor = node.get("authorAssociation", "") not in _COLLABORATOR_ASSOCIATIONS

            # Extract area labels
            labels = [n["name"] for n in (node.get("labels") or {}).get("nodes") or []]
            areas = [lbl.removeprefix("area:") for lbl in labels if lbl.startswith("area:")]
            if not areas:
                areas = ["(no area)"]

            # Check if author responded after triage
            author_responded = False
            for comment in comments:
                commenter = (comment.get("author") or {}).get("login", "")
                comment_date = comment.get("createdAt", "")
                if commenter == author_login and comment_date > triage_comment_date:
                    author_responded = True
                    break

            # Update per-area stats
            for area in areas:
                stats = area_stats.setdefault(area, _ClosedTriagedAreaStats())
                if is_merged:
                    stats.merged += 1
                else:
                    stats.closed += 1
                if author_responded:
                    stats.responded_before_close += 1
                if is_contributor:
                    stats.contributors += 1

            # Update totals
            if is_merged:
                total_stats.merged += 1
            else:
                total_stats.closed += 1
            if author_responded:
                total_stats.responded_before_close += 1
            if is_contributor:
                total_stats.contributors += 1

        page_info = search_data.get("pageInfo", {})
        if not page_info.get("hasNextPage", False):
            break
        after_cursor = page_info.get("endCursor")

    area_stats["__total__"] = total_stats
    console.print(
        f"[info]Found {total_stats.total} triaged PRs in final state "
        f"({total_stats.closed} closed, {total_stats.merged} merged) since {since}.[/]"
    )
    return area_stats


@pr_group.command(name="stats", help="Show statistics of open PRs grouped by area label.")
@option_github_token
@option_github_repository
@click.option(
    "--batch-size",
    type=int,
    default=100,
    show_default=True,
    help="Number of PRs to fetch per GraphQL page.",
)
@click.option(
    "--clear-cache",
    is_flag=True,
    default=False,
    help="Clear cached interaction data before fetching.",
)
def stats(github_token: str | None, github_repository: str, batch_size: int, clear_cache: bool) -> None:
    """Produce aggregate statistics of open PRs, split by area."""
    from airflow_breeze.utils.pr_cache import stats_interaction_cache

    token = _resolve_github_token(github_token)
    if not token:
        console_print("[error]GitHub token is required. Use --github-token or set GITHUB_TOKEN.[/]")
        sys.exit(1)

    console = get_console()

    if clear_cache:
        import shutil

        cache_dir = stats_interaction_cache.cache_dir(github_repository)
        shutil.rmtree(cache_dir, ignore_errors=True)
        console.print("[info]Cleared interaction cache.[/]")

    # Step 1: Fetch all open PRs
    all_prs = _fetch_all_open_prs(token, github_repository, batch_size)
    if not all_prs:
        console.print("[warning]No open PRs found.[/]")
        return

    # Step 2: Resolve viewer login and classify triage status
    viewer_login = _resolve_viewer_login(token)
    console.print(f"[info]Classifying triage status (viewer: {viewer_login})...[/]")
    triage_classification = _classify_already_triaged_prs(token, github_repository, all_prs, viewer_login)

    # Step 3: Fetch interaction data (last author activity + triager draft conversion)
    interaction_data = _fetch_pr_interaction_data(token, github_repository, all_prs, viewer_login)

    # Step 4: Aggregate by area
    area_stats = _aggregate_stats_by_area(all_prs, triage_classification, interaction_data)

    # Step 5: Compute totals (unique PR counts)
    totals = _compute_totals(all_prs, triage_classification, interaction_data)

    # Step 6: Fetch closed/merged triaged PRs since March 11, 2026
    closed_stats = _fetch_closed_triaged_prs(token, github_repository, viewer_login, _CLOSED_TRIAGED_SINCE)

    # Step 7: Render
    _render_stats_tables(
        area_stats,
        totals,
        github_repository,
        closed_stats=closed_stats,
        closed_since_date=_CLOSED_TRIAGED_SINCE,
    )
