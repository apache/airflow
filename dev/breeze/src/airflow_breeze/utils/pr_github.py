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
"""GitHub API interaction layer for PR triage.

Provides GraphQL/REST helpers, mutation wrappers, authentication,
collaborator management, and label operations.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from threading import Thread
from typing import Any

from airflow_breeze.utils.console import console_print, get_console
from airflow_breeze.utils.run_utils import run_command

# authorAssociation values that indicate the author has write access
COLLABORATOR_ASSOCIATIONS = {"COLLABORATOR", "MEMBER", "OWNER"}

TRUSTED_REPOSITORIES = {"apache/airflow"}

# answer-triage values that auto-confirm destructive actions without user review
DANGEROUS_ANSWER_VALUES = {"d", "c", "y"}

# GitHub accounts that should be auto-skipped during triage
BOT_ACCOUNT_LOGINS = {
    "dependabot",
    "dependabot[bot]",
    "renovate[bot]",
    "github-actions",
    "github-actions[bot]",
}

# ---------------------------------------------------------------------------
# GraphQL mutation/query constants
# ---------------------------------------------------------------------------

_VIEWER_QUERY = """
query { viewer { login } }
"""

_GET_LABEL_ID_QUERY = """
query($owner: String!, $repo: String!, $name: String!) {
  repository(owner: $owner, name: $repo) {
    label(name: $name) { id }
  }
}
"""

_CONVERT_TO_DRAFT_MUTATION = """
mutation($prId: ID!) {
  convertPullRequestToDraft(input: {pullRequestId: $prId}) {
    pullRequest { id }
  }
}
"""

_MARK_READY_FOR_REVIEW_MUTATION = """
mutation($prId: ID!) {
  markPullRequestReadyForReview(input: {pullRequestId: $prId}) {
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

_REMOVE_LABELS_MUTATION = """
mutation($labelableId: ID!, $labelIds: [ID!]!) {
  removeLabelsFromLabelable(input: {labelableId: $labelableId, labelIds: $labelIds}) {
    labelable { ... on PullRequest { id } }
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


# ---------------------------------------------------------------------------
# Core API helpers
# ---------------------------------------------------------------------------


def graphql_request(token: str, query: str, variables: dict, *, max_retries: int = 3) -> dict:
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


def graphql_mutation(token: str, mutation: str, variables: dict) -> bool:
    """Execute a GraphQL mutation, returning True on success, False on failure."""
    try:
        graphql_request(token, mutation, variables)
        return True
    except SystemExit:
        return False


def github_rest(
    token: str,
    method: str,
    url: str,
    *,
    json_payload: dict | None = None,
    ok_codes: tuple[int, ...] = (200, 201),
    fail_message: str = "",
) -> bool:
    """Make a GitHub REST API request, returning True on success.

    Consolidates the repeated pattern of POST/PUT with auth headers, status check,
    and optional failure message.
    """
    import requests

    fn = getattr(requests, method.lower())
    kwargs: dict[str, Any] = {
        "headers": {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        "timeout": (10, 20),
    }
    if json_payload is not None:
        kwargs["json"] = json_payload
    for _attempt in range(3):
        try:
            response = fn(url, **kwargs)
            break
        except Exception:
            if _attempt == 2:
                if fail_message:
                    get_console().print(f"[warning]{fail_message}: connection failed after 3 attempts[/]")
                return False
            time.sleep(2 * (_attempt + 1))
            continue
    else:
        return False
    if response.status_code in ok_codes:
        return True
    if fail_message:
        get_console().print(f"[warning]{fail_message}: {response.status_code} {response.text[:200]}[/]")
    return False


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


def resolve_github_token(github_token: str | None) -> str | None:
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


def resolve_viewer_login(token: str) -> str:
    """Resolve the GitHub login of the authenticated user via the viewer query."""
    data = graphql_request(token, _VIEWER_QUERY, {})
    return data["viewer"]["login"]


# ---------------------------------------------------------------------------
# LLM safety validation
# ---------------------------------------------------------------------------


def validate_llm_safety(github_repository: str, answer_triage: str | None) -> None:
    """Verify safety preconditions before starting LLM assessment threads."""
    console = get_console()

    if github_repository not in TRUSTED_REPOSITORIES:
        console.print(
            f"[error]LLM assessment refused: repository '{github_repository}' is not trusted.\n"
            f"Trusted repositories: {', '.join(sorted(TRUSTED_REPOSITORIES))}.\n"
            f"Use --github-repository apache/airflow or run without LLM "
            f"(--llm-use api).[/]"
        )
        sys.exit(1)

    if answer_triage and answer_triage.lower() in DANGEROUS_ANSWER_VALUES:
        label = {"d": "draft", "c": "close", "y": "yes (auto-confirm)"}
        console.print(
            f"[error]LLM assessment refused: --answer-triage={answer_triage} "
            f"({label.get(answer_triage.lower(), answer_triage)}) would auto-confirm "
            f"destructive actions on PRs based on LLM output without user review.\n"
            f"Remove --answer-triage or use a safe value (s=skip, q=quit, n=no) "
            f"to proceed with LLM assessment.[/]"
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# PR mutation operations
# ---------------------------------------------------------------------------


def convert_pr_to_draft(token: str, node_id: str) -> bool:
    """Convert a PR to draft using GitHub GraphQL API."""
    return graphql_mutation(token, _CONVERT_TO_DRAFT_MUTATION, {"prId": node_id})


def mark_pr_ready_for_review(token: str, node_id: str) -> bool:
    """Mark a draft PR as ready for review using GitHub GraphQL API."""
    return graphql_mutation(token, _MARK_READY_FOR_REVIEW_MUTATION, {"prId": node_id})


def close_pr(token: str, node_id: str) -> bool:
    """Close a PR using GitHub GraphQL API."""
    return graphql_mutation(token, _CLOSE_PR_MUTATION, {"prId": node_id})


def post_comment(token: str, node_id: str, body: str) -> bool:
    """Post a comment on a PR using GitHub GraphQL API."""
    return graphql_mutation(token, _ADD_COMMENT_MUTATION, {"subjectId": node_id, "body": body})


def post_review_comment(
    token: str,
    github_repository: str,
    pr_number: int,
    commit_id: str,
    path: str,
    line: int,
    body: str,
) -> bool:
    """Post a single review comment on a specific line of a PR via REST API."""
    owner, repo = github_repository.split("/", 1)
    return github_rest(
        token,
        "post",
        f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/comments",
        json_payload={"commit_id": commit_id, "path": path, "line": line, "side": "RIGHT", "body": body},
        fail_message=f"Failed to post review comment on {path}:{line}",
    )


def submit_pr_review(
    token: str,
    github_repository: str,
    pr_number: int,
    commit_id: str,
    body: str,
    event: str = "COMMENT",
) -> bool:
    """Submit an overall PR review (APPROVE, REQUEST_CHANGES, or COMMENT) via REST API."""
    owner, repo = github_repository.split("/", 1)
    return github_rest(
        token,
        "post",
        f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/reviews",
        json_payload={"commit_id": commit_id, "body": body, "event": event},
        fail_message="Failed to submit review",
    )


def update_pr_branch(token: str, github_repository: str, pr_number: int) -> bool:
    """Update (rebase) a PR branch to the latest base branch via GitHub REST API."""
    owner, repo = github_repository.split("/", 1)
    return github_rest(
        token,
        "put",
        f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/update-branch",
        ok_codes=(200, 202),
        fail_message="Failed to update PR branch",
    )


# ---------------------------------------------------------------------------
# Label operations
# ---------------------------------------------------------------------------

_label_id_cache: dict[str, str | None] = {}


def resolve_label_id(token: str, github_repository: str, label_name: str) -> str | None:
    """Resolve a label name to its node ID via GraphQL. Cached per label name."""
    if label_name in _label_id_cache:
        return _label_id_cache[label_name]
    owner, repo = github_repository.split("/", 1)
    try:
        data = graphql_request(
            token, _GET_LABEL_ID_QUERY, {"owner": owner, "repo": repo, "name": label_name}
        )
        label_data = (data.get("repository") or {}).get("label")
        label_id = label_data["id"] if label_data else None
    except (SystemExit, KeyError):
        label_id = None
    _label_id_cache[label_name] = label_id
    return label_id


def add_label(token: str, github_repository: str, pr_node_id: str, label_name: str) -> bool:
    """Add a label to a PR. Returns True on success."""
    label_id = resolve_label_id(token, github_repository, label_name)
    if not label_id:
        console_print(f"[warning]Label '{label_name}' not found in {github_repository}. Skipping label.[/]")
        return False
    try:
        graphql_request(token, _ADD_LABELS_MUTATION, {"labelableId": pr_node_id, "labelIds": [label_id]})
        return True
    except SystemExit:
        return False


def remove_label(token: str, github_repository: str, pr_node_id: str, label_name: str) -> bool:
    """Remove a label from a PR. Returns True on success."""
    label_id = resolve_label_id(token, github_repository, label_name)
    if not label_id:
        get_console().print(
            f"[warning]Label '{label_name}' not found in {github_repository}. Skipping label removal.[/]"
        )
        return False
    try:
        graphql_request(token, _REMOVE_LABELS_MUTATION, {"labelableId": pr_node_id, "labelIds": [label_id]})
        return True
    except SystemExit:
        return False


# ---------------------------------------------------------------------------
# Collaborator cache management
# ---------------------------------------------------------------------------


def get_collaborators_cache_path(github_repository: str) -> Path:
    """Return the path to the local collaborators cache file."""
    from airflow_breeze.utils.path_utils import BUILD_CACHE_PATH

    safe_name = github_repository.replace("/", "_")
    return Path(BUILD_CACHE_PATH) / f".collaborators_{safe_name}.json"


def fetch_collaborators_from_api(token: str, github_repository: str) -> list[str]:
    """Fetch the list of collaborators from the GitHub REST API."""
    import requests

    collaborators: list[str] = []
    page = 1
    while True:
        try:
            response = requests.get(
                f"https://api.github.com/repos/{github_repository}/collaborators",
                headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
                params={"per_page": 100, "page": page},
            )
        except (requests.ConnectionError, requests.Timeout, OSError):
            break
        if response.status_code != 200:
            break
        data = response.json()
        if not data:
            break
        for user in data:
            login = user.get("login")
            if login:
                collaborators.append(login)
        page += 1
    return sorted(collaborators)


def load_collaborators_cache(github_repository: str) -> list[str]:
    """Load collaborators from local cache file. Returns empty list if no cache."""
    from airflow_breeze.utils.recording import generating_command_images

    if generating_command_images():
        return []
    cache_path = get_collaborators_cache_path(github_repository)
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return []


def save_collaborators_cache(github_repository: str, collaborators: list[str]) -> None:
    """Save collaborators list to local cache file."""
    cache_path = get_collaborators_cache_path(github_repository)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(collaborators))


def refresh_collaborators_cache_in_background(token: str, github_repository: str) -> None:
    """Fetch collaborators from API and update the cache in a background thread."""

    def _refresh():
        collaborators = fetch_collaborators_from_api(token, github_repository)
        if collaborators:
            save_collaborators_cache(github_repository, collaborators)

    thread = Thread(target=_refresh, daemon=True)
    thread.start()


def is_bot_account(login: str) -> bool:
    """Check if a GitHub login belongs to a bot account."""
    return login.lower() in BOT_ACCOUNT_LOGINS or login.endswith("[bot]")
