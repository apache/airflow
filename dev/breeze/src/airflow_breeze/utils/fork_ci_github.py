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
"""
GitHub access for the fork CI reconciler (AIP-120); the only module of it that calls ``gh``.

Part of the fork CI gate: a pull request whose CI runs in the contributor's fork and changes
this file is closed by the fork CI monitor. See "Gate protection" in
dev/breeze/doc/ci/08_fork_ci_for_external_contributors.md.
"""

from __future__ import annotations

import json
import re
import subprocess
import time
from collections import Counter
from collections.abc import Sequence
from typing import Any
from urllib.parse import quote

from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.fork_ci_messages import STATUS_CONTEXT

RATE_LIMIT_MARKERS = ("rate limit", "secondary rate", "abuse detection")
MAX_ATTEMPTS = 4
# GitHub answers with these while overloaded, and GraphQL searches that take too long end in HTTP 504;
# the same call usually succeeds shortly after. A connection dropped mid-request is as transient, and as
# unclear about whether the request took effect.
SERVER_ERROR = re.compile(r"\bHTTP 5\d\d\b|connection reset by peer|unexpected EOF")
# Network failures before the request reached GitHub, so repeating it is safe for any call.
CONNECTION_ERROR = re.compile(r"TLS handshake timeout|dial tcp")
SERVER_ERROR_WAIT_SECONDS = 10
# A hung `gh` would otherwise hold the job until its timeout-minutes.
GH_TIMEOUT_SECONDS = 120


class GitHubError(RuntimeError):
    """A GitHub API call failed for a reason other than a rate limit."""


class GitHubClient:
    def __init__(self, repo: str = "apache/airflow", *, dry_run: bool = False):
        self.repo = repo
        self.dry_run = dry_run
        self.calls: Counter[str] = Counter()

    def _gh(
        self,
        args: Sequence[str],
        body: dict[str, Any] | None,
        kind: str,
        *,
        allow_error_output: bool = False,
        retry_server_errors: bool = True,
    ) -> Any:
        """
        :param allow_error_output: return the JSON output of a failed call when it has any. ``gh api
            graphql`` exits non-zero when some fields of the response errored, with the data GitHub
            could resolve still on stdout.
        :param retry_server_errors: repeat a call that failed with a server error. Off for a call that
            may have taken effect anyway and must not be repeated, such as creating a comment. A call
            that failed to connect is repeated either way.
        """
        payload = json.dumps(body) if body is not None else None
        rate_limited = False
        for attempt in range(MAX_ATTEMPTS):
            self.calls[kind] += 1
            try:
                result = subprocess.run(
                    ["gh", *args, *(["--input", "-"] if payload is not None else [])],
                    input=payload,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=GH_TIMEOUT_SECONDS,
                )
            except subprocess.TimeoutExpired:
                raise GitHubError(f"gh {' '.join(args)} timed out after {GH_TIMEOUT_SECONDS}s")
            if result.returncode == 0:
                return json.loads(result.stdout) if result.stdout.strip() else None
            rate_limited = any(marker in result.stderr.lower() for marker in RATE_LIMIT_MARKERS)
            connection_error = bool(CONNECTION_ERROR.search(result.stderr))
            if (
                rate_limited
                or connection_error
                or (retry_server_errors and SERVER_ERROR.search(result.stderr))
            ):
                if attempt == MAX_ATTEMPTS - 1:
                    break
                reason = (
                    "rate limit"
                    if rate_limited
                    else "connection error"
                    if connection_error
                    else "server error"
                )
                wait = (30 if rate_limited else SERVER_ERROR_WAIT_SECONDS) * 2**attempt
                console_print(f"[warning]GitHub {reason}, retrying in {wait}s[/]")
                time.sleep(wait)
                continue
            if allow_error_output and result.stdout.strip():
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    pass
            raise GitHubError(f"gh {' '.join(args)} failed: {result.stderr.strip()}")
        if rate_limited:
            raise GitHubError(f"gh {' '.join(args)} still rate limited after {MAX_ATTEMPTS} attempts")
        raise GitHubError(
            f"gh {' '.join(args)} still failing after {MAX_ATTEMPTS} attempts: {result.stderr.strip()}"
        )

    def graphql(
        self, query: str, variables: dict[str, Any] | None = None, *, allow_partial: bool = False
    ) -> dict[str, Any]:
        """
        Run a GraphQL query.

        :param allow_partial: return the data GitHub could resolve when some fields failed, e.g.
            one unreadable fork in a batch, instead of failing the whole query.
        """
        response = self._gh(
            ["api", "graphql"],
            {"query": query, "variables": variables or {}},
            "graphql",
            allow_error_output=allow_partial,
        )
        if response.get("errors"):
            if not allow_partial or not response.get("data"):
                raise GitHubError(f"GraphQL errors: {response['errors']}")
            console_print(f"[warning]Partial GraphQL result: {response['errors']}[/]")
        return response["data"]

    def rest_get(self, path: str, *, paginate: bool = False, items_key: str | None = None) -> Any:
        """
        GET a REST path.

        :param paginate: follow every page and return the concatenated items.
        :param items_key: for endpoints whose pages are objects, the key holding the items
            (``workflow_runs``, ``jobs``, ``files``); pages are plain lists otherwise.
        """
        args = ["api", path, *(["--paginate", "--slurp"] if paginate else [])]
        result = self._gh(args, None, "rest")
        if paginate:
            return [item for page in result for item in (page[items_key] if items_key else page)]
        return result

    def rest_get_pages(self, path: str) -> list[Any]:
        """GET every page of a paginated REST path, keeping page-level fields such as ``total_count``."""
        return self._gh(["api", path, "--paginate", "--slurp"], None, "rest")

    def _mutate(
        self,
        description: str,
        args: Sequence[str],
        body: dict[str, Any] | None,
        *,
        retry_server_errors: bool = True,
    ) -> None:
        if self.dry_run:
            console_print(f"[info][dry-run] {description}[/]")
            return
        console_print(f"[info]{description}[/]")
        self._gh(args, body, "mutation", retry_server_errors=retry_server_errors)

    def add_labels(self, pr: int, labels: Sequence[str]) -> None:
        self._mutate(
            f"#{pr}: add labels {list(labels)}",
            ["api", "-X", "POST", f"repos/{self.repo}/issues/{pr}/labels"],
            {"labels": list(labels)},
        )

    def remove_label(self, pr: int, label: str) -> None:
        self._mutate(
            f"#{pr}: remove label {label!r}",
            ["api", "-X", "DELETE", f"repos/{self.repo}/issues/{pr}/labels/{quote(label, safe='')}"],
            None,
        )

    def create_comment(self, pr: int, body: str) -> None:
        self._mutate(
            f"#{pr}: create comment",
            ["api", "-X", "POST", f"repos/{self.repo}/issues/{pr}/comments"],
            {"body": body},
            retry_server_errors=False,
        )

    def update_comment(self, pr: int, comment_id: int, body: str) -> None:
        self._mutate(
            f"#{pr}: update comment {comment_id}",
            ["api", "-X", "PATCH", f"repos/{self.repo}/issues/comments/{comment_id}"],
            {"body": body},
        )

    def close(self, pr: int) -> None:
        self._mutate(
            f"#{pr}: close",
            ["api", "-X", "PATCH", f"repos/{self.repo}/pulls/{pr}"],
            {"state": "closed"},
        )

    def delete_run(self, pr: int, run_id: int) -> None:
        self._mutate(
            f"#{pr}: delete workflow run {run_id}",
            ["api", "-X", "DELETE", f"repos/{self.repo}/actions/runs/{run_id}"],
            None,
        )

    def set_status(
        self,
        pr: int,
        sha: str,
        state: str,
        target_url: str,
        description: str,
        *,
        context: str = STATUS_CONTEXT,
    ) -> None:
        self._mutate(
            f"#{pr}: status {context!r} {state} on {sha[:8]}",
            ["api", "-X", "POST", f"repos/{self.repo}/statuses/{sha}"],
            {"state": state, "target_url": target_url, "description": description, "context": context},
        )
