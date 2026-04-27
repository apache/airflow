#!/usr/bin/env python3
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

# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "httpx>=0.27",
# ]
# ///
"""
Notify open PRs that conflict on ``uv.lock`` after a push to ``main``.

Triggered from ``.github/workflows/notify-uv-lock-conflicts.yml`` on pushes
to ``main`` that modify ``uv.lock``. For every non-draft, non-stale open
PR targeting ``main`` that also modifies ``uv.lock`` and currently
conflicts, posts (or updates) a comment asking the author to rebase and
re-run ``uv lock``.

Squash-merge is the house convention, so the PR that caused the push is
identified from the trailing ``(#NNN)`` of the commit headline; we fall
back to just the commit URL if parsing fails.

Environment variables (all provided by the Actions runner):
  GITHUB_TOKEN         - token with ``pull-requests:write`` and ``contents:read``
  GITHUB_REPOSITORY    - ``owner/repo`` (e.g. ``apache/airflow``)
  GITHUB_SHA           - head commit of the push
  GITHUB_STEP_SUMMARY  - (optional) path to append the job summary to
"""

from __future__ import annotations

import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

MARKER = "<!-- uv-lock-rebase-notice -->"
STALE_DAYS = 14
PAGE_SIZE = 20
INITIAL_DELAY_S = 30
RETRY_DELAY_S = 30
MAX_RETRIES = 5

GRAPHQL_URL = "https://api.github.com/graphql"
PR_NUMBER_IN_COMMIT_RE = re.compile(r"\(#(\d+)\)\s*$")

LIST_QUERY = """
query($owner: String!, $repo: String!, $cursor: String, $size: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequests(
      first: $size, after: $cursor,
      states: OPEN, baseRefName: "main",
      orderBy: {field: UPDATED_AT, direction: DESC}
    ) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id number isDraft updatedAt mergeable
        files(first: 100) { nodes { path } totalCount }
        comments(last: 100) { nodes { id body } }
      }
    }
  }
}
"""

SINGLE_QUERY = """
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      id number isDraft updatedAt mergeable
      files(first: 100) { nodes { path } totalCount }
      comments(last: 100) { nodes { id body } }
    }
  }
}
"""

COMMIT_HEADLINE_QUERY = """
query($owner: String!, $repo: String!, $oid: GitObjectID!) {
  repository(owner: $owner, name: $repo) {
    object(oid: $oid) {
      ... on Commit { messageHeadline }
    }
  }
}
"""

PR_SUMMARY_QUERY = """
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) { number url title }
  }
}
"""

CREATE_MUTATION = """
mutation($subjectId: ID!, $body: String!) {
  addComment(input: {subjectId: $subjectId, body: $body}) { clientMutationId }
}
"""

UPDATE_MUTATION = """
mutation($id: ID!, $body: String!) {
  updateIssueComment(input: {id: $id, body: $body}) { clientMutationId }
}
"""


@dataclass
class Stats:
    scanned: int = 0
    pages: int = 0
    drafts: int = 0
    stale: int = 0
    no_uv_lock: int = 0
    already_notified: int = 0
    mergeable: int = 0
    conflicting: int = 0
    unknown: int = 0
    retries: int = 0
    still_unknown: int = 0
    posted: int = 0
    updated: int = 0


class GitHubGraphQL:
    def __init__(self, token: str) -> None:
        self._http = httpx.Client(
            base_url=GRAPHQL_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            },
            timeout=httpx.Timeout(30.0),
        )

    def __enter__(self) -> GitHubGraphQL:
        return self

    def __exit__(self, *_: object) -> None:
        self._http.close()

    def call(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        r = self._http.post("", json={"query": query, "variables": variables})
        r.raise_for_status()
        data = r.json()
        if "errors" in data:
            raise RuntimeError(f"GraphQL errors: {data['errors']}")
        return data["data"]


def log(msg: str) -> None:
    print(msg, flush=True)


def warn(msg: str) -> None:
    print(f"::warning::{msg}", flush=True)


def parse_updated_at(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def classify(
    pr: dict[str, Any],
    stale_cut: datetime,
    short_sha: str,
) -> tuple[str, dict[str, Any] | None]:
    """
    Return (kind, entry). ``kind`` is one of:
      - skip reasons: ``drafts``, ``stale``, ``no_uv_lock``, ``already_notified``
      - action buckets: ``conflicting``, ``mergeable``, ``unknown``

    For action buckets ``entry`` is ``{"pr": ..., "existing": ...}``; for
    skip reasons it is ``None``.
    """
    if pr["isDraft"]:
        return "drafts", None
    if parse_updated_at(pr["updatedAt"]) < stale_cut:
        return "stale", None
    if not any(f["path"] == "uv.lock" for f in pr["files"]["nodes"]):
        return "no_uv_lock", None
    existing = next(
        (c for c in pr["comments"]["nodes"] if c.get("body") and MARKER in c["body"]),
        None,
    )
    if existing and short_sha in existing["body"]:
        return "already_notified", None
    entry = {"pr": pr, "existing": existing}
    if pr["mergeable"] == "CONFLICTING":
        return "conflicting", entry
    if pr["mergeable"] == "MERGEABLE":
        return "mergeable", entry
    return "unknown", entry


def resolve_source_pr(
    client: GitHubGraphQL, owner: str, repo: str, sha: str, short_sha: str
) -> dict[str, Any] | None:
    """Extract ``(#NNN)`` from the squash-merge commit headline and hydrate PR info."""
    try:
        data = client.call(COMMIT_HEADLINE_QUERY, {"owner": owner, "repo": repo, "oid": sha})
        commit = data.get("repository", {}).get("object") or {}
        headline = commit.get("messageHeadline", "")
        match = PR_NUMBER_IN_COMMIT_RE.search(headline)
        if not match:
            log(f"No PR number found in commit headline: {headline!r}")
            return None
        number = int(match.group(1))
        data = client.call(PR_SUMMARY_QUERY, {"owner": owner, "repo": repo, "number": number})
        return data.get("repository", {}).get("pullRequest")
    except Exception as err:
        warn(f"Could not resolve source PR for {short_sha}: {err}")
        return None


def build_body(source_ref_md: str) -> str:
    return "\n".join(
        [
            MARKER,
            f"`uv.lock` on `main` just moved via {source_ref_md} and this PR currently conflicts.",
            "",
            "Quickest fix:",
            "```bash",
            "git fetch upstream main && git rebase upstream/main",
            "rm uv.lock && uv lock",
            "git add uv.lock && git rebase --continue",
            "git push --force-with-lease",
            "```",
            "",
            "_Automated nudge — ignore if you're not ready to rebase. "
            "This comment is updated in place on future `uv.lock` bumps._",
        ]
    )


def scan_open_prs(
    client: GitHubGraphQL,
    owner: str,
    repo: str,
    stale_cut: datetime,
    short_sha: str,
    stats: Stats,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    confirmed: list[dict[str, Any]] = []
    unknowns: list[dict[str, Any]] = []
    cursor: str | None = None

    log(f"Scanning open PRs targeting main (page size {PAGE_SIZE}, stale cutoff {STALE_DAYS}d)...")
    while True:
        data = client.call(
            LIST_QUERY,
            {"owner": owner, "repo": repo, "cursor": cursor, "size": PAGE_SIZE},
        )
        stats.pages += 1
        conn = data["repository"]["pullRequests"]
        nodes = conn["nodes"]
        log(f"  page {stats.pages}: {len(nodes)} PR(s)")

        hit_stale = False
        for pr in nodes:
            stats.scanned += 1
            if parse_updated_at(pr["updatedAt"]) < stale_cut:
                # List is sorted UPDATED_AT DESC — every later PR is also stale.
                stats.stale += 1
                hit_stale = True
                break
            kind, entry = classify(pr, stale_cut, short_sha)
            setattr(stats, kind, getattr(stats, kind) + 1)
            if kind == "conflicting" and entry is not None:
                confirmed.append(entry)
            elif kind == "unknown" and entry is not None:
                unknowns.append(entry)

        if hit_stale:
            log("  hit stale cutoff — stopping pagination")
            break
        if not conn["pageInfo"]["hasNextPage"]:
            break
        cursor = conn["pageInfo"]["endCursor"]

    log(
        f"Scan complete: {stats.scanned} scanned across {stats.pages} page(s) — "
        f"conflicting={stats.conflicting} unknown={stats.unknown} mergeable={stats.mergeable} "
        f"drafts={stats.drafts} stale={stats.stale} noUvLock={stats.no_uv_lock} "
        f"alreadyNotified={stats.already_notified}"
    )
    return confirmed, unknowns


def retry_unknowns(
    client: GitHubGraphQL,
    owner: str,
    repo: str,
    stale_cut: datetime,
    short_sha: str,
    confirmed: list[dict[str, Any]],
    unknowns: list[dict[str, Any]],
    stats: Stats,
) -> list[dict[str, Any]]:
    attempt = 0
    while unknowns and attempt < MAX_RETRIES:
        attempt += 1
        pr_ids = ", ".join(f"#{u['pr']['number']}" for u in unknowns)
        log(
            f"Retry {attempt}/{MAX_RETRIES}: {len(unknowns)} PR(s) still UNKNOWN "
            f"({pr_ids}) — waiting {RETRY_DELAY_S}s..."
        )
        time.sleep(RETRY_DELAY_S)
        stats.retries += 1

        nxt: list[dict[str, Any]] = []
        for old in unknowns:
            number = old["pr"]["number"]
            data = client.call(SINGLE_QUERY, {"owner": owner, "repo": repo, "number": number})
            pr = data["repository"]["pullRequest"]
            kind, entry = classify(pr, stale_cut, short_sha)
            if kind in {"drafts", "stale", "no_uv_lock", "already_notified"}:
                log(f"  #{number} → skip ({kind})")
                continue
            log(f"  #{number} → {kind}")
            if kind == "conflicting" and entry is not None:
                confirmed.append(entry)
            elif kind == "unknown" and entry is not None:
                nxt.append(entry)
        unknowns = nxt

    return unknowns


def post_notices(
    client: GitHubGraphQL,
    confirmed: list[dict[str, Any]],
    body: str,
    stats: Stats,
) -> None:
    log(f"Notifying {len(confirmed)} PR(s)...")
    for c in confirmed:
        pr = c["pr"]
        existing = c["existing"]
        if existing:
            client.call(UPDATE_MUTATION, {"id": existing["id"], "body": body})
            stats.updated += 1
            log(f"  #{pr['number']} — updated existing notice")
        else:
            client.call(CREATE_MUTATION, {"subjectId": pr["id"], "body": body})
            stats.posted += 1
            log(f"  #{pr['number']} — posted new notice")


def write_summary(
    summary_path: str | None,
    short_sha: str,
    source_ref_md: str,
    stats: Stats,
) -> None:
    if not summary_path:
        return
    rows: list[tuple[str, int]] = [
        ("Scanned", stats.scanned),
        ("Pages", stats.pages),
        ("Conflicting", stats.conflicting),
        ("Unknown (initial)", stats.unknown),
        ("Mergeable", stats.mergeable),
        ("Drafts skipped", stats.drafts),
        ("Stale skipped", stats.stale),
        ("No uv.lock", stats.no_uv_lock),
        ("Already notified", stats.already_notified),
        ("Retry passes", stats.retries),
        ("Still unknown", stats.still_unknown),
        ("Notices posted", stats.posted),
        ("Notices updated", stats.updated),
    ]
    lines = [
        f"# uv.lock conflict notifier — {short_sha}",
        "",
        f"**Source of change:** {source_ref_md}",
        "",
        "| Metric | Count |",
        "|---|---|",
        *(f"| {label} | {value} |" for label, value in rows),
        "",
    ]
    with open(summary_path, "a", encoding="utf-8") as fh:
        fh.write("\n".join(lines))


def main() -> int:
    try:
        token = os.environ["GITHUB_TOKEN"]
        owner, repo = os.environ["GITHUB_REPOSITORY"].split("/", 1)
        sha = os.environ["GITHUB_SHA"]
    except KeyError as err:
        print(f"Missing required environment variable: {err}", file=sys.stderr)
        return 2

    short_sha = sha[:7]
    commit_url = f"https://github.com/{owner}/{repo}/commit/{sha}"
    stale_cut = datetime.now(tz=timezone.utc) - timedelta(days=STALE_DAYS)

    with GitHubGraphQL(token) as client:
        source_pr = resolve_source_pr(client, owner, repo, sha, short_sha)
        if source_pr:
            source_ref_md = (
                f"[#{source_pr['number']}]({source_pr['url']}) "
                f'("{source_pr["title"]}"), commit [`{short_sha}`]({commit_url})'
            )
            source_ref_plain = f"#{source_pr['number']} ({source_pr['url']}) — commit {short_sha}"
        else:
            source_ref_md = f"commit [`{short_sha}`]({commit_url})"
            source_ref_plain = f"commit {short_sha}"

        log(f"Source of uv.lock change: {source_ref_plain}")
        body = build_body(source_ref_md)

        log(
            f"Initial delay: waiting {INITIAL_DELAY_S}s for GitHub to compute "
            f"mergeability after {source_ref_plain}..."
        )
        time.sleep(INITIAL_DELAY_S)

        stats = Stats()
        confirmed, unknowns = scan_open_prs(client, owner, repo, stale_cut, short_sha, stats)
        unknowns = retry_unknowns(client, owner, repo, stale_cut, short_sha, confirmed, unknowns, stats)

        if unknowns:
            stats.still_unknown = len(unknowns)
            pr_ids = ", ".join(f"#{u['pr']['number']}" for u in unknowns)
            warn(
                f"{len(unknowns)} PR(s) still UNKNOWN after {MAX_RETRIES} retries "
                f"({pr_ids}) — they will be re-evaluated on the next uv.lock bump."
            )

        post_notices(client, confirmed, body, stats)
        write_summary(os.environ.get("GITHUB_STEP_SUMMARY"), short_sha, source_ref_md, stats)

    return 0


if __name__ == "__main__":
    sys.exit(main())
