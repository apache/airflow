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
#   "rich>=13.6.0",
# ]
# ///
"""
Close the open PRs of contributors who are over the open pull request limit.

One-time step of introducing the limit, described in
``contributing-docs/32_open_pull_request_limit.rst``. For every author without write access
who has more open PRs than the limit, all their open PRs are closed with a comment asking
them to choose which ones to reopen - except PRs where a maintainer has already engaged
(commented or reviewed). Engagement by the maintainers doing the triage (by default the
user running the script) does not count.

Uses the ``gh`` CLI, so it runs with the credentials of the authenticated maintainer.
Dry-run by default - nothing is changed on GitHub unless ``--execute`` is passed.

Usage::

    uv run dev/close_prs_over_open_pr_limit.py
    uv run dev/close_prs_over_open_pr_limit.py --author some-login --execute
    uv run dev/close_prs_over_open_pr_limit.py --execute
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field

from rich.console import Console
from rich.table import Table

console = Console(width=200)

DOC_URL = "https://github.com/apache/airflow/blob/main/contributing-docs/32_open_pull_request_limit.rst"
DEFAULT_LABEL = "closed because of open PR limit"
LABEL_COLOR = "d4c5f9"

PRS_QUERY = """
query($owner: String!, $name: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(states: OPEN, first: 50, after: $cursor, orderBy: {field: CREATED_AT, direction: ASC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        title
        url
        isDraft
        author { login }
        comments(first: 100) { nodes { author { login } } }
        reviews(first: 100) { nodes { author { login } } }
        reviewThreads(first: 50) { nodes { comments(first: 20) { nodes { author { login } } } } }
      }
    }
  }
}
"""

COMMENT_TEMPLATE = """\
Hello @{author} - thank you for your contributions to Apache Airflow!

The Airflow community has introduced a limit of **{limit} open pull requests at a time** for \
contributors without write access to the repository. You currently have {total} open pull \
requests, so - as a one-time step of introducing the limit - we closed the ones where \
maintainers have not engaged yet:

{closed_list}
{kept_section}
This is **not** a judgement of you or of your changes. We never told contributors before that \
opening many pull requests at once was a problem, so there is nothing to feel bad about - and \
nothing is lost: your branches, commits and the review history stay where they are.

What we ask you to do is to make your **first prioritization decision**: choose which of the pull \
requests above matter most to you, and reopen them (up to {limit} open at a time, including the \
ones still open) with the "Reopen pull request" button or \
`gh pr reopen <PR_NUMBER> --repo apache/airflow`. Reopen the ones you are ready to follow \
through - keep them rebased, respond to review comments and fix failing checks.

While your pull requests are waiting for review, the most valuable thing you can do is help in \
other ways - reviewing other contributors' pull requests, helping with issues, and taking part \
in the discussions on the devlist and Slack.

Why we introduced the limit, what it means for you and how to reopen or restore a pull request \
is explained in {doc_url}.

---
Drafted-by: Claude Code (Opus 5); reviewed by @potiuk before posting
"""


@dataclass
class PullRequest:
    number: int
    title: str
    url: str
    is_draft: bool
    author: str
    participants: set[str] = field(default_factory=set)


def run_gh(args: list[str], stdin: str | None = None) -> str:
    result = subprocess.run(["gh", *args], input=stdin, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        console.print(f"[red]gh {' '.join(args)} failed:[/]\n{result.stderr}")
        sys.exit(1)
    return result.stdout


def get_authenticated_login() -> str:
    return run_gh(["api", "user", "--jq", ".login"]).strip()


def get_logins_with_write_access(repo: str) -> set[str]:
    """Return logins with push (write) access - these users are not subject to the limit."""
    output = run_gh(
        [
            "api",
            "--paginate",
            f"repos/{repo}/collaborators?affiliation=all&permission=push&per_page=100",
            "--jq",
            ".[].login",
        ]
    )
    return {login.lower() for login in output.split()}


def extract_logins(connection: dict) -> set[str]:
    return {node["author"]["login"].lower() for node in connection["nodes"] if node.get("author")}


def get_open_pull_requests(repo: str) -> list[PullRequest]:
    owner, name = repo.split("/")
    pull_requests: list[PullRequest] = []
    cursor: str | None = None
    while True:
        args = ["api", "graphql", "-f", f"query={PRS_QUERY}", "-F", f"owner={owner}", "-F", f"name={name}"]
        if cursor:
            args.extend(["-F", f"cursor={cursor}"])
        page = json.loads(run_gh(args))["data"]["repository"]["pullRequests"]
        for node in page["nodes"]:
            if not node.get("author"):
                continue
            participants = extract_logins(node["comments"]) | extract_logins(node["reviews"])
            for thread in node["reviewThreads"]["nodes"]:
                participants |= extract_logins(thread["comments"])
            pull_requests.append(
                PullRequest(
                    number=node["number"],
                    title=node["title"],
                    url=node["url"],
                    is_draft=node["isDraft"],
                    author=node["author"]["login"],
                    participants=participants,
                )
            )
        console.print(f"Fetched {len(pull_requests)} open PRs...", end="\r")
        if not page["pageInfo"]["hasNextPage"]:
            break
        cursor = page["pageInfo"]["endCursor"]
    console.print()
    return pull_requests


def is_bot(login: str) -> bool:
    return login.endswith("[bot]") or login in {"dependabot", "github-actions"}


def is_maintainer_engaged(pr: PullRequest, maintainers: set[str], triagers: set[str]) -> bool:
    engaged = pr.participants & maintainers
    engaged.discard(pr.author.lower())
    return bool(engaged - triagers)


@dataclass
class AuthorPlan:
    author: str
    total: int
    to_close: list[PullRequest]
    kept: list[PullRequest]


def build_plans(
    pull_requests: list[PullRequest],
    *,
    maintainers: set[str],
    triagers: set[str],
    limit: int,
    count_drafts: bool,
    only_author: str | None,
) -> list[AuthorPlan]:
    by_author: dict[str, list[PullRequest]] = defaultdict(list)
    for pr in pull_requests:
        login = pr.author.lower()
        if login in maintainers or is_bot(login):
            continue
        if only_author and login != only_author.lower():
            continue
        by_author[pr.author].append(pr)

    plans = []
    for author, prs in sorted(by_author.items(), key=lambda item: -len(item[1])):
        counted = [pr for pr in prs if count_drafts or not pr.is_draft]
        if len(counted) <= limit:
            continue
        kept = [pr for pr in prs if is_maintainer_engaged(pr, maintainers, triagers)]
        kept_numbers = {pr.number for pr in kept}
        to_close = [pr for pr in prs if pr.number not in kept_numbers]
        if to_close:
            plans.append(AuthorPlan(author=author, total=len(prs), to_close=to_close, kept=kept))
    return plans


def build_comment(plan: AuthorPlan, limit: int) -> str:
    closed_list = "\n".join(f"* #{pr.number} - {pr.title}" for pr in plan.to_close)
    kept_section = ""
    if plan.kept:
        kept_list = "\n".join(f"* #{pr.number} - {pr.title}" for pr in plan.kept)
        kept_section = (
            "\nThese pull requests stay open because maintainers are already engaged in them - "
            f"they count towards your limit:\n\n{kept_list}\n"
        )
    return COMMENT_TEMPLATE.format(
        author=plan.author,
        limit=limit,
        total=plan.total,
        closed_list=closed_list,
        kept_section=kept_section,
        doc_url=DOC_URL,
    )


def print_plans(plans: list[AuthorPlan]) -> None:
    table = Table(title="Authors over the open PR limit")
    table.add_column("Author")
    table.add_column("Open", justify="right")
    table.add_column("To close", justify="right")
    table.add_column("Kept (maintainer engaged)")
    for plan in plans:
        table.add_row(
            plan.author,
            str(plan.total),
            str(len(plan.to_close)),
            ", ".join(f"#{pr.number}" for pr in plan.kept) or "-",
        )
    console.print(table)
    console.print(
        f"[bold]{len(plans)}[/] authors, [bold]{sum(len(p.to_close) for p in plans)}[/] PRs to close, "
        f"[bold]{sum(len(p.kept) for p in plans)}[/] kept open."
    )


def close_pull_requests(repo: str, plans: list[AuthorPlan], *, limit: int, label: str, delay: float) -> None:
    run_gh(
        [
            "label",
            "create",
            label,
            "--repo",
            repo,
            "--color",
            LABEL_COLOR,
            "--force",
            "--description",
            "Closed as a one-time step of introducing the open pull request limit",
        ]
    )
    for plan in plans:
        comment = build_comment(plan, limit)
        for pr in plan.to_close:
            run_gh(["pr", "edit", str(pr.number), "--repo", repo, "--add-label", label])
            run_gh(["pr", "close", str(pr.number), "--repo", repo, "--comment", comment])
            console.print(f"Closed {pr.url}")
            # GitHub's secondary rate limits throttle bursts of content-creating requests.
            time.sleep(delay)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--repo", default="apache/airflow")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument(
        "--count-drafts",
        action="store_true",
        help="Count draft PRs towards the limit (GitHub does not count them yet).",
    )
    parser.add_argument(
        "--triager",
        action="append",
        default=[],
        help="Maintainer whose engagement does not count, e.g. triage comments. "
        "Can be repeated. Defaults to the authenticated gh user.",
    )
    parser.add_argument("--author", help="Only handle this author - useful to try the closure on one person.")
    parser.add_argument("--label", default=DEFAULT_LABEL)
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds to wait between closures.")
    parser.add_argument("--show-comment", action="store_true", help="Print the comment for each author.")
    parser.add_argument("--execute", action="store_true", help="Actually close PRs (default is dry-run).")
    args = parser.parse_args()

    triagers = {login.lower() for login in (args.triager or [get_authenticated_login()])}
    maintainers = get_logins_with_write_access(args.repo)
    console.print(
        f"{len(maintainers)} users with write access; triage engagement ignored for: {sorted(triagers)}"
    )

    plans = build_plans(
        get_open_pull_requests(args.repo),
        maintainers=maintainers,
        triagers=triagers,
        limit=args.limit,
        count_drafts=args.count_drafts,
        only_author=args.author,
    )
    print_plans(plans)
    if args.show_comment:
        for plan in plans:
            console.rule(plan.author)
            console.print(build_comment(plan, args.limit), markup=False)

    if not args.execute:
        console.print("[yellow]Dry run - nothing was changed. Pass --execute to close the PRs.[/]")
        return
    close_pull_requests(args.repo, plans, limit=args.limit, label=args.label, delay=args.delay)


if __name__ == "__main__":
    main()
