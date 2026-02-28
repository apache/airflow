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

import click
from rich.table import Table

from airflow_breeze.commands.common_options import (
    option_answer,
    option_dry_run,
    option_github_repository,
    option_github_token,
    option_verbose,
)
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.shared_options import get_dry_run


@click.group(cls=BreezeGroup, name="issues", help="Tools for managing GitHub issues.")
def issues_group():
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


def _get_collaborator_logins(repo) -> set[str]:
    """Fetch all collaborator logins for the repository."""
    get_console().print("[info]Fetching repository collaborators...[/]")
    collaborators = {c.login for c in repo.get_collaborators()}
    get_console().print(f"[info]Found {len(collaborators)} collaborators.[/]")
    return collaborators


def _process_batch(
    batch: list[tuple],
    dry_run: bool,
) -> int:
    """Display a batch of proposed unassignments and handle confirmation.

    Returns the number of issues actually unassigned.
    """
    if not batch:
        return 0

    table = Table(title="Proposed unassignments")
    table.add_column("Issue #", style="cyan", justify="right")
    table.add_column("Title", style="white")
    table.add_column("Non-collaborator assignees", style="red")
    for issue, non_collab_logins in batch:
        table.add_row(
            str(issue.number),
            issue.title[:80],
            ", ".join(sorted(non_collab_logins)),
        )
    get_console().print(table)

    if dry_run:
        get_console().print("[warning]Dry run — skipping actual unassignment.[/]")
        return 0

    answer = user_confirm("Unassign the above non-collaborators?")
    if answer == Answer.QUIT:
        get_console().print("[warning]Quitting.[/]")
        sys.exit(0)
    if answer == Answer.NO:
        get_console().print("[info]Skipping this batch.[/]")
        return 0

    unassigned_count = 0
    for issue, non_collab_logins in batch:
        for login in non_collab_logins:
            get_console().print(f"  Removing [red]{login}[/] from issue #{issue.number}")
            issue.remove_from_assignees(login)
            unassigned_count += 1
    return unassigned_count


@issues_group.command(name="unassign", help="Unassign non-collaborators from open issues.")
@option_github_token
@option_github_repository
@click.option(
    "--batch-size",
    type=int,
    default=100,
    show_default=True,
    help="Number of flagged issues to accumulate before prompting.",
)
@option_dry_run
@option_verbose
@option_answer
def unassign(
    github_token: str | None,
    github_repository: str,
    batch_size: int,
):
    from github import Github

    token = _resolve_github_token(github_token)
    if not token:
        get_console().print(
            "[error]GitHub token not found. Provide --github-token, "
            "set GITHUB_TOKEN, or authenticate with `gh auth login`.[/]"
        )
        sys.exit(1)

    g = Github(token)
    repo = g.get_repo(github_repository)
    collaborators = _get_collaborator_logins(repo)

    dry_run = get_dry_run()

    batch: list[tuple] = []
    total_issues_scanned = 0
    total_flagged = 0
    total_unassigned = 0

    get_console().print(f"[info]Scanning open issues in {github_repository}...[/]")

    for issue in repo.get_issues(state="open"):
        total_issues_scanned += 1
        if not issue.assignees:
            continue
        non_collab = {a.login for a in issue.assignees if a.login not in collaborators}
        if not non_collab:
            continue

        batch.append((issue, non_collab))
        total_flagged += 1

        if len(batch) >= batch_size:
            total_unassigned += _process_batch(batch, dry_run)
            batch = []

    # Process remaining batch
    total_unassigned += _process_batch(batch, dry_run)

    get_console().print()
    get_console().print("[success]Done![/]")
    get_console().print(f"  Issues scanned:    {total_issues_scanned}")
    get_console().print(f"  Issues flagged:    {total_flagged}")
    get_console().print(f"  Assignees removed: {total_unassigned}")
