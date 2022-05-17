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
import textwrap
from pathlib import Path
from typing import Optional, Tuple

import rich_click as click
from github import Github
from rich.console import Console

console = Console(width=400, color_system="standard")

MY_DIR_PATH = Path(__file__).parent.resolve()
SOURCE_DIR_PATH = MY_DIR_PATH.parents[1].resolve()


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#    NOTE! GitHub has secondary rate limits for issue creation, and you might be
#          temporarily blocked from creating issues and PRs if you update too many
#          issues in a short time
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


option_github_token = click.option(
    "--github-token",
    type=str,
    help=textwrap.dedent(
        """
        Github token used to authenticate.
        You can omit it if you have the ``GITHUB_TOKEN`` env variable set
        Can be generated with:
        https://github.com/settings/tokens/new?description=Write%20issues&scopes=repo"""
    ),
    envvar='GITHUB_TOKEN',
)

option_verbose = click.option(
    "--verbose",
    is_flag=True,
    help="Print verbose information about performed steps",
)

option_dry_run = click.option(
    "--dry-run",
    is_flag=True,
    help="Do not create issues, just print the issues to be created",
)


option_repository = click.option(
    "--repository",
    type=str,
    default="apache/airflow",
    help="Repo to use",
)

option_labels = click.option(
    "--labels",
    type=str,
    default='AIP-47',
    help="Label to filter the issues on (coma-separated)",
)

option_max_issues = click.option("--max-issues", type=int, help="Maximum number of issues to create")

option_start_from = click.option(
    "--start-from",
    type=int,
    default=0,
    help="Start from issue number N (useful if you are blocked by secondary rate limit)",
)


def process_paths_from_body(body: str, dry_run: bool, verbose: bool) -> Tuple[str, int, int, int, int]:
    count_re_added = 0
    count_completed = 0
    count_done = 0
    count_all = 0
    new_body = []
    for line in body.splitlines(keepends=True):
        if line.startswith("- ["):
            if verbose:
                console.print(line)
            path = SOURCE_DIR_PATH / line[len("- [ ] ") :].strip().split(' ')[0]
            if path.exists():
                count_all += 1
                prefix = ""
                if line.startswith("- [x]"):
                    if dry_run:
                        prefix = "(changed) "
                    count_re_added += 1
                new_body.append(prefix + line.replace("- [x]", "- [ ]"))
            else:
                count_done += 1
                count_all += 1
                prefix = ""
                if line.startswith("- [ ]"):
                    if dry_run:
                        prefix = "(changed) "
                    count_completed += 1
                new_body.append(prefix + line.replace("- [ ]", "- [x]"))
        else:
            if not dry_run:
                new_body.append(line)
    return "".join(new_body), count_re_added, count_completed, count_done, count_all


@option_repository
@option_labels
@option_dry_run
@option_github_token
@option_verbose
@option_max_issues
@option_start_from
@click.command()
def update_issue_status(
    github_token: str,
    max_issues: Optional[int],
    dry_run: bool,
    repository: str,
    start_from: int,
    verbose: bool,
    labels: str,
):
    """Update status of the issues regarding the AIP-47 migration."""
    g = Github(github_token)
    repo = g.get_repo(repository)
    issues = repo.get_issues(labels=labels.split(','))
    max_issues = max_issues if max_issues is not None else issues.totalCount
    total_re_added = 0
    total_completed = 0
    total_count_done = 0
    total_count_all = 0
    num_issues = 0
    for issue in issues[start_from : start_from + max_issues]:
        console.print(f"[blue] {issue.id}: {issue.title}")
        new_body, count_re_added, count_completed, count_done, count_all = process_paths_from_body(
            issue.body, dry_run=dry_run, verbose=verbose
        )
        if count_re_added != 0 or count_completed != 0:
            if dry_run:
                print(new_body)
            else:
                issue.edit(body=new_body)

        console.print()
        console.print(f"[blue]Summary of performed actions: for {issue.title}[/]")
        console.print(f"   Re-added file number (still there): {count_re_added}")
        console.print(f"   Completed file number: {count_completed}")
        console.print(f"   Done {count_done}/{count_all} = {(count_done * 100/ count_all):.2f}%")
        console.print()
        total_re_added += count_re_added
        total_completed += count_completed
        total_count_done += count_done
        total_count_all += count_all
        num_issues += 1

    console.print()
    console.print()
    console.print(f"[green]Summary of ALL performed actions: for {num_issues} issues[/]")
    console.print(f"   Re-added file number: {total_re_added}")
    console.print(f"   Completed file number: {total_completed}")
    console.print(
        f"   Done {total_count_done}/{total_count_all} = " f"{(total_count_done * 100/ total_count_all):.2f}%"
    )
    console.print()


if __name__ == "__main__":
    update_issue_status()
