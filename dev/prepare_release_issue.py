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
from __future__ import annotations

import logging
import os
import re
import subprocess
import textwrap
from collections import defaultdict
from typing import Any, NamedTuple, Union

import rich_click as click
from github import Github, Issue, PullRequest, UnknownObjectException
from rich.console import Console
from rich.progress import Progress

logger = logging.getLogger(__name__)

console = Console(width=400, color_system="standard")

PullRequestOrIssue = Union[PullRequest.PullRequest, Issue.Issue]

MY_DIR_PATH = os.path.dirname(__file__)
SOURCE_DIR_PATH = os.path.abspath(os.path.join(MY_DIR_PATH, os.pardir))
PR_PATTERN = re.compile(r".*\(#([0-9]+)\)")
ISSUE_MATCH_IN_BODY = re.compile(r" #([0-9]+)[^0-9]")


@click.group(context_settings={'help_option_names': ['-h', '--help'], 'max_content_width': 500})
def cli():
    ...


option_verbose = click.option(
    "--verbose",
    is_flag=True,
    help="Print verbose information about performed steps",
)

option_previous_release = click.option(
    "--previous-release",
    type=str,
    required=True,
    help="commit reference (for example hash or tag) of the previous release.",
)

option_current_release = click.option(
    "--current-release",
    type=str,
    required=True,
    help="commit reference (for example hash or tag) of the current release.",
)

option_github_token = click.option(
    "--github-token",
    type=str,
    required=True,
    help=textwrap.dedent(
        """
        GitHub token used to authenticate.
        You can set omit it if you have GITHUB_TOKEN env variable set
        Can be generated with:
        https://github.com/settings/tokens/new?description=Read%20sssues&scopes=repo:status"""
    ),
    envvar='GITHUB_TOKEN',
)

option_excluded_pr_list = click.option(
    "--excluded-pr-list", type=str, default='', help="Coma-separated list of PRs to exclude from the issue."
)

option_limit_pr_count = click.option(
    "--limit-pr-count",
    type=int,
    default=None,
    help="Limit PR count processes (useful for testing small subset of PRs).",
)

option_is_helm_chart = click.option(
    "--is-helm-chart",
    is_flag=True,
    help="Whether the release is for helm chart or not.",
)


def get_git_log_command(
    verbose: bool,
    from_commit: str | None = None,
    to_commit: str | None = None,
    is_helm_chart: bool = True,
) -> list[str]:
    """
    Get git command to run for the current repo from the current folder (which is the package folder).
    :param verbose: whether to print verbose info while getting the command
    :param from_commit: if present - base commit from which to start the log from
    :param to_commit: if present - final commit which should be the start of the log
    :return: git command to run
    """
    git_cmd = [
        "git",
        "log",
        "--pretty=format:%H %h %cd %s",
        "--date=short",
    ]
    if from_commit and to_commit:
        git_cmd.append(f"{from_commit}...{to_commit}")
    elif from_commit:
        git_cmd.append(from_commit)
    if is_helm_chart:
        git_cmd.extend(['--', 'chart/'])
    else:
        git_cmd.extend(['--', '.'])
    if verbose:
        console.print(f"Command to run: '{' '.join(git_cmd)}'")
    return git_cmd


class Change(NamedTuple):
    """Stores details about commits"""

    full_hash: str
    short_hash: str
    date: str
    message: str
    message_without_backticks: str
    pr: int | None


def get_change_from_line(line: str):
    split_line = line.split(" ", maxsplit=3)
    message = split_line[3]
    pr = None
    pr_match = PR_PATTERN.match(message)
    if pr_match:
        pr = pr_match.group(1)
    return Change(
        full_hash=split_line[0],
        short_hash=split_line[1],
        date=split_line[2],
        message=message,
        message_without_backticks=message.replace("`", "'").replace("&#39;", "'").replace('&amp;', "&"),
        pr=int(pr) if pr else None,
    )


def get_changes(
    verbose: bool, previous_release: str, current_release: str, is_helm_chart: bool = False
) -> list[Change]:
    change_strings = subprocess.check_output(
        get_git_log_command(
            verbose, from_commit=previous_release, to_commit=current_release, is_helm_chart=is_helm_chart
        ),
        cwd=SOURCE_DIR_PATH,
        text=True,
    )
    return [get_change_from_line(line) for line in change_strings.split("\n")]


def render_template(
    template_name: str,
    context: dict[str, Any],
    autoescape: bool = True,
    keep_trailing_newline: bool = False,
) -> str:
    """
    Renders template based on it's name. Reads the template from <name>_TEMPLATE.md.jinja2 in current dir.
    :param template_name: name of the template to use
    :param context: Jinja2 context
    :param autoescape: Whether to autoescape HTML
    :param keep_trailing_newline: Whether to keep the newline in rendered output
    :return: rendered template
    """
    import jinja2

    template_loader = jinja2.FileSystemLoader(searchpath=MY_DIR_PATH)
    template_env = jinja2.Environment(
        loader=template_loader,
        undefined=jinja2.StrictUndefined,
        autoescape=autoescape,
        keep_trailing_newline=keep_trailing_newline,
    )
    template = template_env.get_template(f"{template_name}_TEMPLATE.md.jinja2")
    content: str = template.render(context)
    return content


def print_issue_content(
    current_release: str,
    pull_requests: dict[int, PullRequestOrIssue],
    linked_issues: dict[int, list[Issue.Issue]],
    users: dict[int, set[str]],
    is_helm_chart: bool = False,
):
    link = f"https://pypi.org/project/apache-airflow/{current_release}/"
    link_text = f"Apache Airflow RC {current_release}"
    if is_helm_chart:
        link = f"https://dist.apache.org/repos/dist/dev/airflow/{current_release}"
        link_text = f"Apache Airflow Helm Chart {current_release.split('/')[-1]}"
    pr_list = list(pull_requests.keys())
    pr_list.sort()
    user_logins: dict[int, str] = {pr: "@" + " @".join(users[pr]) for pr in users}
    all_users: set[str] = set()
    for user_list in users.values():
        all_users.update(user_list)
    all_user_logins = "@" + " @".join(all_users)
    content = render_template(
        template_name='ISSUE',
        context={
            'link': link,
            'link_text': link_text,
            'pr_list': pr_list,
            'pull_requests': pull_requests,
            'linked_issues': linked_issues,
            'users': users,
            'user_logins': user_logins,
            'all_user_logins': all_user_logins,
        },
        autoescape=False,
        keep_trailing_newline=True,
    )
    print(content)


@cli.command()
@option_github_token
@option_previous_release
@option_current_release
@option_excluded_pr_list
@option_verbose
@option_limit_pr_count
@option_is_helm_chart
def generate_issue_content(
    github_token: str,
    previous_release: str,
    current_release: str,
    excluded_pr_list: str,
    verbose: bool,
    limit_pr_count: int | None,
    is_helm_chart: bool,
):
    if excluded_pr_list:
        excluded_prs = [int(pr) for pr in excluded_pr_list.split(",")]
    else:
        excluded_prs = []
    changes = get_changes(verbose, previous_release, current_release, is_helm_chart)
    change_prs = [change.pr for change in changes]
    prs = [pr for pr in change_prs if pr is not None and pr not in excluded_prs]

    g = Github(github_token)
    repo = g.get_repo("apache/airflow")
    pull_requests: dict[int, PullRequestOrIssue] = {}
    linked_issues: dict[int, list[Issue.Issue]] = defaultdict(lambda: [])
    users: dict[int, set[str]] = defaultdict(lambda: set())
    count_prs = len(prs)
    if limit_pr_count:
        count_prs = limit_pr_count
    with Progress(console=console) as progress:
        task = progress.add_task(f"Retrieving {count_prs} PRs ", total=count_prs)
        for i in range(count_prs):
            pr_number = prs[i]
            progress.console.print(
                f"Retrieving PR#{pr_number}: https://github.com/apache/airflow/pull/{pr_number}"
            )

            pr: PullRequestOrIssue
            try:
                pr = repo.get_pull(pr_number)
            except UnknownObjectException:
                # Fallback to issue if PR not found
                try:
                    pr = repo.get_issue(pr_number)  # (same fields as PR)
                except UnknownObjectException:
                    console.print(f"[red]The PR #{pr_number} could not be found[/]")
                    continue

            # Ignore doc-only and skipped PRs
            label_names = [label.name for label in pr.labels]
            if "type:doc-only" in label_names or "changelog:skip" in label_names:
                continue

            pull_requests[pr_number] = pr
            # GitHub does not have linked issues in PR - but we quite rigorously add Fixes/Closes
            # Relate so we can find those from the body
            if pr.body:
                body = pr.body.replace("\n", " ").replace("\r", " ")
                linked_issue_numbers = {
                    int(issue_match.group(1)) for issue_match in ISSUE_MATCH_IN_BODY.finditer(body)
                }
                for linked_issue_number in linked_issue_numbers:
                    progress.console.print(
                        f"Retrieving Linked issue PR#{linked_issue_number}: "
                        f"https://github.com/apache/airflow/issue/{linked_issue_number}"
                    )
                    try:
                        linked_issues[pr_number].append(repo.get_issue(linked_issue_number))
                    except UnknownObjectException:
                        progress.console.print(
                            f"Failed to retrieve linked issue #{linked_issue_number}: Unknown Issue"
                        )
            users[pr_number].add(pr.user.login)
            for linked_issue in linked_issues[pr_number]:
                users[pr_number].add(linked_issue.user.login)
            progress.advance(task)
    print_issue_content(current_release, pull_requests, linked_issues, users, is_helm_chart)


if __name__ == "__main__":
    cli()
