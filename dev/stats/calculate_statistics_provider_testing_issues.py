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
import textwrap
from pathlib import Path

import rich_click as click
from attr import dataclass
from github import Github
from github.Issue import Issue
from rich.console import Console
from tabulate import tabulate

PROVIDER_TESTING_LABEL = "testing status"

logger = logging.getLogger(__name__)

console = Console(width=400, color_system="standard")

MY_DIR_PATH = Path(os.path.dirname(__file__))
SOURCE_DIR_PATH = MY_DIR_PATH / os.pardir / os.pardir


@click.group(context_settings={"help_option_names": ["-h", "--help"], "max_content_width": 500})
def cli():
    ...


option_table = click.option(
    "--table",
    is_flag=True,
    help="Print output as markdown table1",
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
        https://github.com/settings/tokens/new?description=Read%20Write%20isssues&scopes=repo"""
    ),
    envvar="GITHUB_TOKEN",
)


@dataclass
class Stats:
    issue_number: int
    title: str
    num_providers: int
    num_issues: int
    tested_issues: int
    url: str
    users_involved: set[str]
    users_commented: set[str]

    def percent_tested(self) -> int:
        return int(100.0 * self.tested_issues / self.num_issues)

    def num_involved_users_who_commented(self) -> int:
        return len(self.users_involved.intersection(self.users_commented))

    def num_commenting_not_involved(self) -> int:
        return len(self.users_commented - self.users_involved)

    def percent_commented_among_involved(self) -> int:
        return int(100.0 * self.num_involved_users_who_commented() / len(self.users_involved))

    def __str__(self):
        return (
            f"#{self.issue_number}: {self.title}: Num providers: {self.num_providers}, "
            f"Issues: {self.num_issues}, Tested {self.tested_issues}, "
            f"Percent Tested: {self.percent_tested()}%, "
            f"Involved users: {len(self.users_involved)}, Commenting users: {len(self.users_commented)}, "
            f"Involved who commented: {self.num_involved_users_who_commented()}, "
            f"Extra people: {self.num_commenting_not_involved()}, "
            f"Percent commented: {self.percent_commented_among_involved()}%, "
            f"URL: {self.url}"
        )


def get_users_from_content(content: str) -> set[str]:
    users_match = re.findall(r"@\S*", content, re.MULTILINE)
    users: set[str] = set()
    for user_match in users_match:
        users.add(user_match)
    return users


def get_users_who_commented(issue: Issue) -> set[str]:
    users: set[str] = set()
    for comment in issue.get_comments():
        users.add("@" + comment.user.login)
    return users


def get_stats(issue: Issue) -> Stats:
    content = issue.body
    return Stats(
        issue_number=issue.number,
        title=issue.title,
        num_providers=content.count("Provider "),
        num_issues=content.count("- [") - 1,
        tested_issues=content.count("[x]") + content.count("[X]") - 1,
        url=issue.html_url,
        users_involved=get_users_from_content(content),
        users_commented=get_users_who_commented(issue),
    )


def stats_to_rows(stats_list: list[Stats]) -> list[tuple]:
    total = Stats(
        issue_number=0,
        title="",
        num_providers=0,
        num_issues=0,
        tested_issues=0,
        url="",
        users_commented=set(),
        users_involved=set(),
    )
    rows: list[tuple] = []
    for stat in stats_list:
        total.num_providers += stat.num_providers
        total.num_issues += stat.num_issues
        total.tested_issues += stat.tested_issues
        total.users_involved.update(stat.users_involved)
        total.users_commented.update(stat.users_commented)
        rows.append(
            (
                f"[{stat.issue_number}]({stat.url})",
                stat.num_providers,
                stat.num_issues,
                stat.tested_issues,
                stat.percent_tested(),
                len(stat.users_involved),
                len(stat.users_commented),
                stat.num_involved_users_who_commented(),
                stat.num_commenting_not_involved(),
                stat.percent_commented_among_involved(),
            )
        )
    rows.append(
        (
            "Total",
            total.num_providers,
            total.num_issues,
            total.tested_issues,
            total.percent_tested(),
            len(total.users_involved),
            len(total.users_commented),
            total.num_involved_users_who_commented(),
            total.num_commenting_not_involved(),
            total.percent_commented_among_involved(),
        )
    )
    return rows


@option_github_token
@option_table
@cli.command()
def provide_stats(github_token: str, table: bool):
    g = Github(github_token)
    repo = g.get_repo("apache/airflow")
    issues = repo.get_issues(labels=[PROVIDER_TESTING_LABEL], state="closed", sort="created", direction="asc")
    stats_list: list[Stats] = []
    for issue in issues:
        stat = get_stats(issue)
        if not table:
            print(stat)
        else:
            stats_list.append(stat)
    if table:
        rows = stats_to_rows(stats_list)
        print(
            tabulate(
                rows,
                headers=(
                    "Issue",
                    "Num Providers",
                    "Num Issues",
                    "Tested Issues",
                    "Tested (%)",
                    "Involved",
                    "Commenting",
                    "Involved who commented",
                    "Extra people",
                    "User response (%)",
                ),
                tablefmt="github",
            )
        )


if __name__ == "__main__":
    cli()
