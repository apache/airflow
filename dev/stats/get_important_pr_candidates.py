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
import logging
import math
import sys
import textwrap
from datetime import datetime
from typing import List, Set

import pendulum
import rich_click as click
from github import Github
from github.PullRequest import PullRequest
from rich.console import Console

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

PROVIDER_LABEL = "area:providers"

logger = logging.getLogger(__name__)

console = Console(width=400, color_system="standard")

option_github_token = click.option(
    "--github-token",
    type=str,
    required=True,
    help=textwrap.dedent(
        """
        GitHub token used to authenticate.
        You can set omit it if you have GITHUB_TOKEN env variable set
        Can be generated with:
        https://github.com/settings/tokens/new?description=Read%20issues&scopes=repo:status"""
    ),
    envvar='GITHUB_TOKEN',
)

PROVIDER_SCORE = 0.5
REGULAR_SCORE = 1.0

REVIEW_INTERACTION_VALUE = 1.0
COMMENT_INTERACTION_VALUE = 1.0
REACTION_INTERACTION_VALUE = 0.1


class PrStat:
    def __init__(self, pull_request: PullRequest):
        self.pull_request = pull_request
        self._users: Set[str] = set()

    @cached_property
    def label_score(self) -> float:
        for label in self.pull_request.labels:
            if "provider" in label.name:
                return PROVIDER_SCORE
        return REGULAR_SCORE

    @cached_property
    def num_interactions(self) -> float:
        interactions = 0.0
        for comment in self.pull_request.get_comments():
            interactions += COMMENT_INTERACTION_VALUE
            self._users.add(comment.user.login)
            for _ in comment.get_reactions():
                interactions += REACTION_INTERACTION_VALUE
        for review in self.pull_request.get_reviews():
            interactions += REVIEW_INTERACTION_VALUE
            self._users.add(review.user.login)
        return interactions

    @cached_property
    def num_interacting_users(self) -> int:
        _ = self.num_interactions  # make sure the _users set is populated
        return len(self._users)

    @cached_property
    def num_changed_files(self) -> float:
        return self.pull_request.changed_files

    @cached_property
    def score(self):
        return (
            1.0
            * self.num_interactions
            * self.label_score
            * self.num_interacting_users
            / (math.log10(self.num_changed_files) if self.num_changed_files > 10 else 1.0)
        )

    def __str__(self) -> str:
        return (
            f"Score: {self.score:.2f}: PR{self.pull_request.number} by @{self.pull_request.user.login}: "
            f"`{self.pull_request.title}. "
            f"Merged at {self.pull_request.merged_at}: {self.pull_request.html_url}"
        )


DAYS_BACK = 5
# Current (or previous during first few days of the next month)
DEFAULT_BEGINNING_OF_MONTH = pendulum.now().subtract(days=DAYS_BACK).start_of('month')
DEFAULT_END_OF_MONTH = DEFAULT_BEGINNING_OF_MONTH.end_of('month').add(days=1)

MAX_PR_CANDIDATES = 500
DEFAULT_TOP_PRS = 10


@click.command()
@click.option(
    '--date-start', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(DEFAULT_BEGINNING_OF_MONTH.date())
)
@click.option(
    '--date-end', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(DEFAULT_END_OF_MONTH.date())
)
@click.option('--top-number', type=int, default=DEFAULT_TOP_PRS)
@click.option('--verbose', is_flag="True", help="Print scoring details")
@option_github_token
def main(github_token: str, date_start: datetime, date_end: datetime, top_number: int, verbose: bool):
    console.print(f"Finding best candidate PRs between {date_start} and {date_end}")
    g = Github(github_token)
    repo = g.get_repo("apache/airflow")
    pulls = repo.get_pulls(state="closed", sort="created", direction='desc')
    issue_num = 0
    selected_prs: List[PrStat] = []
    for pr in pulls:
        issue_num += 1
        if not pr.merged:
            continue
        if not (date_start < pr.merged_at < date_end):
            console.print(
                f"[bright_blue]Skipping {pr.number} {pr.title} as it was not "
                f"merged between {date_start} and {date_end}]"
            )
            continue
        if pr.created_at < date_start:
            console.print("[bright_blue]Completed selecting candidates")
            break
        pr_stat = PrStat(pull_request=pr)  # type: ignore
        console.print(
            f"[green]Selecting PR: #{pr.number} `{pr.title}` as candidate."
            f"Score: {pr_stat.score}[/]."
            f" Url: {pr.html_url}"
        )
        if verbose:
            console.print(
                f'[bright_blue]Created at: {pr.created_at}, Merged at: {pr.merged_at}, '
                f'Overall score: {pr_stat.score:.2f}, '
                f'Label score: {pr_stat.label_score}, '
                f'Interactions: {pr_stat.num_interactions}, '
                f'Users interacting: {pr_stat.num_interacting_users}, '
                f'Changed files: {pr_stat.num_changed_files}\n'
            )
        selected_prs.append(pr_stat)
        if issue_num == MAX_PR_CANDIDATES:
            console.print(f'[red]Reached {MAX_PR_CANDIDATES}. Stopping')
            break
    console.print(f"Top {top_number} PRs:")
    for pr_stat in sorted(selected_prs, key=lambda s: -s.score)[:top_number]:
        console.print(f" * {pr_stat}")


if __name__ == "__main__":
    main()
