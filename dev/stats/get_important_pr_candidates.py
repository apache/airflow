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

import heapq
import logging
import math
import pickle
import re
import textwrap
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING

import pendulum
import rich_click as click
from github import Github, UnknownObjectException
from rich.console import Console

if TYPE_CHECKING:
    from github.PullRequest import PullRequest

logger = logging.getLogger(__name__)

console = Console(width=400, color_system="standard")

option_github_token = click.option(
    "--github-token",
    type=str,
    required=True,
    help=textwrap.dedent(
        """
        A GitHub token is required, and can also be provided by setting the GITHUB_TOKEN env variable.
        Can be generated with:
        https://github.com/settings/tokens/new?description=Read%20issues&scopes=repo:status"""
    ),
    envvar="GITHUB_TOKEN",
)


class PrStat:
    PROVIDER_SCORE = 0.8
    REGULAR_SCORE = 1.0
    REVIEW_INTERACTION_VALUE = 2.0
    COMMENT_INTERACTION_VALUE = 1.0
    REACTION_INTERACTION_VALUE = 0.5

    def __init__(self, g, pull_request: PullRequest):
        self.g = g
        self.pull_request = pull_request
        self.title = pull_request.title
        self._users: set[str] = set()
        self.len_comments: int = 0
        self.comment_reactions: int = 0
        self.issue_nums: list[int] = []
        self.len_issue_comments: int = 0
        self.num_issue_comments: int = 0
        self.num_issue_reactions: int = 0
        self.num_comments: int = 0
        self.num_conv_comments: int = 0
        self.num_protm: int = 0
        self.conv_comment_reactions: int = 0
        self.interaction_score = 1.0

    @property
    def label_score(self) -> float:
        """assigns label score"""
        labels = self.pull_request.labels
        for label in labels:
            if "provider" in label.name:
                return PrStat.PROVIDER_SCORE
        return PrStat.REGULAR_SCORE

    def calc_comments(self):
        """counts reviewer comments, checks for #protm tag, counts rxns"""
        for comment in self.pull_request.get_comments():
            self._users.add(comment.user.login)
            lowercase_body = comment.body.lower()
            if "protm" in lowercase_body:
                self.num_protm += 1
            self.num_comments += 1
            if comment.body is not None:
                self.len_comments += len(comment.body)
            for reaction in comment.get_reactions():
                self._users.add(reaction.user.login)
                self.comment_reactions += 1

    def calc_conv_comments(self):
        """counts conversational comments, checks for #protm tag, counts rxns"""
        for conv_comment in self.pull_request.get_issue_comments():
            self._users.add(conv_comment.user.login)
            lowercase_body = conv_comment.body.lower()
            if "protm" in lowercase_body:
                self.num_protm += 1
            self.num_conv_comments += 1
            for reaction in conv_comment.get_reactions():
                self._users.add(reaction.user.login)
                self.conv_comment_reactions += 1
            if conv_comment.body is not None:
                self.len_issue_comments += len(conv_comment.body)

    @cached_property
    def num_reviews(self) -> int:
        """counts reviews"""
        num_reviews = 0
        for review in self.pull_request.get_reviews():
            self._users.add(review.user.login)
            num_reviews += 1
        return num_reviews

    def issues(self):
        """finds issues in PR"""
        if self.pull_request.body is not None:
            regex = r"(?<=closes: #|elated: #)\d{5}"
            issue_strs = re.findall(regex, self.pull_request.body)
            self.issue_nums = [eval(s) for s in issue_strs]

    def issue_reactions(self):
        """counts reactions to issue comments"""
        if self.issue_nums:
            repo = self.g.get_repo("apache/airflow")
            for num in self.issue_nums:
                try:
                    issue = repo.get_issue(num)
                except UnknownObjectException:
                    continue
                for reaction in issue.get_reactions():
                    self._users.add(reaction.user.login)
                    self.num_issue_reactions += 1
                for issue_comment in issue.get_comments():
                    self.num_issue_comments += 1
                    self._users.add(issue_comment.user.login)
                    if issue_comment.body is not None:
                        self.len_issue_comments += len(issue_comment.body)

    def calc_interaction_score(self):
        """calculates interaction score"""
        interactions = (
            self.num_comments + self.num_conv_comments + self.num_issue_comments
        ) * PrStat.COMMENT_INTERACTION_VALUE
        interactions += (
            self.comment_reactions
            + self.conv_comment_reactions
            + self.num_issue_reactions
        ) * PrStat.REACTION_INTERACTION_VALUE
        self.interaction_score += (
            interactions + self.num_reviews * PrStat.REVIEW_INTERACTION_VALUE
        )

    @cached_property
    def num_interacting_users(self) -> int:
        _ = self.interaction_score  # make sure the _users set is populated
        return len(self._users)

    @cached_property
    def num_changed_files(self) -> float:
        return self.pull_request.changed_files

    @cached_property
    def body_length(self) -> int:
        if self.pull_request.body is not None:
            return len(self.pull_request.body)
        else:
            return 0

    @cached_property
    def num_additions(self) -> int:
        return self.pull_request.additions

    @cached_property
    def num_deletions(self) -> int:
        return self.pull_request.deletions

    @property
    def change_score(self) -> float:
        lineactions = self.num_additions + self.num_deletions
        actionsperfile = lineactions / self.num_changed_files
        if self.num_changed_files > 10:
            if actionsperfile > 20:
                return 1.2
            if actionsperfile < 5:
                return 0.7
        return 1.0

    @cached_property
    def comment_length(self) -> int:
        rev_length = 0
        for comment in self.pull_request.get_review_comments():
            if comment.body is not None:
                rev_length += len(comment.body)
        return self.len_comments + self.len_issue_comments + rev_length

    @property
    def length_score(self) -> float:
        score = 1.0
        if self.len_comments > 3000:
            score *= 1.3
        if self.len_comments < 200:
            score *= 0.8
        if self.body_length > 2000:
            score *= 1.4
        if self.body_length < 1000:
            score *= 0.8
        if self.body_length < 20:
            score *= 0.4
        return round(score, 3)

    def adjust_interaction_score(self):
        self.interaction_score *= min(self.num_protm + 1, 3)

    @property
    def score(self):
        #
        # Current principles:
        #
        # Provider and dev-tools PRs should be considered, but should matter 20% less.
        #
        # A review is worth twice as much as a comment, and a comment is worth twice as much as a reaction.
        #
        # If a PR changed more than 20 files, it should matter less the more files there are.
        #
        # If the avg # of changed lines/file is < 5 and there are > 10 files, it should matter 30% less.
        # If the avg # of changed lines/file is > 20 and there are > 10 files, it should matter 20% more.
        #
        # If there are over 3000 characters worth of comments, the PR should matter 30% more.
        # If there are fewer than 200 characters worth of comments, the PR should matter 20% less.
        # If the body contains over 2000 characters, the PR should matter 40% more.
        # If the body contains fewer than 1000 characters, the PR should matter 20% less.
        #
        # Weight PRs with protm tags more heavily:
        # If there is at least one protm tag, multiply the interaction score by the number of tags, up to 3.
        #
        self.calc_comments()
        self.calc_conv_comments()
        self.calc_interaction_score()
        self.adjust_interaction_score()

        return round(
            self.interaction_score
            * self.label_score
            * self.length_score
            * self.change_score
            / (math.log10(self.num_changed_files) if self.num_changed_files > 20 else 1),
            3,
        )

    def __str__(self) -> str:
        if self.num_protm > 0:
            return (
                "[magenta]##Tagged PR## [/]"
                f"Score: {self.score:.2f}: PR{self.pull_request.number}"
                f"by @{self.pull_request.user.login}: "
                f'"{self.pull_request.title}". '
                f"Merged at {self.pull_request.merged_at}: {self.pull_request.html_url}"
            )
        else:
            return (
                f"Score: {self.score:.2f}: PR{self.pull_request.number}"
                f"by @{self.pull_request.user.login}: "
                f'"{self.pull_request.title}". '
                f"Merged at {self.pull_request.merged_at}: {self.pull_request.html_url}"
            )

    def verboseStr(self) -> str:
        if self.num_protm > 0:
            console.print(
                "********************* Tagged with '#protm' *********************",
                style="magenta",
            )
        return (
            f"-- Created at [bright_blue]{self.pull_request.created_at}[/], "
            f"merged at [bright_blue]{self.pull_request.merged_at}[/]\n"
            f"-- Label score: [green]{self.label_score}[/]\n"
            f"-- Length score: [green]{self.length_score}[/] "
            f"(body length: {self.body_length}, "
            f"comment length: {self.len_comments})\n"
            f"-- Interaction score: [green]{self.interaction_score}[/] "
            f"(users interacting: {self.num_interacting_users}, "
            f"reviews: {self.num_reviews}, "
            f"review comments: {self.num_comments}, "
            f"review reactions: {self.comment_reactions}, "
            f"non-review comments: {self.num_conv_comments}, "
            f"non-review reactions: {self.conv_comment_reactions}, "
            f"issue comments: {self.num_issue_comments}, "
            f"issue reactions: {self.num_issue_reactions})\n"
            f"-- Change score: [green]{self.change_score}[/] "
            f"(changed files: {self.num_changed_files}, "
            f"additions: {self.num_additions}, "
            f"deletions: {self.num_deletions})\n"
            f"-- Overall score: [red]{self.score:.2f}[/]\n"
        )


DAYS_BACK = 5
# Current (or previous during first few days of the next month)
DEFAULT_BEGINNING_OF_MONTH = pendulum.now().subtract(days=DAYS_BACK).start_of("month")
DEFAULT_END_OF_MONTH = DEFAULT_BEGINNING_OF_MONTH.end_of("month").add(days=1)

MAX_PR_CANDIDATES = 500
DEFAULT_TOP_PRS = 10


@click.command()
@option_github_token  # TODO: this should only be required if --load isn't provided
@click.option(
    "--date-start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(DEFAULT_BEGINNING_OF_MONTH.date()),
)
@click.option(
    "--date-end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=str(DEFAULT_END_OF_MONTH.date()),
)
@click.option(
    "--top-number", type=int, default=DEFAULT_TOP_PRS, help="The number of PRs to select"
)
@click.option("--save", type=click.File("wb"), help="Save PR data to a pickle file")
@click.option(
    "--load",
    type=click.File("rb"),
    help="Load PR data from a file and recalculate scores",
)
@click.option("--verbose", is_flag="True", help="Print scoring details")
@click.option(
    "--rate-limit",
    is_flag="True",
    help="Print API rate limit reset time using system time, and requests remaining",
)
def main(
    github_token: str,
    date_start: datetime,
    save: click.File(),  # type: ignore
    load: click.File(),  # type: ignore
    date_end: datetime,
    top_number: int,
    verbose: bool,
    rate_limit: bool,
):
    g = Github(github_token)

    if rate_limit:
        r = g.get_rate_limit()
        requests_remaining: int = r.core.remaining
        console.print(
            f"[blue]GitHub API Rate Limit Info\n"
            f"[green]Requests remaining: [red]{requests_remaining}\n"
            f"[green]Reset time: [blue]{r.core.reset.astimezone()}"
        )

    selected_prs: list[PrStat] = []
    if load:
        console.print("Loading PRs from cache and recalculating scores.")
        selected_prs = pickle.load(load, encoding="bytes")
        for pr in selected_prs:
            console.print(
                f"[green]Loading PR: #{pr.pull_request.number} `{pr.pull_request.title}`.[/]"
                f" Score: {pr.score}."
                f" Url: {pr.pull_request.html_url}"
            )

            if verbose:
                console.print(pr.verboseStr())

    else:
        console.print(f"Finding best candidate PRs between {date_start} and {date_end}.")
        repo = g.get_repo("apache/airflow")
        commits = repo.get_commits(since=date_start, until=date_end)
        pulls: list[PullRequest] = [
            pull for commit in commits for pull in commit.get_pulls()
        ]
        scores: dict = {}
        for issue_num, pull in enumerate(pulls, 1):
            p = PrStat(g=g, pull_request=pull)  # type: ignore
            scores.update({pull.number: [p.score, pull.title]})
            console.print(
                f"[green]Selecting PR: #{pull.number} `{pull.title}` as candidate.[/]"
                f" Score: {scores[pull.number][0]}."
                f" Url: {pull.html_url}"
            )

            if verbose:
                console.print(p.verboseStr())

            selected_prs.append(p)
            if issue_num == MAX_PR_CANDIDATES:
                console.print(f"[red]Reached {MAX_PR_CANDIDATES}. Stopping")
                break

    console.print(f"Top {top_number} out of {issue_num} PRs:")
    for pr_scored in heapq.nlargest(top_number, scores.items(), key=lambda s: s[1]):
        console.print(
            f"[green] * PR #{pr_scored[0]}: {pr_scored[1][1]}. Score: [magenta]{pr_scored[1][0]}"
        )

    if save:
        pickle.dump(selected_prs, save)

    if rate_limit:
        r = g.get_rate_limit()
        console.print(
            f"[blue]GitHub API Rate Limit Info\n"
            f"[green]Requests remaining: [red]{r.core.remaining}\n"
            f"[green]Requests made: [red]{requests_remaining - r.core.remaining}\n"
            f"[green]Reset time: [blue]{r.core.reset.astimezone()}"
        )


if __name__ == "__main__":
    main()
