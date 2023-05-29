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
import math
import pickle
import re
import textwrap
from datetime import datetime
from functools import cached_property

import pendulum
import rich_click as click
from github import Github
from github.PullRequest import PullRequest
from rich.console import Console

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
        self._users: set[str] = set()
        self.issue_nums: list[int] = []
        self.len_issue_comments = 0
        self.num_issue_comments = 0
        self.num_issue_reactions = 0
        self.protm_score = 0

    @property
    def label_score(self) -> float:
        """assigns label score"""
        labels = self.pull_request.labels
        for label in labels:
            if "provider" in label.name:
                return PrStat.PROVIDER_SCORE
        return PrStat.REGULAR_SCORE

    @cached_property
    def num_comments(self):
        """counts reviewer comments & checks for #protm tag"""
        num_comments = 0
        num_protm = 0
        for comment in self.pull_request.get_comments():
            self._users.add(comment.user.login)
            lowercase_body = comment.body.lower()
            if "protm" in lowercase_body:
                num_protm += 1
            num_comments += 1
        self.protm_score = num_protm
        return num_comments

    @cached_property
    def num_conv_comments(self) -> int:
        """counts conversational comments & checks for #protm tag"""
        num_conv_comments = 0
        num_protm = 0
        for conv_comment in self.pull_request.get_issue_comments():
            self._users.add(conv_comment.user.login)
            lowercase_body = conv_comment.body.lower()
            if "protm" in lowercase_body:
                num_protm += 1
            num_conv_comments += 1
        self.protm_score = num_protm
        return num_conv_comments

    @cached_property
    def num_reactions(self) -> int:
        """counts reactions to reviewer comments"""
        reactions = 0
        for comment in self.pull_request.get_comments():
            for reaction in comment.get_reactions():
                self._users.add(reaction.user.login)
                reactions += 1
        return reactions

    @cached_property
    def num_conv_reactions(self) -> int:
        """counts reactions to conversational comments"""
        reactions = 0
        for conv_comment in self.pull_request.get_issue_comments():
            for reaction in conv_comment.get_reactions():
                self._users.add(reaction.user.login)
                reactions += 1
        return reactions

    @cached_property
    def num_reviews(self) -> int:
        """counts reviews"""
        num_reviews = 0
        for review in self.pull_request.get_reviews():
            self._users.add(review.user.login)
            num_reviews += 1
        return num_reviews

    @cached_property
    def issues(self):
        """finds issues in PR"""
        if self.pull_request.body is not None:
            regex = r"(?<=closes: #|elated: #)\d{5}"
            issue_strs = re.findall(regex, self.pull_request.body)
            issue_ints = [eval(s) for s in issue_strs]
            self.issue_nums = issue_ints
            return issue_ints

    @cached_property
    def issue_reactions(self) -> int:
        """counts reactions to issue comments"""
        if self.issue_nums:
            repo = self.g.get_repo("apache/airflow")
            issue_reactions = 0
            for num in self.issue_nums:
                issue = repo.get_issue(number=num)
                for reaction in issue.get_reactions():
                    self._users.add(reaction.user.login)
                    issue_reactions += 1
            self.num_issue_reactions = issue_reactions
            return issue_reactions
        return 0

    @cached_property
    def issue_comments(self) -> int:
        """counts issue comments and calculates comment length"""
        if self.issue_nums:
            repo = self.g.get_repo("apache/airflow")
            issue_comments = 0
            len_issue_comments = 0
            for num in self.issue_nums:
                issue = repo.get_issue(number=num)
                issues = issue.get_comments()
                for issue_comment in issues:
                    issue_comments += 1
                    self._users.add(issue_comment.user.login)
                    if issue_comment.body is not None:
                        len_issue_comments += len(issue_comment.body)
            self.len_issue_comments = len_issue_comments
            self.num_issue_comments = issue_comments
            return issue_comments
        return 0

    @property
    def interaction_score(self) -> float:
        interactions = (
            self.num_comments + self.num_conv_comments + self.issue_comments
        ) * PrStat.COMMENT_INTERACTION_VALUE
        interactions += (
            self.num_reactions + self.num_conv_reactions + self.issue_reactions
        ) * PrStat.REACTION_INTERACTION_VALUE
        interactions += self.num_reviews * PrStat.REVIEW_INTERACTION_VALUE
        return interactions

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
        length = 0
        for comment in self.pull_request.get_comments():
            if comment.body is not None:
                length += len(comment.body)
        for comment in self.pull_request.get_review_comments():
            if comment.body is not None:
                length += len(comment.body)
        for conv_comment in self.pull_request.get_issue_comments():
            if conv_comment.body is not None:
                length += len(conv_comment.body)
        length += self.len_issue_comments
        return length

    @property
    def length_score(self) -> float:
        score = 1.0
        if self.comment_length > 3000:
            score *= 1.3
        if self.comment_length < 200:
            score *= 0.8
        if self.body_length > 2000:
            score *= 1.4
        if self.body_length < 1000:
            score *= 0.8
        if self.body_length < 20:
            score *= 0.4
        return round(score, 3)

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
        interaction_score = self.interaction_score
        interaction_score *= min(self.protm_score + 1, 3)
        return round(
            1.0
            * interaction_score
            * self.label_score
            * self.length_score
            * self.change_score
            / (math.log10(self.num_changed_files) if self.num_changed_files > 20 else 1.0),
            3,
        )

    def __str__(self) -> str:
        if self.protm_score > 0:
            return (
                "[magenta]##Tagged PR## [/]"
                f"Score: {self.score:.2f}: PR{self.pull_request.number} by @{self.pull_request.user.login}: "
                f'"{self.pull_request.title}". '
                f"Merged at {self.pull_request.merged_at}: {self.pull_request.html_url}"
            )
        else:
            return (
                f"Score: {self.score:.2f}: PR{self.pull_request.number} by @{self.pull_request.user.login}: "
                f'"{self.pull_request.title}". '
                f"Merged at {self.pull_request.merged_at}: {self.pull_request.html_url}"
            )

    def verboseStr(self) -> str:
        if self.protm_score > 0:
            console.print("********************* Tagged with '#protm' *********************", style="magenta")
        return (
            f"-- Created at [bright_blue]{self.pull_request.created_at}[/], "
            f"merged at [bright_blue]{self.pull_request.merged_at}[/]\n"
            f"-- Label score: [green]{self.label_score}[/]\n"
            f"-- Length score: [green]{self.length_score}[/] "
            f"(body length: {self.body_length}, "
            f"comment length: {self.comment_length})\n"
            f"-- Interaction score: [green]{self.interaction_score}[/] "
            f"(users interacting: {self.num_interacting_users}, "
            f"reviews: {self.num_reviews}, "
            f"review comments: {self.num_comments}, "
            f"review reactions: {self.num_reactions}, "
            f"non-review comments: {self.num_conv_comments}, "
            f"non-review reactions: {self.num_conv_reactions}, "
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
    "--date-start", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(DEFAULT_BEGINNING_OF_MONTH.date())
)
@click.option(
    "--date-end", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(DEFAULT_END_OF_MONTH.date())
)
@click.option("--top-number", type=int, default=DEFAULT_TOP_PRS, help="The number of PRs to select")
@click.option("--save", type=click.File("wb"), help="Save PR data to a pickle file")
@click.option("--load", type=click.File("rb"), help="Load PR data from a file and recalculate scores")
@click.option("--verbose", is_flag="True", help="Print scoring details")
def main(
    github_token: str,
    date_start: datetime,
    save: click.File(),  # type: ignore
    load: click.File(),  # type: ignore
    date_end: datetime,
    top_number: int,
    verbose: bool,
):
    selected_prs: list[PrStat] = []
    if load:
        console.print("Loading PRs from cache and recalculating scores.")
        selected_prs = pickle.load(load, encoding="bytes")
        issue_num = 0
        for pr_stat in selected_prs:
            issue_num += 1
            console.print(
                f"[green]Loading PR: #{pr_stat.pull_request.number} `{pr_stat.pull_request.title}`.[/]"
                f" Score: {pr_stat.score}."
                f" Url: {pr_stat.pull_request.html_url}"
            )

            if verbose:
                console.print(pr_stat.verboseStr())

    else:
        console.print(f"Finding best candidate PRs between {date_start} and {date_end}.")
        g = Github(github_token)
        repo = g.get_repo("apache/airflow")
        pulls = repo.get_pulls(state="closed", sort="created", direction="desc")
        issue_num = 0
        for pr in pulls:
            if not pr.merged:
                continue

            if not (date_start < pr.merged_at < date_end):
                console.print(
                    f"[bright_blue]Skipping {pr.number} {pr.title} as it was not "
                    f"merged between {date_start} and {date_end}]"
                )
                continue

            if pr.merged_at < date_start:
                console.print("[bright_blue]Completed selecting candidates")
                break

            issue_num += 1
            pr_stat = PrStat(pull_request=pr, g=g)  # type: ignore
            console.print(
                f"[green]Selecting PR: #{pr.number} `{pr.title}` as candidate.[/]"
                f" Score: {pr_stat.score}."
                f" Url: {pr.html_url}"
            )

            if verbose:
                console.print(pr_stat.verboseStr())

            selected_prs.append(pr_stat)
            if issue_num == MAX_PR_CANDIDATES:
                console.print(f"[red]Reached {MAX_PR_CANDIDATES}. Stopping")
                break

    console.print(f"Top {top_number} out of {issue_num} PRs:")
    for pr_stat in sorted(selected_prs, key=lambda s: -s.score)[:top_number]:
        console.print(f" * {pr_stat}")

    if save:
        pickle.dump(selected_prs, save)


if __name__ == "__main__":
    main()
