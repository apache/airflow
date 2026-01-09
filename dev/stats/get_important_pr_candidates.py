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
import json
import logging
import math
import os
import pickle
import re
import textwrap
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import cached_property

import pendulum
import requests
import rich_click as click
from github import Github, UnknownObjectException
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


class PRFetcher:
    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self.base_url = "https://api.github.com/graphql"

    def fetch_prs_bulk(self, pr_numbers: list[int]) -> list[dict]:
        """Fetch multiple PRs with COMPLETE data and proper pagination."""

        pr_queries = []
        for i, pr_num in enumerate(pr_numbers[:10]):
            pr_queries.append(f"""
            pr{i}: pullRequest(number: {pr_num}) {{
                number
                title
                body
                createdAt
                mergedAt
                url
                author {{ login }}
                additions
                deletions
                changedFiles
                labels(first: 20) {{
                    nodes {{ name }}
                }}

                comments(first: 50) {{
                    totalCount
                    nodes {{
                        body
                        author {{ login }}
                        reactions(first: 20) {{
                            totalCount
                            nodes {{
                                user {{ login }}
                                content
                            }}
                        }}
                    }}
                }}

                timelineItems(first: 50, itemTypes: [ISSUE_COMMENT]) {{
                    totalCount
                    nodes {{
                        ... on IssueComment {{
                            body
                            author {{ login }}
                            reactions(first: 20) {{
                                totalCount
                                nodes {{
                                    user {{ login }}
                                    content
                                }}
                            }}
                        }}
                    }}
                }}

                reviews(first: 50) {{
                    totalCount
                    nodes {{
                        author {{ login }}
                        body
                        state
                        reactions(first: 20) {{
                            totalCount
                            nodes {{
                                user {{ login }}
                                content
                            }}
                        }}
                    }}
                }}

                reviewThreads(first: 30) {{
                    totalCount
                    nodes {{
                        comments(first: 10) {{
                            nodes {{
                                body
                                author {{ login }}
                                reactions(first: 20) {{
                                    totalCount
                                    nodes {{
                                        user {{ login }}
                                        content
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}

                reactions(first: 50) {{
                    totalCount
                    nodes {{
                        user {{ login }}
                        content
                    }}
                }}
            }}
            """)

        query = f"""
        query {{
            repository(owner: "apache", name: "airflow") {{
                {" ".join(pr_queries)}
            }}
        }}
        """

        try:
            response = requests.post(self.base_url, json={"query": query}, headers=self.headers, timeout=60)

            if response.status_code != 200:
                logger.error("GraphQL request failed: %s %s", response.status_code, response.text)
                return []

            data = response.json()
            if "errors" in data:
                logger.error("GraphQL errors: %s", {data["errors"]})
                if "data" not in data:
                    return []

            prs = []
            repo_data = data.get("data", {}).get("repository", {})
            for key, pr_data in repo_data.items():
                if key.startswith("pr") and pr_data:
                    prs.append(pr_data)

            return prs

        except Exception as e:
            logger.error("GraphQL request exception: %s", {e})
            return []

    def fetch_linked_issues(self, pr_body: str, github_client: Github) -> dict:
        """Fetch reactions from linked issues."""
        if not pr_body:
            return {"issue_comments": 0, "issue_reactions": 0, "issue_users": set()}

        regex = r"(?<=closes: #|elated: #)\d{5}"
        issue_nums = re.findall(regex, pr_body)

        total_issue_comments = 0
        total_issue_reactions = 0
        issue_users = set()

        if issue_nums:
            try:
                repo = github_client.get_repo("apache/airflow")
                for num_str in issue_nums:
                    try:
                        issue_num = int(num_str)
                        issue = repo.get_issue(issue_num)

                        for reaction in issue.get_reactions():
                            issue_users.add(reaction.user.login)
                            total_issue_reactions += 1

                        for issue_comment in issue.get_comments():
                            total_issue_comments += 1
                            issue_users.add(issue_comment.user.login)

                    except (UnknownObjectException, ValueError) as e:
                        console.print(f"[yellow]Issue #{num_str} not found: {e}[/]")
                        continue

            except Exception as e:
                console.print(f"[red]Error fetching issue data: {e}[/]")

        return {
            "issue_comments": total_issue_comments,
            "issue_reactions": total_issue_reactions,
            "issue_users": issue_users,
        }


class PrStat:
    PROVIDER_SCORE = 0.8
    REGULAR_SCORE = 1.0
    REVIEW_INTERACTION_VALUE = 2.0
    COMMENT_INTERACTION_VALUE = 1.0
    REACTION_INTERACTION_VALUE = 0.5

    def __init__(self, pr_data: dict, issue_data: dict | None = None):
        self.pr_data = pr_data
        self.issue_data = issue_data or {}

        self.number = pr_data["number"]
        self.title = pr_data["title"]
        self.body = pr_data.get("body", "") or ""
        self.url = pr_data["url"]
        self.author = pr_data["author"]["login"] if pr_data.get("author") else "unknown"
        self.merged_at = pr_data.get("mergedAt")
        self.created_at = pr_data.get("createdAt")

        self.additions = pr_data.get("additions", 0)
        self.deletions = pr_data.get("deletions", 0)
        self.changed_files = pr_data.get("changedFiles", 1)

        self._users: set[str] = set()
        self.len_comments: int = 0
        self.comment_reactions: int = 0
        self.len_issue_comments: int = 0
        self.num_issue_comments: int = 0
        self.num_issue_reactions: int = 0
        self.num_comments: int = 0
        self.num_conv_comments: int = 0
        self.tagged_protm: bool = False
        self.conv_comment_reactions: int = 0
        self.interaction_score_value = 1.0

        self._score: float | None = None
        self._processed = False

    def calc_comments(self):
        """Process review comments."""
        comments_data = self.pr_data.get("comments", {})
        comments_nodes = comments_data.get("nodes", [])

        for comment in comments_nodes:
            if comment.get("author", {}).get("login"):
                self._users.add(comment["author"]["login"])

            comment_body = comment.get("body", "") or ""
            if "protm" in comment_body.lower():
                self.tagged_protm = True

            self.num_comments += 1
            self.len_comments += len(comment_body)

            reactions = comment.get("reactions", {})
            reaction_nodes = reactions.get("nodes", [])
            for reaction in reaction_nodes:
                if reaction.get("user", {}).get("login"):
                    self._users.add(reaction["user"]["login"])
                self.comment_reactions += 1

    def calc_conv_comments(self):
        """Process conversational comments, check for #protm tag, count reactions."""
        timeline_data = self.pr_data.get("timelineItems", {})
        timeline_nodes = timeline_data.get("nodes", [])

        for item in timeline_nodes:
            if item.get("author", {}).get("login"):
                self._users.add(item["author"]["login"])

            comment_body = item.get("body", "") or ""
            if "protm" in comment_body.lower():
                self.tagged_protm = True

            self.num_conv_comments += 1
            self.len_issue_comments += len(comment_body)

            reactions = item.get("reactions", {})
            reaction_nodes = reactions.get("nodes", [])
            for reaction in reaction_nodes:
                if reaction.get("user", {}).get("login"):
                    self._users.add(reaction["user"]["login"])
                self.conv_comment_reactions += 1

    def calc_review_comments(self):
        """Process review thread comments."""
        review_threads = self.pr_data.get("reviewThreads", {})
        thread_nodes = review_threads.get("nodes", [])

        for thread in thread_nodes:
            comments = thread.get("comments", {})
            comment_nodes = comments.get("nodes", [])

            for comment in comment_nodes:
                if comment.get("author", {}).get("login"):
                    self._users.add(comment["author"]["login"])

                comment_body = comment.get("body", "") or ""
                if "protm" in comment_body.lower():
                    self.tagged_protm = True

                self.len_comments += len(comment_body)

                reactions = comment.get("reactions", {})
                reaction_nodes = reactions.get("nodes", [])
                for reaction in reaction_nodes:
                    if reaction.get("user", {}).get("login"):
                        self._users.add(reaction["user"]["login"])
                    self.comment_reactions += 1

    def calc_reviews(self):
        """Process reviews."""
        reviews_data = self.pr_data.get("reviews", {})
        review_nodes = reviews_data.get("nodes", [])

        for review in review_nodes:
            if review.get("author", {}).get("login"):
                self._users.add(review["author"]["login"])

            review_body = review.get("body", "") or ""
            if "protm" in review_body.lower():
                self.tagged_protm = True

    def calc_pr_reactions(self):
        """Process PR-level reactions."""
        reactions_data = self.pr_data.get("reactions", {})
        reaction_nodes = reactions_data.get("nodes", [])

        for reaction in reaction_nodes:
            if reaction.get("user", {}).get("login"):
                self._users.add(reaction["user"]["login"])

    def calc_issue_reactions(self):
        """Process linked issue data."""
        if self.issue_data:
            self.num_issue_comments = self.issue_data.get("issue_comments", 0)
            self.num_issue_reactions = self.issue_data.get("issue_reactions", 0)
            issue_users = self.issue_data.get("issue_users", set())
            self._users.update(issue_users)

    def calc_interaction_score(self):
        """Calculate interaction score."""
        interactions = (
            self.num_comments + self.num_conv_comments + self.num_issue_comments
        ) * self.COMMENT_INTERACTION_VALUE
        interactions += (
            self.comment_reactions + self.conv_comment_reactions + self.num_issue_reactions
        ) * self.REACTION_INTERACTION_VALUE
        self.interaction_score_value += interactions + self.num_reviews * self.REVIEW_INTERACTION_VALUE

    def adjust_interaction_score(self):
        """Apply protm multiplier."""
        if self.tagged_protm:
            self.interaction_score_value *= 20

    def process_all_data(self):
        """Process all PR data."""
        if self._processed:
            return

        full_text = f"{self.title} {self.body}".lower()
        if "protm" in full_text:
            self.tagged_protm = True

        self.calc_comments()
        self.calc_conv_comments()
        self.calc_review_comments()
        self.calc_reviews()
        self.calc_pr_reactions()
        self.calc_issue_reactions()
        self.calc_interaction_score()
        self.adjust_interaction_score()

        self._processed = True

    @cached_property
    def label_score(self) -> float:
        """Calculate label score from pre-fetched data."""
        labels = self.pr_data.get("labels", {}).get("nodes", [])
        for label in labels:
            if "provider" in label.get("name", "").lower():
                return self.PROVIDER_SCORE
        return self.REGULAR_SCORE

    @cached_property
    def body_length(self) -> int:
        return len(self.body)

    @cached_property
    def change_score(self) -> float:
        lineactions = self.additions + self.deletions
        actionsperfile = lineactions / max(self.changed_files, 1)
        if self.changed_files > 10:
            if actionsperfile > 20:
                return 1.2
            if actionsperfile < 5:
                return 0.7
        return 1.0

    @cached_property
    def length_score(self) -> float:
        """Calculate length score using processed comment data."""
        self.process_all_data()

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

    @cached_property
    def num_reviews(self) -> int:
        """Count reviews."""
        reviews_data = self.pr_data.get("reviews", {})
        return len(reviews_data.get("nodes", []))

    @cached_property
    def interaction_score(self) -> float:
        """Get interaction score."""
        self.process_all_data()
        return self.interaction_score_value

    @property
    def score(self) -> float:
        """Calculate final score based on multiple factors.

        Scoring principles:

        - Provider PRs are weighted 20% less (0.8x multiplier)
        - A review is worth 2x as much as a comment, comment is 2x as much as a reaction
        - PRs with >20 files are penalized by log10(files) to reduce impact of massive changes
        - Change quality scoring:
          * >10 files + <5 lines/file avg: 30% penalty (0.7x)
          * >10 files + >20 lines/file avg: 20% bonus (1.2x)
        - Comment length scoring:
          * >3000 characters in comments: 30% bonus (1.3x)
          * <200 characters in comments: 20% penalty (0.8x)
        - Body length scoring:
          * >2000 characters in body: 40% bonus (1.4x)
          * <1000 characters in body: 20% penalty (0.8x)
          * <20 characters in body: 60% penalty (0.4x)
        - PROTM tag: 20x multiplier on interaction score if found anywhere

        Final formula: interaction_score * label_score * length_score * change_score / file_penalty
        """
        if self._score is not None:
            return self._score

        self.process_all_data()

        self._score = round(
            self.interaction_score
            * self.label_score
            * self.length_score
            * self.change_score
            / (math.log10(self.changed_files) if self.changed_files > 20 else 1),
            3,
        )
        return self._score

    def __str__(self) -> str:
        self.process_all_data()
        prefix = "[magenta]##Tagged PR## [/]" if self.tagged_protm else ""
        return (
            f"{prefix}Score: {self.score:.2f}: PR{self.number} "
            f'by @{self.author}: "{self.title}". '
            f"Merged at {self.merged_at}: {self.url}"
        )


class SuperFastPRFinder:
    """Main class for super-fast PR finding with COMPLETE data capture."""

    def __init__(self, github_token: str):
        self.github_token = github_token
        self.github_client = Github(github_token)
        self.graphql_fetcher = PRFetcher(github_token)

    def search_prs_with_filters(
        self, date_start: datetime, date_end: datetime, limit: int = 1000
    ) -> list[dict]:
        """Use GitHub Search API to find PRs with intelligent pre-filtering."""

        start_str = date_start.strftime("%Y-%m-%d")
        end_str = date_end.strftime("%Y-%m-%d")

        search_queries = [
            f"repo:apache/airflow type:pr is:merged merged:{start_str}..{end_str} protm",
            f"repo:apache/airflow type:pr is:merged merged:{start_str}..{end_str} comments:>15",
            f"repo:apache/airflow type:pr is:merged merged:{start_str}..{end_str} comments:>10 reactions:>5",
            f"repo:apache/airflow type:pr is:merged merged:{start_str}..{end_str} review:approved comments:>8",
            f"repo:apache/airflow type:pr is:merged merged:{start_str}..{end_str} comments:>5",
            f"repo:apache/airflow type:pr is:merged merged:{start_str}..{end_str} reactions:>3",
            f"repo:apache/airflow type:pr is:merged merged:{start_str}..{end_str}",
        ]

        all_prs: list[dict] = []
        seen_numbers = set()

        for query in search_queries:
            if len(all_prs) >= limit:
                break

            console.print(f"[blue]Searching: {query}[/]")

            try:
                search_result = self.github_client.search_issues(query=query, sort="updated", order="desc")

                batch_count = 0
                for issue in search_result:
                    if len(all_prs) >= limit:
                        break

                    if issue.number in seen_numbers:
                        continue

                    seen_numbers.add(issue.number)

                    pr_info = {
                        "number": issue.number,
                        "title": issue.title,
                        "body": issue.body or "",
                        "url": issue.html_url,
                        "comments_count": issue.comments,
                        "created_at": issue.created_at,
                        "updated_at": issue.updated_at,
                        "reactions_count": getattr(issue, "reactions", {}).get("total_count", 0),
                    }

                    all_prs.append(pr_info)
                    batch_count += 1

                    if batch_count >= 200:
                        break

                console.print(f"[green]Found {batch_count} PRs[/]")

            except Exception as e:
                console.print(f"[red]Search failed: {e}[/]")
                continue

        console.print(f"[blue]Total unique PRs: {len(all_prs)}[/]")
        return all_prs

    def quick_score_prs(self, prs: list[dict]) -> list[tuple[int, float]]:
        """Enhanced quick scoring with better protm detection."""
        scored_prs = []

        for pr in prs:
            score = 1.0

            body_len = len(pr.get("body", ""))
            if body_len > 2000:
                score *= 1.4
            elif body_len < 1000:
                score *= 0.8
            elif body_len < 20:
                score *= 0.4

            comments = pr.get("comments_count", 0)
            if comments > 30:
                score *= 4.0
            elif comments > 20:
                score *= 3.0
            elif comments > 10:
                score *= 2.0
            elif comments > 5:
                score *= 1.5
            elif comments < 2:
                score *= 0.6

            reactions = pr.get("reactions_count", 0)
            if reactions > 10:
                score *= 2.0
            elif reactions > 5:
                score *= 1.5
            elif reactions > 2:
                score *= 1.2

            full_text = f"{pr.get('title', '')} {pr.get('body', '')}".lower()
            if "protm" in full_text:
                score *= 20
                console.print(f"[magenta]🔥 Found PROTM PR: #{pr['number']} - {pr['title']}[/]")

            if "provider" in pr.get("title", "").lower():
                score *= 0.8

            scored_prs.append((pr["number"], score))

        return scored_prs

    def fetch_full_pr_data(self, pr_numbers: list[int], max_workers: int = 4) -> list[PrStat]:
        """Fetch COMPLETE PR data with linked issues."""

        console.print(f"[blue]Fetching complete data for {len(pr_numbers)} PRs...[/]")

        batch_size = 8
        batches = [pr_numbers[i : i + batch_size] for i in range(0, len(pr_numbers), batch_size)]

        all_pr_data = []

        def fetch_batch(batch):
            try:
                return self.graphql_fetcher.fetch_prs_bulk(batch)
            except Exception as e:
                console.print(f"[red]GraphQL batch error: {e}[/]")
                return []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_batch, batch) for batch in batches]

            for i, future in enumerate(as_completed(futures)):
                batch_data = future.result()
                all_pr_data.extend(batch_data)
                console.print(f"[green]GraphQL batch {i + 1}/{len(batches)} complete[/]")

        console.print("[blue]Fetching linked issue data...[/]")

        def fetch_issue_data(pr_data):
            try:
                return self.graphql_fetcher.fetch_linked_issues(pr_data.get("body", ""), self.github_client)
            except Exception as e:
                console.print(f"[red]Issue fetch error for PR {pr_data.get('number')}: {e}[/]")
                return {}

        issue_data_list = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_issue_data, pr_data) for pr_data in all_pr_data]

            for future in as_completed(futures):
                issue_data_list.append(future.result())

        pr_stats = []
        for pr_data, issue_data in zip(all_pr_data, issue_data_list):
            if pr_data:
                pr_stats.append(PrStat(pr_data, issue_data))

        console.print(f"[blue]Successfully processed {len(pr_stats)} PRs with complete data[/]")
        return pr_stats


DAYS_BACK = 5
DEFAULT_BEGINNING_OF_MONTH = pendulum.now().subtract(days=DAYS_BACK).start_of("month")
DEFAULT_END_OF_MONTH = DEFAULT_BEGINNING_OF_MONTH.end_of("month").add(days=1)


@click.command()
@option_github_token
@click.option(
    "--date-start", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(DEFAULT_BEGINNING_OF_MONTH.date())
)
@click.option(
    "--date-end", type=click.DateTime(formats=["%Y-%m-%d"]), default=str(DEFAULT_END_OF_MONTH.date())
)
@click.option("--top-number", type=int, default=10, help="The number of PRs to select")
@click.option("--max-candidates", type=int, default=150, help="Max candidates for full analysis")
@click.option("--search-limit", type=int, default=800, help="Max PRs to find with search")
@click.option("--max-workers", type=int, default=4, help="Max parallel workers")
@click.option("--save", type=click.File("wb"), help="Save PR data to a pickle file")
@click.option("--load", type=click.File("rb"), help="Load PR data from cache")
@click.option("--verbose", is_flag=True, help="Print detailed output")
@click.option("--cache-search", type=click.Path(), help="Cache search results to file")
@click.option("--load-search", type=click.Path(), help="Load search results from cache")
def main(
    github_token: str,
    date_start: datetime,
    date_end: datetime,
    top_number: int,
    max_candidates: int,
    search_limit: int,
    max_workers: int,
    save,
    load,
    verbose: bool,
    cache_search: str | None,
    load_search: str | None,
):
    """Super-fast PR finder with COMPLETE data capture and proper GraphQL pagination."""

    console.print("[bold blue]🚀 Fixed Super-Fast PR Candidate Finder[/bold blue]")
    console.print(f"Date range: {date_start.date()} to {date_end.date()}")

    if load:
        console.print("[yellow]Loading from cache...[/]")
        pr_stats = pickle.load(load)
        scores = {pr.number: pr.score for pr in pr_stats}

    else:
        finder = SuperFastPRFinder(github_token)

        if load_search and os.path.exists(load_search):
            console.print(f"[yellow]Loading search results from {load_search}[/]")
            with open(load_search) as f:
                search_results = json.load(f)
        else:
            console.print("[blue]🔍 Phase 1: Enhanced PR discovery[/]")
            search_results = finder.search_prs_with_filters(date_start, date_end, search_limit)

            if cache_search:
                console.print(f"[blue]Caching search results to {cache_search}[/]")
                with open(cache_search, "w") as f:
                    json.dump(search_results, f, default=str, indent=2)

        console.print("[blue]⚡ Phase 2: Quick scoring[/]")
        quick_scores = finder.quick_score_prs(search_results)

        top_candidates = heapq.nlargest(max_candidates, quick_scores, key=lambda x: x[1])
        candidate_numbers = [num for num, _ in top_candidates]

        console.print(f"[green]Selected {len(candidate_numbers)} candidates for complete analysis[/]")

        console.print("[blue]🔥 Phase 3: Complete data fetching with proper pagination[/]")
        pr_stats = finder.fetch_full_pr_data(candidate_numbers, max_workers)

        scores = {pr.number: pr.score for pr in pr_stats}

    console.print(f"\n[bold green]🏆 Top {top_number} PRs:[/bold green]")
    top_final = heapq.nlargest(top_number, scores.items(), key=lambda x: x[1])

    for i, (pr_num, score) in enumerate(top_final, 1):
        pr_stat = next((pr for pr in pr_stats if pr.number == pr_num), None)
        if pr_stat:
            pr_stat.process_all_data()
            protm_indicator = "🔥" if pr_stat.tagged_protm else ""
            console.print(
                f"[green]{i:2d}. {protm_indicator} Score: {score:.2f} - PR#{pr_num}: {pr_stat.title}[/]"
            )
            console.print(f"     [dim]{pr_stat.url}[/dim]")
            if verbose:
                console.print(
                    f"     [dim]Author: {pr_stat.author}, Files: {pr_stat.changed_files}, "
                    f"+{pr_stat.additions}/-{pr_stat.deletions}, Comments: {pr_stat.num_comments + pr_stat.num_conv_comments}[/dim]"
                )
                if pr_stat.tagged_protm:
                    console.print("     [magenta]🔥 CONTAINS #PROTM TAG[/magenta]")

    if save:
        console.print("[blue]💾 Saving complete results...[/]")
        pickle.dump(pr_stats, save)

    console.print(f"\n[bold blue]✅ Analysis complete! Processed {len(pr_stats)} PRs[/bold blue]")


if __name__ == "__main__":
    main()
