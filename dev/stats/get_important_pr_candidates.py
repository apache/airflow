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
# requires-python = ">=3.9"
# dependencies = [
#     "pendulum>=3.0.0",
#     "requests>=2.31.0",
#     "rich-click>=1.7.0",
#     "PyGithub>=2.1.1",
#     "rich>=13.7.0",
# ]
# ///
from __future__ import annotations

import heapq
import json
import logging
import math
import os
import pickle
import re
import subprocess
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


def get_github_token_from_gh_cli() -> str | None:
    """Retrieve GitHub token from gh CLI tool."""
    try:
        result = subprocess.run(
            ["gh", "auth", "token"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        token = result.stdout.strip()
        if token:
            return token
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def get_github_token(token: str | None) -> str:
    """Get GitHub token from parameter, env var, or gh CLI."""
    if token:
        return token

    # Try environment variable
    env_token = os.environ.get("GITHUB_TOKEN")
    if env_token:
        return env_token

    # Try gh CLI
    gh_token = get_github_token_from_gh_cli()
    if gh_token:
        console.print("[blue]Using GitHub token from gh CLI[/]")
        return gh_token

    raise click.ClickException(
        "GitHub token is required. Provide via --github-token, GITHUB_TOKEN env variable, "
        "or authenticate with 'gh auth login'"
    )


option_github_token = click.option(
    "--github-token",
    type=str,
    required=False,
    help=textwrap.dedent(
        """
        A GitHub token is required, and can also be provided by setting the GITHUB_TOKEN env variable
        or retrieved automatically from 'gh' CLI if authenticated.
        Can be generated with:
        https://github.com/settings/tokens/new?description=Read%20issues&scopes=repo:status"""
    ),
)


class PRFetcher:
    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self.base_url = "https://api.github.com/graphql"

    def fetch_prs_bulk(self, pr_numbers: list[int], batch_size: int = 10) -> list[dict]:
        """Fetch multiple PRs with COMPLETE data and proper pagination."""

        pr_queries = []
        for i, pr_num in enumerate(pr_numbers[:batch_size]):
            pr_queries.append(f"""
            pr{i}: pullRequest(number: {pr_num}) {{
                number
                title
                body
                createdAt
                mergedAt
                url
                author {{
                    login
                    ... on User {{
                        name
                    }}
                }}
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
                errors = data["errors"]

                # Check for RESOURCE_LIMIT(S)_EXCEEDED error - handle silently (before logging)
                for error in errors:
                    error_type = error.get("type", "")
                    error_message = error.get("message", "")

                    # Check for both singular and plural variants
                    if (
                        error_type in ("RESOURCE_LIMIT_EXCEEDED", "RESOURCE_LIMITS_EXCEEDED")
                        or "RESOURCE_LIMIT" in error_type
                        or "RESOURCE_LIMIT" in error_message
                    ):
                        # Resource limit exceeded - retry with smaller batch (silently)
                        if batch_size > 1:
                            new_batch_size = max(1, batch_size // 2)
                            console.print(
                                f"[yellow]⚠ GraphQL resource limit exceeded - "
                                f"reducing batch size from {batch_size} to {new_batch_size}[/]"
                            )
                            # Recursively retry with smaller batch
                            return self.fetch_prs_bulk(pr_numbers, batch_size=new_batch_size)
                        console.print(
                            f"[red]❌ Resource limit exceeded even with batch size 1, "
                            f"skipping PR {pr_numbers[0] if pr_numbers else 'unknown'}[/]"
                        )
                        return []

                # Only log non-resource-limit errors
                # (if we got here, it's not a resource limit error)
                logger.error("GraphQL errors: %s", errors)

                if "data" not in data:
                    return []

            prs = []
            repo_data = data.get("data", {}).get("repository", {})
            for key, pr_data in repo_data.items():
                if key.startswith("pr") and pr_data:
                    prs.append(pr_data)

            return prs

        except Exception as e:
            logger.error("GraphQL request exception: %s", e)
            return []

    def fetch_linked_issues(self, pr_body: str, github_client: Github) -> dict:
        """Fetch reactions from linked issues."""
        if not pr_body:
            return {"issue_comments": 0, "issue_reactions": 0, "issue_users": set()}

        regex = r"(?:closes|related): #(\d+)"
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
        self.author_name = pr_data.get("author", {}).get("name") if pr_data.get("author") else None
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
            author = comment.get("author") or {}
            if author.get("login"):
                self._users.add(author["login"])

            comment_body = comment.get("body", "") or ""
            if "protm" in comment_body.lower():
                self.tagged_protm = True

            self.num_comments += 1
            self.len_comments += len(comment_body)

            reactions = comment.get("reactions", {}) or {}
            reaction_nodes = reactions.get("nodes", [])
            for reaction in reaction_nodes:
                user = reaction.get("user") or {}
                if user.get("login"):
                    self._users.add(user["login"])
                self.comment_reactions += 1

    def calc_conv_comments(self):
        """Process conversational comments, check for #protm tag, count reactions."""
        timeline_data = self.pr_data.get("timelineItems", {})
        timeline_nodes = timeline_data.get("nodes", [])

        for item in timeline_nodes:
            author = item.get("author") or {}
            if author.get("login"):
                self._users.add(author["login"])

            comment_body = item.get("body", "") or ""
            if "protm" in comment_body.lower():
                self.tagged_protm = True

            self.num_conv_comments += 1
            self.len_issue_comments += len(comment_body)

            reactions = item.get("reactions", {}) or {}
            reaction_nodes = reactions.get("nodes", [])
            for reaction in reaction_nodes:
                user = reaction.get("user") or {}
                if user.get("login"):
                    self._users.add(user["login"])
                self.conv_comment_reactions += 1

    def calc_review_comments(self):
        """Process review thread comments."""
        review_threads = self.pr_data.get("reviewThreads", {})
        thread_nodes = review_threads.get("nodes", [])

        for thread in thread_nodes:
            comments = thread.get("comments", {})
            comment_nodes = comments.get("nodes", [])

            for comment in comment_nodes:
                author = comment.get("author") or {}
                if author.get("login"):
                    self._users.add(author["login"])

                comment_body = comment.get("body", "") or ""
                if "protm" in comment_body.lower():
                    self.tagged_protm = True

                self.len_comments += len(comment_body)

                reactions = comment.get("reactions", {}) or {}
                reaction_nodes = reactions.get("nodes", [])
                for reaction in reaction_nodes:
                    user = reaction.get("user") or {}
                    if user.get("login"):
                        self._users.add(user["login"])
                    self.comment_reactions += 1

    def calc_reviews(self):
        """Process reviews."""
        reviews_data = self.pr_data.get("reviews", {})
        review_nodes = reviews_data.get("nodes", [])

        for review in review_nodes:
            author = review.get("author") or {}
            if author.get("login"):
                self._users.add(author["login"])

            review_body = review.get("body", "") or ""
            if "protm" in review_body.lower():
                self.tagged_protm = True

    def calc_pr_reactions(self):
        """Process PR-level reactions."""
        reactions_data = self.pr_data.get("reactions", {})
        reaction_nodes = reactions_data.get("nodes", [])

        for reaction in reaction_nodes:
            user = reaction.get("user") or {}
            if user.get("login"):
                self._users.add(user["login"])

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

    def get_author_display(self) -> str:
        """Format author information as 'by @github_id (Name)' or just 'by @github_id' if name not available."""
        if self.author_name:
            return f"by @{self.author} ({self.author_name})"
        return f"by @{self.author}"

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
        self.author_cache: dict[str, dict] = {}

    def is_rookie_author(self, author: str) -> bool:
        """Check if an author is a rookie (less than 5 PRs, first PR within 2 months)."""
        if author in self.author_cache:
            return self.author_cache[author]["is_rookie"]

        # Skip bot accounts - they're not rookies
        bot_accounts = [
            "dependabot",
            "dependabot[bot]",
            "github-actions",
            "github-actions[bot]",
            "pre-commit-ci",
            "pre-commit-ci[bot]",
        ]
        if author.lower() in [b.lower() for b in bot_accounts] or author.endswith("[bot]"):
            self.author_cache[author] = {"is_rookie": False, "total_prs": -1, "reason": "bot account"}
            return False

        try:
            # Use GraphQL to fetch author's PR count and first PR date - more efficient
            query = """
            query($author: String!) {
                search(query: $author, type: ISSUE, first: 5) {
                    issueCount
                    edges {
                        node {
                            ... on PullRequest {
                                number
                                mergedAt
                                closedAt
                            }
                        }
                    }
                }
            }
            """

            search_query = f"repo:apache/airflow type:pr author:{author} is:merged sort:created-asc"
            variables = {"author": search_query}

            response = requests.post(
                "https://api.github.com/graphql",
                json={"query": query, "variables": variables},
                headers=self.graphql_fetcher.headers,
                timeout=10,
            )

            if response.status_code == 403:
                # Rate limit or auth issue - mark as non-rookie to continue
                console.print(f"[dim yellow]⚠ API rate limit for {author}, skipping rookie check[/]")
                self.author_cache[author] = {"is_rookie": False, "error": "rate_limit"}
                return False

            if response.status_code != 200:
                console.print(f"[dim yellow]⚠ API error {response.status_code} for {author}[/]")
                self.author_cache[author] = {"is_rookie": False, "error": f"http_{response.status_code}"}
                return False

            data = response.json()

            if "errors" in data:
                errors = data["errors"]

                # Check for RESOURCE_LIMIT(S)_EXCEEDED error - both singular and plural
                for error in errors:
                    error_type = error.get("type", "")
                    error_message = error.get("message", "")

                    if (
                        error_type in ("RESOURCE_LIMIT_EXCEEDED", "RESOURCE_LIMITS_EXCEEDED")
                        or "RESOURCE_LIMIT" in error_type
                        or "RESOURCE_LIMIT" in error_message
                    ):
                        console.print(
                            f"[dim yellow]⚠ Resource limit exceeded for author {author}, "
                            f"marking as non-rookie to continue[/]"
                        )
                        self.author_cache[author] = {"is_rookie": False, "error": "resource_limit"}
                        return False

                # Other GraphQL errors
                self.author_cache[author] = {"is_rookie": False, "error": "graphql_error"}
                return False

            search_data = data.get("data", {}).get("search", {})
            total_prs = search_data.get("issueCount", 0)

            if total_prs >= 5:
                self.author_cache[author] = {"is_rookie": False, "total_prs": total_prs}
                return False

            # Get the first PR
            edges = search_data.get("edges", [])
            if not edges:
                self.author_cache[author] = {"is_rookie": False, "total_prs": 0}
                return False

            first_pr_node = edges[0].get("node", {})
            first_pr_merged_str = first_pr_node.get("mergedAt") or first_pr_node.get("closedAt")

            if not first_pr_merged_str:
                self.author_cache[author] = {"is_rookie": False, "total_prs": total_prs}
                return False

            # Parse ISO datetime string
            parsed_date = pendulum.parse(first_pr_merged_str)
            # Ensure we have a DateTime instance for comparison
            if not isinstance(parsed_date, pendulum.DateTime):
                self.author_cache[author] = {"is_rookie": False, "total_prs": total_prs}
                return False

            first_pr_merged = parsed_date
            two_months_ago = pendulum.now().subtract(months=2)

            is_rookie = first_pr_merged >= two_months_ago
            self.author_cache[author] = {
                "is_rookie": is_rookie,
                "total_prs": total_prs,
                "first_pr_date": first_pr_merged,
            }
            return is_rookie

        except Exception as e:
            # Silently handle errors - don't clutter output during bulk processing
            error_msg = str(e)
            if "403" not in error_msg and "Forbidden" not in error_msg and "rate" not in error_msg.lower():
                console.print(f"[dim yellow]⚠ Error checking {author}: {e}[/]")
            self.author_cache[author] = {"is_rookie": False, "error": str(e)}
            return False

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

            is_protm_query = "protm" in query.lower()
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
                        "found_by_protm_search": is_protm_query,
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
            elif body_len < 20:
                score *= 0.4
            elif body_len < 1000:
                score *= 0.8

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
            if "protm" in full_text or pr.get("found_by_protm_search"):
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

    def filter_rookie_prs(self, pr_stats: list[PrStat], max_workers: int = 4) -> list[PrStat]:
        """Filter PRs to only include those from rookie authors."""
        console.print("[blue]🆕 Filtering for rookie authors...[/]")

        rookie_prs = []

        # Check authors in parallel for better performance
        def check_pr_author(pr_stat: PrStat) -> tuple[PrStat, bool]:
            is_rookie = self.is_rookie_author(pr_stat.author)
            return (pr_stat, is_rookie)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(check_pr_author, pr) for pr in pr_stats]

            for future in as_completed(futures):
                pr_stat, is_rookie = future.result()
                if is_rookie:
                    rookie_prs.append(pr_stat)
                    author_info = self.author_cache.get(pr_stat.author, {})
                    console.print(
                        f"[green]✓ Rookie: @{pr_stat.author} - {author_info.get('total_prs', 0)} PRs, "
                        f"first PR: {author_info.get('first_pr_date', 'unknown')}[/]"
                    )

        console.print(f"[green]Found {len(rookie_prs)} PRs from rookie authors (out of {len(pr_stats)})[/]")
        return rookie_prs


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
@click.option(
    "--rookie", is_flag=True, help="Only consider PRs from rookie authors (<5 PRs, first PR within 2 months)"
)
@click.option("--show-score", is_flag=True, help="Include score in the output")
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
    rookie: bool,
    show_score: bool,
):
    """Super-fast PR finder with COMPLETE data capture and proper GraphQL pagination."""

    github_token = get_github_token(github_token)

    console.print("[bold blue]🚀 Fixed Super-Fast PR Candidate Finder[/bold blue]")
    console.print(f"Date range: {date_start.date()} to {date_end.date()}")
    if rookie:
        console.print("[bold cyan]🆕 Rookie mode: Only considering PRs from new contributors[/bold cyan]")

    # Initialize finder - needed for rookie mode or for general use
    finder = None

    if load:
        console.print("[yellow]Loading from cache...[/]")
        pr_stats = pickle.load(load)

        if rookie:
            finder = SuperFastPRFinder(github_token)
            pr_stats = finder.filter_rookie_prs(pr_stats, max_workers)

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

        if rookie:
            pr_stats = finder.filter_rookie_prs(pr_stats, max_workers)

        scores = {pr.number: pr.score for pr in pr_stats}

    # Format date range for display
    date_range_str = f"{date_start.strftime('%Y-%m-%d')} to {date_end.strftime('%Y-%m-%d')}"
    console.print(f"\n[bold green]🏆 Top {top_number} PRs ({date_range_str}):[/bold green]\n")
    top_final = heapq.nlargest(top_number, scores.items(), key=lambda x: x[1])

    for i, (pr_num, score) in enumerate(top_final, 1):
        pr_stat = next((pr for pr in pr_stats if pr.number == pr_num), None)
        if pr_stat:
            pr_stat.process_all_data()
            protm_indicator = "🔥" if pr_stat.tagged_protm else ""

            # Build the main PR line
            if show_score:
                console.print(
                    f"[green]{i:2d}. {protm_indicator} Score: {score:.2f} - PR#{pr_num}: {pr_stat.title}[/]"
                )
            else:
                console.print(f"[green]{i:2d}. {protm_indicator} PR#{pr_num}: {pr_stat.title}[/]")

            # Show author information
            author_display = pr_stat.get_author_display()

            # Add rookie information if in rookie mode and we have the data
            if rookie and finder is not None and pr_stat.author in finder.author_cache:
                author_info = finder.author_cache[pr_stat.author]
                if author_info.get("is_rookie"):
                    total_prs = author_info.get("total_prs", 0)
                    first_pr_date = author_info.get("first_pr_date")
                    if first_pr_date:
                        # Format the date nicely
                        if isinstance(first_pr_date, str):
                            date_str = first_pr_date
                        else:
                            date_str = (
                                first_pr_date.strftime("%Y-%m-%d")
                                if hasattr(first_pr_date, "strftime")
                                else str(first_pr_date)
                            )
                        author_display += f" [cyan]🆕 Rookie: PR #{total_prs}, first merged {date_str}[/cyan]"

            console.print(f"     {author_display}")
            console.print(f"     [dim]{pr_stat.url}[/dim]")

            if verbose:
                console.print(
                    f"     [dim]Files: {pr_stat.changed_files}, "
                    f"+{pr_stat.additions}/-{pr_stat.deletions}, Comments: {pr_stat.num_comments + pr_stat.num_conv_comments}[/dim]"
                )
                if pr_stat.tagged_protm:
                    console.print("     [magenta]🔥 CONTAINS #PROTM TAG[/magenta]")

            # Add empty line between PRs for better readability
            console.print()

    if save:
        console.print("[blue]💾 Saving complete results...[/]")
        pickle.dump(pr_stats, save)

    console.print(f"\n[bold blue]✅ Analysis complete! Processed {len(pr_stats)} PRs[/bold blue]")


if __name__ == "__main__":
    main()
