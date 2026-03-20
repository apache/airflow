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
"""Comment building for PR triage actions (draft, close, nudge)."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow_breeze.utils.pr_models import StaleReviewInfo

QUALITY_CRITERIA_LINK = (
    "[Pull Request quality criteria](https://github.com/apache/airflow/blob/main/"
    "contributing-docs/05_pull_requests.rst#pull-request-quality-criteria)"
)


def load_what_to_do_next() -> str:
    """Load 'what to do next' instructions from contributing docs."""
    from airflow_breeze.utils.llm_utils import _read_file_section
    from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

    section = _read_file_section(
        AIRFLOW_ROOT_PATH,
        "contributing-docs/05_pull_requests.rst",
        "What happens when a PR is converted to draft",
        "Converting a PR to draft is",
    )
    if section:
        lines = []
        for line in section.splitlines():
            stripped = line.strip()
            if stripped.startswith("-"):
                lines.append(stripped.replace("-  ", "- ", 1))
            elif stripped:
                lines.append(stripped)
        return "\n".join(lines)
    return (
        "- Fix each issue listed above.\n"
        "- Make sure static checks pass locally (`prek run --from-ref main --stage pre-commit`).\n"
        '- Mark the PR as "Ready for review" when you\'re done.'
    )


def build_collaborator_comment(
    pr_author: str,
    violations: list,
    commits_behind: int,
    base_ref: str,
    comment_only: bool = False,
) -> str:
    """Build a simplified comment for PRs authored by collaborators."""
    violation_lines = []
    for v in violations:
        line = f"- **{v.category}**: {v.explanation}"
        if v.details:
            line += f" {v.details}"
        violation_lines.append(line)
    violations_text = "\n".join(violation_lines)

    rebase_note = ""
    if commits_behind > 0:
        rebase_note = (
            f"\n\nBranch is {commits_behind} "
            f"commit{'s' if commits_behind != 1 else ''} behind `{base_ref}` "
            "-- some failures may be from the base branch."
        )

    if comment_only:
        return f"@{pr_author} Issues found:\n{violations_text}{rebase_note}"

    return f"@{pr_author} Converted to draft. Issues found:\n{violations_text}{rebase_note}"


def build_comment(
    pr_author: str,
    violations: list,
    pr_number: int,
    commits_behind: int,
    base_ref: str,
    comment_only: bool = False,
    is_collaborator: bool = False,
) -> str:
    """Build the comment to post on a flagged PR."""
    if is_collaborator:
        return build_collaborator_comment(
            pr_author, violations, commits_behind, base_ref, comment_only=comment_only
        )

    violation_lines = []
    for v in violations:
        icon = "x" if v.severity == "error" else "warning"
        line = f"- :{icon}: **{v.category}**: {v.explanation}"
        if v.details:
            line += f" {v.details}"
        violation_lines.append(line)
    violations_text = "\n".join(violation_lines)

    what_to_do = load_what_to_do_next()

    rebase_note = ""
    if commits_behind > 50:
        rebase_note = (
            f"\n\n> **Note:** Your branch is **{commits_behind} "
            f"commit{'s' if commits_behind != 1 else ''} behind `{base_ref}`**. "
            "Some check failures may be caused by changes in the base branch rather than by your PR. "
            "Please rebase your branch and push again to get up-to-date CI results."
        )

    no_rush = (
        "There is no rush — take your time and work at your own pace. "
        "We appreciate your contribution and are happy to wait for updates."
    )

    if comment_only:
        return (
            f"@{pr_author} This PR has a few issues that need to be addressed before it can be "
            f"reviewed — please see our {QUALITY_CRITERIA_LINK}.\n\n"
            f"**Issues found:**\n{violations_text}{rebase_note}\n\n"
            f"**What to do next:**\n{what_to_do}\n\n"
            f"{no_rush} "
            "If you have questions, feel free to ask on the "
            "[Airflow Slack](https://s.apache.org/airflow-slack)."
        )

    return (
        f"@{pr_author} This PR has been converted to **draft** because it does not yet meet "
        f"our {QUALITY_CRITERIA_LINK}.\n\n"
        f"**Issues found:**\n{violations_text}{rebase_note}\n\n"
        f"**What to do next:**\n{what_to_do}\n\n"
        "Converting a PR to draft is **not** a rejection — it is an invitation to bring the PR up to "
        "the project's standards so that maintainer review time is spent productively. "
        f"{no_rush} "
        "If you have questions, feel free to ask on the "
        "[Airflow Slack](https://s.apache.org/airflow-slack)."
    )


def build_close_comment(
    pr_author: str,
    violations: list,
    pr_number: int,
    author_flagged_count: int,
    is_collaborator: bool = False,
) -> str:
    """Build the comment to post on a PR being closed due to quality issues."""
    if is_collaborator:
        violation_lines = []
        for v in violations:
            line = f"- **{v.category}**: {v.explanation}"
            if v.details:
                line += f" {v.details}"
            violation_lines.append(line)
        violations_text = "\n".join(violation_lines)
        return f"@{pr_author} Closing due to quality issues:\n{violations_text}"

    violation_lines = []
    for v in violations:
        icon = "x" if v.severity == "error" else "warning"
        line = f"- :{icon}: **{v.category}**: {v.explanation}"
        if v.details:
            line += f" {v.details}"
        violation_lines.append(line)
    if author_flagged_count > 3:
        violation_lines.append(
            f"- :x: **Multiple flagged PRs**: You currently have **{author_flagged_count} "
            f"{'PRs' if author_flagged_count != 1 else 'PR'}** "
            "flagged for quality issues in this repository. We recommend focusing on improving "
            f"your existing {'PRs' if author_flagged_count != 1 else 'PR'} before opening new ones."
        )
    violations_text = "\n".join(violation_lines)

    return (
        f"@{pr_author} This PR has been **closed because of multiple quality violations** "
        f"— it does not meet our {QUALITY_CRITERIA_LINK}.\n\n"
        f"**Issues found:**\n{violations_text}\n\n"
        "You are welcome to open a new PR that addresses the issues listed above. "
        "There is no rush — take your time and work at your own pace. "
        "If you have questions, feel free to ask on the "
        "[Airflow Slack](https://s.apache.org/airflow-slack)."
    )


def build_review_nudge_comment(pr_author: str, stale_reviews: list[StaleReviewInfo]) -> str:
    """Build a comment to nudge review follow-up on stale CHANGES_REQUESTED reviews."""
    pinged = [r for r in stale_reviews if r.author_pinged_reviewer]
    not_pinged = [r for r in stale_reviews if not r.author_pinged_reviewer]

    parts: list[str] = []

    if pinged:
        reviewer_tags = " ".join(f"@{r.reviewer_login}" for r in pinged)
        parts.append(
            f"@{pr_author} {reviewer_tags} — This PR has new commits since the last review "
            "requesting changes, and it looks like the author has followed up. "
            "Could you take another look when you have a chance to see if the review "
            "comments have been addressed? Thanks!"
        )

    if not_pinged:
        reviewer_names = ", ".join(f"**{r.reviewer_login}**" for r in not_pinged)
        parts.append(
            f"@{pr_author} — This PR has new commits since the last review requesting changes "
            f"from {reviewer_names}. If you believe you've addressed the feedback, "
            "please ping the reviewer(s) to request a re-review. Thanks!"
        )

    return "\n\n".join(parts)
