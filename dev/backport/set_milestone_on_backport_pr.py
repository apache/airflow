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
"""
Set milestone on backport PRs that were merged without a milestone.

This script is called by the backport workflow after a successful backport PR is created.
It checks if the backport PR has a milestone set, and if not, finds the appropriate
milestone based on the target branch and sets it on the PR.
"""

from __future__ import annotations

import os
import re
import sys

import requests


def parse_version_from_branch(target_branch: str) -> tuple[int, int] | None:
    """
    Parse major and minor version from a branch name.

    :param target_branch: Branch name like 'v3-1-test' or 'v2-11-test'
    :return: Tuple of (major, minor) version numbers, or None if parsing fails

    Examples:
        >>> parse_version_from_branch("v3-1-test")
        (3, 1)
        >>> parse_version_from_branch("v2-11-test")
        (2, 11)
    """
    # Match pattern like v3-1-test, v2-11-test
    match = re.match(r"v(\d+)-(\d+)-test", target_branch)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None


def get_milestone_prefix(major: int, minor: int) -> str:
    """
    Get the milestone prefix for a given version.

    :param major: Major version number
    :param minor: Minor version number
    :return: Milestone prefix like 'Airflow 3.1' or 'Airflow 2.11'
    """
    return f"Airflow {major}.{minor}"


def get_headers() -> dict[str, str]:
    """Get HTTP headers for GitHub API requests."""
    token = os.getenv("GH_TOKEN")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def get_pr_info(pr_number: str) -> dict | None:
    """
    Get PR information including milestone and merged_by.

    :param pr_number: The PR number
    :return: PR data dict or None if request fails
    """
    repository = os.getenv("REPOSITORY")
    url = f"https://api.github.com/repos/{repository}/pulls/{pr_number}"

    try:
        response = requests.get(url, headers=get_headers())
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as e:
        print(f"Error: Failed to get PR info for #{pr_number}: {e}")
        return None


def get_open_milestones() -> list[dict]:
    """
    Get all open milestones for the repository.

    :return: List of milestone dicts
    """
    repository = os.getenv("REPOSITORY")
    url = f"https://api.github.com/repos/{repository}/milestones"
    params = {"state": "open", "per_page": 100}

    try:
        response = requests.get(url, headers=get_headers(), params=params)
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as e:
        print(f"Error: Failed to get milestones: {e}")
        return []


def find_matching_milestone(milestones: list[dict], milestone_prefix: str) -> dict | None:
    """
    Find the first matching milestone that starts with the given prefix.

    Milestones are sorted by title to get the latest patch version.

    :param milestones: List of milestone dicts
    :param milestone_prefix: Prefix to match like 'Airflow 3.1'
    :return: Matching milestone dict or None
    """
    matching = [m for m in milestones if m["title"].startswith(milestone_prefix)]
    if not matching:
        return None
    # Sort by title to get the latest patch version (e.g., Airflow 3.1.8 > Airflow 3.1.7)
    matching.sort(key=lambda m: m["title"], reverse=True)
    return matching[0]


def set_milestone_on_pr(pr_number: str, milestone_number: int) -> bool:
    """
    Set milestone on a PR.

    :param pr_number: The PR number
    :param milestone_number: The milestone number to set
    :return: True if successful, False otherwise
    """
    repository = os.getenv("REPOSITORY")
    url = f"https://api.github.com/repos/{repository}/issues/{pr_number}"
    data = {"milestone": milestone_number}

    try:
        response = requests.patch(url, headers=get_headers(), json=data)
        response.raise_for_status()
        return True
    except requests.HTTPError as e:
        print(f"Error: Failed to set milestone on PR #{pr_number}: {e}")
        return False


def add_comment_to_pr(pr_number: str, comment: str) -> bool:
    """
    Add a comment to a PR.

    :param pr_number: The PR number
    :param comment: The comment text
    :return: True if successful, False otherwise
    """
    repository = os.getenv("REPOSITORY")
    url = f"https://api.github.com/repos/{repository}/issues/{pr_number}/comments"
    data = {"body": comment}

    try:
        response = requests.post(url, headers=get_headers(), json=data)
        response.raise_for_status()
        return True
    except requests.HTTPError as e:
        print(f"Error: Failed to add comment to PR #{pr_number}: {e}")
        return False


def get_milestone_notification_comment(
    milestone_title: str, milestone_number: int, merged_by_login: str
) -> str:
    """
    Generate the notification comment for auto-set milestone.

    :param milestone_title: The milestone title that was set
    :param merged_by_login: The GitHub username of the person who merged the PR
    :return: Comment text
    """
    # double check the merged_by_login
    if merged_by_login == "unknown":
        merged_by_login = "maintainer"
    else:
        merged_by_login = f"@{merged_by_login}"

    return f"""
Hi {merged_by_login}, this PR was merged without a milestone set.
We've automatically set the milestone to **[{milestone_title}](https://github.com/apache/airflow/milestone/{milestone_number})** based on the target branch.
If this milestone is not correct, please update it to the appropriate milestone.

> This comment was generated by [Milestone Tag Assistant](https://github.com/apache/airflow/tree/main/dev/backport/set_milestone_on_backport_pr.py).
"""


def set_milestone_on_backport_pr(backport_pr_number: str, target_branch: str) -> None:
    """
    Set milestone on a backport PR if it doesn't have one.

    :param backport_pr_number: The backport PR number
    :param target_branch: The target branch like 'v3-1-test'
    """
    # Parse version from branch
    version = parse_version_from_branch(target_branch)
    if not version:
        print(f"Could not parse version from branch: {target_branch}")
        return

    major, minor = version
    milestone_prefix = get_milestone_prefix(major, minor)
    print(f"Looking for milestone with prefix: {milestone_prefix}")

    # Get PR info
    pr_info = get_pr_info(backport_pr_number)
    if not pr_info:
        return

    # Check if milestone is already set
    if pr_info.get("milestone"):
        print(f"PR #{backport_pr_number} already has milestone: {pr_info['milestone']['title']}")
        return

    # Get merged_by info for notification
    merged_by = pr_info.get("merged_by")
    merged_by_login = merged_by["login"] if merged_by else "unknown"

    # Find matching milestone
    milestones = get_open_milestones()
    milestone = find_matching_milestone(milestones, milestone_prefix)
    if not milestone:
        print(f"No open milestone found with prefix: {milestone_prefix}")
        return

    print(f"Found milestone: {milestone['title']} (#{milestone['number']})")

    # Set milestone on PR
    if not set_milestone_on_pr(backport_pr_number, milestone["number"]):
        return

    print(f"Successfully set milestone '{milestone['title']}' on PR #{backport_pr_number}")

    # Add notification comment
    comment = get_milestone_notification_comment(milestone["title"], milestone["number"], merged_by_login)
    if add_comment_to_pr(backport_pr_number, comment):
        print(f"Added notification comment to PR #{backport_pr_number}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: set_milestone_on_backport_pr.py <backport_pr_number> <target_branch>")
        sys.exit(1)

    backport_pr = sys.argv[1]
    branch = sys.argv[2]

    # Skip if backport PR number is not valid (e.g., "EMPTY" from failed backport)
    if not backport_pr.isdigit():
        print(f"Skipping milestone set - invalid PR number: {backport_pr}")
        sys.exit(0)

    set_milestone_on_backport_pr(backport_pr, branch)
