#!/usr/bin/env python
#
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
Check things about newsfragments:
  - Only a single line, except for `significant` changes which can have a blank line, then the body
  - In CI, where ``PR_NUMBER`` is set, the newsfragments the pull request adds are named after it.
    Locally the pull request may not exist yet, so any number is accepted.
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

VALID_CHANGE_TYPES = {"significant", "feature", "improvement", "bugfix", "doc", "misc"}
SKIP_PR_NUMBER_LABEL = "skip newsfragment check"
PR_REPOSITORY = "apache/airflow"
NEWSFRAGMENT_PATH = re.compile(r"/newsfragments/[^/]+\.rst$")


def validate_newsfragment(filename: str, lines: list[str]) -> list[str]:
    """Validate a single newsfragment file. Returns a list of error messages."""
    errors: list[str] = []
    num_lines = len(lines)

    name_parts = Path(filename).name.split(".")
    if len(name_parts) != 3:
        errors.append(
            f"Newsfragment {filename} has an unexpected filename. Should be {{pr_number}}.{{type}}.rst."
        )
        return errors

    change_type = name_parts[1]
    if change_type not in VALID_CHANGE_TYPES:
        errors.append(
            f"Newsfragment {filename} has an unexpected type. Should be one of {VALID_CHANGE_TYPES}."
        )
        return errors

    if change_type != "significant":
        if num_lines != 1:
            errors.append(f"Newsfragment {filename} can only have a single line.")
    else:
        # significant newsfragment
        if num_lines == 1:
            pass  # OK
        elif num_lines == 2:
            errors.append(f"Newsfragment {filename} can have 1, or 3+ lines.")
        elif lines[1] != "":
            errors.append(f"Newsfragment {filename} must have an empty second line.")

    return errors


def find_misnumbered_newsfragments(pr_number: str, filenames: list[str]) -> list[str]:
    return [
        filename
        for filename in filenames
        if NEWSFRAGMENT_PATH.search(filename) and not Path(filename).name.startswith(f"{pr_number}.")
    ]


def get_github_items(path: str) -> list[dict]:
    """Return every item of a paginated GitHub REST API listing."""
    headers = {"Accept": "application/vnd.github+json"}
    if token := os.environ.get("GITHUB_TOKEN"):
        headers["Authorization"] = f"Bearer {token}"
    items: list[dict] = []
    page = 1
    while True:
        request = urllib.request.Request(
            f"https://api.github.com/{path}?per_page=100&page={page}", headers=headers
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            batch = json.load(response)
        items.extend(batch)
        if len(batch) < 100:
            return items
        page += 1


def check_pr_number(pr_number: str) -> list[str]:
    """Return the errors for the newsfragments the pull request adds under another number."""
    labels = get_github_items(f"repos/{PR_REPOSITORY}/issues/{pr_number}/labels")
    if any(label["name"] == SKIP_PR_NUMBER_LABEL for label in labels):
        print(f"Newsfragment PR number not checked: the pull request has the {SKIP_PR_NUMBER_LABEL!r} label.")
        return []
    # Removed files are left out, so deleting a misnumbered newsfragment fixes the check.
    files = [
        f["filename"]
        for f in get_github_items(f"repos/{PR_REPOSITORY}/pulls/{pr_number}/files")
        if f["status"] != "removed"
    ]
    return [
        f"Newsfragment {filename} is not named after pull request {pr_number}. Rename it to "
        f"{pr_number}.<type>.rst, or have a maintainer add the {SKIP_PR_NUMBER_LABEL!r} label and "
        "re-run the failed static checks (with CI in your fork, re-run them there)."
        for filename in find_misnumbered_newsfragments(pr_number, files)
    ]


def main(filenames: list[str]) -> int:
    failed = False
    for filename in filenames:
        with open(filename) as f:
            lines = [line.strip() for line in f]
        errors = validate_newsfragment(filename, lines)
        for error in errors:
            print(error)
        if errors:
            failed = True
    pr_number = os.environ.get("PR_NUMBER")
    if pr_number:
        try:
            errors = check_pr_number(pr_number)
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            errors = [
                f"Could not read pull request {pr_number} from GitHub to check newsfragment numbers: {e}"
            ]
        for error in errors:
            print(error)
        if errors:
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
