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
"""Cross-references and overlapping PR detection for auto-triage."""

from __future__ import annotations

import re


def extract_file_paths_from_diff(diff_text: str) -> list[str]:
    """Extract file paths from a unified diff.

    Parses ``diff --git a/path b/path`` headers and returns
    deduplicated file paths in order of appearance.
    """
    paths: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"^diff --git a/(.+?) b/", diff_text, re.MULTILINE):
        path = match.group(1)
        if path not in seen:
            paths.append(path)
            seen.add(path)
    return paths


def extract_cross_references(body: str, exclude_number: int = 0) -> list[int]:
    """Extract issue/PR numbers referenced as #N in the PR body.

    Returns deduplicated numbers in order of appearance, excluding
    the PR's own number.
    """
    refs: list[int] = []
    seen: set[int] = set()
    for match in re.finditer(r"(?<!\w)#(\d+)(?!\d)", body):
        num = int(match.group(1))
        if num != exclude_number and num not in seen:
            refs.append(num)
            seen.add(num)
    return refs


def find_overlapping_prs(
    target_files: list[str],
    target_number: int,
    other_prs: dict[int, list[str]],
) -> dict[int, list[str]]:
    """Find open PRs that touch the same files as the target PR.

    Args:
        target_files: File paths changed by the target PR.
        target_number: PR number of the target (excluded from results).
        other_prs: Mapping of PR number -> list of file paths for other open PRs.

    Returns:
        Dict of PR number -> list of overlapping file paths, sorted by
        overlap count descending.
    """
    if not target_files:
        return {}

    target_set = set(target_files)
    overlaps: dict[int, list[str]] = {}

    for pr_num, pr_files in other_prs.items():
        if pr_num == target_number:
            continue
        common = target_set & set(pr_files)
        if common:
            overlaps[pr_num] = sorted(common)

    return dict(sorted(overlaps.items(), key=lambda x: -len(x[1])))
