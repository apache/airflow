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
"""
Reconcile the newest ``changelog.rst`` version section against ``git log``.

The release skill writes changelog sections by hand, so nothing guarantees they still
match the commits actually being released. This reconciles the two and reports four
defects, all of which have shipped in real waves:

* ``MISSING``  — a commit in the release range has no entry (typically a commit that
  landed on ``main`` after the changelog was generated and arrived via a rebase).
* ``UNKNOWN``  — an entry cites a PR that is not in the release range at all (typically
  a pre-written entry a contributor left at the top of ``changelog.rst``, sometimes with
  a PR number that does not exist).
* ``SECTION``  — a heading that is not one of the ``CHANGELOG_TEMPLATE.rst.jinja2``
  headings, so the entry does not render where readers look for it.
* ``ORDER``    — bullets not in ``git log`` order. The template renders every section
  newest-merge-first; hand-written and incrementally-appended entries drift off it.
  This is the only defect that can be repaired mechanically (``--fix``).

The release range runs from the tag of the *previous* version in ``changelog.rst`` (not
the newest tag — providers can carry placeholder ``99.x`` tags) to ``HEAD``. Only the topmost
version section is inspected, and only while it is still unreleased — once its tag exists the
provider is skipped, so published waves are never rewritten.

Usage:
    check_changelog_entries.py [--fix] [--base main] [CHANGELOG ...]

With no paths, operates on the ``docs/changelog.rst`` files that changed since ``--base``.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

BULLET = re.compile(r"^\s*\* ``.*``\s*$")
VERSION_HEADER = re.compile(r"^\d+\.\d+\.\d+$")
VERSION_UNDERLINE = re.compile(r"^\.+$")
SECTION_UNDERLINE = re.compile(r"^~+$")
PR_NUMBER = re.compile(r"#(\d+)")

TEMPLATE_SECTIONS = ("Breaking changes", "Features", "Bug Fixes", "Misc", "Doc-only")


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()


def tag_exists(tag: str) -> bool:
    rev_parse = ["git", "rev-parse", "-q", "--verify", tag]
    return subprocess.run(rev_parse, cwd=ROOT, capture_output=True, check=False).returncode == 0


def find_newest_section(lines: list[str]) -> tuple[int, int]:
    """Return the [start, end) line range of the topmost version section, empty if none."""
    underlines = [
        i
        for i, line in enumerate(lines)
        if VERSION_UNDERLINE.fullmatch(line) and i and VERSION_HEADER.fullmatch(lines[i - 1])
    ]
    if not underlines:
        return 0, 0
    return underlines[0], underlines[1] - 1 if len(underlines) > 1 else len(lines)


def sort_key(line: str, rank: dict[str, int]) -> int | None:
    """
    Rank a bullet by the *first* commit behind it, or None when no PR resolves to a commit.

    An entry that collapses a chain of PRs — a feature added and then reworked before it ever
    shipped — belongs where the feature became relevant, which is its earliest commit. Ranks are
    ``git log`` positions and grow going back in time, so the earliest commit is the largest.
    """
    ranks = [rank[pr] for pr in PR_NUMBER.findall(line) if pr in rank]
    return max(ranks) if ranks else None


def sort_run(run: list[str], rank: dict[str, int]) -> list[str]:
    """Reorder only the bullets whose PR resolves; the rest stay pinned to their slot."""
    ranked = [(i, key) for i, line in enumerate(run) if (key := sort_key(line, rank)) is not None]
    ordered = [run[i] for i, _ in sorted(ranked, key=lambda pair: pair[1])]
    out = list(run)
    for (slot, _), line in zip(ranked, ordered):
        out[slot] = line
    return out


def reorder(lines: list[str], start: int, end: int, rank: dict[str, int]) -> list[str]:
    """Sort every run of consecutive bullets in [start, end) back onto git-log order."""
    out, run = lines[:start], []
    for line in lines[start:end]:
        if BULLET.fullmatch(line):
            run.append(line)
            continue
        out.extend(sort_run(run, rank))
        run = []
        out.append(line)
    out.extend(sort_run(run, rank))
    return out + lines[end:]


def check(path: Path, fix: bool) -> list[str]:
    provider = path.as_posix().removeprefix("providers/").removesuffix("/docs/changelog.rst")
    lines = (ROOT / path).read_text().splitlines()
    versions = [line for line in lines if VERSION_HEADER.fullmatch(line)]
    if len(versions) < 2:
        return [f"SKIP    {provider}: fewer than two version sections"]
    tag_prefix = "providers-" + provider.replace("/", "-") + "/"
    if tag_exists(tag_prefix + versions[0]):
        return [f"SKIP    {provider}: {versions[0]} is already released, nothing pending to check"]
    tag = tag_prefix + versions[1]
    if not tag_exists(tag):
        return [f"SKIP    {provider}: no tag {tag}"]

    log = git("log", "--pretty=format:%s", f"{tag}..HEAD", "--", f"providers/{provider}").splitlines()
    rank: dict[str, int] = {}
    for position, subject in enumerate(log):
        for pr in PR_NUMBER.findall(subject):
            rank.setdefault(pr, position)

    start, end = find_newest_section(lines)
    section = lines[start:end]
    text = "\n".join(lines)
    problems = []

    for subject in log:
        prs = PR_NUMBER.findall(subject)
        if prs and not any(f"#{pr}" in text for pr in prs):
            problems.append(f"MISSING {provider}: {subject}")

    for line in section:
        if BULLET.fullmatch(line):
            problems += [
                f"UNKNOWN {provider}: #{pr} is not in {tag}..HEAD — {line.strip()}"
                for pr in PR_NUMBER.findall(line)
                if pr not in rank
            ]

    for i, line in enumerate(section[:-1]):
        if SECTION_UNDERLINE.fullmatch(section[i + 1]) and line not in TEMPLATE_SECTIONS:
            problems.append(f"SECTION {provider}: '{line}' is not a CHANGELOG_TEMPLATE heading")

    reordered = reorder(lines, start, end, rank)
    if reordered != lines:
        problems.append(f"ORDER   {provider}: entries are not in git-log order")
        if fix:
            (ROOT / path).write_text("\n".join(reordered) + "\n")
            problems[-1] += " (fixed)"
    return problems


def self_test() -> None:
    lines = """\
Changelog
---------

1.1.0
.....

Features
~~~~~~~~

* ``Collapsed chain (#400, #200)``
* ``Newest (#400)``
* ``No PR reference``
* ``Oldest (#100)``

1.0.0
.....

Features
~~~~~~~~

* ``Untouched (#100)``
* ``Untouched (#400)``
""".splitlines()
    start, end = find_newest_section(lines)
    got = reorder(lines, start, end, {"400": 0, "300": 1, "200": 2, "100": 3})
    assert got[9:13] == [  # noqa: S101
        "* ``Newest (#400)``",
        "* ``Collapsed chain (#400, #200)``",  # ranked by #200, its first commit, not by #400
        "* ``No PR reference``",  # unresolvable entries stay pinned to their slot
        "* ``Oldest (#100)``",
    ], got[9:13]
    assert got[-2:] == ["* ``Untouched (#100)``", "* ``Untouched (#400)``"], got[-2:]  # noqa: S101
    print("self-test OK")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("changelogs", nargs="*", type=Path)
    parser.add_argument("--fix", action="store_true", help="repair ORDER defects in place")
    parser.add_argument("--base", default="main", help="ref to diff against when no paths are given")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0

    paths = args.changelogs or [
        Path(p)
        for p in git(
            "diff", "--name-only", git("merge-base", args.base, "HEAD"), "--", "*/docs/changelog.rst"
        ).splitlines()
    ]
    problems = [problem for path in sorted(paths) for problem in check(path, args.fix)]
    for problem in problems:
        print(problem)
    unresolved = [p for p in problems if not p.startswith("SKIP") and not p.endswith("(fixed)")]
    print(f"\n{len(paths)} changelog(s) checked, {len(unresolved)} unresolved problem(s)")
    return 1 if unresolved else 0


if __name__ == "__main__":
    sys.exit(main())
