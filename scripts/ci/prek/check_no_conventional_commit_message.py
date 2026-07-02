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
Reject Conventional Commits style commit subjects.

Airflow does not use Conventional Commits (``feat:``, ``fix:``, ``chore: ...``).
Commit messages should describe user impact in plain prose, e.g.
``Fix airflow dags test command failure without serialized Dags`` rather than
``fix(cli): dags test failure``.

As a ``commit-msg`` stage hook it receives the path to the file holding the commit
message being created and fails if the subject line looks like a Conventional
Commit.

With ``--from-stdin`` it instead reads subjects (one per line) from standard input
and fails if any of them looks like a Conventional Commit. This is how CI validates
the pull request title, which is the subject that lands on the target branch under
"squash and merge".
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Standard Conventional Commits types (https://www.conventionalcommits.org).
CONVENTIONAL_TYPES = (
    "bugfix",
    "build",
    "chore",
    "ci",
    "docs",
    "feat",
    "fix",
    "perf",
    "refactor",
    "revert",
    "style",
    "test",
)

# Matches "type: ...", "type(scope): ..." and "type!: ..." (optionally combined),
# case-insensitively so "Fix:", "FEAT(api)!:" etc. are caught too. Airflow prose
# prefixes such as "UI:" or "API:" are not Conventional Commit types and are not
# matched.
CONVENTIONAL_COMMIT_REGEX = re.compile(
    rf"^(?:{'|'.join(CONVENTIONAL_TYPES)})(?:\([^)]*\))?!?[:/]",
    re.IGNORECASE,
)


def get_subject_line(commit_msg_file: str) -> str | None:
    """Return the first non-empty, non-comment line of the commit message, if any."""
    for raw_line in Path(commit_msg_file).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        return line
    return None


def is_conventional_commit(subject: str) -> bool:
    return bool(CONVENTIONAL_COMMIT_REGEX.match(subject))


def report(subject: str) -> None:
    print(
        "ERROR: Conventional Commits style commit message detected:\n\n"
        f"    {subject}\n\n"
        "Airflow does not use Conventional Commits (feat:, fix:, chore: ...).\n"
        "Write the subject as plain prose focused on user impact, for example:\n\n"
        "    Fix airflow dags test command failure without serialized Dags\n"
        "    UI: Fix Grid view not refreshing after task actions\n\n"
        "See contributing-docs/05_pull_requests.rst for commit message guidance."
    )


def iter_subjects(args: argparse.Namespace) -> list[str]:
    if args.from_stdin:
        # CI passes commit subjects, one per line, on stdin.
        return [line.strip() for line in sys.stdin.read().splitlines() if line.strip()]
    # commit-msg hooks pass the path(s) to the commit message file(s).
    subjects = []
    for commit_msg_file in args.files:
        subject = get_subject_line(commit_msg_file)
        if subject is not None:
            subjects.append(subject)
    return subjects


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from-stdin",
        action="store_true",
        help="Read commit subjects (one per line) from stdin instead of from commit message files.",
    )
    parser.add_argument("files", nargs="*", help="Commit message file(s) passed by the commit-msg hook.")
    args = parser.parse_args(argv)

    failed = False
    for subject in iter_subjects(args):
        if is_conventional_commit(subject):
            report(subject)
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
