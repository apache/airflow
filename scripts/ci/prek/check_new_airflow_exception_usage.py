#!/usr/bin/env python
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
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.0.0",
# ]
# ///
"""Check that no new ``raise AirflowException`` usages are introduced.

All *existing* usages are recorded in ``generated/known_airflow_exceptions.txt``
as ``relative/path::N`` entries (one per file), where ``N`` is the
maximum number of ``raise AirflowException`` occurrences allowed in that file.
A file whose current count exceeds the recorded limit is treated as a violation
– use a dedicated exception class instead.

Modes
-----
Default (files passed by prek/pre-commit):
    Check only the supplied files; fail if any file's count exceeds the limit.
    When a file's count has *decreased*, the allowlist entry is tightened
    automatically and the hook exits with a non-zero code so that pre-commit
    reports the modified allowlist — just stage
    ``generated/known_airflow_exceptions.txt`` and re-run.

``--all-files``:
    Walk the whole repository and check every ``.py`` file.

``--cleanup``:
    Remove entries for files that no longer exist. Safe to run at any time;
    does not add new entries or raise limits.

``--generate``:
    Scan the whole repository and *rebuild* the allowlist from scratch.
    Intended for the initial setup or after a large-scale clean-up sprint.
"""

from __future__ import annotations

import argparse
import re
from collections.abc import Iterable
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH, AllowlistManager
from rich.console import Console

console = Console(color_system="standard", width=200)

REPO_ROOT = AIRFLOW_ROOT_PATH

# Match lines that actually raise AirflowException. Comment filtering is done
# in _raise_lines() by skipping lines whose stripped form starts with "#".
_RAISE_RE = re.compile(r"raise\s+AirflowException\b")


class AirflowExceptionAllowlistManager(AllowlistManager):
    def __init__(self, allowlist_file: Path) -> None:
        super().__init__(allowlist_file, repo_root=REPO_ROOT)

    def iter_files(self) -> Iterable[Path]:
        return _iter_python_files()

    def count_occurrences(self, path: Path) -> int:
        return len(_raise_lines(path))

    def violation_panel_text(self) -> str:
        return (
            "New [bold]raise AirflowException[/bold] usage detected.\n"
            "Define a dedicated exception class or use an existing specific exception.\n"
            "If this usage is intentional and pre-existing, run:\n\n"
            "  [cyan]uv run ./scripts/ci/prek/check_new_airflow_exception_usage.py --generate[/cyan]\n\n"
            "to regenerate the allowlist, then commit the updated\n"
            "[cyan]generated/known_airflow_exceptions.txt[/cyan]."
        )


def _raise_lines(path: Path) -> list[str]:
    """Return stripped raise-lines from *path* that match the pattern."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []

    result = []
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        # Skip comment lines
        if stripped.startswith("#"):
            continue
        if _RAISE_RE.search(raw_line):
            result.append(stripped)
    return result


def _iter_python_files() -> list[Path]:
    return [
        p.resolve() for p in REPO_ROOT.rglob("*.py") if ".tox" not in p.parts and "__pycache__" not in p.parts
    ]


def _check_airflow_exception_usage(
    files: list[Path], allowlist: dict[str, int], manager: AirflowExceptionAllowlistManager
) -> int:
    return manager.check(files, allowlist)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prevent new `raise AirflowException` usages.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("files", nargs="*", metavar="FILE", help="Files to check (provided by prek)")
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="Check every Python file in the repository",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove stale entries from the allowlist and exit",
    )
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Regenerate the allowlist from the current codebase and exit",
    )
    args = parser.parse_args(argv)

    manager = AirflowExceptionAllowlistManager(REPO_ROOT / "generated" / "known_airflow_exceptions.txt")

    if args.generate:
        return manager.generate()

    if args.cleanup:
        return manager.cleanup()

    allowlist = manager.load()

    if args.all_files:
        return _check_airflow_exception_usage(_iter_python_files(), allowlist, manager)

    if not args.files:
        console.print(
            "[yellow]No files provided. Pass filenames or use --all-files to scan the whole repo.[/yellow]"
        )
        return 0

    return _check_airflow_exception_usage([Path(f).resolve() for f in args.files], allowlist, manager)


if __name__ == "__main__":
    raise SystemExit(main())
