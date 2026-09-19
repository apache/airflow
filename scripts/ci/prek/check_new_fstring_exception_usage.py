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
"""Check that no new f-string exception messages are introduced.

This is ruff's ``EM102`` (``f-string-in-exception``) applied as a ratchet. The rule
cannot simply be added to ``extend-select`` in ``pyproject.toml``: the repository
carries thousands of pre-existing occurrences, and rewriting them all at once would
produce a diff that conflicts with every open pull request.

All *existing* usages are recorded in ``generated/known_fstring_exceptions.txt``
as ``relative/path::N`` entries (one per file), where ``N`` is the maximum number
of f-string exception messages allowed in that file. A file whose current count
exceeds the recorded limit is treated as a violation – assign the message to a
variable first.

Modes
-----
Default (files passed by prek/pre-commit):
    Check only the supplied files; fail if any file's count exceeds the limit.
    When a file's count has *decreased*, the allowlist entry is tightened
    automatically and the hook exits with a non-zero code so that pre-commit
    reports the modified allowlist — just stage
    ``generated/known_fstring_exceptions.txt`` and re-run.

``--all-files``:
    Walk every scanned distribution and check each ``.py`` file.

``--cleanup``:
    Remove entries for files that no longer exist. Safe to run at any time;
    does not add new entries or raise limits.

``--generate``:
    Scan the scanned distributions and *rebuild* the allowlist from scratch.
    Intended for the initial setup or after a large-scale clean-up sprint.
"""

from __future__ import annotations

import argparse
import ast
from collections.abc import Iterable, Iterator
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH, AllowlistManager
from rich.console import Console

console = Console(color_system="standard", width=200)

REPO_ROOT = AIRFLOW_ROOT_PATH

# Kept in sync with the `files:` pattern of the `check-no-new-fstring-in-exception`
# hook, so that `--generate` records exactly the files the hook can later check.
SCANNED_ROOTS = ("airflow-core", "airflow-ctl", "providers", "shared", "task-sdk")

SKIPPED_DIRECTORIES = {".tox", ".venv", "__pycache__", "_vendor"}


class FStringExceptionAllowlistManager(AllowlistManager):
    def __init__(self, allowlist_file: Path) -> None:
        super().__init__(allowlist_file, repo_root=REPO_ROOT)

    def iter_files(self) -> Iterable[Path]:
        return _find_python_files()

    def count_occurrences(self, path: Path) -> int:
        return len(_find_fstring_exception_messages(path) or [])

    def format_violation_details(self, path: Path) -> list[str]:
        messages = _find_fstring_exception_messages(path) or []
        return [f"    [dim]line {message.lineno}[/dim]" for message in messages]

    def check(self, files: list[Path], allowlist: dict[str, int]) -> int:
        # A file that does not parse counts zero occurrences, which the base class would
        # read as a clean-up and drop the file's grandfathered entry — leaving its
        # pre-existing usages to be reported as new once the file parses again.
        return super().check([path for path in files if _is_parsable(path)], allowlist)

    def violation_panel_text(self) -> str:
        return (
            "New f-string exception message detected (ruff [bold]EM102[/bold]).\n"
            "Assign the message to a variable first:\n\n"
            '  [red]raise ValueError(f"no such pool: {name}")[/red]\n'
            '  [green]msg = f"no such pool: {name}"[/green]\n'
            "  [green]raise ValueError(msg)[/green]\n\n"
            "If this usage is intentional and pre-existing, run:\n\n"
            "  [cyan]uv run ./scripts/ci/prek/check_new_fstring_exception_usage.py --generate[/cyan]\n\n"
            "to regenerate the allowlist, then commit the updated\n"
            "[cyan]generated/known_fstring_exceptions.txt[/cyan]."
        )


def _find_fstring_exception_messages(path: Path) -> list[ast.JoinedStr] | None:
    """Return the f-string message of every ``raise`` in *path*, or None if it does not parse."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, SyntaxError, ValueError):
        return None
    return sorted(_iter_fstring_exception_messages(tree), key=lambda message: message.lineno)


def _is_parsable(path: Path) -> bool:
    return _find_fstring_exception_messages(path) is not None


def _iter_fstring_exception_messages(tree: ast.Module) -> Iterator[ast.JoinedStr]:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Raise) or not isinstance(node.exc, ast.Call):
            continue
        # EM102 only looks at the first positional argument: that is the one
        # exception classes render as the message, so keyword and starred
        # arguments are out of scope.
        message = node.exc.args[0] if node.exc.args else None
        if isinstance(message, ast.JoinedStr):
            yield message


def _find_python_files() -> list[Path]:
    return [
        path
        for root in SCANNED_ROOTS
        for path in (REPO_ROOT / root).rglob("*.py")
        # Only the parts below the repository root are considered, so a checkout that
        # happens to live under a directory named like an excluded one still scans.
        if SKIPPED_DIRECTORIES.isdisjoint(path.relative_to(REPO_ROOT).parts)
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prevent new f-string exception messages.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("files", nargs="*", metavar="FILE", help="Files to check (provided by prek)")
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="Check every Python file in the scanned distributions",
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

    manager = FStringExceptionAllowlistManager(REPO_ROOT / "generated" / "known_fstring_exceptions.txt")

    if args.generate:
        return manager.generate()

    if args.cleanup:
        return manager.cleanup()

    allowlist = manager.load()

    if args.all_files:
        return manager.check(_find_python_files(), allowlist)

    if not args.files:
        console.print(
            "[yellow]No files provided. Pass filenames or use --all-files to scan the whole repo.[/yellow]"
        )
        return 0

    return manager.check([Path(f).resolve() for f in args.files], allowlist)


if __name__ == "__main__":
    raise SystemExit(main())
