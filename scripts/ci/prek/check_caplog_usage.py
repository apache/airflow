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
"""Check that no new ``caplog`` usage is added in test files.

Existing usages are recorded in ``known_caplog_usage.txt``; a file's count may
not exceed its recorded limit. See ``CLAUDE.md#testing-standards``.
"""

from __future__ import annotations

import argparse
import re
import subprocess
from collections.abc import Iterable
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH, AllowlistManager
from rich.console import Console
from rich.panel import Panel

console = Console(color_system="standard", width=200)

REPO_ROOT = AIRFLOW_ROOT_PATH

_CAPLOG_RE = re.compile(r"\bcaplog\b")

# Test directories scanned by ``--all-files`` / ``--generate``. Keep in sync with the
# ``files:`` pattern for this hook in ``.pre-commit-config.yaml``.
_TEST_ROOTS = ("airflow-core/tests", "task-sdk/tests")


def _iter_caplog_lines(path: Path) -> list[str]:
    """Return stripped, non-comment lines in *path* that reference ``caplog``."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []

    result = []
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("#"):
            continue
        if _CAPLOG_RE.search(raw_line):
            result.append(stripped)
    return result


def _count_violations(path: Path) -> int:
    return len(_iter_caplog_lines(path))


class CaplogAllowlistManager(AllowlistManager):
    def __init__(self, allowlist_file: Path) -> None:
        super().__init__(allowlist_file, repo_root=REPO_ROOT)

    def iter_files(self) -> Iterable[Path]:
        return _iter_test_files()

    def count_occurrences(self, path: Path) -> int:
        return _count_violations(path)

    def violation_panel_text(self) -> str:
        return (
            "New [bold]caplog[/bold] usage in a test file — see CLAUDE.md#testing-standards.\n"
            "If pre-existing, run [cyan]./scripts/ci/prek/check_caplog_usage.py --generate[/cyan] "
            "and commit [cyan]known_caplog_usage.txt[/cyan]."
        )

    def format_violation_details(self, path: Path) -> list[str]:
        return [f"      [dim]{line}[/dim]" for line in _iter_caplog_lines(path)]


def _iter_test_files() -> list[Path]:
    candidates: list[Path] = []
    for top in _TEST_ROOTS:
        candidates.extend(
            p.resolve()
            for p in (REPO_ROOT / top).rglob("*.py")
            if ".tox" not in p.parts and "__pycache__" not in p.parts
        )
    for provider_tests_dir in (REPO_ROOT / "providers").glob("*/tests"):
        candidates.extend(
            p.resolve()
            for p in provider_tests_dir.rglob("*.py")
            if ".tox" not in p.parts and "__pycache__" not in p.parts
        )
    return candidates


def _check_caplog_usage(files: list[Path], allowlist: dict[str, int], manager: CaplogAllowlistManager) -> int:
    allowlist_file = manager.allowlist_file.resolve()
    if any(p.resolve() == allowlist_file for p in files) and not allowlist_file.exists():
        console.print(
            Panel.fit(
                f"Allowlist file [cyan]{allowlist_file}[/cyan] is missing.\n"
                "Restore it from git or regenerate with "
                "[cyan]./scripts/ci/prek/check_caplog_usage.py --generate[/cyan].",
                title="[red]Check failed[/red]",
                border_style="red",
            )
        )
        return 1
    return manager.check(files, allowlist)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prevent new caplog usage in test files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("files", nargs="*", metavar="FILE", help="Files to check (provided by prek)")
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="Check every test file (airflow-core/tests, task-sdk/tests, providers/*/tests)",
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

    manager = CaplogAllowlistManager(Path(__file__).parent / "known_caplog_usage.txt")

    if args.generate:
        return manager.generate()

    if args.cleanup:
        return manager.cleanup()

    allowlist = manager.load()

    if args.all_files:
        return _check_caplog_usage(_iter_test_files(), allowlist, manager)

    if not args.files:
        console.print(
            "[yellow]No files provided. Pass filenames or use --all-files to scan the whole repo.[/yellow]"
        )
        return 0

    paths = [Path(f).resolve() for f in args.files]
    paths = _expand_for_allowlist_edits(paths, manager, allowlist)
    return _check_caplog_usage(paths, allowlist, manager)


def _parse_tracked_allowlist(manager: CaplogAllowlistManager) -> dict[str, int]:
    """Return the allowlist as recorded at ``HEAD``, so removed entries still get re-checked."""
    try:
        rel = manager.allowlist_file.resolve().relative_to(REPO_ROOT.resolve())
    except ValueError:
        return {}
    try:
        completed = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "show", f"HEAD:{rel.as_posix()}"],
            capture_output=True,
            text=True,
            check=False,
        )
    except (FileNotFoundError, OSError):
        return {}
    if completed.returncode != 0:
        return {}
    return manager.parse(completed.stdout)


def _expand_for_allowlist_edits(
    paths: list[Path], manager: CaplogAllowlistManager, allowlist: dict[str, int]
) -> list[Path]:
    """When the allowlist itself is edited, add its files so loosening it can't bypass the check."""
    allowlist_file = manager.allowlist_file.resolve()
    if not any(p.resolve() == allowlist_file for p in paths):
        return paths

    expanded = list(paths)
    seen = {p.resolve() for p in paths if p.suffix == ".py"}
    tracked = _parse_tracked_allowlist(manager)
    for rel in {*allowlist, *tracked}:
        candidate = (REPO_ROOT / rel).resolve()
        if candidate.exists() and candidate not in seen:
            seen.add(candidate)
            expanded.append(candidate)
    return expanded


if __name__ == "__main__":
    raise SystemExit(main())
