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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""Check that no new ``airflow.sdk`` imports are introduced in ``airflow-core``.

All *existing* imports are recorded in ``generated/known_sdk_imports_in_core.txt``
as ``relative/path::N`` entries (one per file), where ``N`` is the maximum
number of ``airflow.sdk`` import statements allowed in that file. A file whose
current count exceeds the recorded limit is treated as a violation -- core
should not gain new runtime dependencies on the Task SDK. Two escape hatches
exist: ``# noqa: SDK001`` on a single import line, which keeps that line out of
the count and out of the allowlist entirely, or raising the file's recorded
count via ``--generate FILE`` when a file genuinely needs several.

``airflow-core/src/airflow/example_dags`` is skipped entirely: those files are
Dags authored with the Task SDK, not core runtime code.

Modes
-----
Default (files passed by prek/pre-commit):
    Check only the supplied files; fail if any file's count exceeds the limit.
    When a file's count has *decreased*, the allowlist entry is tightened
    automatically and the hook exits with a non-zero code so that pre-commit
    reports the modified allowlist -- just stage
    ``generated/known_sdk_imports_in_core.txt`` and re-run.

``--all-files``:
    Walk all of ``airflow-core/src/airflow`` and check every ``.py`` file.

``--cleanup``:
    Remove entries for files that no longer exist. Safe to run at any time;
    does not add new entries or raise limits.

``--generate [FILE ...]``:
    With files, record the current counts for just those files and leave every
    other entry alone -- the usual way to approve a deliberate new import.
    With no files, *rebuild* the allowlist from scratch; that also approves any
    unrelated drift, so it is meant for the initial setup or a clean-up sprint.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from pathlib import Path

from common_prek_utils import AIRFLOW_CORE_ROOT_PATH, AllowlistManager, find_import_violations
from rich.console import Console

console = Console(color_system="standard", width=200)

REPO_ROOT = AIRFLOW_CORE_ROOT_PATH.parent
CORE_SRC_ROOT = AIRFLOW_CORE_ROOT_PATH / "src" / "airflow"

NOCHECK_CODE = "SDK001"

# Dags are authored *with* the task SDK, so example Dags shipped in core are not core code
# for the purpose of this check and never take part in the ratchet.
EXCLUDED_DIR_NAMES = frozenset({"example_dags"})


def check_file_for_sdk_imports(file_path: Path) -> list[tuple[int, str]]:
    """Check file for airflow.sdk imports. Returns list of (line_num, import_statement)."""
    return find_import_violations(
        file_path,
        is_violating_module=lambda module: "airflow.sdk" in module,
        nocheck_code=NOCHECK_CODE,
    )


class SdkImportsAllowlistManager(AllowlistManager):
    def __init__(self, allowlist_file: Path) -> None:
        super().__init__(allowlist_file, repo_root=REPO_ROOT)

    def is_excluded(self, path: Path) -> bool:
        return not EXCLUDED_DIR_NAMES.isdisjoint(path.parts)

    def iter_files(self) -> Iterable[Path]:
        return (path for path in CORE_SRC_ROOT.rglob("*.py") if not self.is_excluded(path))

    def check(self, files: list[Path], allowlist: dict[str, int]) -> int:
        return super().check([path for path in files if not self.is_excluded(path)], allowlist)

    def count_occurrences(self, path: Path) -> int:
        return len(check_file_for_sdk_imports(path))

    def generate_for(self, files: list[Path]) -> int:
        """Rewrite the allowlist entries for *files* only, leaving every other entry untouched."""
        allowlist = self.load()
        updated: list[tuple[str, int, int]] = []

        for path in files:
            if self.is_excluded(path) or not path.exists() or path.suffix != ".py":
                continue
            try:
                rel = str(path.resolve().relative_to(self.repo_root.resolve()))
            except ValueError:
                console.print(f"[yellow]Skipping file outside the repository:[/yellow] {path}")
                continue
            before = allowlist.get(rel, 0)
            after = self.count_occurrences(path)
            if before == after:
                continue
            if after:
                allowlist[rel] = after
            else:
                del allowlist[rel]
            updated.append((rel, before, after))

        if not updated:
            console.print("[green]Allowlist already up to date for the given files.[/green]")
            return 0

        self.save(allowlist)
        console.print(
            f"[green]Updated[/green] [cyan]{self.allowlist_file.relative_to(self.repo_root)}[/cyan]:"
        )
        for rel, before, after in updated:
            console.print(f"  [cyan]{rel}[/cyan]  {before} -> {after}")
        return 0

    def violation_panel_text(self) -> str:
        return (
            "New [bold]airflow.sdk[/bold] import detected in airflow-core.\n"
            "Core (scheduler/API server) should not gain new runtime dependencies "
            "on the Task SDK.\n\n"
            "The preferred fix is to drop the import. If it has to stay, pick one:\n\n"
            "[bold]1. A single deliberate import line[/bold] -- append `# noqa: SDK001` to it.\n"
            "   The line is then invisible to this check and never reaches the allowlist.\n\n"
            "[bold]2. A file that genuinely needs several[/bold] -- record its new count:\n\n"
            "  [cyan]uv run ./scripts/ci/prek/check_sdk_imports_in_core.py --generate FILE ...[/cyan]\n\n"
            "   then commit the updated [cyan]generated/known_sdk_imports_in_core.txt[/cyan].\n"
            "   Passing no FILE rebuilds the whole allowlist and approves any unrelated\n"
            "   drift with it, so prefer the scoped form."
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prevent new airflow.sdk imports in airflow-core.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("files", nargs="*", metavar="FILE", help="Files to check (provided by prek)")
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="Check every Python file under airflow-core/src/airflow",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove stale entries from the allowlist and exit",
    )
    parser.add_argument(
        "--generate",
        action="store_true",
        help=(
            "Record the current counts for the given FILEs and exit; "
            "with no FILE, rebuild the whole allowlist from the current codebase"
        ),
    )
    args = parser.parse_args(argv)

    manager = SdkImportsAllowlistManager(REPO_ROOT / "generated" / "known_sdk_imports_in_core.txt")
    files = [Path(f).resolve() for f in args.files]

    if args.generate:
        return manager.generate_for(files) if files else manager.generate()

    if args.cleanup:
        return manager.cleanup()

    allowlist = manager.load()

    if args.all_files:
        return manager.check(list(manager.iter_files()), allowlist)

    if not files:
        console.print(
            "[yellow]No files provided. Pass filenames or use --all-files to scan the whole repo.[/yellow]"
        )
        return 0

    return manager.check(files, allowlist)


if __name__ == "__main__":
    raise SystemExit(main())
