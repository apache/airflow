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

All *existing* usages are recorded in ``known_airflow_exceptions.txt`` next to
this script as ``relative/path::N`` entries (one per file), where ``N`` is the
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
    ``scripts/ci/prek/known_airflow_exceptions.txt`` and re-run.

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
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

console = Console()

REPO_ROOT = Path(__file__).parents[3]

# Match lines that actually raise AirflowException. Comment filtering is done
# in _raise_lines() by skipping lines whose stripped form starts with "#".
_RAISE_RE = re.compile(r"raise\s+AirflowException\b")


class AllowlistManager:
    def __init__(self, allowlist_file: Path) -> None:
        self.allowlist_file = allowlist_file

    def load(self) -> dict[str, int]:
        """Return mapping of ``relative_path -> allowed_count``."""
        if not self.allowlist_file.exists():
            return {}

        result: dict[str, int] = {}
        for raw_line in self.allowlist_file.read_text().splitlines():
            if not (stripped := raw_line.strip()):
                continue

            rel_str, _, count_str = stripped.rpartition("::")
            if not rel_str or not count_str:
                continue

            try:
                result[rel_str] = int(count_str)
            except ValueError:
                continue

        return result

    def save(self, counts: dict[str, int]) -> None:
        lines = [f"{rel}::{count}" for rel, count in sorted(counts.items())]
        self.allowlist_file.write_text("\n".join(lines) + "\n")

    def generate(self) -> int:
        console.print(f"Scanning [cyan]{REPO_ROOT}[/cyan] for raise AirflowException …")
        counts: dict[str, int] = {}
        for path in _iter_python_files():
            n = len(_raise_lines(path))
            if n > 0:
                counts[str(path.relative_to(REPO_ROOT))] = n

        self.save(counts)
        total = sum(counts.values())
        console.print(
            f"[green]✓ Generated[/green] [cyan]{self.allowlist_file.relative_to(REPO_ROOT)}[/cyan] "
            f"with [bold]{len(counts)}[/bold] files / [bold]{total}[/bold] occurrences."
        )
        return 0

    def cleanup(self) -> int:
        allowlist = self.load()
        if not allowlist:
            console.print("[yellow]Allowlist is empty – nothing to clean up.[/yellow]")
            return 0

        stale: list[str] = [rel for rel in allowlist if not (REPO_ROOT / rel).exists()]
        if stale:
            console.print(
                f"[yellow]Removing {len(stale)} stale entr{'y' if len(stale) == 1 else 'ies'}:[/yellow]"
            )
            for s in sorted(stale):
                console.print(f"  [dim]-[/dim] {s}")
            for s in stale:
                del allowlist[s]
            self.save(allowlist)
            console.print(
                f"\n[green]Updated[/green] [cyan]{self.allowlist_file.relative_to(REPO_ROOT)}[/cyan]"
            )
        else:
            console.print("[green]✓ No stale entries found.[/green]")
        return 0


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
    files: list[Path], allowlist: dict[str, int], manager: AllowlistManager
) -> int:
    violations: list[tuple[Path, int, int]] = []
    tightened: list[tuple[str, int, int]] = []  # (rel, old_count, new_count)

    for path in files:
        if not path.exists() or path.suffix != ".py":
            continue
        actual = len(_raise_lines(path))
        rel = str(path.relative_to(REPO_ROOT))
        allowed = allowlist.get(rel, 0)
        if actual > allowed:
            violations.append((path, actual, allowed))
        elif actual < allowed:
            # Usage was reduced — tighten the allowlist entry so it can't creep back up.
            if actual == 0:
                del allowlist[rel]
            else:
                allowlist[rel] = actual
            tightened.append((rel, allowed, actual))

    if tightened:
        manager.save(allowlist)
        console.print(
            f"[green]✓ Tightened {len(tightened)} entr{'y' if len(tightened) == 1 else 'ies'} "
            f"in [cyan]{manager.allowlist_file.relative_to(REPO_ROOT)}[/cyan][/green] "
            "(stage the updated file):"
        )
        for rel, old, new in tightened:
            console.print(f"  [cyan]{rel}[/cyan]  {old} → {new}")

    if violations:
        console.print(
            Panel.fit(
                "New [bold]raise AirflowException[/bold] usage detected.\n"
                "Define a dedicated exception class or use an existing specific exception.\n"
                "If this usage is intentional and pre-existing, run:\n\n"
                "  [cyan]uv run ./scripts/ci/prek/check_new_airflow_exception_usage.py --generate[/cyan]\n\n"
                "to regenerate the allowlist, then commit the updated\n"
                "[cyan]scripts/ci/prek/known_airflow_exceptions.txt[/cyan].",
                title="[red]❌ Check failed[/red]",
                border_style="red",
            )
        )
        for path, actual, allowed in violations:
            console.print(f"  [cyan]{path.relative_to(REPO_ROOT)}[/cyan]  count={actual} (allowed={allowed})")
        return 1

    # Return 1 when the allowlist was tightened so pre-commit reports the file as modified
    # and prompts the user to stage the updated allowlist.
    return 1 if tightened else 0


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

    manager = AllowlistManager(Path(__file__).parent / "known_airflow_exceptions.txt")

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
