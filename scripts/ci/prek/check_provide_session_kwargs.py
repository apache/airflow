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
"""Check that no new ``@provide_session`` functions declare ``session`` positionally.

The project convention is that any function decorated with ``@provide_session``
must declare ``session`` as keyword-only (after a bare ``*`` in the signature),
so callers cannot pass it positionally by accident. See
``contributing-docs/05_pull_requests.rst#database-session-handling``.

All *existing* offenders are recorded in ``known_provide_session_positional.txt``
next to this script as ``relative/path::N`` entries (one per file), where ``N``
is the maximum number of ``@provide_session`` functions with a positional
``session`` argument allowed in that file. A file whose current count exceeds
the recorded limit is treated as a violation – move the ``session`` argument
behind a bare ``*`` instead.

Modes
-----
Default (files passed by prek/pre-commit):
    Check only the supplied files; fail if any file's count exceeds the limit.
    When a file's count has *decreased*, the allowlist entry is tightened
    automatically and the hook exits with a non-zero code so that pre-commit
    reports the modified allowlist – just stage
    ``scripts/ci/prek/known_provide_session_positional.txt`` and re-run.

``--all-files``:
    Walk every ``.py`` file under the project source roots
    (``airflow-core``, ``providers``, ``shared``) —
    the same scope the pre-commit hook applies to.

``--cleanup``:
    Remove entries for files that no longer exist. Safe to run at any time;
    does not add new entries or raise limits.

``--generate``:
    Scan the same project source roots as ``--all-files`` and *rebuild* the
    allowlist from scratch. Intended for the initial setup or after a
    large-scale clean-up sprint.
"""

from __future__ import annotations

import argparse
import ast
import subprocess
import typing
from collections.abc import Iterable
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH, AllowlistManager
from rich.console import Console
from rich.panel import Panel

console = Console(color_system="standard", width=200)

REPO_ROOT = AIRFLOW_ROOT_PATH

_PROVIDE_SESSION_DECORATOR = "provide_session"

# Top-level directories scanned by ``--all-files`` / ``--generate``. Keep in sync with the
# ``files:`` pattern for this hook in ``.pre-commit-config.yaml``.
_PROJECT_SOURCE_ROOTS = ("airflow-core", "providers", "shared")


def _has_provide_session_decorator(nodes: list[ast.expr]) -> bool:
    """Whether one of ``nodes`` is a ``@provide_session`` decorator.

    Accepts both bare names (``@provide_session``) and attribute access
    (``@something.provide_session``).
    """
    for node in nodes:
        if isinstance(node, ast.Name) and node.id == _PROVIDE_SESSION_DECORATOR:
            return True
        if isinstance(node, ast.Attribute) and node.attr == _PROVIDE_SESSION_DECORATOR:
            return True
    return False


def _session_is_positional(args: ast.arguments) -> ast.arg | None:
    """Return the ``session`` arg if it is positional (not keyword-only).

    Covers both regular positional args and positional-only args (``def f(session, /, ...)``).
    """
    for argument in (*args.posonlyargs, *args.args):
        if argument.arg == "session":
            return argument
    return None


def _iter_positional_session_in_provide_session(
    path: Path,
) -> typing.Iterator[tuple[ast.FunctionDef | ast.AsyncFunctionDef, ast.arg]]:
    """Yield ``@provide_session`` functions in *path* whose ``session`` is positional."""
    try:
        source = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return
    try:
        tree = ast.parse(source, str(path))
    except SyntaxError:
        return
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not _has_provide_session_decorator(node.decorator_list):
            continue
        argument = _session_is_positional(node.args)
        if argument is None:
            continue
        yield node, argument


def _count_violations(path: Path) -> int:
    return sum(1 for _ in _iter_positional_session_in_provide_session(path))


class ProvideSessionAllowlistManager(AllowlistManager):
    def __init__(self, allowlist_file: Path) -> None:
        super().__init__(allowlist_file, repo_root=REPO_ROOT)

    def iter_files(self) -> Iterable[Path]:
        return _iter_python_files()

    def count_occurrences(self, path: Path) -> int:
        return _count_violations(path)

    def violation_panel_text(self) -> str:
        return (
            "New [bold]@provide_session[/bold] function with positional ``session`` detected.\n"
            "Move ``session`` after a bare ``*`` in the signature so callers must pass it by keyword:\n\n"
            "  [cyan]@provide_session\n"
            "  def foo(arg, *, session: Session = NEW_SESSION) -> None: ...[/cyan]\n\n"
            "If this usage is intentional and pre-existing, run:\n\n"
            "  [cyan]uv run ./scripts/ci/prek/check_provide_session_kwargs.py --generate[/cyan]\n\n"
            "to regenerate the allowlist, then commit the updated\n"
            "[cyan]scripts/ci/prek/known_provide_session_positional.txt[/cyan]."
        )

    def format_violation_details(self, path: Path) -> list[str]:
        return [
            f"      [dim]L{argument.lineno}[/dim] def {func.name}(...)"
            for func, argument in _iter_positional_session_in_provide_session(path)
        ]


def _iter_python_files() -> list[Path]:
    candidates: list[Path] = []
    for top in _PROJECT_SOURCE_ROOTS:
        candidates.extend(
            p.resolve()
            for p in (REPO_ROOT / top).rglob("*.py")
            if ".tox" not in p.parts and "__pycache__" not in p.parts
        )
    return candidates


def _check_provide_session_kwargs(
    files: list[Path], allowlist: dict[str, int], manager: ProvideSessionAllowlistManager
) -> int:
    allowlist_file = manager.allowlist_file.resolve()
    if any(p.resolve() == allowlist_file for p in files) and not allowlist_file.exists():
        console.print(
            Panel.fit(
                f"Allowlist file [cyan]{allowlist_file}[/cyan] is missing.\n"
                "It was passed to the hook but cannot be read, so the check cannot proceed.\n"
                "Restore it from git or regenerate it with:\n\n"
                "  [cyan]uv run ./scripts/ci/prek/check_provide_session_kwargs.py --generate[/cyan]",
                title="[red]Check failed[/red]",
                border_style="red",
            )
        )
        return 1
    return manager.check(files, allowlist)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prevent new @provide_session functions from declaring `session` positionally.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("files", nargs="*", metavar="FILE", help="Files to check (provided by prek)")
    parser.add_argument(
        "--all-files",
        action="store_true",
        help=("Check every Python file under the project source roots (airflow-core, providers, shared)"),
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

    manager = ProvideSessionAllowlistManager(Path(__file__).parent / "known_provide_session_positional.txt")

    if args.generate:
        return manager.generate()

    if args.cleanup:
        return manager.cleanup()

    allowlist = manager.load()

    if args.all_files:
        return _check_provide_session_kwargs(_iter_python_files(), allowlist, manager)

    if not args.files:
        console.print(
            "[yellow]No files provided. Pass filenames or use --all-files to scan the whole repo.[/yellow]"
        )
        return 0

    paths = [Path(f).resolve() for f in args.files]
    paths = _expand_for_allowlist_edits(paths, manager, allowlist)
    return _check_provide_session_kwargs(paths, allowlist, manager)


def _parse_tracked_allowlist(manager: ProvideSessionAllowlistManager) -> dict[str, int]:
    """Return the allowlist as recorded at ``HEAD`` (the git-tracked version).

    Used by :func:`_expand_for_allowlist_edits` so that *removing* an entry
    cannot silently drop coverage: the previously-listed file is still
    re-validated against the new (post-edit) allowlist. Returns an empty mapping
    when git is unavailable, the file does not yet exist at ``HEAD``, or the
    allowlist sits outside ``REPO_ROOT``.
    """
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
    paths: list[Path], manager: ProvideSessionAllowlistManager, allowlist: dict[str, int]
) -> list[Path]:
    """Add allowlisted files when the allowlist itself is being changed.

    Without this, a contributor could raise counts in
    ``known_provide_session_positional.txt`` and the hook would do no validation
    (since only the ``.txt`` file is passed), letting the loosened allowlist
    sail through. We also union the git-tracked allowlist (from ``HEAD``) so
    that removing an entry cannot silently bypass the check for a file that
    still has positional ``session`` arguments.

    Both sides of the allowlist-file comparison are resolved so the detection is
    robust to symlinks and unresolved inputs (the hook can be invoked with either).
    """
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
