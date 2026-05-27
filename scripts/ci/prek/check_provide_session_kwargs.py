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
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

console = Console(color_system="standard", width=200)

REPO_ROOT = Path(__file__).parents[3]

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


def _is_safe_relative(rel: str) -> bool:
    """Whether ``rel`` is a plain relative path that stays inside ``REPO_ROOT``.

    Rejects absolute paths and any entry that resolves outside the repo root so
    callers can ``relative_to(REPO_ROOT)`` without fear of a ``ValueError``.
    """
    candidate = Path(rel)
    if candidate.is_absolute():
        return False
    try:
        (REPO_ROOT / candidate).resolve().relative_to(REPO_ROOT.resolve())
    except ValueError:
        return False
    return True


class AllowlistManager:
    def __init__(self, allowlist_file: Path) -> None:
        self.allowlist_file = allowlist_file

    @staticmethod
    def parse(text: str) -> dict[str, int]:
        """Parse allowlist *text* into a ``{rel_path: count}`` mapping.

        Same validation rules as :meth:`load` so we can reuse parsing for the
        on-disk allowlist *and* for the git-tracked version fetched from
        ``HEAD`` when guarding against entry-removal bypasses.
        """
        result: dict[str, int] = {}
        for raw_line in text.splitlines():
            if not (stripped := raw_line.strip()):
                continue

            rel_str, _, count_str = stripped.rpartition("::")
            if not rel_str or not count_str:
                continue

            try:
                count = int(count_str)
            except ValueError:
                continue

            if not _is_safe_relative(rel_str):
                console.print(
                    f"[yellow]Ignoring unsafe allowlist entry (escapes repo root):[/yellow] {rel_str}"
                )
                continue

            result[rel_str] = count

        return result

    def load(self) -> dict[str, int]:
        if not self.allowlist_file.exists():
            return {}
        return self.parse(self.allowlist_file.read_text())

    def save(self, counts: dict[str, int]) -> None:
        lines = [f"{rel}::{count}" for rel, count in sorted(counts.items())]
        self.allowlist_file.write_text("\n".join(lines) + "\n")

    def generate(self) -> int:
        roots = ", ".join(_PROJECT_SOURCE_ROOTS)
        console.print(
            f"Scanning project source roots ([cyan]{roots}[/cyan]) under [cyan]{REPO_ROOT}[/cyan] "
            "for @provide_session functions with positional session …"
        )
        counts: dict[str, int] = {}
        for path in _iter_python_files():
            n = _count_violations(path)
            if n > 0:
                counts[str(path.relative_to(REPO_ROOT))] = n

        self.save(counts)
        total = sum(counts.values())
        console.print(
            f"[green]Generated[/green] [cyan]{self.allowlist_file.relative_to(REPO_ROOT)}[/cyan] "
            f"with [bold]{len(counts)}[/bold] files / [bold]{total}[/bold] offenders."
        )
        return 0

    def cleanup(self) -> int:
        allowlist = self.load()
        if not allowlist:
            console.print("[yellow]Allowlist is empty - nothing to clean up.[/yellow]")
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
            console.print("[green]No stale entries found.[/green]")
        return 0


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
    files: list[Path], allowlist: dict[str, int], manager: AllowlistManager
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

    violations: list[tuple[Path, int, int]] = []
    tightened: list[tuple[str, int, int]] = []

    for path in files:
        if not path.exists() or path.suffix != ".py":
            continue
        actual = _count_violations(path)
        rel = str(path.relative_to(REPO_ROOT))
        allowed = allowlist.get(rel, 0)
        if actual > allowed:
            violations.append((path, actual, allowed))
        elif actual < allowed:
            if actual == 0:
                del allowlist[rel]
            else:
                allowlist[rel] = actual
            tightened.append((rel, allowed, actual))

    if tightened:
        manager.save(allowlist)
        console.print(
            f"[green]Tightened {len(tightened)} entr{'y' if len(tightened) == 1 else 'ies'} "
            f"in [cyan]{manager.allowlist_file.relative_to(REPO_ROOT)}[/cyan][/green] "
            "(stage the updated file):"
        )
        for rel, old, new in tightened:
            console.print(f"  [cyan]{rel}[/cyan]  {old} -> {new}")

    if violations:
        console.print(
            Panel.fit(
                "New [bold]@provide_session[/bold] function with positional ``session`` detected.\n"
                "Move ``session`` after a bare ``*`` in the signature so callers must pass it by keyword:\n\n"
                "  [cyan]@provide_session\n"
                "  def foo(arg, *, session: Session = NEW_SESSION) -> None: ...[/cyan]\n\n"
                "If this usage is intentional and pre-existing, run:\n\n"
                "  [cyan]uv run ./scripts/ci/prek/check_provide_session_kwargs.py --generate[/cyan]\n\n"
                "to regenerate the allowlist, then commit the updated\n"
                "[cyan]scripts/ci/prek/known_provide_session_positional.txt[/cyan].",
                title="[red]Check failed[/red]",
                border_style="red",
            )
        )
        for path, actual, allowed in violations:
            console.print(f"  [cyan]{path.relative_to(REPO_ROOT)}[/cyan]  count={actual} (allowed={allowed})")
            for func, argument in _iter_positional_session_in_provide_session(path):
                console.print(f"      [dim]L{argument.lineno}[/dim] def {func.name}(...)")
        return 1

    return 1 if tightened else 0


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

    manager = AllowlistManager(Path(__file__).parent / "known_provide_session_positional.txt")

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


def _parse_tracked_allowlist(manager: AllowlistManager) -> dict[str, int]:
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
    return AllowlistManager.parse(completed.stdout)


def _expand_for_allowlist_edits(
    paths: list[Path], manager: AllowlistManager, allowlist: dict[str, int]
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
