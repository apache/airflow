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
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.0.0",
# ]
# ///
"""Check that no new ``raise SomeError("... %s ...", value)`` usages are introduced.

Exception constructors do not interpolate their arguments the way ``logger``
calls do -- ``Exception.__init__`` just stores everything in ``args``. So::

    raise AirflowException("TaskInstance %s is not found", ti.task_id)

renders as ``('TaskInstance %s is not found', 'my_task')`` rather than the
intended sentence, and ``AirflowException.serialize()`` carries that same
``str(self)`` across the trigger/task boundary. Exceptions whose ``__init__``
forwards only the message to ``super()`` -- ``google.api_core``'s
``GoogleAPICallError`` family, for instance -- drop the trailing arguments
outright, so the identifier the message exists to carry never reaches the user.
Bind an f-string to a variable and raise that instead.

Detection is AST-based because the pattern routinely spans several lines, and a
call is only flagged when the number of ``%`` placeholders in the leading string
literal exactly matches the number of trailing arguments -- counted the way
Python's own ``%`` operator would consume them, flags and width and precision
included. Anything looser misfires on the ten or so places that already pass a
literal message alongside unrelated positional parameters, such as
``TypeError("Could not parse hits.", response)`` in the elasticsearch provider or
SQLAlchemy's ``OperationalError(statement, params, orig)``.

Precision costs some recall, deliberately. The check sees only an exception
raised inline, so ``err = ValueError("%s", x); raise err`` slips past, as does
any call whose keyword arguments make the count disagree. Dict-style
``%(name)s`` and ``*`` widths are skipped outright: both consume arguments in a
way a plain count cannot describe.

The one place this knowingly parts company with Python is the space flag.
Python reads the ``% c`` in ``"download is 50% complete"`` as a conversion, so
a message writing a percentage in English and passing one context object beside
it would be flagged with nothing to fix. No message in the tree uses ``% d`` or
``% i`` for its intended purpose, so the flag is dropped from the grammar and
the English reading wins.

What the hook blocks is a net increase per file, not every new occurrence: the
allowlist stores a count, so fixing one message and adding another in the same
file goes unnoticed. That is the same trade-off every ``AllowlistManager`` hook
in this directory makes, and it keeps the record stable when lines move.

All *existing* usages are recorded in ``generated/known_exception_format_args.txt``
as ``relative/path::N`` entries (one per file), where ``N`` is the maximum number
of occurrences allowed in that file. A file whose current count exceeds the
recorded limit is treated as a violation.

Modes
-----
Default (files passed by prek/pre-commit):
    Check only the supplied files; fail if any file's count exceeds the limit.
    When a file's count has *decreased*, the allowlist entry is tightened
    automatically and the hook exits with a non-zero code so that pre-commit
    reports the modified allowlist -- just stage
    ``generated/known_exception_format_args.txt`` and re-run.

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
import ast
import re
from collections.abc import Iterable
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH, AllowlistManager
from rich.console import Console

console = Console(color_system="standard", width=200)

REPO_ROOT = AIRFLOW_ROOT_PATH

_FORMAT_TOKEN_RE = re.compile(
    r"""
    %
    (?:
        %                                   # an escaped literal percent
      | (?P<mapping>\([^)]*\))?            # mapping key, for dict-style formatting
        [#0\-+]*                            # flags, minus the space flag (see below)
        (?:\*|\d+)?                        # minimum field width
        (?:\.(?:\*|\d+))?                  # precision
        [hlLqjzt]?                          # length modifier, accepted and ignored
        (?P<conversion>[diouxXeEfFgGcrsa])
    )
    """,
    re.VERBOSE,
)

_SKIPPED_DIR_NAMES = frozenset({".git", ".tox", ".venv", "__pycache__", "node_modules", "site-packages"})


class ExceptionFormatArgsAllowlistManager(AllowlistManager):
    def __init__(self, allowlist_file: Path) -> None:
        super().__init__(allowlist_file, repo_root=REPO_ROOT)

    def iter_files(self) -> Iterable[Path]:
        return _iter_python_files()

    def count_occurrences(self, path: Path) -> int:
        return len(find_format_arg_raises(path))

    def format_violation_details(self, path: Path) -> list[str]:
        return [
            f"      line {lineno}: [yellow]{name}[/yellow]" for lineno, name in find_format_arg_raises(path)
        ]

    def violation_panel_text(self) -> str:
        return (
            'New [bold]raise SomeError("... %s ...", value)[/bold] usage detected.\n'
            "Exception constructors do not interpolate: the arguments are stored in\n"
            "[bold]args[/bold] and the message keeps its literal [bold]%s[/bold]. Some exceptions\n"
            "([bold]google.api_core[/bold]'s, for instance) drop them entirely.\n\n"
            "Fix it by binding an f-string and raising that:\n\n"
            '  [cyan]msg = f"TaskInstance {ti.task_id} is not found"[/cyan]\n'
            "  [cyan]raise AirflowException(msg)[/cyan]\n\n"
            "If the message writes a percentage in English that was never meant as a\n"
            "conversion, reword it: [bold]50%off[/bold] reads as one, [bold]50% off[/bold] does not.\n\n"
            "[yellow]--generate is not the fix.[/yellow] It records the line as allowed and leaves\n"
            "the broken message in place. Reach for it only when a file carrying\n"
            "existing occurrences is moved or renamed."
        )


def count_positional_placeholders(message: str) -> int | None:
    """Count ``%`` conversions in *message*, or return None if it is out of scope."""
    count = 0
    for token in _FORMAT_TOKEN_RE.finditer(message):
        if token.group("conversion") is None:
            continue
        # Dict-style formatting takes a single mapping and ``*`` widths consume an
        # argument of their own, so neither can be reconciled with a plain count.
        if token.group("mapping") is not None or "*" in token.group(0):
            return None
        count += 1
    return count


def find_format_arg_raises(path: Path) -> list[tuple[int, str]]:
    """Return ``(lineno, exception name)`` for every format-style raise in *path*."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
    except (OSError, SyntaxError, ValueError):
        return []

    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Raise) or not isinstance(node.exc, ast.Call):
            continue
        args = node.exc.args
        if len(args) < 2 or any(isinstance(arg, ast.Starred) for arg in args):
            continue
        message = args[0]
        if not (isinstance(message, ast.Constant) and isinstance(message.value, str)):
            continue
        if count_positional_placeholders(message.value) != len(args) - 1:
            continue
        func = node.exc.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "<expr>")
        found.append((node.lineno, name))
    return found


def _iter_python_files() -> list[Path]:
    return [p.resolve() for p in REPO_ROOT.rglob("*.py") if not _SKIPPED_DIR_NAMES.intersection(p.parts)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prevent new format-string arguments passed to exception constructors.",
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

    manager = ExceptionFormatArgsAllowlistManager(REPO_ROOT / "generated" / "known_exception_format_args.txt")

    if args.generate:
        return manager.generate()

    if args.cleanup:
        return manager.cleanup()

    allowlist = manager.load()

    if args.all_files:
        return manager.check(_iter_python_files(), allowlist)

    if not args.files:
        console.print(
            "[yellow]No files provided. Pass filenames or use --all-files to scan the whole repo.[/yellow]"
        )
        return 0

    return manager.check([Path(f).resolve() for f in args.files], allowlist)


if __name__ == "__main__":
    raise SystemExit(main())
