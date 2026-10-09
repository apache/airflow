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
"""Check that no ``raise SomeError("... %s ...", value)`` usages are introduced.

Exception constructors do not interpolate their arguments the way ``logger``
calls do -- ``Exception.__init__`` just stores everything in ``args``. So::

    raise AirflowException("TaskInstance %s is not found", ti.task_id)

renders as ``('TaskInstance %s is not found', 'my_task')`` rather than the
intended sentence, and ``AirflowException.serialize()`` carries that same
``str(self)`` across the trigger/task boundary. Exceptions whose ``__init__``
forwards only the message to ``super()`` -- ``google.api_core``'s
``GoogleAPICallError`` family, for instance -- drop the trailing arguments
outright, so the identifier the message exists to carry never reaches the user.
Interpolate the message with an f-string instead.

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
"""

from __future__ import annotations

import argparse
import ast
import re
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH
from rich.console import Console
from rich.panel import Panel

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

_VIOLATION_PANEL_TEXT = (
    '[bold]raise SomeError("... %s ...", value)[/bold] usage detected.\n'
    "Exception constructors do not interpolate: the arguments are stored in\n"
    "[bold]args[/bold] and the message keeps its literal [bold]%s[/bold]. Some exceptions\n"
    "([bold]google.api_core[/bold]'s, for instance) drop them entirely.\n\n"
    "Fix it by interpolating the message with an f-string:\n\n"
    '  [cyan]raise AirflowException(f"TaskInstance {ti.task_id} is not found")[/cyan]\n\n'
    "If the message writes a percentage in English that was never meant as a\n"
    "conversion, reword it: [bold]50%off[/bold] reads as one, [bold]50% off[/bold] does not."
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
    return sorted(found)


def check_files(files: list[Path]) -> int:
    violations: dict[Path, list[tuple[int, str]]] = {}
    for path in files:
        if path.suffix == ".py" and (found := find_format_arg_raises(path)):
            violations[path] = found
    if not violations:
        return 0

    console.print(Panel.fit(_VIOLATION_PANEL_TEXT, title="[red]Check failed[/red]", border_style="red"))
    for path, found in violations.items():
        console.print(f"  [cyan]{path.relative_to(REPO_ROOT)}[/cyan]")
        for lineno, name in found:
            console.print(f"      line {lineno}: [yellow]{name}[/yellow]")
    return 1


def _iter_python_files() -> list[Path]:
    return [p.resolve() for p in REPO_ROOT.rglob("*.py") if not _SKIPPED_DIR_NAMES.intersection(p.parts)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Prevent format-string arguments passed to exception constructors.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("files", nargs="*", metavar="FILE", help="Files to check (provided by prek)")
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="Check every Python file in the repository",
    )
    args = parser.parse_args(argv)

    if args.all_files:
        return check_files(_iter_python_files())

    return check_files([Path(f).resolve() for f in args.files])


if __name__ == "__main__":
    raise SystemExit(main())
