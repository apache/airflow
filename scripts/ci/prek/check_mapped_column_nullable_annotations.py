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
"""Check that ``mapped_column(..., nullable=True)`` attributes are annotated ``Mapped[X | None]``.

SQLAlchemy 2 derives a column's nullability from its ``Mapped[...]`` annotation only when
``nullable=`` is *not* passed explicitly. Once ``nullable=True`` is spelled out the DDL is
right, but nothing cross-checks the annotation any more: ``Mapped[datetime]`` on such a column
tells mypy the attribute can never be ``None``, so callers skip the ``None`` guard and fail at
runtime with ``AttributeError`` the first time they touch a NULL row.

The check is deliberately one-directional. An Optional annotation on a NOT NULL column is
harmless and is not reported.

Modes
-----
Default (files passed by prek):
    Check only the supplied files.

``--all-files``:
    Walk the directories that define ORM models (``models`` and ``jobs`` in airflow-core,
    and the ``models`` packages of the edge3 and FAB providers) and check every ``.py`` file.
"""

from __future__ import annotations

import argparse
import ast
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH
from rich.console import Console
from rich.markup import escape

console = Console(color_system="standard", width=200)

REPO_ROOT = AIRFLOW_ROOT_PATH
# Keep in sync with the hook's ``files`` pattern in .pre-commit-config.yaml.
SCAN_ROOTS: tuple[str, ...] = (
    "airflow-core/src/airflow/models",
    "airflow-core/src/airflow/jobs",
    "providers/edge3/src/airflow/providers/edge3/models",
    "providers/fab/src/airflow/providers/fab/auth_manager/models",
)

_NONE_ADMITTING_NAMES = frozenset({"Any", "Optional"})


@dataclass(frozen=True)
class NullableAnnotationMismatch:
    path: Path
    lineno: int
    attribute: str
    annotation: str


def _extract_trailing_name(node: ast.expr) -> str | None:
    """Return the last identifier of a ``Name``/``Attribute`` chain (``orm.Mapped`` -> ``Mapped``)."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _resolve_string_annotation(node: ast.expr) -> ast.expr:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        try:
            return ast.parse(node.value, mode="eval").body
        except SyntaxError:
            return node
    return node


def admits_none(annotation: ast.expr) -> bool:
    """Return True if the annotation can hold ``None``: ``X | None``, ``Optional[X]``, ``Union[X, None]``, ``Any``."""
    annotation = _resolve_string_annotation(annotation)
    if isinstance(annotation, ast.Constant):
        return annotation.value is None
    if isinstance(annotation, ast.BinOp) and isinstance(annotation.op, ast.BitOr):
        return admits_none(annotation.left) or admits_none(annotation.right)
    if _extract_trailing_name(annotation) in _NONE_ADMITTING_NAMES:
        return True
    if isinstance(annotation, ast.Subscript):
        outer = _extract_trailing_name(annotation.value)
        if outer == "Optional":
            return True
        if outer == "Union":
            members = annotation.slice.elts if isinstance(annotation.slice, ast.Tuple) else [annotation.slice]
            return any(admits_none(member) for member in members)
    return False


def _explicit_nullable(call: ast.Call) -> bool | None:
    """Return the literal ``nullable=`` value, or ``None`` when absent or not a plain boolean."""
    for keyword in call.keywords:
        if keyword.arg == "nullable":
            value = keyword.value
            if isinstance(value, ast.Constant) and isinstance(value.value, bool):
                return value.value
            return None
    return None


def iter_mismatches(path: Path) -> Iterator[NullableAnnotationMismatch]:
    """Yield every ``Mapped[...] = mapped_column(..., nullable=True)`` whose annotation cannot be ``None``."""
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return
    if "mapped_column" not in source:
        return
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return
    for node in ast.walk(tree):
        if not isinstance(node, ast.AnnAssign) or node.value is None:
            continue
        annotation = _resolve_string_annotation(node.annotation)
        if not (
            isinstance(annotation, ast.Subscript) and _extract_trailing_name(annotation.value) == "Mapped"
        ):
            continue
        call = node.value
        if not (isinstance(call, ast.Call) and _extract_trailing_name(call.func) == "mapped_column"):
            continue
        if _explicit_nullable(call) is not True or admits_none(annotation.slice):
            continue
        yield NullableAnnotationMismatch(
            path=path,
            lineno=node.lineno,
            attribute=ast.unparse(node.target),
            annotation=ast.unparse(annotation),
        )


def iter_python_files(roots: Iterable[Path]) -> Iterator[Path]:
    for root in roots:
        for path in sorted(root.rglob("*.py")):
            relative_parts = path.relative_to(root).parts
            if any(part.startswith(".") or part == "node_modules" for part in relative_parts):
                continue
            yield path


def _format_display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def check_files(files: Iterable[Path]) -> int:
    mismatches = [mismatch for path in files for mismatch in iter_mismatches(path)]
    if not mismatches:
        return 0
    console.print(
        f"[red]Found {len(mismatches)} nullable mapped_column attribute(s) whose "
        "Mapped annotation does not admit None:[/]\n",
        highlight=False,
    )
    for mismatch in mismatches:
        console.print(
            f"  {_format_display_path(mismatch.path)}:{mismatch.lineno}: "
            f"[bold]{escape(mismatch.attribute)}[/]: {escape(mismatch.annotation)}",
            highlight=False,
            soft_wrap=True,
        )
    console.print(
        "\nA column declared with [cyan]nullable=True[/] can hold NULL, so its annotation must say so: "
        f"annotate it as [cyan]{escape('Mapped[X | None]')}[/], or drop [cyan]nullable=True[/] if the column "
        "is meant to be NOT NULL. Otherwise mypy believes the attribute is never None and lets callers "
        "skip the guard that the NULL rows require.",
        highlight=False,
    )
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check that nullable mapped_column attributes are annotated Mapped[X | None].",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("files", nargs="*", metavar="FILE", help="Files to check (provided by prek)")
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="Check every Python file in the ORM model directories",
    )
    args = parser.parse_args(argv)

    if args.all_files:
        return check_files(iter_python_files(REPO_ROOT / root for root in SCAN_ROOTS))

    if not args.files:
        console.print("[yellow]No files provided. Pass filenames or use --all-files.[/yellow]")
        return 0

    return check_files(Path(file) for file in args.files)


if __name__ == "__main__":
    raise SystemExit(main())
