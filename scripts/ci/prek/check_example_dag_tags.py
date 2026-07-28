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
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""Check that every example DAG carries the ``example`` tag.

Example DAGs are filtered and grouped by tag across the docs and the UI, so a
consistent ``example`` tag must be present on every DAG defined under an
``example_dags`` folder. This is enforced statically: the ``tags`` argument
must be an inline list literal that contains the string ``"example"``.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from rich.console import Console

console = Console(color_system="standard", width=200)

REQUIRED_TAG = "example"


def _resolves_to_dag(node: ast.AST) -> bool:
    """Return ``True`` when ``node`` references the ``DAG`` class or the ``dag`` decorator.

    The ``dag`` decorator is always imported and used as a bare name (``@dag``),
    so it is only matched as an ``ast.Name``. ``DAG`` is additionally matched as
    an attribute (``models.DAG``). This keeps an unrelated ``something.dag(...)``
    method call from being mistaken for a DAG definition.
    """
    if isinstance(node, ast.Name):
        return node.id in ("DAG", "dag")
    if isinstance(node, ast.Attribute):
        return node.attr == "DAG"
    return False


def _tags_keyword(call: ast.Call) -> ast.keyword | None:
    for keyword in call.keywords:
        if keyword.arg == "tags":
            return keyword
    return None


def _dag_call_error(location: str, call: ast.Call) -> str | None:
    """Return an error message when a ``DAG(...)`` / ``@dag(...)`` call lacks the tag."""
    tags_keyword = _tags_keyword(call)
    if tags_keyword is None:
        return (
            f"[red]{location}: example DAG is missing the '{REQUIRED_TAG}' tag.[/]\n"
            f'Add [yellow]tags=["{REQUIRED_TAG}"][/] to the DAG definition.\n'
        )
    value = tags_keyword.value
    if not isinstance(value, (ast.List, ast.Tuple)):
        return (
            f"[red]{location}: 'tags' must be an inline list literal so the "
            f"'{REQUIRED_TAG}' tag can be verified statically.[/]\n"
        )
    literal_tags = [element.value for element in value.elts if isinstance(element, ast.Constant)]
    if REQUIRED_TAG not in literal_tags:
        return (
            f"[red]{location}: example DAG is missing the '{REQUIRED_TAG}' tag.[/]\n"
            f"Add [yellow]\"{REQUIRED_TAG}\"[/] to the existing 'tags' list.\n"
        )
    return None


def check_file(file: Path) -> list[str]:
    """Return the list of tagging errors found in a single example DAG file."""
    try:
        tree = ast.parse(file.read_text(), filename=str(file))
    except SyntaxError as exc:
        return [f"[red]{file}: could not be parsed: {exc}[/]\n"]
    errors: list[str] = []
    for node in ast.walk(tree):
        # ``DAG(...)``, ``with DAG(...) as ...:`` and ``@dag(...)`` are all calls.
        if isinstance(node, ast.Call) and _resolves_to_dag(node.func):
            error = _dag_call_error(f"{file}:{node.lineno}", node)
            if error:
                errors.append(error)
            continue
        # A bare ``@dag`` decorator (no parentheses) takes no arguments at all,
        # so it can never carry the required tag.
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in node.decorator_list:
                # ``@dag(...)`` is an ast.Call already handled by the branch
                # above; only a bare ``@dag`` needs to be flagged here.
                if isinstance(decorator, ast.Call) or not _resolves_to_dag(decorator):
                    continue
                errors.append(
                    f"[red]{file}:{decorator.lineno}: example DAG is missing the "
                    f"'{REQUIRED_TAG}' tag.[/]\n"
                    f'Add [yellow]tags=["{REQUIRED_TAG}"][/] to the [yellow]@dag[/] decorator.\n'
                )
    return errors


def main(argv: list[str]) -> int:
    errors: list[str] = []
    for argument in argv:
        errors.extend(check_file(Path(argument)))
    if not errors:
        return 0
    console.print(f"[red]Found example DAGs without the '{REQUIRED_TAG}' tag:[/]\n")
    for error in errors:
        console.print(error)
    console.print(
        f'Every example DAG must be tagged with [yellow]"{REQUIRED_TAG}"[/] '
        "so it can be filtered consistently.\n"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
