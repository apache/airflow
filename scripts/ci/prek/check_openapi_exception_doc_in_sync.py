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
"""Check API route handlers declare every HTTP status they raise.

``responses=`` is what the generated OpenAPI spec — and every client built from
it — uses to model error responses, but nothing ties it to the statuses a
handler actually raises. The two drift apart silently, and that drift has been
patched by hand repeatedly (#67570, #67571, #70992, #71011).

A handler violates the rule when it raises ``HTTPException(<status>)`` -- in its
own body, or in a helper it calls -- with a status neither its own ``responses=``
block nor its router's declares. Helper calls are followed across modules up to
``MAX_CALL_DEPTH``, which is how a status raised by something like
``get_latest_version_of_dag`` is attributed to the route that calls it.

The check is deliberately conservative so it can gate CI: ``401``, ``403`` and
``422`` are never required (FastAPI and the routers' auth dependencies supply
them), only calls in a handler's *body* are followed, so the security
dependencies in a route decorator stay exempt, and anything that cannot be
resolved statically is skipped rather than guessed at. It therefore
under-reports rather than over-reports.
"""

# /// script
# requires-python = ">=3.11,<3.12"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path
from typing import NamedTuple

from common_prek_utils import console

ROUTE_METHODS = {"get", "post", "put", "patch", "delete", "head", "options"}
ROUTER_CLASSES = {"APIRouter", "AirflowRouter", "VersionedAPIRouter"}
DOC_HELPER = "create_openapi_http_exception_doc"
# 422 is added to every route by FastAPI itself; 401/403 come from the router's auth
# dependencies, declared once on a router this file-scoped check often cannot reach.
ALWAYS_DOCUMENTED = {401, 403, 422}

# Helper chains in these routes are shallow; the bound only stops pathological recursion.
MAX_CALL_DEPTH = 3

_STATUS_CONSTANT = re.compile(r"^HTTP_(\d{3})_")


class Violation(NamedTuple):
    handler: str
    status: int
    lineno: int
    # Name of the helper that raises the status, or None when the handler raises it itself.
    via: str | None

    def describe(self) -> str:
        source = f" via {self.via}()" if self.via else ""
        return f"  Line {self.lineno}: {self.handler}() raises {self.status}{source} but never declares it"


def _resolve_status(node: ast.expr) -> int | None:
    """Resolve a status code expression to its numeric value, or None if unknown."""
    if isinstance(node, ast.Attribute):
        name = node.attr
    elif isinstance(node, ast.Name):
        name = node.id
    elif isinstance(node, ast.Constant) and isinstance(node.value, int):
        return node.value
    else:
        return None
    match = _STATUS_CONSTANT.match(name)
    return int(match.group(1)) if match else None


def _statuses_from_responses(responses: ast.expr) -> set[int] | None:
    """Resolve a ``responses=`` value to its statuses, or None when unanalyzable."""
    # Routes that document a success body spell it as a mapping that unpacks the helper
    # alongside literal entries; routers use a plain ``{status: {"description": ...}}``.
    if isinstance(responses, ast.Dict):
        collected: set[int] = set()
        for key, value in zip(responses.keys, responses.values):
            if key is None:
                # A ``None`` key is a ``**`` unpacking; its value carries the real statuses.
                unpacked = _statuses_from_responses(value)
                if unpacked is None:
                    return None
                collected |= unpacked
            else:
                status = _resolve_status(key)
                if status is None:
                    return None
                collected.add(status)
        return collected

    if not (
        isinstance(responses, ast.Call)
        and isinstance(responses.func, ast.Name)
        and responses.func.id == DOC_HELPER
        and responses.args
        and isinstance(entries := responses.args[0], (ast.List, ast.Tuple))
    ):
        return None

    declared: set[int] = set()
    for entry in entries.elts:
        # Entries are either a bare status or a ``(status, description)`` pair.
        target = entry.elts[0] if isinstance(entry, ast.Tuple) and entry.elts else entry
        status = _resolve_status(target)
        if status is None:
            return None
        declared.add(status)
    return declared


def _declared_statuses(decorator: ast.Call) -> set[int] | None:
    """Return statuses declared by the route's own ``responses=``."""
    responses = next((kw.value for kw in decorator.keywords if kw.arg == "responses"), None)
    return set() if responses is None else _statuses_from_responses(responses)


def _router_statuses(tree: ast.Module) -> dict[str, set[int] | None]:
    """Map each router built in this module to the statuses it declares for every route on it."""
    routers: dict[str, set[int] | None] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        call = node.value
        if not (
            isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id in ROUTER_CLASSES
        ):
            continue
        responses = next((kw.value for kw in call.keywords if kw.arg == "responses"), None)
        statuses = set() if responses is None else _statuses_from_responses(responses)
        for target in node.targets:
            if isinstance(target, ast.Name):
                routers[target.id] = statuses
    return routers


FunctionNode = ast.FunctionDef | ast.AsyncFunctionDef


def _body_calls(function: FunctionNode) -> list[ast.Call]:
    """Return calls made in the function's body, excluding its decorators.

    Route decorators carry the ``Depends(...)`` security dependencies, whose statuses
    the router supplies and this check does not require, so they must not be followed.
    """
    return [node for statement in function.body for node in ast.walk(statement) if isinstance(node, ast.Call)]


def _raised_statuses(handler: FunctionNode) -> dict[int, int]:
    """Map each status raised as ``HTTPException`` in the body to its first line."""
    raised: dict[int, int] = {}
    for node in _body_calls(handler):
        if not isinstance(node.func, ast.Name) or node.func.id != "HTTPException":
            continue
        argument = next(
            (kw.value for kw in node.keywords if kw.arg == "status_code"),
            node.args[0] if node.args else None,
        )
        if argument is None:
            continue
        if (status := _resolve_status(argument)) is not None:
            raised.setdefault(status, node.lineno)
    return raised


def _source_root(file_path: Path) -> Path | None:
    """Return the import root (the ``src`` directory) this file lives under."""
    return next((parent for parent in file_path.parents if parent.name == "src"), None)


class _ModuleIndex:
    """Lazily parsed view of the functions each module defines, keyed by import path."""

    def __init__(self, source_root: Path | None) -> None:
        self._source_root = source_root
        self._cache: dict[str, dict[str, FunctionNode]] = {}

    def functions(self, module: str) -> dict[str, FunctionNode]:
        if (source_root := self._source_root) is None:
            return {}
        if module not in self._cache:
            self._cache[module] = self._parse(source_root, module)
        return self._cache[module]

    @staticmethod
    def _parse(source_root: Path, module: str) -> dict[str, FunctionNode]:
        relative = Path(*module.split("."))
        for candidate in (
            source_root / relative.with_suffix(".py"),
            source_root / relative / "__init__.py",
        ):
            try:
                tree = ast.parse(candidate.read_text(encoding="utf-8"), filename=str(candidate))
            except (OSError, UnicodeDecodeError, SyntaxError):
                continue
            return {
                node.name: node
                for node in ast.walk(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
        return {}


def _imported_functions(tree: ast.Module) -> dict[str, tuple[str, str]]:
    """Map each name bound by ``from <module> import <name>`` to ``(module, original)``."""
    imported: dict[str, tuple[str, str]] = {}
    for node in ast.walk(tree):
        # ``level`` is non-zero for relative imports, which this resolver does not handle.
        if isinstance(node, ast.ImportFrom) and node.module and not node.level:
            for alias in node.names:
                imported[alias.asname or alias.name] = (node.module, alias.name)
    return imported


class _CallGraph:
    """Resolves the statuses a handler raises through the helpers it calls."""

    def __init__(self, tree: ast.Module, index: _ModuleIndex) -> None:
        self._index = index
        self._local = {
            node.name: node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self._imported = _imported_functions(tree)

    def _resolve(
        self, name: str, namespace: dict[str, FunctionNode]
    ) -> tuple[FunctionNode, dict[str, FunctionNode]] | None:
        """Return the called function and the namespace defining it.

        Imports are followed only from the route module itself; inside an imported
        module just that module's own definitions are visible, which is what keeps a
        chain from fanning out across the whole package.
        """
        if (function := namespace.get(name)) is not None:
            return function, namespace
        if namespace is not self._local or (origin := self._imported.get(name)) is None:
            return None
        module, original = origin
        imported = self._index.functions(module)
        if (function := imported.get(original)) is None:
            return None
        return function, imported

    def statuses_via_helpers(self, handler: FunctionNode) -> dict[int, str]:
        """Map each status raised only by a helper to the name of the helper raising it."""
        found: dict[int, str] = {}
        self._walk(handler, self._local, depth=0, seen={handler.name}, attribute_to=None, found=found)
        for status in _raised_statuses(handler):
            found.pop(status, None)
        return found

    def _walk(
        self,
        function: FunctionNode,
        namespace: dict[str, FunctionNode],
        depth: int,
        seen: set[str],
        attribute_to: str | None,
        found: dict[int, str],
    ) -> None:
        if depth >= MAX_CALL_DEPTH:
            return
        for call in _body_calls(function):
            if not isinstance(call.func, ast.Name):
                continue
            name = call.func.id
            if name in seen:
                continue
            resolved = self._resolve(name, namespace)
            if resolved is None:
                continue
            callee, callee_namespace = resolved
            # The first helper in the chain is the one worth naming in the message.
            credit = attribute_to or name
            for status in _raised_statuses(callee):
                found.setdefault(status, credit)
            self._walk(callee, callee_namespace, depth + 1, seen | {name}, credit, found)


def _route_decorators(handler: FunctionNode) -> list[ast.Call]:
    return [
        decorator
        for decorator in handler.decorator_list
        if isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Attribute)
        and decorator.func.attr in ROUTE_METHODS
    ]


def check_file(file_path: Path) -> list[Violation]:
    """Return a :class:`Violation` for each status a route raises but never declares."""
    try:
        tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []

    routers = _router_statuses(tree)
    call_graph = _CallGraph(tree, _ModuleIndex(_source_root(file_path)))
    violations: list[Violation] = []
    for handler in ast.walk(tree):
        if not isinstance(handler, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in _route_decorators(handler):
            declared = _declared_statuses(decorator)
            # A route inherits whatever its router declares for every route on it.
            router = decorator.func.value if isinstance(decorator.func, ast.Attribute) else None
            inherited = routers.get(router.id, set()) if isinstance(router, ast.Name) else set()
            if declared is None or inherited is None:
                continue
            documented = declared | inherited | ALWAYS_DOCUMENTED
            found = [
                Violation(handler.name, status, lineno, None)
                for status, lineno in _raised_statuses(handler).items()
                if status not in documented
            ]
            found += [
                Violation(handler.name, status, handler.lineno, helper)
                for status, helper in call_graph.statuses_via_helpers(handler).items()
                if status not in documented
            ]
            violations.extend(sorted(found, key=lambda violation: violation.status))
    return violations


def main() -> int:
    parser = argparse.ArgumentParser(description="Check API routes declare the statuses they raise")
    parser.add_argument("files", nargs="*", help="Files to check")
    args = parser.parse_args()

    total = 0
    for file_path in (Path(f) for f in args.files):
        violations = check_file(file_path)
        if not violations:
            continue
        total += len(violations)
        lines = [violation.describe() for violation in violations]
        if console:
            console.print(f"[red]{file_path}[/red]:")
            for line in lines:
                console.print(f"[yellow]{line}[/yellow]")
        else:
            print(f"{file_path}:")
            print("\n".join(lines))

    if total:
        message = (
            f"Found {total} HTTP status(es) raised by a route handler but missing from its "
            f"`responses=` block.\n"
            f"Add each one to `{DOC_HELPER}([...])` on the route decorator so the generated "
            "OpenAPI spec — and the clients generated from it — model the response the API "
            "really returns."
        )
        if console:
            console.print()
            console.print(f"[red]{message}[/red]")
        else:
            print()
            print(message)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
