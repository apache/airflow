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

``create_openapi_http_exception_doc(...)`` feeds the ``responses=`` block of a
route, which is what the generated OpenAPI spec — and every client built from
it — uses to model error responses. Nothing ties that list to the statuses the
handler actually raises, so the two drift apart silently and a client ends up
with no model for a response the API really returns. That drift has been fixed
by hand repeatedly (#67570, #67571, #70992, #71011); this hook catches it
instead.

A handler violates the rule when it raises ``HTTPException(<status>)`` in its
own body with a status neither its own ``responses=`` block nor its router's
declares. To stay free of false positives the check is deliberately
conservative and stays silent when it cannot see the whole picture:

* ``422`` is never required — FastAPI documents validation errors natively.
* ``401`` and ``403`` are never required. Routers contribute them wholesale via
  their auth dependencies, and the router that does so is often built in
  another module (``routes/public/__init__.py`` declares both for every public
  route), which a per-file check cannot see.
* A status that is not a resolvable constant (``status.HTTP_404_NOT_FOUND``,
  a bare ``HTTP_404_NOT_FOUND``, or a literal ``404``) is skipped.
* A handler whose ``responses=`` is not a ``create_openapi_http_exception_doc``
  call over a literal list is skipped entirely, as are handlers where any
  declared entry cannot be resolved.

Because only the handler's own body is inspected, statuses raised by a shared
dependency or a service helper are not required to be declared. The hook
therefore under-reports rather than over-reports.
"""

# /// script
# requires-python = ">=3.10,<3.11"
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

from common_prek_utils import console

ROUTE_METHODS = {"get", "post", "put", "patch", "delete", "head", "options"}
ROUTER_CLASSES = {"APIRouter", "AirflowRouter"}
DOC_HELPER = "create_openapi_http_exception_doc"
# 422 is added to every route by FastAPI itself; 401/403 come from the router's auth
# dependencies, declared once on a router this file-scoped check often cannot reach.
ALWAYS_DOCUMENTED = {401, 403, 422}

_STATUS_CONSTANT = re.compile(r"^HTTP_(\d{3})_")


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


def _raised_statuses(handler: ast.FunctionDef | ast.AsyncFunctionDef) -> dict[int, int]:
    """Map each status raised as ``HTTPException`` in the body to its first line."""
    raised: dict[int, int] = {}
    for node in ast.walk(handler):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)):
            continue
        if node.func.id != "HTTPException":
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


def _route_decorators(handler: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.Call]:
    return [
        decorator
        for decorator in handler.decorator_list
        if isinstance(decorator, ast.Call)
        and isinstance(decorator.func, ast.Attribute)
        and decorator.func.attr in ROUTE_METHODS
    ]


def check_file(file_path: Path) -> list[tuple[str, int, int]]:
    """Return ``(handler_name, status, line_number)`` for each undeclared status."""
    try:
        tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []

    routers = _router_statuses(tree)
    violations: list[tuple[str, int, int]] = []
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
            undeclared = {
                status: lineno
                for status, lineno in _raised_statuses(handler).items()
                if status not in declared | inherited | ALWAYS_DOCUMENTED
            }
            violations.extend((handler.name, status, lineno) for status, lineno in sorted(undeclared.items()))
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
        lines = [
            f"  Line {lineno}: {handler}() raises {status} but never declares it"
            for handler, status, lineno in violations
        ]
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
