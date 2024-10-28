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
from __future__ import annotations

import ast
import enum
import itertools
import pathlib
import sys
import typing


class _SessionArgument(typing.NamedTuple):
    argument: ast.arg
    default: ast.expr | None


def _get_session_arg_and_default(args: ast.arguments) -> _SessionArgument | None:
    arguments = reversed([*args.args, *args.kwonlyargs])
    defaults = reversed([*args.defaults, *args.kw_defaults])
    for argument, default in itertools.zip_longest(arguments, defaults, fillvalue=None):
        if argument is None:
            continue  # Impossible since a function can't have more defaults than arguments.
        if argument.arg != "session":
            continue
        return _SessionArgument(argument, default)
    return None


class _SessionDefault(enum.Enum):
    none = "none"
    new_session = "new_session"


def _is_new_session_or_none(value: ast.expr) -> _SessionDefault | None:
    """Whether an expression is NEW_SESSION.

    Old code written before the introduction of NEW_SESSION (and even some new
    if the contributor wasn't made aware of the addition) generally uses None
    as the default value, so we add that to the check as well.
    """
    if isinstance(value, ast.Constant) and value.value is None:
        return _SessionDefault.none
    if isinstance(value, ast.Name) and value.id == "NEW_SESSION":
        return _SessionDefault.new_session
    # It's possible to do FOO = NEW_SESSION and reference FOO to work around
    # this check, but let's rely on reviewers to catch this kind of shenanigans.
    return None


_ALLOWED_DECORATOR_NAMES = ("overload", "provide_session", "abstractmethod")


def _is_decorated_correctly(nodes: list[ast.expr]) -> bool:
    """Whether expected decorators are provided.

    Three decorators would allow NEW_SESSION usages:

    * ``@provide_session``: The canonical case.
    * ``@overload``: A typing overload and not something to actually execute.
    * ``@abstractmethod``: This will be overridden in a subclass anyway.
    """
    # This only accepts those decorators literally. Should be enough?
    return any(
        isinstance(node, ast.Name) and node.id in _ALLOWED_DECORATOR_NAMES
        for node in nodes
    )


def _annotation_has_none(value: ast.expr | None) -> bool:
    if value is None:
        return False
    if isinstance(value, ast.Constant) and value.value is None:
        return True
    if isinstance(value, ast.BinOp) and isinstance(value.op, ast.BitOr):  # Type union.
        return _annotation_has_none(value.left) or _annotation_has_none(value.right)
    return False


def _iter_incorrect_new_session_usages(
    path: pathlib.Path,
) -> typing.Iterator[ast.FunctionDef]:
    """Check NEW_SESSION usages outside functions decorated with provide_session."""
    for node in ast.walk(ast.parse(path.read_text("utf-8"), str(path))):
        if not isinstance(node, ast.FunctionDef):
            continue
        session = _get_session_arg_and_default(node.args)
        if session is None or session.default is None:
            continue  # No session argument or the argument has no default, we're good.
        if _is_decorated_correctly(node.decorator_list):
            continue  # Has @provide_session so the default is expected.
        default_kind = _is_new_session_or_none(session.default)
        if default_kind is None:
            continue  # Default value is not NEW_SESSION or None.
        if default_kind == _SessionDefault.none and _annotation_has_none(
            session.argument.annotation
        ):
            continue  # None is OK if the argument is explicitly typed as None.
        yield node


def main(argv: list[str]) -> int:
    paths = (pathlib.Path(filename) for filename in argv[1:])
    errors = [
        (path, error)
        for path in paths
        for error in _iter_incorrect_new_session_usages(path)
    ]
    if errors:
        print("Incorrect @provide_session and NEW_SESSION usages:", end="\n\n")
        for path, error in errors:
            print(f"{path}:{error.lineno}")
            print(f"\tdef {error.name}(...", end="\n\n")
        print(
            "Only function decorated with @provide_session should use 'session: Session = NEW_SESSION'."
        )
        print(
            "See: https://github.com/apache/airflow/blob/main/"
            "contributing-docs/creating_issues_and_pull_requests#database-session-handling"
        )
    return len(errors)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
