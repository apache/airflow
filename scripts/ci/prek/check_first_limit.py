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
# dependencies = ["rich>=13.6.0"]
# ///
"""Require an explicit statement-level limit for core's single-row lookups.

Track straight-line local assignments, without inferring limits across helper
calls or control-flow joins. Put ``.limit(1)`` at the execution site when a
query's bound cannot be established locally.
"""

from __future__ import annotations

import ast
import sys
from enum import Enum, auto
from pathlib import Path
from typing import NamedTuple

from common_prek_utils import console


class QueryState(Enum):
    UNKNOWN = auto()
    LIMITED = auto()
    RESULT = auto()
    LIMITED_RESULT = auto()


class ErrorLocation(NamedTuple):
    line: int
    column: int


class FirstLimitVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.bindings: dict[str, QueryState] = {}
        self.errors: list[ErrorLocation] = []

    def get_state(self, node: ast.AST | None) -> QueryState:
        if isinstance(node, ast.Name):
            return self.bindings.get(node.id, QueryState.UNKNOWN)
        if isinstance(node, (ast.Await, ast.NamedExpr)):
            return self.get_state(node.value)
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            return QueryState.UNKNOWN
        method = node.func.attr
        receiver = self.get_state(node.func.value)
        if receiver in {QueryState.RESULT, QueryState.LIMITED_RESULT}:
            if method in {"scalars", "unique", "mappings", "tuples", "columns", "yield_per"}:
                return receiver
            return QueryState.RESULT
        if method in {"execute", "scalars", "stream", "stream_scalars"} and (
            node.args or any(keyword.arg == "statement" for keyword in node.keywords)
        ):
            statement = (
                node.args[0]
                if node.args
                else next(keyword.value for keyword in node.keywords if keyword.arg == "statement")
            )
            return (
                QueryState.LIMITED_RESULT
                if self.get_state(statement) == QueryState.LIMITED
                else QueryState.RESULT
            )
        if method == "limit":
            if (
                len(node.args) == 1
                and not node.keywords
                and isinstance(node.args[0], ast.Constant)
                and type(node.args[0].value) is int
                and node.args[0].value == 1
            ):
                return QueryState.LIMITED
            return QueryState.UNKNOWN
        if method in {
            "where",
            "filter",
            "filter_by",
            "order_by",
            "options",
            "with_only_columns",
            "offset",
            "join",
            "outerjoin",
            "select_from",
            "with_for_update",
            "execution_options",
            "distinct",
        }:
            return receiver
        return QueryState.UNKNOWN

    def visit_Call(self, node: ast.Call) -> None:
        self.generic_visit(node)
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "first"
            and self.get_state(node.func.value) not in {QueryState.LIMITED, QueryState.LIMITED_RESULT}
        ):
            self.errors.append(ErrorLocation(node.lineno, node.col_offset + 1))

    def visit_Assign(self, node: ast.Assign) -> None:
        self.visit(node.value)
        state = self.get_state(node.value)
        for target in node.targets:
            self.set_binding(target, state)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self.visit(node.value)
            self.set_binding(node.target, self.get_state(node.value))

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self.visit(node.value)
        self.set_binding(node.target, self.get_state(node.value))

    def set_binding(self, target: ast.AST, state: QueryState) -> None:
        if isinstance(target, ast.Name):
            self.bindings[target.id] = state
        else:
            self.invalidate_bindings(target)

    def invalidate_bindings(self, node: ast.AST) -> None:
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and isinstance(child.ctx, (ast.Store, ast.Del)):
                self.bindings.pop(child.id, None)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            self.bindings.pop(node.id, None)

    def generic_visit(self, node: ast.AST) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            previous = self.bindings
            self.bindings = {}
            super().generic_visit(node)
            self.bindings = previous
        elif isinstance(
            node,
            (
                ast.If,
                ast.For,
                ast.AsyncFor,
                ast.While,
                ast.Try,
                ast.Match,
                ast.ListComp,
                ast.SetComp,
                ast.DictComp,
                ast.GeneratorExp,
            ),
        ):
            self.invalidate_bindings(node)
            previous = self.bindings.copy()
            for field, value in ast.iter_fields(node):
                self.bindings = previous.copy()
                if isinstance(value, list):
                    for child in value:
                        if field in {"handlers", "cases"}:
                            self.bindings = previous.copy()
                        self.visit(child)
                elif isinstance(value, ast.AST):
                    self.visit(value)
            self.bindings = previous
        else:
            if isinstance(node, (ast.AugAssign, ast.Delete)):
                self.invalidate_bindings(node)
            super().generic_visit(node)


def check_source(source: str, filename: str = "<unknown>") -> list[ErrorLocation]:
    visitor = FirstLimitVisitor()
    visitor.visit(ast.parse(source, filename=filename))
    return sorted(visitor.errors)


def main(filenames: list[str]) -> int:
    failed = False
    for filename in filenames:
        try:
            errors = check_source(Path(filename).read_text(encoding="utf-8"), filename)
        except SyntaxError as error:
            console.print(f"{filename}:{error.lineno}: {error.msg}", markup=False, highlight=False)
            failed = True
            continue
        for location in errors:
            console.print(
                f"{filename}:{location.line}:{location.column}: .first() requires .limit(1) on the statement before execution.",
                markup=False,
                highlight=False,
            )
            failed = True
    return int(failed)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
