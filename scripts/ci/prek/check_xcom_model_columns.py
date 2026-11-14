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
# requires-python = ">=3.11"
# dependencies = ["rich>=13.6.0"]
# ///
"""
Reject ``XComModel.<column>`` used directly in SQLAlchemy queries instead of through ``xcom_entity``.

``XComModel`` maps the unfiltered union of ``xcom_v1`` and ``xcom_v2``. A column of it used
in a query built by ``XComModel.get_many`` adds that union as a second ``FROM`` (a cartesian
product), and used on its own it relies on the planner pushing the predicate into the union.
Read the columns from the entity of the query instead: ``xcom_entity(query).<column>``.

Providers also run on Airflow releases where ``XComModel`` is a plain table, so there only
code inside an ``if AIRFLOW_V_3_N_PLUS:`` body (N >= 4) is checked.

Append ``# xcom-model-column: allow`` to a line that deliberately names the class attribute,
e.g. a declaration that is rebound to the query's entity before use.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path
from typing import NamedTuple

from common_prek_utils import console

COLUMNS = frozenset(
    {
        "dag_id",
        "dag_result",
        "dag_run",
        "dag_run_id",
        "key",
        "map_index",
        "mapped_length",
        "run_id",
        "task",
        "task_id",
        "task_instance_id",
        "timestamp",
        "value",
    }
)
ALLOW_MARKER = "xcom-model-column: allow"
DEFINING_MODULE = Path("airflow-core/src/airflow/models/xcom.py")


class ErrorLocation(NamedTuple):
    line: int
    column: int
    attribute: str


NEW_XCOM_GUARD = re.compile(r"AIRFLOW_V_3_(\d+)_PLUS")
MINIMUM_GUARD_MINOR = 4


def is_new_xcom_guard(test: ast.expr) -> bool:
    if not isinstance(test, ast.Name):
        return False
    match = NEW_XCOM_GUARD.fullmatch(test.id)
    return match is not None and int(match.group(1)) >= MINIMUM_GUARD_MINOR


class ColumnVisitor(ast.NodeVisitor):
    def __init__(self, lines: list[str], *, only_guarded: bool) -> None:
        self.lines = lines
        self.only_guarded = only_guarded
        self.guard_depth = 0
        self.errors: list[ErrorLocation] = []

    def visit_If(self, node: ast.If) -> None:
        self.visit(node.test)
        guarded = is_new_xcom_guard(node.test)
        self.guard_depth += guarded
        for child in node.body:
            self.visit(child)
        self.guard_depth -= guarded
        for child in node.orelse:
            self.visit(child)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if (
            isinstance(node.value, ast.Name)
            and node.value.id == "XComModel"
            and node.attr in COLUMNS
            and (self.guard_depth or not self.only_guarded)
            and ALLOW_MARKER not in self.lines[node.lineno - 1]
        ):
            self.errors.append(ErrorLocation(node.lineno, node.col_offset + 1, node.attr))
        self.generic_visit(node)


def check_source(source: str, filename: str = "<unknown>") -> list[ErrorLocation]:
    visitor = ColumnVisitor(source.splitlines(), only_guarded="providers/" in Path(filename).as_posix())
    visitor.visit(ast.parse(source, filename=filename))
    return sorted(visitor.errors)


def main(filenames: list[str]) -> int:
    failed = False
    for filename in filenames:
        if Path(filename).as_posix().endswith(DEFINING_MODULE.as_posix()):
            continue
        try:
            errors = check_source(Path(filename).read_text(encoding="utf-8"), filename)
        except SyntaxError as syntax_error:
            console.print(f"[red]{filename}:{syntax_error.lineno}: could not parse: {syntax_error.msg}[/]")
            failed = True
            continue
        for error in errors:
            console.print(
                f"[red]{filename}:{error.line}:{error.column}: `XComModel.{error.attribute}` is used directly "
                "in a SQLAlchemy query; read it through `xcom_entity(query).<column>` instead.[/]"
            )
            failed = True
    return int(failed)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
