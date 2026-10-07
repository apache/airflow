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

import logging
import os
import re
import traceback
import warnings
from collections import Counter
from contextlib import contextmanager
from typing import TYPE_CHECKING, NamedTuple

from sqlalchemy import event
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.exc import SAWarning
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select
from sqlalchemy.sql.compiler import COLLECT_CARTESIAN_PRODUCTS, WARN_LINTING
from sqlalchemy.sql.expression import AliasedReturnsRows, TableClause

# Long import to not create a copy of the reference, but to refer to one place.
import airflow.settings

if TYPE_CHECKING:
    from collections.abc import Generator

    from sqlalchemy.orm import ORMExecuteState

log = logging.getLogger(__name__)


def assert_equal_ignore_multiple_spaces(first, second, msg=None):
    def _trim(s):
        return re.sub(r"\s+", " ", s.strip())

    first_trim = _trim(first)
    second_trim = _trim(second)
    msg = msg or f"{first_trim} != {second_trim}"
    assert first_trim == second_trim, msg


class QueriesTraceRecord(NamedTuple):
    """QueriesTraceRecord holds information about the query executed in the context."""

    module: str
    name: str
    lineno: int | None

    @classmethod
    def from_frame(cls, frame_summary: traceback.FrameSummary):
        return cls(
            module=frame_summary.filename.rsplit(os.sep, 1)[-1],
            name=frame_summary.name,
            lineno=frame_summary.lineno,
        )

    def __str__(self):
        return f"{self.module}:{self.name}:{self.lineno}"


class QueriesTraceInfo(NamedTuple):
    """QueriesTraceInfo holds information about the queries executed in the context."""

    traces: tuple[QueriesTraceRecord, ...]

    @classmethod
    def from_traceback(cls, trace: traceback.StackSummary) -> QueriesTraceInfo:
        records = [
            QueriesTraceRecord.from_frame(f)
            for f in trace
            if "sqlalchemy" not in f.filename
            and __file__ != f.filename
            and ("session.py" not in f.filename and f.name != "wrapper")
        ]
        return cls(traces=tuple(records))

    def module_level(self, module: str) -> int:
        stacklevel = 0
        for ix, record in enumerate(reversed(self.traces), start=1):
            if record.module == module:
                stacklevel = ix
        if stacklevel == 0:
            raise LookupError(f"Unable to find module {stacklevel} in traceback")
        return stacklevel


class CountQueries:
    """
    Counts the number of queries sent to Airflow Database in a given context.

    Does not support multiple processes. When a new process is started in context, its queries will
    not be included.
    """

    def __init__(
        self,
        *,
        stacklevel: int = 1,
        stacklevel_from_module: str | None = None,
        session: Session | None = None,
    ):
        self.result: Counter[str] = Counter()
        self.stacklevel = stacklevel
        self.stacklevel_from_module = stacklevel_from_module
        self.session = session

    def __enter__(self):
        if self.session:
            event.listen(self.session, "do_orm_execute", self.after_cursor_execute)
        else:
            event.listen(airflow.settings.engine, "after_cursor_execute", self.after_cursor_execute)
        return self.result

    def __exit__(self, type_, value, tb):
        if self.session:
            event.remove(self.session, "do_orm_execute", self.after_cursor_execute)
        else:
            event.remove(airflow.settings.engine, "after_cursor_execute", self.after_cursor_execute)
        log.debug("Queries count: %d", sum(self.result.values()))

    def after_cursor_execute(self, *args, **kwargs):
        stack = QueriesTraceInfo.from_traceback(traceback.extract_stack())
        if not self.stacklevel_from_module:
            stacklevel = self.stacklevel
        else:
            stacklevel = stack.module_level(self.stacklevel_from_module)

        stack_info = " > ".join(map(str, stack.traces[-stacklevel:]))
        self.result[stack_info] += 1


count_queries = CountQueries


@contextmanager
def assert_queries_count(
    expected_count: int,
    message_fmt: str | None = None,
    margin: int = 0,
    stacklevel: int = 5,
    stacklevel_from_module: str | None = None,
    session: Session | None = None,
):
    """
    Assert that the number of queries is as expected with the margin applied.

    The margin is helpful in case of complex cases where we do not want to change it every time we
    changed queries, but we want to catch cases where we spin out of control
    :param expected_count: expected number of queries
    :param message_fmt: message printed optionally if the number is exceeded
    :param margin: margin to add to expected number of calls
    :param stacklevel: limits the output stack trace to that numbers of frame
    :param stacklevel_from_module: Filter stack trace from specific module.
    """
    with count_queries(
        stacklevel=stacklevel, stacklevel_from_module=stacklevel_from_module, session=session
    ) as result:
        yield None

    count = sum(result.values())
    if count > expected_count + margin:
        message_fmt = (
            message_fmt
            or "The expected number of db queries is {expected_count} with extra margin: {margin}. "
            "The current number is {current_count}.\n\n"
            "Recorded query locations:"
        )
        message = message_fmt.format(current_count=count, expected_count=expected_count, margin=margin)

        for location, count in result.items():
            message += f"\n\t{location}:\t{count}"

        raise AssertionError(message)


@contextmanager
def capture_orm_selects(table: str) -> Generator[list[str], None, None]:
    """
    Collect the ORM ``SELECT`` statements issued against ``table`` while the context is active.

    Each statement is rendered with SQLAlchemy's default dialect and with its bound values inlined,
    so assertions about the shape of a query (``LIMIT 1``, ``OFFSET`` ...) read the same on every
    backend. The raw text seen by ``before_cursor_execute`` is not enough for ``LIMIT``: SQLite,
    Postgres and MySQL all emit a ``LIMIT`` of some sort for an offset-only query.

    The listener is attached to the ``Session`` class, so statements executed by sessions the code
    under test opens itself (for example inside an API request handler) are captured too.

    :param table: Name of the table the captured statements must select from.
    """
    statements: list[str] = []
    selects_from_table = re.compile(rf"\b(?:FROM|JOIN) {re.escape(table)}\b")

    def capture(orm_execute_state: ORMExecuteState) -> None:
        statement = orm_execute_state.statement
        if not isinstance(statement, Select):
            return
        if not selects_from_table.search(" ".join(str(statement).split())):
            return
        rendered = str(statement.compile(compile_kwargs={"literal_binds": True}))
        statements.append(" ".join(rendered.split()))

    event.listen(Session, "do_orm_execute", capture)
    try:
        yield statements
    finally:
        event.remove(Session, "do_orm_execute", capture)


def _unclone(element):
    """Follow a clone back to the element it was copied from, which is how the FROM linter keys elements."""
    while element._is_clone_of is not None:
        element = element._is_clone_of
    return element


@contextmanager
def assert_no_cartesian_products() -> Generator[list[Select], None, None]:
    """
    Fail if an ORM ``SELECT`` issued in the context joins FROM elements that nothing connects.

    A cartesian product returns wrong rows, or far too many, without raising on the backend a test happens
    to run on. Each captured statement is therefore compiled for SQLite, PostgreSQL and MySQL with
    SQLAlchemy's FROM linting enabled, because the join shape the ORM renders (and so what the linter can
    see) differs between dialects.

    The listener is attached to the ``Session`` class, so statements executed by sessions the code under
    test opens itself, such as inside an API request handler, are checked too.
    """
    statements: list[Select] = []

    def capture(orm_execute_state: ORMExecuteState) -> None:
        if isinstance(orm_execute_state.statement, Select):
            statements.append(orm_execute_state.statement)

    event.listen(Session, "do_orm_execute", capture)
    try:
        yield statements
    finally:
        event.remove(Session, "do_orm_execute", capture)

    assert statements, "No ORM SELECT was executed, so nothing was checked for cartesian products"
    problems: list[str] = []
    for statement in statements:
        for dialect in (sqlite.dialect(), postgresql.dialect(), mysql.dialect()):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always", SAWarning)
                compiled = statement.compile(
                    dialect=dialect, linting=COLLECT_CARTESIAN_PRODUCTS | WARN_LINTING
                )
            problems.extend(
                f"[{dialect.name}] {warning.message}"
                for warning in caught
                if issubclass(warning.category, SAWarning) and "cartesian product" in str(warning.message)
            )
            linter = compiled.from_linter
            if linter is None:
                continue
            joined = {
                _unclone(element)
                for edge in linter.edges
                for element in edge
                if isinstance(element, (AliasedReturnsRows, TableClause))
            }
            problems.extend(
                f"[{dialect.name}] a join condition refers to {element.name!r}, which is not in the FROM clause"
                for element in joined - set(linter.froms)
            )
    assert not problems, "Cartesian product in generated SQL:\n" + "\n".join(sorted(set(problems)))
