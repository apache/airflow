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
from collections import Counter
from contextlib import contextmanager
from typing import TYPE_CHECKING, NamedTuple

from sqlalchemy import event

# Long import to not create a copy of the reference, but to refer to one place.
import airflow.settings

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

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
            event.listen(
                airflow.settings.engine, "after_cursor_execute", self.after_cursor_execute
            )
        return self.result

    def __exit__(self, type_, value, tb):
        if self.session:
            event.remove(self.session, "do_orm_execute", self.after_cursor_execute)
        else:
            event.remove(
                airflow.settings.engine, "after_cursor_execute", self.after_cursor_execute
            )
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
        stacklevel=stacklevel,
        stacklevel_from_module=stacklevel_from_module,
        session=session,
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
        message = message_fmt.format(
            current_count=count, expected_count=expected_count, margin=margin
        )

        for location, count in result.items():
            message += f"\n\t{location}:\t{count}"

        raise AssertionError(message)
