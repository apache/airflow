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
import os
import time
import traceback
from typing import Callable

from sqlalchemy import event


def _pretty_format_sql(text: str):
    import pygments
    from pygments.formatters.terminal import TerminalFormatter
    from pygments.lexers.sql import SqlLexer
    text = pygments.highlight(
        code=text, formatter=TerminalFormatter(), lexer=SqlLexer()
    ).rstrip()
    return text


# noinspection PyUnusedLocal
class TraceQueries:
    """
    Tracking SQL queries in a code block.

    :param display_num: If True, displays the query number.
    :param display_time: If True, displays the query execution time.
    :param display_trace: If True, displays the simplified (one-line) stack trace
    :param display_sql: If True, displays the SQL statements
    :param display_parameters: If True, display SQL statement parameters
    :param print_fn: The function used to display the text. By default,``builtins.print``
    """
    def __init__(
        self,
        *,
        display_num: bool = True,
        display_time: bool = True,
        display_trace: bool = True,
        display_sql: bool = False,
        display_parameters: bool = True,
        print_fn: Callable[[str], None] = print
    ):
        self.display_num = display_num
        self.display_time = display_time
        self.display_trace = display_trace
        self.display_sql = display_sql
        self.display_parameters = display_parameters
        self.print_fn = print_fn
        self.query_count = 0

    def before_cursor_execute(self,
                              conn,
                              cursor,  # pylint: disable=unused-argument
                              statement,  # pylint: disable=unused-argument
                              parameters,  # pylint: disable=unused-argument
                              context,  # pylint: disable=unused-argument
                              executemany):  # pylint: disable=unused-argument
        """
        Executed before cursor.

        :param conn:  connection
        :param cursor:  cursor
        :param statement: statement
        :param parameters: parameters
        :param context: context
        :param executemany: whether many statements executed
        :return:
        """

        conn.info.setdefault("query_start_time", []).append(time.monotonic())
        self.query_count += 1

    def after_cursor_execute(self,
                             conn,
                             cursor,  # pylint: disable=unused-argument
                             statement,
                             parameters,
                             context,  # pylint: disable=unused-argument
                             executemany):  # pylint: disable=unused-argument
        """
        Executed after cursor.

        :param conn:  connection
        :param cursor:  cursor
        :param statement: statement
        :param parameters: parameters
        :param context: context
        :param executemany: whether many statements executed
        :return:
        """
        total = time.monotonic() - conn.info["query_start_time"].pop()
        file_names = [
            f"{f.filename}:{f.name}:{f.lineno}"
            for f in traceback.extract_stack()
            if "sqlalchemy" not in f.filename
        ]
        file_name = file_names[-1] if file_names else ""
        stack = [f for f in traceback.extract_stack() if "sqlalchemy" not in f.filename]
        stack_info = " > ".join([f"{f.filename.rpartition('/')[-1]}:{f.name}:{f.lineno}" for f in stack][-7:])
        conn.info.setdefault("query_start_time", []).append(time.monotonic())

        output_parts = []
        if self.display_num:
            output_parts.append(f"{self.query_count:>3}")

        if self.display_time:
            output_parts.append(f"{total:.5f}")

        if self.display_trace:
            output_parts.extend([f"{file_name}", f"{stack_info}"])

        if self.display_sql:
            sql_oneline = statement.replace("\n", " ")
            output_parts.append(f"{_pretty_format_sql(sql_oneline)}")

        if self.display_parameters:
            output_parts.append(f"{parameters}")

        self.print_fn(" | ".join(output_parts))

    def __enter__(self):
        import airflow.settings

        event.listen(airflow.settings.engine, "before_cursor_execute", self.before_cursor_execute)
        event.listen(airflow.settings.engine, "after_cursor_execute", self.after_cursor_execute)

    # noinspection PyShadowingNames
    def __exit__(self, type_, value, traceback):  # pylint: disable=redefined-outer-name
        import airflow.settings
        event.remove(airflow.settings.engine, "before_cursor_execute", self.before_cursor_execute)
        event.remove(airflow.settings.engine, "after_cursor_execute", self.after_cursor_execute)


trace_queries = TraceQueries  # pylint: disable=invalid-name


class CountQueriesResult:
    """
    Counter for number of queries.
    """
    def __init__(self):
        self.count = 0


class CountQueries:
    """
    Counts the number of queries sent to Airflow Database in a given context.

    Does not support multiple processes. When a new process is started in context, its queries will
    not be included.

    :param print_fn: The function used to display the text. By default, ``builtins.print``
    """

    def __init__(self, print_fn: Callable[[str], None] = print):
        self.result = CountQueriesResult()
        self.print_fn = print_fn

    def __enter__(self):
        import airflow.settings

        event.listen(airflow.settings.engine, "after_cursor_execute", self.after_cursor_execute)
        return self.result

    # noinspection PyShadowingNames
    def __exit__(self, type_, value, traceback):  # pylint: disable=redefined-outer-name
        import airflow.settings

        event.remove(airflow.settings.engine, "after_cursor_execute", self.after_cursor_execute)
        self.print_fn(f"Count SQL queries: {self.result.count}")

    def after_cursor_execute(self,
                             conn,  # pylint: disable=unused-argument
                             cursor,  # pylint: disable=unused-argument
                             statement,  # pylint: disable=unused-argument
                             parameters,  # pylint: disable=unused-argument
                             context,  # pylint: disable=unused-argument
                             executemany):  # pylint: disable=unused-argument
        """
        Executed after cursor.

        :param conn:  connection
        :param cursor:  cursor
        :param statement: statement
        :param parameters: parameters
        :param context: context
        :param executemany: whether many statements executed
        """
        self.result.count += 1


count_queries = CountQueries  # pylint: disable=invalid-name

if __name__ == "__main__":

    # Example:
    def case():
        "Case of logging om/"
        import logging
        from unittest import mock

        from airflow.jobs.scheduler_job import DagFileProcessor

        with mock.patch.dict("os.environ", {
            "PERF_DAGS_COUNT": "200",
            "PERF_TASKS_COUNT": "10",
            "PERF_START_AGO": "2d",
            "PERF_SCHEDULE_INTERVAL": "None",
            "PERF_SHAPE": "no_structure",
        }):
            log = logging.getLogger(__name__)
            processor = DagFileProcessor(dag_ids=[], log=log)
            dag_file = os.path.join(os.path.dirname(__file__), os.path.pardir, "dags", "elastic_dag.py")
            processor.process_file(file_path=dag_file, failure_callback_requests=[])

    with trace_queries(), count_queries():
        case()
