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

import contextlib
import os
import time
import traceback

from sqlalchemy import event


@contextlib.contextmanager
def trace_queries(display_time=True, display_trace=True, display_sql=False, display_parameters=True):
    """
    Tracking SQL queries in a code block. The result is displayed directly on the screen - ``print``

    :param display_time: If True, displays the query execution time.
    :param display_trace: If True, displays the simplified (one-line) stack trace
    :param display_sql: If True, displays the SQL statements
    :param display_parameters: If True, display SQL statement parameters
    :return:
    """
    import airflow.settings

    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        conn.info.setdefault("query_start_time", []).append(time.monotonic())

    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
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
        if display_time:
            output_parts.append(f"{total:.5f}")

        if display_trace:
            output_parts.extend([f"{file_name}", f"{stack_info}"])

        if display_sql:
            sql_oneline = statement.replace("\n", " ")
            output_parts.append(f"{sql_oneline}")

        if display_parameters:
            output_parts.append(f"{parameters}")

        print(" | ".join(output_parts))

    event.listen(airflow.settings.engine, "before_cursor_execute", before_cursor_execute)
    event.listen(airflow.settings.engine, "after_cursor_execute", after_cursor_execute)
    try:
        yield
    finally:
        event.remove(airflow.settings.engine, "before_cursor_execute", before_cursor_execute)
        event.remove(airflow.settings.engine, "after_cursor_execute", after_cursor_execute)


class CountQueriesResult:
    def __init__(self):
        self.count = 0


class CountQueries:
    """
    Counts the number of queries sent to Airflow Database in a given context.

    Does not support multiple processes. When a new process is started in context, its queries will
    not be included.
    """

    def __init__(self):
        self.result = CountQueriesResult()

    def __enter__(self):
        import airflow.settings

        event.listen(airflow.settings.engine, "after_cursor_execute", self.after_cursor_execute)
        return self.result

    def __exit__(self, type_, value, traceback):
        import airflow.settings

        event.remove(airflow.settings.engine, "after_cursor_execute", self.after_cursor_execute)
        print(f"Count queries: {self.result.count}")

    def after_cursor_execute(self, *args, **kwargs):
        self.result.count += 1


count_queries = CountQueries  # pylint: disable=invalid-name

if __name__ == "__main__":

    # Example:
    def case():
        from unittest import mock

        from airflow.jobs.scheduler_job import DagFileProcessor
        import logging

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
