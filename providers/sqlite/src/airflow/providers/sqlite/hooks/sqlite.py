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

import sqlite3
import sys
import threading
from collections.abc import Generator
from contextlib import contextmanager
from urllib.parse import unquote

from airflow.providers.common.sql.hooks.sql import DbApiHook


def _run_signal_handlers() -> int:
    """
    SQLite progress handler that lets Python run pending signal handlers while a statement runs.

    ``sqlite3`` does not return to Python until a statement completes, so neither the ``SIGTERM`` handler
    (``on_kill``) nor the ``SIGALRM`` handler of ``execution_timeout`` can run before that. Python runs
    pending signal handlers when it calls this function. Returning 0 lets the statement continue; an
    exception raised by a signal handler aborts it.
    """
    return 0


class SqliteHook(DbApiHook):
    """Interact with SQLite."""

    conn_name_attr = "sqlite_conn_id"
    default_conn_name = "sqlite_default"
    conn_type = "sqlite"
    hook_name = "Sqlite"

    # Number of SQLite virtual machine instructions between two calls of the progress handler that lets
    # signal handlers run while a statement runs
    progress_handler_interval = 100_000

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._placeholder: str = "?"

    @contextmanager
    def _track_running_cursor(self, cur) -> Generator[None, None, None]:
        """
        Make ``cur`` the cursor that ``cancel_query`` cancels, and keep its statement interruptible.

        While the statement runs in the main thread, where Python runs signal handlers, the connection calls
        :func:`_run_signal_handlers` every ``progress_handler_interval`` SQLite instructions. An exception
        raised there by a signal handler (``AirflowTaskTimeout``) aborts the statement, but ``sqlite3``
        reports it through ``sys.unraisablehook`` and raises ``OperationalError("interrupted")`` instead, so
        the original exception is caught there and raised in its place.
        """
        conn = getattr(cur, "connection", None)
        if (
            not self.cancel_on_interrupt
            or self._running_cursor is cur
            or threading.current_thread() is not threading.main_thread()
            or not isinstance(conn, sqlite3.Connection)
        ):
            with super()._track_running_cursor(cur):
                yield
            return
        interruptions: list[BaseException] = []
        previous_unraisablehook = sys.unraisablehook

        def unraisablehook(unraisable) -> None:
            tb = unraisable.exc_traceback
            if tb is not None and tb.tb_frame.f_code is _run_signal_handlers.__code__:
                interruptions.append(unraisable.exc_value)
            else:
                previous_unraisablehook(unraisable)

        conn.set_progress_handler(_run_signal_handlers, self.progress_handler_interval)
        # Without callback tracebacks, sqlite3 discards the exception instead of reporting it. sqlite3 has no
        # getter for this module wide flag, so it is switched off again (its default) after the statement.
        sqlite3.enable_callback_tracebacks(True)
        sys.unraisablehook = unraisablehook
        try:
            with super()._track_running_cursor(cur):
                try:
                    yield
                except sqlite3.OperationalError:
                    if interruptions:
                        raise interruptions[0] from None
                    raise
        finally:
            sys.unraisablehook = previous_unraisablehook
            sqlite3.enable_callback_tracebacks(False)
            conn.set_progress_handler(None, 0)

    def cancel_query(self) -> bool:
        """Cancel the running statement through ``sqlite3.Connection.interrupt()``."""
        cur = self._running_cursor
        if cur is None:
            return super().cancel_query()
        self.log.info("Cancelling the running SQL statement through connection.interrupt()")
        cur.connection.interrupt()
        return True

    def get_conn(self) -> sqlite3.dbapi2.Connection:
        """Return SQLite connection object."""
        sqlalchemy_uri = self.get_uri()
        # The sqlite3 connection does not use the sqlite scheme.
        # See https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#uri-connections for details.
        sqlite_uri = sqlalchemy_uri.replace("sqlite:///", "file:")
        conn = sqlite3.connect(sqlite_uri, uri=True)
        return conn

    def get_uri(self) -> str:
        """Override DbApiHook get_uri method for get_sqlalchemy_engine()."""
        airflow_conn = self.get_connection(self.get_conn_id())
        airflow_uri = unquote(airflow_conn.get_uri())
        # For sqlite, there is no schema in the connection URI. So we need to drop the trailing slash.
        airflow_sqlite_uri = airflow_uri.replace("/?", "?")
        # The sqlite connection has one more slash for path specification.
        # See https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#connect-strings for details.
        sqlalchemy_uri = airflow_sqlite_uri.replace("sqlite://", "sqlite:///")
        return sqlalchemy_uri
