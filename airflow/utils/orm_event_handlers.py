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

import logging
import os
import time
import traceback

from sqlalchemy import event, exc

from airflow.configuration import conf

log = logging.getLogger(__name__)


def setup_event_handlers(engine):
    """
    Setups event handlers.
    """

    idle_session_timeout = conf.getfloat("core", "sql_alchemy_idle_transaction_timeout", fallback=10)

    # pylint: disable=unused-argument, unused-variable
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    if engine.dialect.name == "sqlite":
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    # this ensures sanity in mysql when storing datetimes (not required for postgres)
    if engine.dialect.name == "mysql":
        @event.listens_for(engine, "connect")
        def set_mysql_timezone(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("SET time_zone = '+00:00'")
            cursor.close()

        is_mariadb = engine.dialect.server_version_info and "MariaDB" in engine.dialect.server_version_info
        if idle_session_timeout > 0:
            if is_mariadb:
                # https://mariadb.com/kb/en/mariadb-1032-release-notes/#variables
                if engine.dialect.server_version_info > (10, 3):
                    setting = "idle_write_transaction_timeout"
                else:
                    setting = "idle_readwrite_transaction_timeout"

                @event.listens_for(engine, "connect")
                def set_mysql_transaction_idle_timeout(dbapi_connection, connection_record):
                    cursor = dbapi_connection.cursor()
                    cursor.execute("SET @@SESSION.%s = %s", (setting, int(idle_session_timeout),))
                    cursor.close()
            else:
                # MySQL doesn't have the equivalent of postgres' idle_in_transaction_session_timeout -- the
                # best we can do is set the time we wait just set the timeout for any idle connection
                @event.listens_for(engine, "connect")
                def set_mysql_transaction_idle_timeout(dbapi_connection, connection_record):
                    cursor = dbapi_connection.cursor()
                    cursor.execute("SET @@SESSION.wait_timeout = %s", (int(idle_session_timeout),))
                    cursor.close()

    if engine.dialect.name == "postgresql":
        if idle_session_timeout > 0:
            # Ensure that active transactions don't hang around for ever (default) -- this ensures that a
            # run-away process can't hold a row level lock for too long.
            @event.listens_for(engine, "connect")
            def set_postgres_transaction_idle_timeout(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("SET SESSION idle_in_transaction_session_timeout = %s",
                               (int(idle_session_timeout * 1000),))
                cursor.close()

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                "Connection record belongs to pid {}, "
                "attempting to check out in pid {}".format(connection_record.info['pid'], pid)
            )
    if conf.getboolean('debug', 'sqlalchemy_stats', fallback=False):
        @event.listens_for(engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            conn.info.setdefault('query_start_time', []).append(time.time())

        @event.listens_for(engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            total = time.time() - conn.info['query_start_time'].pop()
            file_name = [
                f"'{f.name}':{f.filename}:{f.lineno}" for f
                in traceback.extract_stack() if 'sqlalchemy' not in f.filename][-1]
            stack = [f for f in traceback.extract_stack() if 'sqlalchemy' not in f.filename]
            stack_info = ">".join([f"{f.filename.rpartition('/')[-1]}:{f.name}" for f in stack][-3:])
            conn.info.setdefault('query_start_time', []).append(time.monotonic())
            log.info("@SQLALCHEMY %s |$ %s |$ %s |$  %s ",
                     total, file_name, stack_info, statement.replace("\n", " ")
                     )
    # pylint: enable=unused-argument, unused-variable
