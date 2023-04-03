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

import logging
import os
import sqlalchemy.orm.mapper
import time
import traceback

import sqlalchemy.orm.mapper
from airflow.utils.module_loading import import_string

from airflow.configuration import conf
from sqlalchemy import event, exc

log = logging.getLogger(__name__)


def setup_event_handlers(engine):
    """Setups event handlers."""
    from airflow.models import import_all_models

    event.listen(sqlalchemy.orm.mapper, "before_configured", import_all_models, once=True)

    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info["pid"] = os.getpid()

    if engine.dialect.name == "sqlite":
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
    else:
        @event.listens_for(engine, "do_connect")
        def customize_connection(dialect, conn_rec, cargs, cparams):
            sql_alchemy_amend_connection_method_path = conf.get("database", "amend_connection_method_path")
            if sql_alchemy_amend_connection_method_path:
                sql_alchemy_amend_connection_method = import_string(sql_alchemy_amend_connection_method_path)
                sql_alchemy_amend_connection_method(dialect, conn_rec, cargs, cparams)

    # this ensures coherence in mysql when storing datetimes (not required for postgres)
    if engine.dialect.name == "mysql":
        @event.listens_for(engine, "connect")
        def set_mysql_timezone(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("SET time_zone = '+00:00'")
            cursor.close()

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info["pid"] != pid:
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                f"Connection record belongs to pid {connection_record.info['pid']}, "
                f"attempting to check out in pid {pid}"
            )

    if conf.getboolean("debug", "sqlalchemy_stats", fallback=False):
        @event.listens_for(engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            conn.info.setdefault("query_start_time", []).append(time.perf_counter())

        @event.listens_for(engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            total = time.perf_counter() - conn.info["query_start_time"].pop()
            file_name = [
                f"'{f.name}':{f.filename}:{f.lineno}"
                for f in traceback.extract_stack()
                if "sqlalchemy" not in f.filename
            ][-1]
            stack = [f for f in traceback.extract_stack() if "sqlalchemy" not in f.filename]
            stack_info = ">".join([f"{f.filename.rpartition('/')[-1]}:{f.name}" for f in stack][-3:])
            conn.info.setdefault("query_start_time", []).append(time.monotonic())
            log.info(
                "@SQLALCHEMY %s |$ %s |$ %s |$  %s ",
                total,
                file_name,
                stack_info,
                statement.replace("\n", " "),
            )
