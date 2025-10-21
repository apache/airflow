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

import asyncio
import gc as gc_module
import os
import weakref
from contextlib import contextmanager

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

from airflow.settings import SQL_ALCHEMY_CONN, SQL_ALCHEMY_CONN_ASYNC

pytestmark = [
    pytest.mark.db_test,
]


# Common helpers
def create_test_engine():
    """Create a test engine with standard configuration."""
    return create_engine(
        SQL_ALCHEMY_CONN,
        pool_size=5,
        max_overflow=10,
        pool_recycle=1800,
        future=True,
    )


def create_test_async_engine():
    """Create a test async engine with standard configuration."""
    return create_async_engine(
        SQL_ALCHEMY_CONN_ASYNC,
        pool_size=5,
        max_overflow=10,
        pool_recycle=1800,
        future=True,
    )


def get_connection_id_query(dialect_name):
    return "SELECT CONNECTION_ID()" if dialect_name == "mysql" else "SELECT pg_backend_pid()"


def register_connection_finalizers(engine):
    """Register weakref finalizers to track connection cleanup."""

    @event.listens_for(engine, "connect")
    def set_mysql_timezone(dbapi_connection, connection_record):
        weakref.finalize(dbapi_connection, lambda: print(f"finalize dbapi_connection in {os.getpid()}"))
        weakref.finalize(connection_record, lambda: print(f"finalize connection_record in {os.getpid()}"))


@contextmanager
def fork_process():
    """
    Context manager for forking a process which rigger garbage collection manually
    """

    pid = os.fork()
    if pid == 0:
        # Child process
        try:
            gc_module.collect()
        finally:
            os._exit(0)

    # Parent process
    try:
        yield pid
    finally:
        os.waitpid(pid, 0)


class TestLocalTaskJobForkSafety:
    """
    Test fork safety for LocalTaskJobRunner with MySQL backend.

    These tests verify that database connections are properly handled
    when forking processes, ensuring that parent process connections
    remain valid after child process cleanup.
    """

    @pytest.mark.backend("mysql", "postgres")
    def test_dispose_breaks_parent_connection(self):
        """
        Test that dispose(close=False) in child process breaks parent connection.

        This test demonstrates the bug: when a child process calls
        engine.dispose(close=False), it invalidates the parent's connection
        pool, causing the parent to lose its database (only MYSQL) connection.

        Expected result:
        - Parent connection fails with OperationalError in MYSQL backend
        - Don't modify the parent process's database connections in Postgres backend
        """
        engine1 = create_test_engine()
        register_connection_finalizers(engine1)

        query = get_connection_id_query(engine1.dialect.name)

        if register_at_fork := getattr(os, "register_at_fork", None):
            register_at_fork(after_in_child=lambda: engine1.dispose(close=False))

        # Establish connection before fork
        with engine1.connect() as conn:
            before_cid = conn.execute(text(query)).scalar()
            before_engine_id = id(engine1)

        with fork_process():
            pass

        if engine1.dialect.name == "mysql":
            # Verify parent connection is broken
            with engine1.connect() as conn:
                with pytest.raises(OperationalError, match="Lost connection to server during query"):
                    conn.execute(text("SELECT 1"))
        else:
            with engine1.connect() as conn:
                after_cid = conn.execute(text(query)).scalar()
                after_engine_id = id(engine1)

            assert before_cid == after_cid
            assert before_engine_id == after_engine_id

    @pytest.mark.backend("mysql", "postgres")
    @pytest.mark.asyncio
    async def test_async_dispose_breaks_parent_connection(self):
        """
        Test that sync_engine.dispose(close=False) breaks async parent connection.

        Similar to the sync version, this demonstrates that calling dispose()
        on the underlying sync_engine in a child process breaks the parent's
        async database connection. It affects both MySQL and postgres.

        Expected result: Parent connection hangs and times out
        """
        async_engine1 = create_test_async_engine()

        if register_at_fork := getattr(os, "register_at_fork", None):
            register_at_fork(after_in_child=lambda: async_engine1.sync_engine.dispose(close=False))

        query = get_connection_id_query(async_engine1.sync_engine.dialect.name)

        async with async_engine1.connect() as conn:
            conn_id = await conn.scalar(text(query))
            print(f"Connection ID: {conn_id}")

        with fork_process():
            pass

        async with async_engine1.connect() as conn:
            with pytest.raises(asyncio.exceptions.TimeoutError):
                await asyncio.wait_for(conn.execute(text(query)), timeout=5)

    @pytest.mark.backend("mysql", "postgres")
    def test_parent_process_retains_same_connection_after_child_fork(self):
        """
        Test the parent process maintains its original MySQL / postgres connection after forking a child process.

        This test verifies that:
        1. The parent process keeps the same connection ID before and after fork
        2. The engine object identity remains unchanged in the parent process
        3. Forking a child process (which triggers garbage collection) does not affect the parent's DB state

        This ensures that the fork cleanup mechanism (register_at_fork) only affects child processes
        and does not inadvertently modify the parent process's database connections.
        """
        from sqlalchemy import text

        from airflow.settings import engine

        query = get_connection_id_query(engine.dialect.name)

        with engine.connect() as conn:
            before_cid = conn.execute(text(query)).scalar()
            before_engine_id = id(engine)

        with fork_process():
            pass

        with engine.connect() as conn:
            after_cid = conn.execute(text(query)).scalar()
            after_engine_id = id(engine)

        assert before_cid == after_cid
        assert before_engine_id == after_engine_id

    @pytest.mark.backend("mysql", "postgres")
    @pytest.mark.asyncio
    async def test_parent_process_retains_same_async_connection_after_child_fork(self):
        """
        Test the parent process maintains its original MySQL / POSTGRES connection after forking a child process.

        This test verifies that:
        1. The parent process keeps the same connection ID before and after fork
        2. The engine object identity remains unchanged in the parent process
        3. Forking a child process (which triggers garbage collection) does not affect the parent's DB state

        This ensures that the fork cleanup mechanism (register_at_fork) only affects child processes
        and does not inadvertently modify the parent process's database connections.
        """
        from sqlalchemy import text

        from airflow.settings import async_engine

        query = get_connection_id_query(async_engine.sync_engine.dialect.name)

        async with async_engine.connect() as conn:
            before_cid = await conn.scalar(text(query))
            before_engine_id = id(async_engine)

        with fork_process():
            pass

        async with async_engine.connect() as conn:
            after_cid = await conn.scalar(text(query))
            after_engine_id = id(async_engine)

        assert before_cid == after_cid
        assert before_engine_id == after_engine_id
