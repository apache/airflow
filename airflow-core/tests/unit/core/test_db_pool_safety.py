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
import os
import weakref
import gc as gc_module

import pytest
from sqlalchemy.exc import OperationalError

from airflow import settings
from airflow.utils.session import create_session


@pytest.mark.backend("mysql")
class TestLocalTaskJobForkSafety:
    """
    Test fork safety for LocalTaskJobRunner with MySQL backend
    """
    def test_old_dispose_causes_parent_connection_loss(self):
        """
        BEFORE FIX: Demonstrates the problem
        Using dispose(close=False) in child causes parent connection to die
        """
        # WeakRef로 Pool GC 추적
        gc_callback_called = []

        # Airflow의 실제 engine 사용
        engine = settings.engine
        pool = engine.pool
        weak_pool = weakref.ref(pool, lambda ref: gc_callback_called.append(True))

        if register_at_fork := getattr(os, "register_at_fork", None):
            # https://docs.sqlalchemy.org/en/20/core/pooling.html#using-connection-pools-with-multiprocessing-or-os-fork
            def clean_in_fork():
                print("engine disposed")
                engine.dispose(close=False)

            # Won't work on Windows
            register_at_fork(after_in_child=clean_in_fork)

        with engine.connect() as conn:
            thread_id = conn.execute("SELECT CONNECTION_ID()").scalar()

        # Fork
        pid = os.fork()
        if pid == 0:  # Child
            try:
                gc_module.collect()
            finally:
                os._exit(0)

        # Parent
        os.waitpid(pid, 0)

        # Verify GC happened
        assert len(gc_callback_called) > 0, "Pool was garbage collected in child"

        # Verify connection is dead
        with pytest.raises(
            OperationalError,
            match="MySQL server has gone away|2006|2013"
        ):
            conn.execute("SELECT 1")

#     def test_new_engine_creation_preserves_parent_connection(self):
#         """
#         AFTER FIX: Demonstrates the solution
#         Creating new engine in child preserves parent connection
#         """
#         gc_callback_called = []
#
#         engine = settings.engine
#         pool = engine.pool
#         weak_pool = weakref.ref(pool, lambda ref: gc_callback_called.append(True))
#
#         with engine.connect() as conn:
#             thread_id = conn.execute("SELECT CONNECTION_ID()").scalar()
#
#             pid = os.fork()
#             if pid == 0:  # Child
#                 try:
#                     # NEW approach - create new engine
#                     from airflow.settings import configure_orm
#                     configure_orm()
#                     # Parent engine/pool은 건드리지 않음!
#                 finally:
#                     os._exit(0)
#
#             os.waitpid(pid, 0)
#
#             # Verify NO GC happened to parent pool
#             assert len(gc_callback_called) == 0, "Pool was NOT garbage collected"
#
#             # Verify connection is alive
#             result = conn.execute("SELECT 1").scalar()
#             assert result == 1
#
#             # Verify same MySQL thread_id
#             current_id = conn.execute("SELECT CONNECTION_ID()").scalar()
#             assert current_id == thread_id
#
#     @pytest.mark.parametrize("approach,expect_failure", [
#         ("old_dispose", True),
#         ("new_engine", False),
#     ])
#     def test_fork_approaches_comparison(self, approach, expect_failure):
#         """
#         Parameterized test comparing old vs new approach
#         Shows both behaviors in one test
#         """
#         engine = settings.engine
#
#         with engine.connect() as conn:
#             thread_id = conn.execute("SELECT CONNECTION_ID()").scalar()
#
#             pid = os.fork()
#             if pid == 0:  # Child
#                 try:
#                     if approach == "old_dispose":
#                         # OLD: causes parent connection to die
#                         engine.dispose(close=False)
#                         gc_module.collect()
#                     else:  # new_engine
#                         # NEW: preserves parent connection
#                         from airflow.settings import configure_orm
#                         configure_orm()
#                 finally:
#                     os._exit(0)
#
#             os.waitpid(pid, 0)
#
#             # Verify expected behavior
#             if expect_failure:
#                 with pytest.raises(OperationalError):
#                     conn.execute("SELECT 1")
#             else:
#                 result = conn.execute("SELECT 1").scalar()
#                 assert result == 1
#                 current_id = conn.execute("SELECT CONNECTION_ID()").scalar()
#                 assert current_id == thread_id
#
#
# @pytest.mark.backend("mysql")
# class TestMySQLProcessListVerification:
#     """
#     Verify connection persistence using MySQL's SHOW PROCESSLIST
#     """
#
#     def test_connection_survives_in_processlist_with_new_approach(self):
#         """
#         Verify parent connection persists in MySQL SHOW PROCESSLIST
#         after child fork with new approach
#         """
#         engine = settings.engine
#
#         with create_session() as session:
#             # Get connection ID
#             thread_id = session.execute("SELECT CONNECTION_ID()").scalar()
#
#             # Verify in processlist before fork
#             result = session.execute(
#                 "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = :tid",
#                 {"tid": thread_id}
#             ).scalar()
#             assert result == 1, "Connection not found in PROCESSLIST before fork"
#
#             # Fork with NEW approach
#             pid = os.fork()
#             if pid == 0:
#                 try:
#                     from airflow.settings import configure_orm
#                     configure_orm()
#                 finally:
#                     os._exit(0)
#
#             os.waitpid(pid, 0)
#
#             # Verify connection still in processlist after fork
#             result = session.execute(
#                 "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = :tid",
#                 {"tid": thread_id}
#             ).scalar()
#             assert result == 1, "Connection disappeared from SHOW PROCESSLIST"
#
#     def test_connection_disappears_from_processlist_with_old_approach(self):
#         """
#         Demonstrate that old approach causes connection to disappear
#         from SHOW PROCESSLIST
#         """
#         engine = settings.engine
#
#         with create_session() as session:
#             thread_id = session.execute("SELECT CONNECTION_ID()").scalar()
#
#             # Verify in processlist before fork
#             result = session.execute(
#                 "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = :tid",
#                 {"tid": thread_id}
#             ).scalar()
#             assert result == 1
#
#             # Fork with OLD approach
#             pid = os.fork()
#             if pid == 0:
#                 try:
#                     engine.dispose(close=False)
#                     gc_module.collect()
#                 finally:
#                     os._exit(0)
#
#             os.waitpid(pid, 0)
#
#             # Connection should be gone (old bug)
#             result = session.execute(
#                 "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = :tid",
#                 {"tid": thread_id}
#             ).scalar()
#             assert result == 0, "Connection should have disappeared (old bug)"
#
#
# @pytest.mark.backend("mysql")
# class TestFileDescriptorState:
#     """
#     Test file descriptor state after fork
#     """
#
#     def test_fd_remains_valid_with_new_approach(self):
#         """
#         Verify parent's file descriptor remains valid
#         """
#         import fcntl
#
#         engine = settings.engine
#
#         with engine.connect() as conn:
#             # Get the underlying socket file descriptor
#             fd = conn.connection.connection.fileno()
#
#             # Verify fd is valid
#             flags = fcntl.fcntl(fd, fcntl.F_GETFD)
#             assert flags >= 0, "FD should be valid before fork"
#
#             # Fork with NEW approach
#             pid = os.fork()
#             if pid == 0:
#                 try:
#                     from airflow.settings import configure_orm
#                     configure_orm()
#                 finally:
#                     os._exit(0)
#
#             os.waitpid(pid, 0)
#
#             # Verify fd is still valid in parent
#             flags = fcntl.fcntl(fd, fcntl.F_GETFD)
#             assert flags >= 0, "FD should still be valid after fork"
#
#             # Verify can still use connection
#             result = conn.execute("SELECT 1").scalar()
#             assert result == 1
#
#
# @pytest.mark.backend("postgres")
# class TestPostgreSQLBaseline:
#     """
#     PostgreSQL doesn't have this issue (baseline comparison)
#     """
#
#     def test_postgres_safe_with_old_approach(self):
#         """
#         Demonstrate that PostgreSQL is safe even with old approach
#         """
#         engine = settings.engine
#
#         with engine.connect() as conn:
#             # Query works before fork
#             result = conn.execute("SELECT 1").scalar()
#             assert result == 1
#
#             pid = os.fork()
#             if pid == 0:
#                 try:
#                     # Even with old approach, PostgreSQL is safe
#                     engine.dispose(close=False)
#                     gc_module.collect()
#                 finally:
#                     os._exit(0)
#
#             os.waitpid(pid, 0)
#
#             # PostgreSQL connection should still work
#             result = conn.execute("SELECT 1").scalar()
#             assert result == 1
#
#
# # Fixtures
#
# @pytest.fixture
# def clean_engine():
#     """
#     Ensure clean engine state for each test
#     """
#     from airflow.settings import configure_orm
#
#     # Setup
#     configure_orm()
#     engine = settings.engine
#
#     yield engine
#
#     # Teardown
#     engine.dispose()
#
#
# @pytest.fixture
# def isolated_fork_test():
#     """
#     Fixture for isolated fork testing
#     Returns helper functions for fork testing
#     """
#     children = []
#
#     def fork_and_wait(child_func):
#         """Fork, execute child_func, and wait"""
#         pid = os.fork()
#         if pid == 0:  # Child
#             try:
#                 child_func()
#             finally:
#                 os._exit(0)
#         else:  # Parent
#             children.append(pid)
#             os.waitpid(pid, 0)
#
#     yield fork_and_wait
#
#     # Cleanup: wait for any remaining children
#     for pid in children:
#         try:
#             os.waitpid(pid, os.WNOHANG)
#         except:
#             pass
#
#
# # Example usage with fixtures
#
# @pytest.mark.backend("mysql")
# def test_with_fixture(isolated_fork_test):
#     """
#     Example test using the isolated_fork_test fixture
#     """
#     engine = settings.engine
#
#     with engine.connect() as conn:
#         thread_id = conn.execute("SELECT CONNECTION_ID()").scalar()
#
#         def child_work():
#             from airflow.settings import configure_orm
#             configure_orm()
#
#         # Use fixture helper
#         isolated_fork_test(child_work)
#
#         # Verify parent connection still works
#         result = conn.execute("SELECT 1").scalar()
#         assert result == 1


# Markers for different test categories

pytestmark = [
    pytest.mark.db_test,  # Airflow의 DB 테스트 마커
]
