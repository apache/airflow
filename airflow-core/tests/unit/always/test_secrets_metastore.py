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

import pytest

from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_connections, clear_db_variables

pytestmark = pytest.mark.db_test


class TestMetastoreBackendSessionSafety:
    """MetastoreBackend must not corrupt the shared scoped session.

    Regression tests for https://github.com/apache/airflow/issues/62244.
    """

    def setup_method(self) -> None:
        clear_db_connections()
        clear_db_variables()

    def teardown_method(self) -> None:
        clear_db_connections()
        clear_db_variables()

    @pytest.mark.parametrize("conn_exists", [True, False], ids=["found", "not_found"])
    def test_get_connection_preserves_pending_session_objects(self, conn_exists):
        """get_connection must not remove unrelated pending objects from session.new."""
        if conn_exists:
            with create_session() as session:
                session.add(Connection(conn_id="target_conn", conn_type="mysql"))
                session.commit()

        with create_session() as session:
            # Simulate pending work from another function sharing the session
            pending = Connection(conn_id="pending_conn", conn_type="http")
            session.add(pending)

            # Same session passed to simulate shared scoped session behavior
            backend = MetastoreBackend()
            result = backend.get_connection("target_conn", session=session)

            if conn_exists:
                assert result is not None
                assert result.conn_id == "target_conn"
            else:
                assert result is None
            # The pending object must still be in session.new — expunge(conn) should only
            # detach the queried Connection, not wipe unrelated pending objects.
            assert pending in session.new

    @pytest.mark.parametrize("var_exists", [True, False], ids=["found", "not_found"])
    def test_get_variable_preserves_pending_session_objects(self, var_exists):
        """get_variable must not remove unrelated pending objects from session.new."""
        if var_exists:
            Variable.set(key="test_key", value="test_value")

        with create_session() as session:
            # Use any ORM model as the pending object to detect session corruption
            pending = Connection(conn_id="pending_conn", conn_type="http")
            session.add(pending)

            backend = MetastoreBackend()
            result = backend.get_variable("test_key", session=session)

            if var_exists:
                assert result == "test_value"
            else:
                assert result is None
            # The pending object must still be in session.new — expunge(var_value) should only
            # detach the queried Variable, not wipe unrelated pending objects.
            assert pending in session.new

    def test_get_connection_returns_detached_object(self):
        """Returned connection must be detached so callers can use it freely."""
        from sqlalchemy import inspect as sa_inspect

        with create_session() as session:
            session.add(Connection(conn_id="test_conn", conn_type="mysql", host="localhost"))
            session.commit()

        backend = MetastoreBackend()
        conn = backend.get_connection("test_conn")

        assert conn is not None
        # Object should be detached — not tracked by any session
        assert sa_inspect(conn).detached
        # Attributes should still be accessible
        assert conn.conn_id == "test_conn"
        assert conn.host == "localhost"
