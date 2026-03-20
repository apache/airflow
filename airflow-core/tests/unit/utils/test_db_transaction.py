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
from sqlalchemy import select

from airflow import settings
from airflow.models.connection import Connection
from airflow.models.pool import Pool
from airflow.utils.db import add_default_pool_if_not_exists, merge_conn

pytestmark = pytest.mark.db_test


class TestDbTransaction:
    def setup_method(self):
        from tests_common.test_utils.db import clear_db_connections, clear_db_pools

        clear_db_connections()
        clear_db_pools()

    def teardown_method(self):
        from tests_common.test_utils.db import clear_db_connections, clear_db_pools

        clear_db_connections()
        clear_db_pools()

    def test_merge_conn_provide_session(self):
        """Test that merge_conn commits when @provide_session handles the session."""
        conn_id = "test_conn"
        conn = Connection(conn_id=conn_id, conn_type="http")

        # Call without providing session - @provide_session will create one and commit it on exit
        merge_conn(conn)

        with settings.Session() as session:
            stored_conn = session.scalar(select(Connection).where(Connection.conn_id == conn_id))
            assert stored_conn is not None
            assert stored_conn.conn_id == conn_id

    def test_merge_conn_external_session(self, mocker):
        """Test that merge_conn flushes but does not commit when an external session is provided."""
        conn_id = "test_conn_ext"
        conn = Connection(conn_id=conn_id, conn_type="http")

        # Mock session to verify commit is NOT called
        session = mocker.Mock()
        # Mock scalar to return None so it proceeds to add
        session.scalar.return_value = None

        merge_conn(conn, session=session)

        session.add.assert_called_once_with(conn)
        session.flush.assert_called_once()
        session.commit.assert_not_called()

    def test_add_default_pool_provide_session(self):
        """Test that add_default_pool_if_not_exists commits with @provide_session."""
        # Ensure no default pool
        with settings.Session() as session:
            session.execute(Pool.__table__.delete().where(Pool.pool == Pool.DEFAULT_POOL_NAME))
            session.commit()

        # Call without session
        add_default_pool_if_not_exists()

        # Verify
        with settings.Session() as session:
            pool = session.scalar(select(Pool).where(Pool.pool == Pool.DEFAULT_POOL_NAME))
            assert pool is not None

    def test_add_default_pool_external_session(self, mocker):
        """Test that add_default_pool_if_not_exists flushes but does not commit with external session."""
        session = mocker.Mock()
        session.scalar.return_value = None

        add_default_pool_if_not_exists(session=session)

        session.add.assert_called_once()
        session.flush.assert_called_once()
        session.commit.assert_not_called()
