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

from unittest import mock

import pytest

from airflow.models.connection import Connection
from airflow.models.connection_test import ConnectionTest, ConnectionTestState, run_connection_test

pytestmark = pytest.mark.db_test


class TestConnectionTestModel:
    def test_token_is_generated(self):
        ct = ConnectionTest(connection_id="test_conn")
        assert ct.token is not None
        assert len(ct.token) > 0

    def test_initial_state_is_pending(self):
        ct = ConnectionTest(connection_id="test_conn")
        assert ct.state == ConnectionTestState.PENDING

    def test_tokens_are_unique(self):
        ct1 = ConnectionTest(connection_id="test_conn")
        ct2 = ConnectionTest(connection_id="test_conn")
        assert ct1.token != ct2.token

    def test_repr(self):
        ct = ConnectionTest(connection_id="test_conn")
        r = repr(ct)
        assert "test_conn" in r
        assert "pending" in r


class TestRunConnectionTest:
    def test_successful_connection_test(self, session):
        """Worker function updates state to SUCCESS on successful test."""
        ct = ConnectionTest(connection_id="test_conn")
        session.add(ct)
        session.commit()
        ct_id = str(ct.id)

        with mock.patch.object(
            Connection, "get_connection_from_secrets", return_value=mock.MagicMock()
        ) as mock_get_conn:
            mock_get_conn.return_value.test_connection.return_value = (True, "Connection OK")
            run_connection_test(connection_id="test_conn", connection_test_id=ct_id)

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.state == ConnectionTestState.SUCCESS
        assert ct.result_message == "Connection OK"

    def test_failed_connection_test(self, session):
        """Worker function updates state to FAILED when test_connection returns False."""
        ct = ConnectionTest(connection_id="test_conn")
        session.add(ct)
        session.commit()
        ct_id = str(ct.id)

        with mock.patch.object(
            Connection, "get_connection_from_secrets", return_value=mock.MagicMock()
        ) as mock_get_conn:
            mock_get_conn.return_value.test_connection.return_value = (False, "Connection failed")
            run_connection_test(connection_id="test_conn", connection_test_id=ct_id)

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.state == ConnectionTestState.FAILED
        assert ct.result_message == "Connection failed"

    def test_exception_during_connection_test(self, session):
        """Worker function handles exceptions gracefully."""
        ct = ConnectionTest(connection_id="test_conn")
        session.add(ct)
        session.commit()
        ct_id = str(ct.id)

        with mock.patch.object(
            Connection,
            "get_connection_from_secrets",
            side_effect=Exception("Could not resolve host: db.example.com"),
        ):
            run_connection_test(connection_id="test_conn", connection_test_id=ct_id)

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.state == ConnectionTestState.FAILED
        assert "Could not resolve host" in ct.result_message
