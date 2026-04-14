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

from airflow.models.connection_test import ConnectionTestRequest, ConnectionTestState

from tests_common.test_utils.db import clear_db_connection_tests

pytestmark = pytest.mark.db_test


@pytest.fixture
def old_ver_client(client):
    """Client configured to use API version before connection-tests endpoint was added."""
    client.headers["Airflow-API-Version"] = "2025-12-08"
    return client


class TestConnectionTestEndpointVersioning:
    """Test that the connection-tests endpoint didn't exist in older API versions."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        clear_db_connection_tests()
        yield
        clear_db_connection_tests()

    def test_old_version_returns_404(self, old_ver_client, session):
        """PATCH /connection-tests/{id} should not exist in older API versions."""
        ct = ConnectionTestRequest(conn_type="test_type", connection_id="test_conn")
        ct.state = ConnectionTestState.RUNNING
        session.add(ct)
        session.commit()

        response = old_ver_client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={"state": "success", "result_message": "ok"},
        )
        assert response.status_code == 404

    def test_head_version_works(self, client, session):
        """PATCH /connection-tests/{id} should work in the current API version."""
        ct = ConnectionTestRequest(conn_type="test_type", connection_id="test_conn")
        ct.state = ConnectionTestState.RUNNING
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={"state": "success", "result_message": "ok"},
        )
        assert response.status_code == 204
