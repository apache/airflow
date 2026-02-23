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

from airflow.models.connection_test import ConnectionTest, ConnectionTestState

pytestmark = pytest.mark.db_test


class TestPatchConnectionTest:
    def test_patch_updates_result(self, client, session):
        """PATCH sets the state and result fields."""
        ct = ConnectionTest(connection_id="test_conn")
        ct.state = ConnectionTestState.RUNNING
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={
                "state": "success",
                "result_status": True,
                "result_message": "Connection successfully tested",
            },
        )
        assert response.status_code == 204

        session.expire_all()
        ct = session.get(ConnectionTest, ct.id)
        assert ct.state == "success"
        assert ct.result_status is True
        assert ct.result_message == "Connection successfully tested"

    def test_patch_returns_404_for_nonexistent(self, client):
        """PATCH with unknown id returns 404."""
        response = client.patch(
            "/execution/connection-tests/00000000-0000-0000-0000-000000000000",
            json={"state": "success", "result_status": True, "result_message": "ok"},
        )
        assert response.status_code == 404

    def test_patch_returns_422_for_invalid_uuid(self, client):
        """PATCH with invalid uuid returns 422."""
        response = client.patch(
            "/execution/connection-tests/not-a-uuid",
            json={"state": "success", "result_status": True, "result_message": "ok"},
        )
        assert response.status_code == 422

    def test_patch_returns_409_for_terminal_state(self, client, session):
        """PATCH on a test already in terminal state returns 409."""
        ct = ConnectionTest(connection_id="test_conn")
        ct.state = ConnectionTestState.SUCCESS
        ct.result_status = True
        ct.result_message = "Already done"
        session.add(ct)
        session.commit()

        response = client.patch(
            f"/execution/connection-tests/{ct.id}",
            json={"state": "failed", "result_status": False, "result_message": "retry"},
        )
        assert response.status_code == 409
        assert "terminal state" in response.json()["detail"]["message"]
