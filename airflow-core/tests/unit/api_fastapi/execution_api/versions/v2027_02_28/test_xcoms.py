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

pytestmark = pytest.mark.db_test


class TestXComKeysEndpointVersioning:
    """The POST xcoms/{dag_id}/{run_id}/{task_id} endpoint didn't exist before the 2027-02-28 API version."""

    def test_old_version_does_not_route_to_endpoint(self, client, create_task_instance):
        ti = create_task_instance()
        client.headers["Airflow-API-Version"] = "2026-10-30"

        response = client.post(
            f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}", json={"keys": ["return_value"]}
        )

        # Without the endpoint the path falls to ``set_xcom``, which rejects the empty key.
        assert response.status_code == 422
        assert response.json()["detail"][0]["loc"] == ["path", "key"]

    def test_head_version_routes_to_endpoint(self, client, create_task_instance):
        ti = create_task_instance()

        response = client.post(
            f"/execution/xcoms/{ti.dag_id}/{ti.run_id}/{ti.task_id}", json={"keys": ["return_value"]}
        )

        assert response.status_code == 200
        assert response.json() == [None]
