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

from airflow.utils import timezone
from airflow.utils.state import State

from tests_common.test_utils.db import clear_db_assets, clear_db_runs

pytestmark = pytest.mark.db_test


DEFAULT_START_DATE = timezone.parse("2024-10-31T11:00:00Z")
DEFAULT_END_DATE = timezone.parse("2024-10-31T12:00:00Z")


class TestTIUpdateState:
    def setup_method(self):
        clear_db_assets()
        clear_db_runs()

    def teardown_method(self):
        clear_db_assets()
        clear_db_runs()

    @pytest.mark.parametrize(
        ("state", "expected_status_code"),
        [
            (State.RUNNING, 204),
            (State.SUCCESS, 409),
            (State.QUEUED, 409),
            (State.FAILED, 409),
        ],
    )
    def test_ti_runtime_checks_success(
        self, client, session, create_task_instance, state, expected_status_code
    ):
        # Last version this endpoint exists in
        client.headers["Airflow-API-Version"] = "2025-03-19"

        ti = create_task_instance(
            task_id="test_ti_runtime_checks",
            state=state,
        )
        session.commit()

        response = client.post(
            f"/execution/task-instances/{ti.id}/runtime-checks",
            json={
                "inlets": [],
                "outlets": [],
            },
        )
        assert response.status_code == expected_status_code
        assert response.headers["airflow-api-version"] == "2025-03-19"

        session.expire_all()
