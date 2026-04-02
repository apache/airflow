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

from airflow._shared.timezones import timezone
from airflow.utils.state import DagRunState, State

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


@pytest.fixture
def ver_client(client):
    """Client configured to use API version 2025-09-23."""
    client.headers["Airflow-API-Version"] = "2025-09-23"
    return client


class TestTIRunStateV20250923:
    """Test that API version 2025-09-23 does NOT include triggering_user_name field."""

    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_ti_run_excludes_triggering_user_name(
        self,
        ver_client,
        session,
        create_task_instance,
        time_machine,
    ):
        """
        Test that the triggering_user_name field is NOT present in API version 2025-09-23.

        This field was added in version 2025-10-10, so older API clients should not
        receive it in the response.
        """
        instant_str = "2024-09-30T12:00:00Z"
        instant = timezone.parse(instant_str)
        time_machine.move_to(instant, tick=False)

        ti = create_task_instance(
            task_id="test_triggering_user_exclusion",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=instant,
        )
        session.commit()

        response = ver_client.patch(
            f"/execution/task-instances/{ti.id}/run",
            json={
                "state": "running",
                "hostname": "test-hostname",
                "unixname": "test-user",
                "pid": 12345,
                "start_date": instant_str,
            },
        )

        assert response.status_code == 200
        json_response = response.json()

        # Verify the dag_run is present
        assert "dag_run" in json_response
        dag_run = json_response["dag_run"]

        # The triggering_user_name field should NOT be present in this API version
        assert "triggering_user_name" not in dag_run, (
            "triggering_user_name should not be present in API version 2025-09-23"
        )

        # Verify other expected fields are still present
        assert dag_run["dag_id"] == ti.dag_id
        assert dag_run["run_id"] == "test"
        assert dag_run["state"] == "running"
        assert dag_run["conf"] == {}


class TestTIRunConfV20250923:
    """Test that API version 2025-09-23 converts NULL conf to empty dict."""

    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_ti_run_null_conf_converted_to_dict(
        self,
        ver_client,
        session,
        create_task_instance,
    ):
        """
        Test that NULL conf is converted to empty dict in API version 2025-09-23.

        In version 2025-10-10, the conf field became nullable to match database schema.
        Older API clients (2025-09-23 and earlier) should receive an empty dict instead
        of None for backward compatibility.
        """
        ti = create_task_instance(
            task_id="test_ti_run_null_conf_v2",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
        )
        # Set conf to NULL to simulate Airflow 2.x upgrade or offline migration
        ti.dag_run.conf = None
        session.commit()

        response = ver_client.patch(
            f"/execution/task-instances/{ti.id}/run",
            json={
                "state": "running",
                "pid": 100,
                "hostname": "test-hostname",
                "unixname": "test-user",
                "start_date": timezone.utcnow().isoformat(),
            },
        )

        assert response.status_code == 200
        json_response = response.json()

        assert "dag_run" in json_response
        dag_run = json_response["dag_run"]

        # In older API versions, None should be converted to empty dict
        assert dag_run["conf"] == {}, "NULL conf should be converted to empty dict in API version 2025-09-23"

        # Verify other expected fields
        assert dag_run["dag_id"] == ti.dag_id
        assert dag_run["run_id"] == "test"
