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
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.utils.state import DagRunState, State

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

TIMESTAMP_STR = "2024-09-30T12:00:00Z"
TIMESTAMP = timezone.parse(TIMESTAMP_STR)

RUN_PATCH_BODY = {
    "state": "running",
    "hostname": "test-hostname",
    "unixname": "test-user",
    "pid": 12345,
    "start_date": TIMESTAMP_STR,
}


@pytest.fixture
def old_ver_client(client):
    """Last released execution API before nullable DagRun.start_date (2026-04-06 bundle)."""
    client.headers["Airflow-API-Version"] = "2025-11-05"
    return client


class TestDagRunStartDateNullableBackwardCompat:
    """Test that older API versions get a non-null start_date fallback."""

    @pytest.fixture(autouse=True)
    def _freeze_time(self, time_machine):
        time_machine.move_to(TIMESTAMP_STR, tick=False)

    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_old_version_gets_run_after_when_start_date_is_null(
        self,
        old_ver_client,
        session,
        create_task_instance,
    ):
        ti = create_task_instance(
            task_id="test_start_date_nullable",
            state=State.QUEUED,
            dagrun_state=DagRunState.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.dag_run.start_date = None  # DagRun has not started yet
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        dag_run = response.json()["dag_run"]

        assert response.status_code == 200
        assert dag_run["start_date"] is not None
        assert dag_run["start_date"] == dag_run["run_after"]

    def test_head_version_allows_null_start_date(
        self,
        client,
        session,
        create_task_instance,
    ):
        ti = create_task_instance(
            task_id="test_start_date_null_head",
            state=State.QUEUED,
            dagrun_state=DagRunState.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.dag_run.start_date = None  # DagRun has not started yet
        session.commit()

        response = client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        dag_run = response.json()["dag_run"]

        assert response.status_code == 200
        assert dag_run["start_date"] is None

    def test_old_version_preserves_real_start_date(
        self,
        old_ver_client,
        session,
        create_task_instance,
    ):
        ti = create_task_instance(
            task_id="test_start_date_preserved",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=TIMESTAMP,
        )
        assert ti.dag_run.start_date == TIMESTAMP  # DagRun has already started
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        dag_run = response.json()["dag_run"]

        assert response.status_code == 200
        assert dag_run["start_date"] is not None, "start_date should not be None when DagRun has started"
        assert dag_run["start_date"] == TIMESTAMP.isoformat().replace("+00:00", "Z")


class TestNextKwargsBackwardCompat:
    """Old workers only know BaseSerialization.deserialize -- SDK serde plain dicts cause KeyError."""

    @pytest.fixture(autouse=True)
    def _freeze_time(self, time_machine):
        time_machine.move_to(TIMESTAMP_STR, tick=False)

    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_old_version_gets_base_serialization_format(self, old_ver_client, session, create_task_instance):
        """Old API version receives next_kwargs wrapped in __type/__var so BaseSerialization can parse it."""
        ti = create_task_instance(
            task_id="test_next_kwargs_compat",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        # Store SDK serde format (plain dict) in DB -- this is what trigger.py handle_event_submit produces
        ti.next_method = "execute_complete"
        ti.next_kwargs = {"cheesecake": True, "event": "payload"}
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        next_kwargs = response.json()["next_kwargs"]
        # Old workers call BaseSerialization.deserialize on this -- verify it works
        result = BaseSerialization.deserialize(next_kwargs)
        assert result == {"cheesecake": True, "event": "payload"}

    def test_old_version_deserializes_complex_types(self, old_ver_client, session, create_task_instance):
        """Non-primitive values (datetime) must round-trip through serde -> BaseSerialization correctly."""
        from airflow.sdk.serde import serialize as serde_serialize

        original = {"event": TIMESTAMP, "simple": True}
        # Store SDK serde format with a datetime -- this is what handle_event_submit produces
        # when the trigger payload contains a datetime (e.g. DateTimeSensorAsync)
        serde_encoded = serde_serialize(original)

        ti = create_task_instance(
            task_id="test_next_kwargs_datetime",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        ti.next_kwargs = serde_encoded
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        next_kwargs = response.json()["next_kwargs"]
        result = BaseSerialization.deserialize(next_kwargs)
        assert result["simple"] is True
        # datetime must come back as a datetime, not a {"__classname__": ...} dict
        assert result["event"] == TIMESTAMP

    def test_old_version_handles_already_base_serialization_in_db(
        self, old_ver_client, session, create_task_instance
    ):
        """Rolling upgrade: DB still has BaseSerialization format from old handle_event_submit."""
        ti = create_task_instance(
            task_id="test_next_kwargs_already_base",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        # Pre-upgrade data: BaseSerialization format already in DB
        ti.next_kwargs = BaseSerialization.serialize({"cheesecake": True, "event": "payload"})
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        next_kwargs = response.json()["next_kwargs"]
        # Should still be parseable by old workers
        result = BaseSerialization.deserialize(next_kwargs)
        assert result == {"cheesecake": True, "event": "payload"}

    def test_old_version_handles_submit_failure_plain_dict(
        self, old_ver_client, session, create_task_instance
    ):
        """submit_failure and scheduler timeout write raw plain dicts -- converter must handle those too."""
        ti = create_task_instance(
            task_id="test_next_kwargs_failure",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "__fail__"
        # This is what submit_failure / scheduler timeout writes -- plain dict, no wrapping
        ti.next_kwargs = {"error": "Trigger timeout"}
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        next_kwargs = response.json()["next_kwargs"]
        result = BaseSerialization.deserialize(next_kwargs)
        assert result == {"error": "Trigger timeout"}

    def test_head_version_returns_raw_serde_format(self, client, session, create_task_instance):
        """Head API version returns next_kwargs as-is (SDK serde format)."""
        ti = create_task_instance(
            task_id="test_next_kwargs_head",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.next_method = "execute_complete"
        ti.next_kwargs = {"cheesecake": True, "event": "payload"}
        session.commit()

        response = client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        # Head version gets the plain dict directly -- no BaseSerialization wrapping
        assert response.json()["next_kwargs"] == {"cheesecake": True, "event": "payload"}
