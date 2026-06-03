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
from sqlalchemy import func, select

from airflow._shared.timezones import timezone
from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.models.taskinstance import TaskInstance
from airflow.models.trigger import Trigger
from airflow.sdk.serde import deserialize as serde_deserialize, serialize as serde_serialize
from airflow.serialization.serialized_objects import BaseSerialization
from airflow.utils.state import State

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test

HEALTH_CHECK_THRESHOLD = 30


def _clear():
    clear_db_runs()
    clear_db_serialized_dags()
    clear_db_dags()


def _make_triggerer(session) -> Job:
    """Create an alive TriggererJob so triggers can be assigned to it."""
    job = Job(heartrate=10, state=State.RUNNING, latest_heartbeat=timezone.utcnow())
    TriggererJobRunner(job)
    session.add(job)
    session.flush()
    return job


class TestLoadTriggers:
    def setup_method(self):
        _clear()

    def teardown_method(self):
        _clear()

    def test_load_assigns_and_returns_ids(self, client, session, create_task_instance):
        job = _make_triggerer(session)
        trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
        session.add(trigger)
        session.flush()
        # assign_unassigned only picks up triggers referenced by a task, asset, or callback.
        ti = create_task_instance(
            session=session,
            logical_date=timezone.utcnow(),
            state=State.DEFERRED,
        )
        ti.trigger_id = trigger.id
        session.commit()

        assert trigger.triggerer_id is None

        response = client.post(
            "/execution/triggers/load",
            json={
                "triggerer_id": job.id,
                "capacity": 100,
                "health_check_threshold": HEALTH_CHECK_THRESHOLD,
            },
        )

        assert response.status_code == 200
        assert response.json() == {"trigger_ids": [trigger.id]}

        # The trigger should now be assigned to the triggerer in the DB.
        session.expire_all()
        assert session.get(Trigger, trigger.id).triggerer_id == job.id

    def test_load_returns_empty_when_nothing_to_assign(self, client, session):
        job = _make_triggerer(session)
        session.commit()

        response = client.post(
            "/execution/triggers/load",
            json={
                "triggerer_id": job.id,
                "capacity": 100,
                "health_check_threshold": HEALTH_CHECK_THRESHOLD,
            },
        )

        assert response.status_code == 200
        assert response.json() == {"trigger_ids": []}


class TestTriggerWorkloads:
    def setup_method(self):
        _clear()

    def teardown_method(self):
        _clear()

    def test_workloads_for_deferred_ti(self, client, session, create_task_instance):
        trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
        session.add(trigger)
        session.flush()

        ti = create_task_instance(
            session=session,
            logical_date=timezone.utcnow(),
            state=State.DEFERRED,
        )
        ti.trigger_id = trigger.id
        session.commit()

        response = client.post(
            "/execution/triggers/workloads",
            json={"trigger_ids": [trigger.id]},
        )

        assert response.status_code == 200
        body = response.json()
        assert len(body["workloads"]) == 1
        workload = body["workloads"][0]
        assert workload["id"] == trigger.id
        assert workload["type"] == "RunTrigger"
        assert workload["classpath"] == "airflow.triggers.testing.SuccessTrigger"
        # Task-backed trigger carries the serialized task instance.
        assert workload["ti"] is not None
        assert workload["ti"]["task_id"] == ti.task_id

    def test_workloads_empty_for_unknown_id(self, client, session):
        response = client.post(
            "/execution/triggers/workloads",
            json={"trigger_ids": [123456]},
        )

        assert response.status_code == 200
        assert response.json() == {"workloads": []}


class TestSubmitEvent:
    def setup_method(self):
        _clear()

    def teardown_method(self):
        _clear()

    def test_event_resumes_deferred_ti(self, client, session, create_task_instance, mocker):
        trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
        session.add(trigger)
        session.flush()

        ti = create_task_instance(
            session=session,
            logical_date=timezone.utcnow(),
            state=State.DEFERRED,
        )
        ti.trigger_id = trigger.id
        session.commit()

        payload = serde_serialize({"result": "ok"})

        # The route must never run the unrestricted BaseSerialization.deserialize on the
        # worker-supplied body -- that would be an arbitrary import/instantiation gadget.
        # (BaseSerialization.deserialize is still used internally by SQLAlchemy column types,
        # so we spy as a passthrough and assert it is never handed the worker payload.)
        spy = mocker.spy(BaseSerialization, "deserialize")

        response = client.post(
            f"/execution/triggers/{trigger.id}/event",
            # The real client serde-serializes the payload before sending; the server stores it
            # without deserializing and the worker deserializes it on resume.
            json={"payload": payload},
        )

        assert response.status_code == 204
        for call in spy.call_args_list:
            assert payload not in call.args, "route deserialized the worker payload"

        session.expire_all()
        resumed = session.get(TaskInstance, ti.id)
        assert resumed.state == State.SCHEDULED
        assert resumed.trigger_id is None
        # The stored next_kwargs holds the serde-spliced payload; deserializing it (what the
        # worker does on resume) round-trips back to the original event.
        assert serde_deserialize(resumed.next_kwargs) == {"event": {"result": "ok"}}


class TestSubmitFailure:
    def setup_method(self):
        _clear()

    def teardown_method(self):
        _clear()

    def test_failure_marks_ti_to_fail(self, client, session, create_task_instance):
        trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
        session.add(trigger)
        session.flush()

        ti = create_task_instance(
            session=session,
            logical_date=timezone.utcnow(),
            state=State.DEFERRED,
        )
        ti.trigger_id = trigger.id
        session.commit()

        response = client.post(
            f"/execution/triggers/{trigger.id}/failure",
            # The traceback is a list of lines end-to-end (not a joined string), so the worker
            # renders it line-by-line rather than one character per line.
            json={"error": ["Traceback (most recent call last):\n", "  boom\n"]},
        )

        assert response.status_code == 204

        session.expire_all()
        failed = session.get(TaskInstance, ti.id)
        assert failed.state == State.SCHEDULED
        assert failed.next_method == "__fail__"
        assert failed.trigger_id is None
        assert failed.next_kwargs["traceback"] == ["Traceback (most recent call last):\n", "  boom\n"]


class TestCleanup:
    def setup_method(self):
        _clear()

    def teardown_method(self):
        _clear()

    def test_cleanup_deletes_orphan_trigger(self, client, session):
        trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
        session.add(trigger)
        session.commit()
        trigger_id = trigger.id

        assert session.scalar(select(func.count()).select_from(Trigger).where(Trigger.id == trigger_id)) == 1

        response = client.post("/execution/triggers/cleanup")

        assert response.status_code == 204

        session.expire_all()
        assert session.get(Trigger, trigger_id) is None
