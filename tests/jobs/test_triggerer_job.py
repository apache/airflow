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

import asyncio
import datetime
import time
from threading import Thread

import pytest

from airflow.jobs.triggerer_job import TriggererJob, TriggerRunner
from airflow.models import DagModel, DagRun, TaskInstance, Trigger
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.triggers.base import TriggerEvent
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.triggers.testing import FailureTrigger, SuccessTrigger
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State, TaskInstanceState
from tests.test_utils.db import clear_db_dags, clear_db_runs


class TimeDeltaTrigger_(TimeDeltaTrigger):
    def __init__(self, delta, filename):
        super().__init__(delta=delta)
        self.filename = filename
        self.delta = delta

    async def run(self):
        with open(self.filename, "a") as f:
            f.write("hi\n")
        async for event in super().run():
            yield event

    def serialize(self):
        return (
            "tests.jobs.test_triggerer_job.TimeDeltaTrigger_",
            {"delta": self.delta, "filename": self.filename},
        )


@pytest.fixture(autouse=True)
def clean_database():
    """Fixture that cleans the database before and after every test."""
    clear_db_runs()
    clear_db_dags()
    yield  # Test runs here
    clear_db_dags()
    clear_db_runs()


@pytest.fixture
def session():
    """Fixture that provides a SQLAlchemy session"""
    with create_session() as session:
        yield session


def test_is_alive():
    """Checks the heartbeat logic"""
    # Current time
    triggerer_job = TriggererJob(None, heartrate=10, state=State.RUNNING)
    assert triggerer_job.is_alive()

    # Slightly old, but still fresh
    triggerer_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=20)
    assert triggerer_job.is_alive()

    # Old enough to fail
    triggerer_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=31)
    assert not triggerer_job.is_alive()

    # Completed state should not be alive
    triggerer_job.state = State.SUCCESS
    triggerer_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=10)
    assert not triggerer_job.is_alive(), "Completed jobs even with recent heartbeat should not be alive"


def test_is_needed(session):
    """Checks the triggerer-is-needed logic"""
    # No triggers, no need
    triggerer_job = TriggererJob(None, heartrate=10, state=State.RUNNING)
    assert triggerer_job.is_needed() is False
    # Add a trigger, it's needed
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)
    session.commit()
    assert triggerer_job.is_needed() is True


def test_capacity_decode():
    """
    Tests that TriggererJob correctly sets capacity to a valid value passed in as a CLI arg,
    handles invalid args, or sets it to a default value if no arg is passed.
    """
    # Positive cases
    variants = [
        42,
        None,
    ]
    for input_str in variants:
        job = TriggererJob(capacity=input_str)
        assert job.capacity == input_str or job.capacity == 1000

    # Negative cases
    variants = [
        "NAN",
        0.5,
        -42,
        4 / 2,  # Resolves to a float, in addition to being just plain weird
    ]
    for input_str in variants:
        with pytest.raises(ValueError):
            TriggererJob(capacity=input_str)


def test_trigger_lifecycle(session):
    """
    Checks that the triggerer will correctly see a new Trigger in the database
    and send it to the trigger runner, and then delete it when it vanishes.
    """
    # Use a trigger that will not fire for the lifetime of the test
    # (we want to avoid it firing and deleting itself)
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)
    session.commit()
    # Make a TriggererJob and have it retrieve DB tasks
    job = TriggererJob()
    job.load_triggers()
    # Make sure it turned up in TriggerRunner's queue
    assert [x for x, y in job.runner.to_create] == [1]
    # Now, start TriggerRunner up (and set it as a daemon thread during tests)
    job.runner.daemon = True
    job.runner.start()
    try:
        # Wait for up to 3 seconds for it to appear in the TriggerRunner's storage
        for _ in range(30):
            if job.runner.triggers:
                assert list(job.runner.triggers.keys()) == [1]
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never created trigger")
        # OK, now remove it from the DB
        session.delete(trigger_orm)
        session.commit()
        # Re-load the triggers
        job.load_triggers()
        # Wait for up to 3 seconds for it to vanish from the TriggerRunner's storage
        for _ in range(30):
            if not job.runner.triggers:
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never deleted trigger")
    finally:
        # We always have to stop the runner
        job.runner.stop = True


def test_trigger_create_race_condition_18392(session, tmp_path):
    """
    This verifies the resolution of race condition documented in github issue #18392.
    Triggers are queued for creation by TriggerJob.load_triggers.
    There was a race condition where multiple triggers would be created unnecessarily.
    What happens is the runner completes the trigger and purges from the "running" list.
    Then job.load_triggers is called and it looks like the trigger is not running but should,
    so it queues it again.

    The scenario is as follows:
        1. job.load_triggers (trigger now queued)
        2. runner.create_triggers (trigger now running)
        3. job.handle_events (trigger still appears running so state not updated in DB)
        4. runner.cleanup_finished_triggers (trigger completed at this point; trigger from "running" set)
        5. job.load_triggers (trigger not running, but also not purged from DB, so it is queued again)
        6. runner.create_triggers (trigger created again)

    This test verifies that under this scenario only one trigger is created.
    """
    path = tmp_path / "test_trigger_bad_respawn.txt"

    class TriggerRunner_(TriggerRunner):
        """We do some waiting for main thread looping"""

        async def wait_for_job_method_count(self, method, count):
            for _ in range(30):
                await asyncio.sleep(0.1)
                if getattr(self, f"{method}_count", 0) >= count:
                    break
            else:
                pytest.fail(f"did not observe count {count} in job method {method}")

        async def create_triggers(self):
            """
            On first run, wait for job.load_triggers to make sure they are queued
            """
            if getattr(self, "loop_count", 0) == 0:
                await self.wait_for_job_method_count("load_triggers", 1)
            await super().create_triggers()
            self.loop_count = getattr(self, "loop_count", 0) + 1

        async def cleanup_finished_triggers(self):
            """On loop 1, make sure that job.handle_events was already called"""
            if self.loop_count == 1:
                await self.wait_for_job_method_count("handle_events", 1)
            await super().cleanup_finished_triggers()

    class TriggererJob_(TriggererJob):
        """We do some waiting for runner thread looping (and track calls in job thread)"""

        def wait_for_runner_loop(self, runner_loop_count):
            for _ in range(30):
                time.sleep(0.1)
                if getattr(self.runner, "call_count", 0) >= runner_loop_count:
                    break
            else:
                pytest.fail("did not observe 2 loops in the runner thread")

        def load_triggers(self):
            """On second run, make sure that runner has called create_triggers in its second loop"""
            super().load_triggers()
            self.runner.load_triggers_count = getattr(self.runner, "load_triggers_count", 0) + 1
            if self.runner.load_triggers_count == 2:
                self.wait_for_runner_loop(runner_loop_count=2)

        def handle_events(self):
            super().handle_events()
            self.runner.handle_events_count = getattr(self.runner, "handle_events_count", 0) + 1

    trigger = TimeDeltaTrigger_(delta=datetime.timedelta(microseconds=1), filename=path.as_posix())
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)

    dag = DagModel(dag_id="test-dag")
    dag_run = DagRun(dag.dag_id, run_id="abc", run_type="none")
    ti = TaskInstance(PythonOperator(task_id="dummy-task", python_callable=print), run_id=dag_run.run_id)
    ti.dag_id = dag.dag_id
    ti.trigger_id = 1
    session.add(dag)
    session.add(dag_run)
    session.add(ti)

    session.commit()

    job = TriggererJob_()
    job.runner = TriggerRunner_()
    thread = Thread(target=job._execute)
    thread.start()
    try:
        for _ in range(40):
            time.sleep(0.1)
            # ready to evaluate after 2 loops
            if getattr(job.runner, "loop_count", 0) >= 2:
                break
        else:
            pytest.fail("did not observe 2 loops in the runner thread")
    finally:
        job.runner.stop = True
        job.runner.join()
        thread.join()
    instances = path.read_text().splitlines()
    assert len(instances) == 1


def test_trigger_from_dead_triggerer(session):
    """
    Checks that the triggerer will correctly claim a Trigger that is assigned to a
    triggerer that does not exist.
    """
    # Use a trigger that has an invalid triggerer_id
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    trigger_orm.triggerer_id = 999  # Non-existent triggerer
    session.add(trigger_orm)
    session.commit()
    # Make a TriggererJob and have it retrieve DB tasks
    job = TriggererJob()
    job.load_triggers()
    # Make sure it turned up in TriggerRunner's queue
    assert [x for x, y in job.runner.to_create] == [1]


def test_trigger_from_expired_triggerer(session):
    """
    Checks that the triggerer will correctly claim a Trigger that is assigned to a
    triggerer that has an expired heartbeat.
    """
    # Use a trigger assigned to the expired triggerer
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    trigger_orm.triggerer_id = 42
    session.add(trigger_orm)
    # Use a TriggererJob with an expired heartbeat
    triggerer_job_orm = TriggererJob()
    triggerer_job_orm.id = 42
    triggerer_job_orm.start_date = timezone.utcnow() - datetime.timedelta(hours=1)
    triggerer_job_orm.end_date = None
    triggerer_job_orm.latest_heartbeat = timezone.utcnow() - datetime.timedelta(hours=1)
    session.add(triggerer_job_orm)
    session.commit()
    # Make a TriggererJob and have it retrieve DB tasks
    job = TriggererJob()
    job.load_triggers()
    # Make sure it turned up in TriggerRunner's queue
    assert [x for x, y in job.runner.to_create] == [1]


def test_trigger_firing(session):
    """
    Checks that when a trigger fires, it correctly makes it into the
    event queue.
    """
    # Use a trigger that will immediately succeed
    trigger = SuccessTrigger()
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)
    session.commit()
    # Make a TriggererJob and have it retrieve DB tasks
    job = TriggererJob()
    job.load_triggers()
    # Now, start TriggerRunner up (and set it as a daemon thread during tests)
    job.runner.daemon = True
    job.runner.start()
    try:
        # Wait for up to 3 seconds for it to fire and appear in the event queue
        for _ in range(30):
            if job.runner.events:
                assert list(job.runner.events) == [(1, TriggerEvent(True))]
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never sent the trigger event out")
    finally:
        # We always have to stop the runner
        job.runner.stop = True


def test_trigger_failing(session):
    """
    Checks that when a trigger fails, it correctly makes it into the
    failure queue.
    """
    # Use a trigger that will immediately fail
    trigger = FailureTrigger()
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)
    session.commit()
    # Make a TriggererJob and have it retrieve DB tasks
    job = TriggererJob()
    job.load_triggers()
    # Now, start TriggerRunner up (and set it as a daemon thread during tests)
    job.runner.daemon = True
    job.runner.start()
    try:
        # Wait for up to 3 seconds for it to fire and appear in the event queue
        for _ in range(30):
            if job.runner.failed_triggers:
                assert len(job.runner.failed_triggers) == 1
                trigger_id, exc = list(job.runner.failed_triggers)[0]
                assert trigger_id == 1
                assert isinstance(exc, ValueError)
                assert exc.args[0] == "Deliberate trigger failure"
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never marked the trigger as failed")
    finally:
        # We always have to stop the runner
        job.runner.stop = True


def test_trigger_cleanup(session):
    """
    Checks that the triggerer will correctly clean up triggers that do not
    have any task instances depending on them.
    """
    # Use a trigger that will not fire for the lifetime of the test
    # (we want to avoid it firing and deleting itself)
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)
    session.commit()
    # Trigger the cleanup code
    Trigger.clean_unused(session=session)
    session.commit()
    # Make sure it's gone
    assert session.query(Trigger).count() == 0


def test_invalid_trigger(session, dag_maker):
    """
    Checks that the triggerer will correctly fail task instances that depend on
    triggers that can't even be loaded.
    """
    # Create a totally invalid trigger
    trigger_orm = Trigger(classpath="fake.classpath", kwargs={})
    trigger_orm.id = 1
    session.add(trigger_orm)
    session.commit()

    # Create the test DAG and task
    with dag_maker(dag_id="test_invalid_trigger", session=session):
        EmptyOperator(task_id="dummy1")

    dr = dag_maker.create_dagrun()
    task_instance = dr.task_instances[0]
    # Make a task instance based on that and tie it to the trigger
    task_instance.state = TaskInstanceState.DEFERRED
    task_instance.trigger_id = 1
    session.commit()

    # Make a TriggererJob and have it retrieve DB tasks
    job = TriggererJob()
    job.load_triggers()

    # Make sure it turned up in the failed queue
    assert len(job.runner.failed_triggers) == 1

    # Run the failed trigger handler
    job.handle_failed_triggers()

    # Make sure it marked the task instance as failed (which is actually the
    # scheduled state with a payload to make it fail)
    task_instance.refresh_from_db()
    assert task_instance.state == TaskInstanceState.SCHEDULED
    assert task_instance.next_method == "__fail__"
    assert task_instance.next_kwargs["error"] == "Trigger failure"
    assert task_instance.next_kwargs["traceback"][-1] == "ModuleNotFoundError: No module named 'fake'\n"
