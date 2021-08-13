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

import datetime
import sys
import time

import pytest

from airflow import DAG
from airflow.jobs.triggerer_job import TriggererJob
from airflow.models import Trigger
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.triggers.base import TriggerEvent
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.triggers.testing import FailureTrigger, SuccessTrigger
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State, TaskInstanceState
from tests.test_utils.db import clear_db_runs


@pytest.fixture(autouse=True)
def clean_database():
    """Fixture that cleans the database before and after every test."""
    clear_db_runs()
    yield  # Test runs here
    clear_db_runs()


@pytest.fixture
def session():
    """Fixture that provides a SQLAlchemy session"""
    with create_session() as session:
        yield session


@pytest.mark.skipif(sys.version_info.minor <= 6 and sys.version_info.major <= 3, reason="No triggerer on 3.6")
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


@pytest.mark.skipif(sys.version_info.minor <= 6 and sys.version_info.major <= 3, reason="No triggerer on 3.6")
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
        assert job.capacity == input_str or 1000

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


@pytest.mark.skipif(sys.version_info.minor <= 6 and sys.version_info.major <= 3, reason="No triggerer on 3.6")
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


@pytest.mark.skipif(sys.version_info.minor <= 6 and sys.version_info.major <= 3, reason="No triggerer on 3.6")
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


@pytest.mark.skipif(sys.version_info.minor <= 6 and sys.version_info.major <= 3, reason="No triggerer on 3.6")
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


@pytest.mark.skipif(sys.version_info.minor <= 6 and sys.version_info.major <= 3, reason="No triggerer on 3.6")
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


@pytest.mark.skipif(sys.version_info.minor <= 6 and sys.version_info.major <= 3, reason="No triggerer on 3.6")
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
                assert list(job.runner.failed_triggers) == [1]
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never marked the trigger as failed")
    finally:
        # We always have to stop the runner
        job.runner.stop = True


@pytest.mark.skipif(sys.version_info.minor <= 6 and sys.version_info.major <= 3, reason="No triggerer on 3.6")
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


@pytest.mark.skipif(sys.version_info.minor <= 6 and sys.version_info.major <= 3, reason="No triggerer on 3.6")
def test_invalid_trigger(session):
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
    with DAG(
        dag_id='test_invalid_trigger',
        start_date=timezone.datetime(2016, 1, 1),
        schedule_interval='@once',
        max_active_runs=1,
    ):
        task1 = DummyOperator(task_id='dummy1')

    # Make a task instance based on that and tie it to the trigger
    task_instance = TaskInstance(
        task1,
        execution_date=timezone.datetime(2016, 1, 1),
        state=TaskInstanceState.DEFERRED,
    )
    task_instance.trigger_id = 1
    session.add(task_instance)
    session.commit()

    # Make a TriggererJob and have it retrieve DB tasks
    job = TriggererJob()
    job.load_triggers()

    # Make sure it turned up in the failed queue
    assert list(job.runner.failed_triggers) == [1]

    # Run the failed trigger handler
    job.handle_failed_triggers()

    # Make sure it marked the task instance as failed (which is actually the
    # scheduled state with a payload to make it fail)
    task_instance.refresh_from_db()
    assert task_instance.state == TaskInstanceState.SCHEDULED
    assert task_instance.next_method == "__fail__"
    assert task_instance.next_kwargs == {'error': 'Trigger failure'}
