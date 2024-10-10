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
import importlib
import time
from threading import Thread
from unittest.mock import MagicMock, patch

import aiofiles
import pendulum
import pytest

from airflow.config_templates import airflow_local_settings
from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner, TriggerRunner, setup_queue_listener
from airflow.logging_config import configure_logging
from airflow.models import DagModel, DagRun, TaskInstance, Trigger
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.triggers.base import TriggerEvent
from airflow.triggers.temporal import DateTimeTrigger, TimeDeltaTrigger
from airflow.triggers.testing import FailureTrigger, SuccessTrigger
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import RedirectStdHandler
from airflow.utils.log.trigger_handler import LocalQueueHandler
from airflow.utils.session import create_session
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.types import DagRunType
from tests.core.test_logging_config import reset_logging

from dev.tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test


class TimeDeltaTrigger_(TimeDeltaTrigger):
    def __init__(self, delta, filename):
        super().__init__(delta=delta)
        self.filename = filename
        self.delta = delta

    async def run(self):
        async with aiofiles.open(self.filename, mode="a") as f:
            await f.write("hi\n")
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


def create_trigger_in_db(session, trigger, operator=None):
    dag_model = DagModel(dag_id="test_dag")
    dag = DAG(dag_id=dag_model.dag_id, schedule="@daily", start_date=pendulum.datetime(2023, 1, 1))
    run = DagRun(
        dag_id=dag_model.dag_id,
        run_id="test_run",
        execution_date=pendulum.datetime(2023, 1, 1),
        run_type=DagRunType.MANUAL,
    )
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    if operator:
        operator.dag = dag
    else:
        operator = BaseOperator(task_id="test_ti", dag=dag)
    task_instance = TaskInstance(operator, run_id=run.run_id)
    task_instance.trigger_id = trigger_orm.id
    session.add(dag_model)
    session.add(run)
    session.add(trigger_orm)
    session.add(task_instance)
    session.commit()
    return dag_model, run, trigger_orm, task_instance


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_trigger_logging_sensitive_info(session, caplog):
    """
    Checks that when a trigger fires, it doesn't log any sensitive
    information from arguments
    """

    class SensitiveArgOperator(BaseOperator):
        def __init__(self, password, **kwargs):
            self.password = password
            super().__init__(**kwargs)

    # Use a trigger that will immediately succeed
    trigger = SuccessTrigger()
    op = SensitiveArgOperator(task_id="sensitive_arg_task", password="some_password")
    create_trigger_in_db(session, trigger, operator=op)
    triggerer_job = Job()
    triggerer_job_runner = TriggererJobRunner(triggerer_job)
    triggerer_job_runner.load_triggers()
    # Now, start TriggerRunner up (and set it as a daemon thread during tests)
    triggerer_job_runner.daemon = True
    triggerer_job_runner.trigger_runner.start()
    try:
        # Wait for up to 3 seconds for it to fire and appear in the event queue
        for _ in range(30):
            if triggerer_job_runner.trigger_runner.events:
                assert list(triggerer_job_runner.trigger_runner.events) == [(1, TriggerEvent(True))]
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never sent the trigger event out")
    finally:
        # We always have to stop the runner
        triggerer_job_runner.trigger_runner.stop = True
        triggerer_job_runner.trigger_runner.join(30)

    # Since we have now an in-memory process of forwarding the logs to stdout,
    # give it more time for the trigger event to write the log.
    time.sleep(0.5)

    assert "test_dag/test_run/sensitive_arg_task/-1/0 (ID 1) starting" in caplog.text
    assert "some_password" not in caplog.text


def test_is_alive():
    """Checks the heartbeat logic"""
    # Current time
    triggerer_job = Job(heartrate=10, state=State.RUNNING)
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


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_is_needed(session):
    """Checks the triggerer-is-needed logic"""
    # No triggers, no need
    triggerer_job = Job(heartrate=10, state=State.RUNNING)
    triggerer_job_runner = TriggererJobRunner(triggerer_job)
    assert triggerer_job_runner.is_needed() is False
    # Add a trigger, it's needed
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)
    session.commit()
    assert triggerer_job_runner.is_needed() is True


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
        job = Job()
        job_runner = TriggererJobRunner(job, capacity=input_str)
        assert job_runner.capacity == input_str or job_runner.capacity == 1000

    # Negative cases
    variants = [
        "NAN",
        0.5,
        -42,
        4 / 2,  # Resolves to a float, in addition to being just plain weird
    ]
    for input_str in variants:
        job = Job()
        with pytest.raises(ValueError):
            TriggererJobRunner(job=job, capacity=input_str)


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_trigger_lifecycle(session):
    """
    Checks that the triggerer will correctly see a new Trigger in the database
    and send it to the trigger runner, and then delete it when it vanishes.
    """
    # Use a trigger that will not fire for the lifetime of the test
    # (we want to avoid it firing and deleting itself)
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    dag_model, run, trigger_orm, task_instance = create_trigger_in_db(session, trigger)
    # Make a TriggererJobRunner and have it retrieve DB tasks
    job = Job()
    job_runner = TriggererJobRunner(job)
    job_runner.load_triggers()
    # Make sure it turned up in TriggerRunner's queue
    assert [x for x, y in job_runner.trigger_runner.to_create] == [1]
    # Now, start TriggerRunner up (and set it as a daemon thread during tests)
    job_runner.daemon = True
    job_runner.trigger_runner.start()
    try:
        # Wait for up to 3 seconds for it to appear in the TriggerRunner's storage
        for _ in range(30):
            if job_runner.trigger_runner.triggers:
                assert list(job_runner.trigger_runner.triggers.keys()) == [1]
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never created trigger")
        # OK, now remove it from the DB
        session.delete(trigger_orm)
        session.commit()
        # Re-load the triggers
        job_runner.load_triggers()
        # Wait for up to 3 seconds for it to vanish from the TriggerRunner's storage
        for _ in range(30):
            if not job_runner.trigger_runner.triggers:
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never deleted trigger")
    finally:
        # We always have to stop the runner
        job_runner.trigger_runner.stop = True
        job_runner.trigger_runner.join(30)


class TestTriggerRunner:
    @pytest.mark.asyncio
    @patch("airflow.jobs.triggerer_job_runner.TriggerRunner.set_individual_trigger_logging")
    async def test_run_inline_trigger_canceled(self, session) -> None:
        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {1: {"task": MagicMock(), "name": "mock_name", "events": 0}}
        mock_trigger = MagicMock()
        mock_trigger.task_instance.trigger_timeout = None
        mock_trigger.run.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await trigger_runner.run_trigger(1, mock_trigger)

    @pytest.mark.asyncio
    @patch("airflow.jobs.triggerer_job_runner.TriggerRunner.set_individual_trigger_logging")
    async def test_run_inline_trigger_timeout(self, session, caplog) -> None:
        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {1: {"task": MagicMock(), "name": "mock_name", "events": 0}}
        mock_trigger = MagicMock()
        mock_trigger.task_instance.trigger_timeout = timezone.utcnow() - datetime.timedelta(hours=1)
        mock_trigger.run.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await trigger_runner.run_trigger(1, mock_trigger)
        assert "Trigger cancelled due to timeout" in caplog.text

    @patch("airflow.models.trigger.Trigger.bulk_fetch")
    @patch(
        "airflow.jobs.triggerer_job_runner.TriggerRunner.get_trigger_by_classpath",
        return_value=DateTimeTrigger,
    )
    def test_update_trigger_with_triggerer_argument_change(
        self, mock_bulk_fetch, mock_get_trigger_by_classpath, session, caplog
    ) -> None:
        trigger_runner = TriggerRunner()
        mock_trigger_orm = MagicMock()
        mock_trigger_orm.kwargs = {"moment": ..., "not_exists_arg": ...}
        mock_get_trigger_by_classpath.return_value = {1: mock_trigger_orm}

        trigger_runner.update_triggers({1})

        assert "Trigger failed" in caplog.text
        assert "got an unexpected keyword argument 'not_exists_arg'" in caplog.text


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
@pytest.mark.asyncio
async def test_trigger_create_race_condition_38599(session, tmp_path):
    """
    This verifies the resolution of race condition documented in github issue #38599.
    More details in the issue description.

    The race condition may occur in the following scenario:
        1. TaskInstance TI1 defers itself, which creates Trigger T1, which holds a
            reference to TI1.
        2. T1 gets picked up by TriggererJobRunner TJR1 and starts running T1.
        3. TJR1 misses a heartbeat, most likely due to high host load causing delays in
            each TriggererJobRunner._run_trigger_loop loop.
        4. A second TriggererJobRunner TJR2 notices that T1 has missed its heartbeat,
            so it starts the process of picking up any Triggers that TJR1 may have had,
            including T1.
        5. Before TJR2 starts executing T1, TJR1 finishes execution of T1 and cleans it
            up by clearing the trigger_id of TI1.
        6. TJR2 tries to execute T1, but it crashes (with the above error) while trying to
            look up TI1 (because T1 no longer has a TaskInstance linked to it).
    """
    path = tmp_path / "test_trigger_create_after_completion.txt"
    trigger = TimeDeltaTrigger_(delta=datetime.timedelta(microseconds=1), filename=path.as_posix())
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)

    dag = DagModel(dag_id="test-dag")
    dag_run = DagRun(dag.dag_id, run_id="abc", run_type="none")
    ti = TaskInstance(
        PythonOperator(task_id="dummy-task", python_callable=print),
        run_id=dag_run.run_id,
        state=TaskInstanceState.DEFERRED,
    )
    ti.dag_id = dag.dag_id
    ti.trigger_id = 1
    session.add(dag)
    session.add(dag_run)
    session.add(ti)

    job1 = Job()
    job2 = Job()
    session.add(job1)
    session.add(job2)

    session.commit()

    job_runner1 = TriggererJobRunner(job1)
    job_runner2 = TriggererJobRunner(job2)

    # Assign and run the trigger on the first TriggererJobRunner
    # Instead of running job_runner1._execute, we will run the individual methods
    # to control the timing of the execution.
    job_runner1.load_triggers()
    assert len(job_runner1.trigger_runner.to_create) == 1
    # Before calling job_runner1.handle_events, run the trigger synchronously
    await job_runner1.trigger_runner.create_triggers()
    assert len(job_runner1.trigger_runner.triggers) == 1
    _, trigger_task_info = next(iter(job_runner1.trigger_runner.triggers.items()))
    await trigger_task_info["task"]
    assert trigger_task_info["task"].done()

    # In a real execution environment, a missed heartbeat would cause the trigger to be picked up
    # by another TriggererJobRunner.
    # In this test, however, this is not necessary because we are controlling the execution
    # of the TriggererJobRunner.
    # job1.latest_heartbeat = timezone.utcnow() - datetime.timedelta(hours=1)
    # session.commit()

    # This calls Trigger.submit_event, which will unlink the trigger from the task instance
    job_runner1.handle_events()

    # Simulate the second TriggererJobRunner picking up the trigger
    job_runner2.trigger_runner.update_triggers({trigger_orm.id})
    # The race condition happens here.
    # AttributeError: 'NoneType' object has no attribute 'dag_id'
    await job_runner2.trigger_runner.create_triggers()

    assert path.read_text() == "hi\n"


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
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

    class TriggererJob_(TriggererJobRunner):
        """We do some waiting for runner thread looping (and track calls in job thread)"""

        def wait_for_runner_loop(self, runner_loop_count):
            for _ in range(30):
                time.sleep(0.1)
                if getattr(self.trigger_runner, "call_count", 0) >= runner_loop_count:
                    break
            else:
                pytest.fail("did not observe 2 loops in the runner thread")

        def load_triggers(self):
            """On second run, make sure that runner has called create_triggers in its second loop"""
            super().load_triggers()
            self.trigger_runner.load_triggers_count = (
                getattr(self.trigger_runner, "load_triggers_count", 0) + 1
            )
            if self.trigger_runner.load_triggers_count == 2:
                self.wait_for_runner_loop(runner_loop_count=2)

        def handle_events(self):
            super().handle_events()
            self.trigger_runner.handle_events_count = (
                getattr(self.trigger_runner, "handle_events_count", 0) + 1
            )

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

    job = Job()
    job_runner = TriggererJob_(job)
    job_runner.trigger_runner = TriggerRunner_()
    thread = Thread(target=job_runner._execute)
    thread.start()
    try:
        for _ in range(40):
            time.sleep(0.1)
            # ready to evaluate after 2 loops
            if getattr(job_runner.trigger_runner, "loop_count", 0) >= 2:
                break
        else:
            pytest.fail("did not observe 2 loops in the runner thread")
    finally:
        job_runner.trigger_runner.stop = True
        job_runner.trigger_runner.join(30)
        thread.join()
    instances = path.read_text().splitlines()
    assert len(instances) == 1


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_trigger_from_dead_triggerer(session, create_task_instance):
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
    ti_orm = create_task_instance(
        task_id="ti_orm",
        execution_date=timezone.utcnow(),
        run_id="orm_run_id",
    )
    ti_orm.trigger_id = trigger_orm.id
    session.add(trigger_orm)
    session.commit()
    # Make a TriggererJobRunner and have it retrieve DB tasks
    job = Job()
    job_runner = TriggererJobRunner(job)
    job_runner.load_triggers()
    # Make sure it turned up in TriggerRunner's queue
    assert [x for x, y in job_runner.trigger_runner.to_create] == [1]


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_trigger_from_expired_triggerer(session, create_task_instance):
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
    ti_orm = create_task_instance(
        task_id="ti_orm",
        execution_date=timezone.utcnow(),
        run_id="orm_run_id",
    )
    ti_orm.trigger_id = trigger_orm.id
    session.add(trigger_orm)
    # Use a TriggererJobRunner with an expired heartbeat
    triggerer_job_orm = Job(TriggererJobRunner.job_type)
    triggerer_job_orm.id = 42
    triggerer_job_orm.start_date = timezone.utcnow() - datetime.timedelta(hours=1)
    triggerer_job_orm.end_date = None
    triggerer_job_orm.latest_heartbeat = timezone.utcnow() - datetime.timedelta(hours=1)
    session.add(triggerer_job_orm)
    session.commit()
    # Make a TriggererJobRunner and have it retrieve DB tasks
    job = Job(TriggererJobRunner.job_type)
    job_runner = TriggererJobRunner(job)
    job_runner.load_triggers()
    # Make sure it turned up in TriggerRunner's queue
    assert [x for x, y in job_runner.trigger_runner.to_create] == [1]


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_trigger_runner_exception_stops_triggerer(session):
    """
    Checks that if an exception occurs when creating triggers, that the triggerer
    process stops
    """

    class MockTriggerException(Exception):
        pass

    class TriggerRunner_(TriggerRunner):
        async def create_triggers(self):
            raise MockTriggerException("Trigger creation failed")

    # Use a trigger that will immediately succeed
    trigger = SuccessTrigger()
    create_trigger_in_db(session, trigger)

    # Make a TriggererJobRunner and have it retrieve DB tasks
    job = Job()
    job_runner = TriggererJobRunner(job)
    job_runner.trigger_runner = TriggerRunner_()
    thread = Thread(target=job_runner._execute)
    thread.start()

    # Wait 4 seconds for the triggerer to stop
    try:
        for _ in range(40):
            time.sleep(0.1)
            if not thread.is_alive():
                break
        else:
            pytest.fail("TriggererJobRunner did not stop after exception in TriggerRunner")

        if not job_runner.trigger_runner.stop:
            pytest.fail("TriggerRunner not marked as stopped after exception in TriggerRunner")

    finally:
        job_runner.trigger_runner.stop = True
        # with suppress(MockTriggerException):
        job_runner.trigger_runner.join(30)
        thread.join()


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_trigger_firing(session):
    """
    Checks that when a trigger fires, it correctly makes it into the
    event queue.
    """
    # Use a trigger that will immediately succeed
    trigger = SuccessTrigger()
    create_trigger_in_db(session, trigger)
    # Make a TriggererJobRunner and have it retrieve DB tasks
    job = Job()
    job_runner = TriggererJobRunner(job)
    job_runner.load_triggers()
    # Now, start TriggerRunner up (and set it as a daemon thread during tests)
    job_runner.daemon = True
    job_runner.trigger_runner.start()
    try:
        # Wait for up to 3 seconds for it to fire and appear in the event queue
        for _ in range(30):
            if job_runner.trigger_runner.events:
                assert list(job_runner.trigger_runner.events) == [(1, TriggerEvent(True))]
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never sent the trigger event out")
    finally:
        # We always have to stop the runner
        job_runner.trigger_runner.stop = True
        job_runner.trigger_runner.join(30)


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
def test_trigger_failing(session):
    """
    Checks that when a trigger fails, it correctly makes it into the
    failure queue.
    """
    # Use a trigger that will immediately fail
    trigger = FailureTrigger()
    create_trigger_in_db(session, trigger)
    # Make a TriggererJobRunner and have it retrieve DB tasks
    job = Job()
    job_runner = TriggererJobRunner(job)
    job_runner.load_triggers()
    # Now, start TriggerRunner up (and set it as a daemon thread during tests)
    job_runner.daemon = True
    job_runner.trigger_runner.start()
    try:
        # Wait for up to 3 seconds for it to fire and appear in the event queue
        for _ in range(30):
            if job_runner.trigger_runner.failed_triggers:
                assert len(job_runner.trigger_runner.failed_triggers) == 1
                trigger_id, exc = next(iter(job_runner.trigger_runner.failed_triggers))
                assert trigger_id == 1
                assert isinstance(exc, ValueError)
                assert exc.args[0] == "Deliberate trigger failure"
                break
            time.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never marked the trigger as failed")
    finally:
        # We always have to stop the runner
        job_runner.trigger_runner.stop = True
        job_runner.trigger_runner.join(30)


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
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


@pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
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

    # Make a TriggererJobRunner and have it retrieve DB tasks
    job = Job()
    job_runner = TriggererJobRunner(job)
    job_runner.load_triggers()

    # Make sure it turned up in the failed queue
    assert len(job_runner.trigger_runner.failed_triggers) == 1

    # Run the failed trigger handler
    job_runner.handle_failed_triggers()

    # Make sure it marked the task instance as failed (which is actually the
    # scheduled state with a payload to make it fail)
    task_instance.refresh_from_db()
    assert task_instance.state == TaskInstanceState.SCHEDULED
    assert task_instance.next_method == "__fail__"
    assert task_instance.next_kwargs["error"] == "Trigger failure"
    assert task_instance.next_kwargs["traceback"][-1] == "ModuleNotFoundError: No module named 'fake'\n"


@pytest.mark.parametrize("should_wrap", (True, False))
@patch("airflow.jobs.triggerer_job_runner.configure_trigger_log_handler")
def test_handler_config_respects_donot_wrap(mock_configure, should_wrap):
    from airflow.jobs import triggerer_job_runner

    triggerer_job_runner.DISABLE_WRAPPER = not should_wrap
    job = Job()
    TriggererJobRunner(job=job)
    if should_wrap:
        mock_configure.assert_called()
    else:
        mock_configure.assert_not_called()


@patch("airflow.jobs.triggerer_job_runner.setup_queue_listener")
def test_triggerer_job_always_creates_listener(mock_setup):
    mock_setup.assert_not_called()
    job = Job()
    TriggererJobRunner(job=job)
    mock_setup.assert_called()


def test_queue_listener():
    """
    When listener func called, root handlers should be moved to queue listener
    and replaced with queuehandler.
    """
    reset_logging()
    importlib.reload(airflow_local_settings)
    configure_logging()

    def non_pytest_handlers(val):
        return [h for h in val if "pytest" not in h.__module__]

    import logging

    log = logging.getLogger()
    handlers = non_pytest_handlers(log.handlers)
    assert len(handlers) == 1
    handler = handlers[0]
    assert handler.__class__ == RedirectStdHandler
    listener = setup_queue_listener()
    assert handler not in non_pytest_handlers(log.handlers)
    qh = log.handlers[-1]
    assert qh.__class__ == LocalQueueHandler
    assert qh.queue == listener.queue
    listener.stop()
