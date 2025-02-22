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
import os
import selectors
import time
from typing import TYPE_CHECKING
from unittest.mock import ANY, MagicMock, patch

import pendulum
import pytest

from airflow.executors import workloads
from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import (
    TriggererJobRunner,
    TriggerRunner,
    TriggerRunnerSupervisor,
    messages,
)
from airflow.models import DagModel, DagRun, TaskInstance, Trigger
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.triggers.temporal import DateTimeTrigger, TimeDeltaTrigger
from airflow.triggers.base import TriggerEvent
from airflow.triggers.testing import FailureTrigger, SuccessTrigger
from airflow.utils import timezone
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_dags, clear_db_runs

if TYPE_CHECKING:
    from kgb import SpyAgency

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clean_database():
    """Fixture that cleans the database before and after every test."""
    clear_db_runs()
    clear_db_dags()
    yield  # Test runs here
    clear_db_dags()
    clear_db_runs()


def create_trigger_in_db(session, trigger, operator=None):
    dag_model = DagModel(dag_id="test_dag")
    dag = DAG(dag_id=dag_model.dag_id, schedule="@daily", start_date=pendulum.datetime(2023, 1, 1))
    date = pendulum.datetime(2023, 1, 1)
    run = DagRun(
        dag_id=dag_model.dag_id,
        run_id="test_run",
        logical_date=date,
        data_interval=(date, date),
        run_after=date,
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


@pytest.fixture
def supervisor_builder(mocker, session):
    def builder(job=None):
        import psutil

        if not job:
            job = Job()
            session.add(job)
            session.flush()

        process = mocker.Mock(spec=psutil.Process, pid=10 * job.id + 1)
        proc = TriggerRunnerSupervisor(
            id=job.id,
            job=job,
            pid=process.pid,
            stdin=mocker.Mock(),
            process=process,
            requests_fd=-1,
            capacity=10,
        )
        # Mock the selector
        mock_selector = mocker.Mock(spec=selectors.DefaultSelector)
        mock_selector.select.return_value = []

        # Set the selector on the process
        proc.selector = mock_selector
        return proc

    return builder


def test_trigger_lifecycle(spy_agency: SpyAgency, session):
    """
    Checks that the triggerer will correctly see a new Trigger in the database
    and send it to the trigger runner, and then delete it when it vanishes.
    """
    # Use a trigger that will not fire for the lifetime of the test
    # (we want to avoid it firing and deleting itself)
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    dag_model, run, trigger_orm, task_instance = create_trigger_in_db(session, trigger)
    # Make a TriggererJobRunner and have it retrieve DB tasks
    trigger_runner_supervisor = TriggerRunnerSupervisor.start(job=Job(), capacity=10)

    try:
        # Spy on it so we can see what gets send, but also call the original.
        send_spy = spy_agency.spy_on(TriggerRunnerSupervisor._send, owner=TriggerRunnerSupervisor)
        trigger_runner_supervisor.load_triggers()
        # Make sure it turned up in TriggerRunner's queue
        assert trigger_runner_supervisor.running_triggers == {1}

        spy_agency.assert_spy_called_with(
            send_spy,
            workloads.RunTrigger.model_construct(
                id=trigger_orm.id,
                ti=ANY,
                classpath=trigger.serialize()[0],
                encrypted_kwargs=trigger_orm.encrypted_kwargs,
                kind="RunTrigger",
            ),
        )
        # OK, now remove it from the DB
        session.delete(trigger_orm)
        session.commit()

        # Re-load the triggers
        trigger_runner_supervisor.load_triggers()

        # Wait for up to 3 seconds for it to vanish from the TriggerRunner's storage
        for _ in range(30):
            if not trigger_runner_supervisor.running_triggers:
                break
            trigger_runner_supervisor._service_subprocess(0.1)
        else:
            pytest.fail("TriggerRunnerSupervisor never deleted trigger")
    finally:
        # We always have to stop the runner
        trigger_runner_supervisor.kill(force=False)


class TestTriggerRunner:
    @pytest.mark.asyncio
    async def test_run_inline_trigger_canceled(self, session) -> None:
        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {1: {"task": MagicMock(), "name": "mock_name", "events": 0}}
        mock_trigger = MagicMock()
        mock_trigger.timeout_after = None
        mock_trigger.run.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await trigger_runner.run_trigger(1, mock_trigger)

    @pytest.mark.asyncio
    async def test_run_inline_trigger_timeout(self, session, cap_structlog) -> None:
        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {1: {"task": MagicMock(), "name": "mock_name", "events": 0}}
        mock_trigger = MagicMock()
        mock_trigger.timeout_after = timezone.utcnow() - datetime.timedelta(hours=1)
        mock_trigger.run.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await trigger_runner.run_trigger(1, mock_trigger)
        assert {"event": "Trigger cancelled due to timeout", "log_level": "error"} in cap_structlog

    @patch("airflow.jobs.triggerer_job_runner.Trigger._decrypt_kwargs")
    @patch(
        "airflow.jobs.triggerer_job_runner.TriggerRunner.get_trigger_by_classpath",
        return_value=DateTimeTrigger,
    )
    @pytest.mark.asyncio
    async def test_update_trigger_with_triggerer_argument_change(
        self, mock_get_trigger_by_classpath, mock_decrypt_kwargs, session, cap_structlog
    ) -> None:
        trigger_runner = TriggerRunner()

        def fn(moment): ...

        mock_decrypt_kwargs.return_value = {"moment": ..., "not_exists_arg": ...}
        mock_get_trigger_by_classpath.return_value = fn

        trigger_runner.to_create.append(
            workloads.RunTrigger.model_construct(id=1, classpath="abc", encrypted_kwargs="fake"),
        )
        await trigger_runner.create_triggers()

        assert "Trigger failed" in cap_structlog.text
        err = cap_structlog[0]["error"]
        assert isinstance(err, TypeError)
        assert "got an unexpected keyword argument 'not_exists_arg'" in str(err)

    @pytest.mark.asyncio
    async def test_invalid_trigger(self):
        """Test the behaviour when we try to run an invalid Trigger"""
        workload = workloads.RunTrigger.model_construct(
            id=1, ti=None, classpath="fake.classpath", encrypted_kwargs={}
        )
        trigger_runner = TriggerRunner()
        trigger_runner.requests_sock = MagicMock()

        trigger_runner.to_create.append(workload)

        await trigger_runner.create_triggers()
        assert (1, ANY) in trigger_runner.failed_triggers
        ids = await trigger_runner.cleanup_finished_triggers()
        await trigger_runner.sync_state_to_supervisor(ids)

        # Check that we sent the right info in the failure message
        assert trigger_runner.requests_sock.write.call_count == 1
        blob = trigger_runner.requests_sock.write.mock_calls[0].args[0]
        msg = messages.TriggerStateChanges.model_validate_json(blob)

        assert msg.events is None
        assert msg.failures is not None
        assert len(msg.failures) == 1
        trigger_id, traceback = msg.failures[0]
        assert trigger_id == 1
        assert traceback[-1] == "ModuleNotFoundError: No module named 'fake'\n"


@pytest.mark.asyncio
async def test_trigger_create_race_condition_38599(session, supervisor_builder):
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
    trigger = TimeDeltaTrigger(delta=datetime.timedelta(microseconds=1))
    trigger_orm = Trigger.from_object(trigger)
    trigger_orm.id = 1
    session.add(trigger_orm)

    dag = DagModel(dag_id="test-dag")
    dag_run = DagRun(dag.dag_id, run_id="abc", run_type="none", run_after=timezone.utcnow())
    ti = TaskInstance(
        PythonOperator(task_id="dummy-task", python_callable=print),
        run_id=dag_run.run_id,
        state=TaskInstanceState.DEFERRED,
    )
    ti.dag_id = dag.dag_id
    ti.trigger_id = trigger_orm.id
    session.add(dag)
    session.add(dag_run)
    session.add(ti)

    job1 = Job()
    job2 = Job()
    session.add(job1)
    session.add(job2)

    session.commit()

    supervisor1 = supervisor_builder(job1)
    supervisor2 = supervisor_builder(job2)

    # Assign and run the trigger on the first TriggererJobRunner
    # Instead of running job_runner1._execute, we will run the individual methods
    # to control the timing of the execution.
    supervisor1.load_triggers()
    assert len(supervisor1.running_triggers) == 1
    trigger_orm = session.get(Trigger, trigger_orm.id)
    assert trigger_orm.task_instance is not None, "Pre-condition"

    # In a real execution environment, a missed heartbeat would cause the trigger to be picked up
    # by another TriggererJobRunner.
    # In this test, however, this is not necessary because we are controlling the execution
    # of the TriggererJobRunner.
    # job1.latest_heartbeat = timezone.utcnow() - datetime.timedelta(hours=1)
    # session.commit()

    # This calls Trigger.submit_event, which will unlink the trigger from the task instance

    # Simulate this call: supervisor1._service_subprocess()
    supervisor1.events.append((trigger_orm.id, TriggerEvent(True)))
    supervisor1.handle_events()
    trigger_orm = session.get(Trigger, trigger_orm.id)
    # This is the "pre"-condition we need to assert to test the race condition
    assert trigger_orm.task_instance is None

    # Simulate the second TriggererJobRunner picking up the trigger
    # The race condition happens here.
    # AttributeError: 'NoneType' object has no attribute 'dag_id'
    supervisor2.update_triggers({trigger_orm.id})
    assert supervisor2.running_triggers == set()
    # We should have not sent anything to the async runner process
    supervisor2.stdin.write.assert_not_called()


def test_trigger_create_race_condition_18392(session, supervisor_builder, spy_agency: SpyAgency):
    """
    This verifies the resolution of race condition documented in github issue #18392.
    Triggers are queued for creation by TriggerJob.load_triggers.
    There was a race condition where multiple triggers would be created unnecessarily.
    What happens is the runner completes the trigger and purges from the "running" list.
    Then job.load_triggers is called and it looks like the trigger is not running but should,
    so it queues it again.

    The scenario is as follows:
        1. job.load_triggers (trigger now queued and sent to subprocess)
        2. runner.create_triggers (trigger now running)
        3. job.handle_events (trigger still appears running so state not updated in DB)
        4. runner.cleanup_finished_triggers (trigger completed at this point; trigger from "running" set)
        5. job.load_triggers (trigger not running, but also not purged from DB, so it is queued again)
        6. runner.create_triggers (trigger created again)

    This test verifies that under this scenario only one trigger is created.
    """
    trigger = TimeDeltaTrigger(delta=datetime.timedelta(microseconds=1))
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

    supervisor = supervisor_builder()

    iteration = 0

    # Hook into something in each iteration of the loop
    @spy_agency.spy_for(TriggerRunnerSupervisor.is_alive)
    def is_alive(self):
        nonlocal iteration
        iteration += 1
        if iteration >= 2:
            self.stop = True

        return True

    supervisor.run()
    assert supervisor.stdin.write.call_count == 1


@pytest.mark.execution_timeout(5)
def test_trigger_runner_exception_stops_triggerer(session):
    """
    Checks that if an exception occurs when creating triggers, that the triggerer
    process stops
    """
    import signal

    job_runner = TriggererJobRunner(Job())
    time.sleep(0.1)

    # Wait 4 seconds for the triggerer to stop
    try:

        def on_timeout(signum, frame):
            os.kill(job_runner.trigger_runner.pid, signal.SIGKILL)

        signal.signal(signal.SIGALRM, on_timeout)
        signal.setitimer(signal.ITIMER_REAL, 0.1)
        # This either returns cleanly, or the pytest timeout hits.
        assert job_runner._execute() == -9
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


@pytest.mark.asyncio
async def test_trigger_firing():
    """
    Checks that when a trigger fires, it correctly makes it into the
    event queue.
    """
    runner = TriggerRunner()

    runner.to_create.append(
        # Use a trigger that will immediately succeed
        workloads.RunTrigger.model_construct(
            id=1,
            ti=None,
            classpath=f"{SuccessTrigger.__module__}.{SuccessTrigger.__name__}",
            encrypted_kwargs='{"__type":"dict", "__var":{}}',
        ),
    )
    await runner.create_triggers()
    try:
        # Wait for up to 3 seconds for it to fire and appear in the event queue
        for _ in range(30):
            await asyncio.sleep(0.1)
            finished = await runner.cleanup_finished_triggers()
            if runner.events:
                assert list(runner.events) == [(1, TriggerEvent(True))]
                assert finished == [1]
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never sent the trigger event out")
    finally:
        for info in runner.triggers.values():
            info["task"].cancel()


@pytest.mark.asyncio
async def test_trigger_failing():
    """
    Checks that when a trigger fails, it correctly makes it into the
    failure queue.
    """
    runner = TriggerRunner()

    runner.to_create.append(
        # Use a trigger that will immediately fail
        workloads.RunTrigger.model_construct(
            id=1,
            ti=None,
            classpath=f"{FailureTrigger.__module__}.{FailureTrigger.__name__}",
            encrypted_kwargs='{"__type":"dict", "__var":{}}',
        ),
    )
    await runner.create_triggers()
    try:
        # Wait for up to 3 seconds for it to fire and appear in the event queue
        for _ in range(30):
            await asyncio.sleep(0.1)
            await runner.cleanup_finished_triggers()
            if runner.failed_triggers:
                assert len(runner.failed_triggers) == 1
                trigger_id, exc = runner.failed_triggers[0]
                assert trigger_id == 1
                assert isinstance(exc, ValueError)
                assert exc.args[0] == "Deliberate trigger failure"
                break
        else:
            pytest.fail("TriggerRunner never marked the trigger as failed")
    finally:
        for info in runner.triggers.values():
            info["task"].cancel()


def test_failed_trigger(session, dag_maker, supervisor_builder):
    """
    Checks that the triggerer will correctly fail task instances that depend on
    triggers that can't even be loaded.

    This is the Supervisor side of the error reported in TestTriggerRunner::test_invalid_trigger
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
    task_instance.trigger_id = trigger_orm.id
    session.commit()

    supervisor = supervisor_builder()

    supervisor.load_triggers()

    # Make sure it got picked up
    assert supervisor.running_triggers == {1}, "Pre-condition"
    # Simulate receiving the state update message

    supervisor._handle_request(
        messages.TriggerStateChanges(
            events=None,
            finished=None,
            failures=[
                (
                    1,
                    [
                        "Traceback (most recent call last):\n",
                        'File "<frozen importlib._bootstrap>", line 1324, in _find_and_load_unlocked\n',
                        "ModuleNotFoundError: No module named 'fake'\n",
                    ],
                )
            ],
        ),
        log=MagicMock(),
    )

    # Run the failed trigger handler
    supervisor.handle_failed_triggers()

    # Make sure it marked the task instance as failed (which is actually the
    # scheduled state with a payload to make it fail)
    task_instance.refresh_from_db()
    assert task_instance.state == TaskInstanceState.SCHEDULED
    assert task_instance.next_method == "__fail__"
    assert task_instance.next_kwargs["error"] == "Trigger failure"
    assert task_instance.next_kwargs["traceback"][-1] == "ModuleNotFoundError: No module named 'fake'\n"
