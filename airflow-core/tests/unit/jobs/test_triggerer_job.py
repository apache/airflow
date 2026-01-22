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
import itertools
import os
import selectors
import time
import typing
from collections.abc import AsyncIterator
from socket import socket
from typing import TYPE_CHECKING, Any
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pendulum
import pytest
from asgiref.sync import sync_to_async
from structlog.typing import FilteringBoundLogger

from airflow._shared.timezones import timezone
from airflow.executors import workloads
from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import (
    ToTriggerRunner,
    ToTriggerSupervisor,
    TriggerCommsDecoder,
    TriggererJobRunner,
    TriggerRunner,
    TriggerRunnerSupervisor,
    messages,
)
from airflow.models import Connection, DagModel, DagRun, Trigger, Variable
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.triggers.file import FileDeleteTrigger
from airflow.providers.standard.triggers.temporal import DateTimeTrigger, TimeDeltaTrigger
from airflow.sdk import DAG, BaseHook, BaseOperator
from airflow.sdk.execution_time.comms import ToSupervisor, ToTask
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.triggers.testing import FailureTrigger, SuccessTrigger
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import (
    clear_db_connections,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_jobs,
    clear_db_runs,
    clear_db_triggers,
    clear_db_variables,
    clear_db_xcom,
)
from tests_common.test_utils.taskinstance import create_task_instance

if TYPE_CHECKING:
    from kgb import SpyAgency

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clean_database():
    """Fixture that cleans the database before and after every test."""
    clear_db_connections()
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_xcom()
    clear_db_variables()
    clear_db_triggers()
    clear_db_jobs()
    yield  # Test runs here
    clear_db_connections()
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_xcom()
    clear_db_variables()
    clear_db_triggers()
    clear_db_jobs()


def create_trigger_in_db(session, trigger, operator=None):
    bundle_name = "testing"

    testing_bundle = DagBundleModel(name=bundle_name)
    session.merge(testing_bundle)
    session.flush()

    date = pendulum.datetime(2023, 1, 1)
    dag_model = DagModel(dag_id="test_dag", bundle_name=bundle_name)
    dag = DAG(dag_id=dag_model.dag_id, schedule="@daily", start_date=date)
    run = DagRun(
        dag_id=dag_model.dag_id,
        run_id="test_run",
        logical_date=date,
        data_interval=(date, date),
        run_after=date,
        run_type=DagRunType.MANUAL,
    )
    trigger_orm = Trigger.from_object(trigger)
    if operator:
        operator.dag = dag
    else:
        operator = BaseOperator(task_id="test_ti", dag=dag)
    session.add(dag_model)

    SerializedDagModel.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name=bundle_name)
    session.add(run)
    session.add(trigger_orm)
    session.flush()
    dag_version = DagVersion.get_latest_version(dag.dag_id)
    task_instance = create_task_instance(operator, run_id=run.run_id, dag_version_id=dag_version.id)
    task_instance.trigger_id = trigger_orm.id
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
        with pytest.raises(ValueError, match=r"Capacity number .+ is invalid"):
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
        # Create a mock stdin that has both write and sendall methods
        mock_stdin = mocker.Mock(spec=socket)
        mock_stdin.write = mocker.Mock()
        mock_stdin.sendall = mocker.Mock()

        proc = TriggerRunnerSupervisor(
            process_log=mocker.Mock(spec=FilteringBoundLogger),
            id=job.id,
            job=job,
            pid=process.pid,
            stdin=mock_stdin,
            process=process,
            capacity=10,
        )
        # Mock the selector
        mock_selector = mocker.Mock(spec=selectors.DefaultSelector)
        mock_selector.select.return_value = []

        # Set the selector on the process
        proc.selector = mock_selector
        return proc

    return builder


def test_trigger_lifecycle(spy_agency: SpyAgency, session, testing_dag_bundle):
    """
    Checks that the triggerer will correctly see a new Trigger in the database
    and send it to the trigger runner, and then delete it when it vanishes.
    """
    # Use a trigger that will not fire for the lifetime of the test
    # (we want to avoid it firing and deleting itself)
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    dag_model, run, trigger_orm, task_instance = create_trigger_in_db(session, trigger)
    # Make a TriggererJobRunner and have it retrieve DB tasks
    trigger_runner_supervisor = TriggerRunnerSupervisor.start(job=Job(id=12345), capacity=10)

    try:
        # Spy on it so we can see what gets send, but also call the original.
        message = None

        @spy_agency.spy_for(TriggerRunnerSupervisor.send_msg)
        def send_msg_spy(self, msg, *args, **kwargs):
            nonlocal message
            message = msg
            TriggerRunnerSupervisor.send_msg.call_original(self, msg, *args, **kwargs)

        trigger_runner_supervisor.load_triggers()
        trigger_runner_supervisor._service_subprocess(0.1)

        # Make sure it turned up in TriggerRunner's queue
        assert trigger_runner_supervisor.running_triggers == {trigger_orm.id}

        assert message is not None, "spy was not called"
        assert len(message.to_create) == 1
        assert message.to_create[0] == (
            workloads.RunTrigger.model_construct(
                id=trigger_orm.id,
                ti=ANY,
                classpath=trigger.serialize()[0],
                encrypted_kwargs=trigger_orm.encrypted_kwargs,
                kind="RunTrigger",
                dag_data=ANY,
            )
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


@pytest.mark.parametrize(
    ("trigger", "watcher_count", "trigger_count"),
    [
        (TimeDeltaTrigger(datetime.timedelta(days=7)), 0, 1),
        (FileDeleteTrigger("/tmp/foo.txt", poke_interval=1), 1, 0),
    ],
)
@patch("time.monotonic", side_effect=itertools.count(start=1, step=60))
def test_trigger_log(mock_monotonic, trigger, watcher_count, trigger_count, session, capsys):
    """
    Checks that the triggerer will log watcher and trigger in separate lines.
    """
    create_trigger_in_db(session, trigger)

    trigger_runner_supervisor = TriggerRunnerSupervisor.start(job=Job(id=123456), capacity=10)
    trigger_runner_supervisor.load_triggers()

    for _ in range(30):
        trigger_runner_supervisor._service_subprocess(0.1)

    stdout = capsys.readouterr().out
    assert f"{trigger_count} triggers currently running" in stdout
    assert f"{watcher_count} watchers currently running" in stdout

    trigger_runner_supervisor.kill(force=False)


class TestTriggerRunner:
    def test_run_inline_trigger_canceled(self, session) -> None:
        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {
            1: {"task": MagicMock(spec=asyncio.Task), "is_watcher": False, "name": "mock_name", "events": 0}
        }
        mock_trigger = MagicMock(spec=BaseTrigger)
        mock_trigger.timeout_after = None
        mock_trigger.run.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(trigger_runner.run_trigger(1, mock_trigger))

    # @pytest.mark.asyncio
    def test_run_inline_trigger_timeout(self, session, cap_structlog) -> None:
        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {
            1: {"task": MagicMock(spec=asyncio.Task), "is_watcher": False, "name": "mock_name", "events": 0}
        }
        mock_trigger = MagicMock(spec=BaseTrigger)
        mock_trigger.run.side_effect = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(
                trigger_runner.run_trigger(
                    1, mock_trigger, timeout_after=timezone.utcnow() - datetime.timedelta(hours=1)
                )
            )
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
    @patch("airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True)
    async def test_invalid_trigger(self, supervisor_builder):
        """Test the behaviour when we try to run an invalid Trigger"""
        workload = workloads.RunTrigger.model_construct(
            id=1, ti=None, classpath="fake.classpath", encrypted_kwargs={}
        )
        trigger_runner = TriggerRunner()
        trigger_runner.comms_decoder = AsyncMock(spec=TriggerCommsDecoder)
        trigger_runner.comms_decoder.asend.return_value = messages.TriggerStateSync(
            to_create=[], to_cancel=[]
        )

        trigger_runner.to_create.append(workload)

        await trigger_runner.create_triggers()
        assert (1, ANY) in trigger_runner.failed_triggers
        ids = await trigger_runner.cleanup_finished_triggers()
        await trigger_runner.sync_state_to_supervisor(ids)

        # Check that we sent the right info in the failure message
        assert trigger_runner.comms_decoder.asend.call_count == 1
        msg = trigger_runner.comms_decoder.asend.mock_calls[0].args[0]
        assert isinstance(msg, messages.TriggerStateChanges)

        assert msg.events is None
        assert msg.failures is not None
        assert len(msg.failures) == 1
        trigger_id, traceback = msg.failures[0]
        assert trigger_id == 1
        assert traceback[-1] == "ModuleNotFoundError: No module named 'fake'\n"

    @pytest.mark.asyncio
    async def test_trigger_kwargs_serialization_cleanup(self, session):
        """
        Test that trigger kwargs are properly cleaned of serialization artifacts
        (__var, __type keys).
        """
        from airflow.serialization.serialized_objects import BaseSerialization

        kw = {"simple": "test", "tuple": (), "dict": {}, "list": []}

        serialized_kwargs = BaseSerialization.serialize(kw)

        trigger_orm = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs=serialized_kwargs)
        session.add(trigger_orm)
        session.commit()

        stored_kwargs = trigger_orm.kwargs
        assert stored_kwargs == {
            "Encoding.TYPE": "dict",
            "Encoding.VAR": {
                "dict": {"Encoding.TYPE": "dict", "Encoding.VAR": {}},
                "list": [],
                "simple": "test",
                "tuple": {"Encoding.TYPE": "tuple", "Encoding.VAR": []},
            },
        }

        runner = TriggerRunner()
        runner.to_create.append(
            workloads.RunTrigger.model_construct(
                id=trigger_orm.id,
                ti=None,
                classpath=trigger_orm.classpath,
                encrypted_kwargs=trigger_orm.encrypted_kwargs,
            )
        )

        await runner.create_triggers()
        assert trigger_orm.id in runner.triggers
        trigger_instance = runner.triggers[trigger_orm.id]["task"]

        # The test passes if no exceptions were raised during trigger creation
        trigger_instance.cancel()
        await runner.cleanup_finished_triggers()


@pytest.mark.asyncio
@pytest.mark.usefixtures("testing_dag_bundle")
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
    session.add(trigger_orm)
    session.flush()

    bundle_name = "testing"
    with DAG(dag_id="test-dag") as dag:
        task = PythonOperator(task_id="dummy-task", python_callable=print)
    dm = DagModel(dag_id="test-dag", bundle_name=bundle_name)
    session.add(dm)

    SerializedDagModel.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name=bundle_name)
    dag_run = DagRun(dag.dag_id, run_id="abc", run_type="none", run_after=timezone.utcnow())
    dag_version = DagVersion.get_latest_version(dag.dag_id)
    ti = create_task_instance(
        task,
        run_id=dag_run.run_id,
        state=TaskInstanceState.DEFERRED,
        dag_version_id=dag_version.id,
    )
    ti.trigger_id = trigger_orm.id
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
    assert {t.id for t in supervisor1.creating_triggers} == {trigger_orm.id}
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


@pytest.mark.execution_timeout(5)
def test_trigger_runner_exception_stops_triggerer():
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
    session.add(trigger_orm)
    session.flush()

    # Create the test DAG and task
    with dag_maker(dag_id="test_invalid_trigger", session=session):
        EmptyOperator(task_id="dummy1")

    dr = dag_maker.create_dagrun()
    task_instance = dr.task_instances[0]
    # Make a task instance based on that and tie it to the trigger
    task_instance.state = TaskInstanceState.DEFERRED
    task_instance.trigger_id = trigger_orm.id
    session.commit()

    supervisor: TriggerRunnerSupervisor = supervisor_builder()

    supervisor.load_triggers()

    # Make sure it got picked up
    assert {t.id for t in supervisor.creating_triggers} == {trigger_orm.id}, "Pre-condition"
    # Simulate receiving the state update message

    supervisor._handle_request(
        messages.TriggerStateChanges(
            events=None,
            finished=None,
            failures=[
                (
                    trigger_orm.id,
                    [
                        "Traceback (most recent call last):\n",
                        'File "<frozen importlib._bootstrap>", line 1324, in _find_and_load_unlocked\n',
                        "ModuleNotFoundError: No module named 'fake'\n",
                    ],
                )
            ],
        ),
        req_id=1,
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


class CustomTrigger(BaseTrigger):
    """Custom Trigger that will access one Variable and one Connection."""

    def __init__(self, dag_id, run_id, task_id, map_index):
        self.dag_id = dag_id
        self.run_id = run_id
        self.task_id = task_id
        self.map_index = map_index

    async def run(self, **args) -> AsyncIterator[TriggerEvent]:
        import attrs

        from airflow.sdk import Variable
        from airflow.sdk.execution_time.xcom import XCom

        # Use sdk masker in dag processor and triggerer because those use the task sdk machinery
        from airflow.sdk.log import mask_secret

        conn = await sync_to_async(BaseHook.get_connection)("test_connection")
        self.log.info("Loaded conn %s", conn.conn_id)

        get_variable_value = await sync_to_async(Variable.get)("test_get_variable")
        await sync_to_async(mask_secret)(get_variable_value)
        self.log.info("Loaded variable %s", get_variable_value)

        get_xcom_value = await sync_to_async(XCom.get_one)(
            key="test_get_xcom",
            dag_id=self.dag_id,
            run_id=self.run_id,
            task_id=self.task_id,
            map_index=self.map_index,
        )
        self.log.info("Loaded XCom %s", get_xcom_value)

        set_variable_key = "test_set_variable"
        set_variable_value = "set_value"
        await sync_to_async(Variable.set)(key=set_variable_key, value=set_variable_value)
        self.log.info("Set variable with key %s and value %s", set_variable_key, set_variable_value)

        set_xcom_key = "test_set_xcom"
        set_xcom_value = "set_xcom"
        await sync_to_async(XCom.set)(
            key=set_xcom_key,
            dag_id=self.dag_id,
            run_id=self.run_id,
            task_id=self.task_id,
            map_index=self.map_index,
            value=set_xcom_value,
        )
        self.log.info("Set xcom with key %s and value %s", set_xcom_key, set_xcom_value)

        delete_variable_key = "test_delete_variable"
        await sync_to_async(Variable.delete)(delete_variable_key)
        self.log.info("Deleted variable with key %s", delete_variable_key)

        delete_xcom_key = "test_delete_xcom"
        await sync_to_async(XCom.delete)(
            key=delete_xcom_key,
            dag_id=self.dag_id,
            run_id=self.run_id,
            task_id=self.task_id,
            map_index=self.map_index,
        )
        self.log.info("Delete xcom with key %s", delete_xcom_key)

        yield TriggerEvent(
            {
                "connection": attrs.asdict(conn),
                "variable": {
                    "get_variable": get_variable_value,
                    "set_variable": set_variable_value,
                    "delete_variable": delete_variable_key,
                },
                "xcom": {
                    "get_xcom": get_xcom_value,
                    "set_xcom": set_xcom_value,
                    "delete_xcom": delete_xcom_key,
                },
            }
        )

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {
                "dag_id": self.dag_id,
                "run_id": self.run_id,
                "task_id": self.task_id,
                "map_index": self.map_index,
            },
        )


class DummyTriggerRunnerSupervisor(TriggerRunnerSupervisor):
    """
    Make sure that the Supervisor stops after handling the events and do not keep running forever so the
    test can continue.
    """

    def handle_events(self):
        self.stop = bool(self.events)
        super().handle_events()


@pytest.mark.asyncio
@pytest.mark.execution_timeout(20)
async def test_trigger_can_call_variables_connections_and_xcoms_methods(session, dag_maker):
    """Checks that the trigger will successfully call Variables, Connections and XComs methods."""
    # Create the test DAG and task
    with dag_maker(dag_id="trigger_accessing_variable_connection_and_xcom", session=session):
        EmptyOperator(task_id="dummy1")
    dr = dag_maker.create_dagrun()
    task_instance = dr.task_instances[0]
    # Make a task instance based on that and tie it to the trigger
    task_instance.state = TaskInstanceState.DEFERRED

    # Create a Trigger
    trigger = CustomTrigger(dag_id=dr.dag_id, run_id=dr.run_id, task_id=task_instance.task_id, map_index=-1)
    trigger_orm = Trigger(
        classpath=trigger.serialize()[0],
        kwargs={"dag_id": dr.dag_id, "run_id": dr.run_id, "task_id": task_instance.task_id, "map_index": -1},
    )
    session.add(trigger_orm)
    session.flush()
    task_instance.trigger_id = trigger_orm.id

    # Create the appropriate Connection, Variable and XCom
    connection = Connection(
        conn_id="test_connection",
        conn_type="http",
        schema="https",
        login="user",
        password="pass",
        extra={"key": "value"},
        port=443,
        host="example.com",
    )
    get_variable = Variable(key="test_get_variable", val="some_variable_value")
    delete_variable = Variable(key="test_delete_variable", val="delete_value")

    session.add(connection)
    session.add(get_variable)
    session.add(delete_variable)

    XComModel.set(
        key="test_get_xcom",
        value="some_xcom_value",
        task_id=task_instance.task_id,
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        map_index=-1,
        session=session,
    )

    XComModel.set(
        key="test_delete_xcom",
        value="some_xcom_value",
        task_id=task_instance.task_id,
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        map_index=-1,
        session=session,
    )

    job = Job()
    session.add(job)
    session.commit()

    supervisor = DummyTriggerRunnerSupervisor.start(job=job, capacity=1, logger=None)
    supervisor.run()

    task_instance.refresh_from_db()
    assert task_instance.state == TaskInstanceState.SCHEDULED
    assert task_instance.next_method != "__fail__"
    expected_event = {
        "event": {
            "connection": {
                "conn_id": "test_connection",
                "conn_type": "http",
                "description": None,
                "host": "example.com",
                "schema": "https",
                "login": "user",
                "password": "pass",
                "port": 443,
                "extra": '{"key": "value"}',
            },
            "variable": {
                "get_variable": "some_variable_value",
                "set_variable": "set_value",
                "delete_variable": "test_delete_variable",
            },
            "xcom": {
                "get_xcom": '"some_xcom_value"',
                "set_xcom": "set_xcom",
                "delete_xcom": "test_delete_xcom",
            },
        }
    }
    assert task_instance.next_kwargs == expected_event


class CustomTriggerDagRun(BaseTrigger):
    def __init__(self, trigger_dag_id, run_ids, states, logical_dates):
        self.trigger_dag_id = trigger_dag_id
        self.run_ids = run_ids
        self.states = states
        self.logical_dates = logical_dates

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {
                "trigger_dag_id": self.trigger_dag_id,
                "run_ids": self.run_ids,
                "states": self.states,
                "logical_dates": self.logical_dates,
            },
        )

    async def run(self, **args) -> AsyncIterator[TriggerEvent]:
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

        dag_run_states_count = await sync_to_async(RuntimeTaskInstance.get_dr_count)(
            dag_id=self.trigger_dag_id,
            run_ids=self.run_ids,
            states=self.states,
            logical_dates=self.logical_dates,
        )
        dag_run_state = await sync_to_async(RuntimeTaskInstance.get_dagrun_state)(
            dag_id=self.trigger_dag_id,
            run_id=self.run_ids[0],
        )
        yield TriggerEvent({"count": dag_run_states_count, "dag_run_state": dag_run_state})


@pytest.mark.asyncio
@pytest.mark.execution_timeout(20)
async def test_trigger_can_fetch_trigger_dag_run_count_and_state_in_deferrable(session, dag_maker):
    """Checks that the trigger will successfully fetch the count of trigger DAG runs."""
    # Create the test DAG and task
    with dag_maker(dag_id="trigger_can_fetch_trigger_dag_run_count_and_state_in_deferrable", session=session):
        EmptyOperator(task_id="dummy1")
    dr = dag_maker.create_dagrun()
    task_instance = dr.task_instances[0]
    task_instance.state = TaskInstanceState.DEFERRED

    # Use the same dag run with states deferred to fetch the count
    trigger = CustomTriggerDagRun(
        trigger_dag_id=dr.dag_id, run_ids=[dr.run_id], states=[dr.state], logical_dates=[dr.logical_date]
    )
    trigger_orm = Trigger(
        classpath=trigger.serialize()[0],
        kwargs={
            "trigger_dag_id": dr.dag_id,
            "run_ids": [dr.run_id],
            "states": [dr.state],
            "logical_dates": [dr.logical_date],
        },
    )

    session.add(trigger_orm)
    session.commit()
    task_instance.trigger_id = trigger_orm.id

    job = Job()
    session.add(job)
    session.commit()

    supervisor = DummyTriggerRunnerSupervisor.start(job=job, capacity=1, logger=None)
    supervisor.run()

    task_instance.refresh_from_db()
    assert task_instance.state == TaskInstanceState.SCHEDULED
    assert task_instance.next_method != "__fail__"
    assert task_instance.next_kwargs == {"event": {"count": 1, "dag_run_state": "running"}}


class CustomTriggerWorkflowStateTrigger(BaseTrigger):
    """Custom Trigger to check the triggerer can access the get_ti_count and get_dr_count."""

    def __init__(self, external_dag_id, execution_dates, external_task_ids, allowed_states, run_ids):
        self.external_dag_id = external_dag_id
        self.execution_dates = execution_dates
        self.external_task_ids = external_task_ids
        self.allowed_states = allowed_states
        self.run_ids = run_ids

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {
                "external_dag_id": self.external_dag_id,
                "execution_dates": self.execution_dates,
                "external_task_ids": self.external_task_ids,
                "allowed_states": self.allowed_states,
                "run_ids": self.run_ids,
            },
        )

    async def run(self, **args) -> AsyncIterator[TriggerEvent]:
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

        ti_count = await sync_to_async(RuntimeTaskInstance.get_ti_count)(
            dag_id=self.external_dag_id,
            task_ids=self.external_task_ids,
            task_group_id=None,
            run_ids=self.run_ids,
            logical_dates=self.execution_dates,
            states=self.allowed_states,
        )
        dr_count = await sync_to_async(RuntimeTaskInstance.get_dr_count)(
            dag_id=self.external_dag_id,
            run_ids=self.run_ids,
            logical_dates=self.execution_dates,
            states=["running"],
        )
        task_states = await sync_to_async(RuntimeTaskInstance.get_task_states)(
            dag_id=self.external_dag_id,
            task_ids=self.external_task_ids,
            run_ids=self.run_ids,
            task_group_id=None,
            logical_dates=self.execution_dates,
        )
        yield TriggerEvent({"ti_count": ti_count, "dr_count": dr_count, "task_states": task_states})


@pytest.mark.asyncio
@pytest.mark.execution_timeout(20)
async def test_trigger_can_fetch_dag_run_count_ti_count_in_deferrable(session, dag_maker):
    """Checks that the trigger will successfully fetch the count of DAG runs, Task count and task states."""
    # Create the test DAG and task
    with dag_maker(dag_id="parent_dag", session=session):
        EmptyOperator(task_id="parent_task")
    parent_dag_run = dag_maker.create_dagrun()
    parent_task = parent_dag_run.task_instances[0]
    parent_task.state = TaskInstanceState.SUCCESS

    with dag_maker(dag_id="trigger_can_fetch_dag_run_count_ti_count_in_deferrable", session=session):
        EmptyOperator(task_id="dummy1")
    dr = dag_maker.create_dagrun()
    task_instance = dr.task_instances[0]
    task_instance.state = TaskInstanceState.DEFERRED

    # Use the same dag run with states deferred to fetch the count
    trigger = CustomTriggerWorkflowStateTrigger(
        external_dag_id=parent_task.dag_id,
        execution_dates=[parent_task.logical_date],
        external_task_ids=[parent_task.task_id],
        allowed_states=[State.SUCCESS],
        run_ids=[parent_task.run_id],
    )
    trigger_orm = Trigger(
        classpath=trigger.serialize()[0],
        kwargs={
            "external_dag_id": parent_dag_run.dag_id,
            "execution_dates": [parent_dag_run.logical_date],
            "external_task_ids": [parent_task.task_id],
            "allowed_states": [State.SUCCESS],
            "run_ids": [parent_dag_run.run_id],
        },
    )
    session.add(trigger_orm)
    session.commit()
    task_instance.trigger_id = trigger_orm.id

    job = Job()
    session.add(job)
    session.commit()

    supervisor = DummyTriggerRunnerSupervisor.start(job=job, capacity=1, logger=None)
    supervisor.run()

    parent_task.refresh_from_db()
    task_instance.refresh_from_db()
    assert task_instance.state == TaskInstanceState.SCHEDULED
    assert task_instance.next_method != "__fail__"
    assert task_instance.next_kwargs == {
        "event": {"ti_count": 1, "dr_count": 1, "task_states": {"test": {"parent_task": "success"}}}
    }


def test_update_triggers_prevents_duplicate_creation_queue_entries(session, supervisor_builder):
    """
    Test that update_triggers prevents adding triggers to the creation queue
    if they are already queued for creation.
    """
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    dag_model, run, trigger_orm, task_instance = create_trigger_in_db(session, trigger)

    supervisor = supervisor_builder()

    # First call to update_triggers should add the trigger to creating_triggers
    supervisor.update_triggers({trigger_orm.id})
    assert len(supervisor.creating_triggers) == 1
    assert supervisor.creating_triggers[0].id == trigger_orm.id

    # Second call to update_triggers with the same trigger_id should not add it again
    supervisor.update_triggers({trigger_orm.id})
    assert len(supervisor.creating_triggers) == 1
    assert supervisor.creating_triggers[0].id == trigger_orm.id

    # Verify that the trigger is not in running_triggers yet (it's still queued)
    assert trigger_orm.id not in supervisor.running_triggers

    # Verify that the trigger is not in any other tracking sets
    assert trigger_orm.id not in supervisor.cancelling_triggers
    assert not any(trigger_id == trigger_orm.id for trigger_id, _ in supervisor.events)
    assert not any(trigger_id == trigger_orm.id for trigger_id, _ in supervisor.failed_triggers)


def test_update_triggers_prevents_duplicate_creation_queue_entries_with_multiple_triggers(
    session, supervisor_builder, dag_maker
):
    """
    Test that update_triggers prevents adding multiple triggers to the creation queue
    if they are already queued for creation.
    """
    trigger1 = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger2 = TimeDeltaTrigger(datetime.timedelta(days=14))

    dag_model1, run1, trigger_orm1, task_instance1 = create_trigger_in_db(session, trigger1)

    with dag_maker("test_dag_2"):
        EmptyOperator(task_id="test_ti_2")

    run2 = dag_maker.create_dagrun()
    trigger_orm2 = Trigger.from_object(trigger2)
    ti2 = run2.task_instances[0]
    session.add(trigger_orm2)
    session.flush()
    ti2.trigger_id = trigger_orm2.id
    session.merge(ti2)
    session.flush()
    # Create a supervisor
    supervisor = supervisor_builder()

    # First call to update_triggers should add both triggers to creating_triggers
    supervisor.update_triggers({trigger_orm1.id, trigger_orm2.id})
    assert len(supervisor.creating_triggers) == 2
    trigger_ids = {trigger.id for trigger in supervisor.creating_triggers}
    assert trigger_orm1.id in trigger_ids
    assert trigger_orm2.id in trigger_ids

    # Second call to update_triggers with the same trigger_ids should not add them again
    supervisor.update_triggers({trigger_orm1.id, trigger_orm2.id})
    assert len(supervisor.creating_triggers) == 2
    trigger_ids = {trigger.id for trigger in supervisor.creating_triggers}
    assert trigger_orm1.id in trigger_ids
    assert trigger_orm2.id in trigger_ids

    # Third call with just one trigger should not add duplicates
    supervisor.update_triggers({trigger_orm1.id})
    assert len(supervisor.creating_triggers) == 2
    trigger_ids = {trigger.id for trigger in supervisor.creating_triggers}
    assert trigger_orm1.id in trigger_ids
    assert trigger_orm2.id in trigger_ids


def test_update_triggers_skips_when_ti_has_no_dag_version(session, supervisor_builder, dag_maker):
    """
    Ensure supervisor skips creating a trigger when the linked TaskInstance has no dag_version_id.
    """
    with dag_maker(dag_id="test_no_dag_version"):
        EmptyOperator(task_id="t1")
    dr = dag_maker.create_dagrun()
    ti = dr.task_instances[0]

    # Create a Trigger and link it to the TaskInstance
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm = Trigger.from_object(trigger)
    session.add(trigger_orm)
    session.flush()

    ti.trigger_id = trigger_orm.id
    # Explicitly remove dag_version_id
    ti.dag_version_id = None
    session.merge(ti)
    session.commit()

    supervisor = supervisor_builder()

    # Attempt to enqueue creation of this trigger
    supervisor.update_triggers({trigger_orm.id})

    # Assert that nothing was queued for creation and no subprocess writes happened
    assert len(supervisor.creating_triggers) == 0
    assert trigger_orm.id not in supervisor.running_triggers
    supervisor.stdin.write.assert_not_called()


class TestTriggererMessageTypes:
    def test_message_types_in_triggerer(self):
        """
        Test that ToSupervisor is a superset of ToTriggerSupervisor and ToTask is a superset of ToTriggerRunner.

        This test ensures that when new message types are added to ToSupervisor or ToTask,
        they are also properly handled in ToTriggerSupervisor and ToTriggerSupervisor.
        """

        def get_type_names(union_type):
            union_args = typing.get_args(union_type.__args__[0])
            return {arg.__name__ for arg in union_args}

        supervisor_types = get_type_names(ToSupervisor)
        task_types = get_type_names(ToTask)

        trigger_supervisor_types = get_type_names(ToTriggerSupervisor)
        trigger_runner_types = get_type_names(ToTriggerRunner)

        in_supervisor_but_not_in_trigger_supervisor = {
            "DeferTask",
            "GetAssetByName",
            "GetAssetByUri",
            "GetAssetEventByAsset",
            "GetAssetEventByAssetAlias",
            "GetDagRun",
            "GetPrevSuccessfulDagRun",
            "GetPreviousDagRun",
            "GetTaskBreadcrumbs",
            "GetTaskRescheduleStartDate",
            "GetXComCount",
            "GetXComSequenceItem",
            "GetXComSequenceSlice",
            "RescheduleTask",
            "RetryTask",
            "SetRenderedFields",
            "SkipDownstreamTasks",
            "SucceedTask",
            "ValidateInletsAndOutlets",
            "TaskState",
            "TriggerDagRun",
            "ResendLoggingFD",
            "CreateHITLDetailPayload",
            "SetRenderedMapIndex",
        }

        in_task_but_not_in_trigger_runner = {
            "AssetResult",
            "AssetEventsResult",
            "DagRunResult",
            "SentFDs",
            "StartupDetails",
            "TaskBreadcrumbsResult",
            "TaskRescheduleStartDate",
            "InactiveAssetsResult",
            "CreateHITLDetailPayload",
            "PrevSuccessfulDagRunResult",
            "XComCountResponse",
            "XComSequenceIndexResult",
            "XComSequenceSliceResult",
            "PreviousDagRunResult",
            "PreviousTIResult",
            "HITLDetailRequestResult",
        }

        supervisor_diff = (
            supervisor_types - trigger_supervisor_types - in_supervisor_but_not_in_trigger_supervisor
        )
        task_diff = task_types - trigger_runner_types - in_task_but_not_in_trigger_runner

        assert not supervisor_diff, (
            f"New message types in ToSupervisor not handled in ToTriggerSupervisor: "
            f"{len(supervisor_diff)} types found:\n"
            + "\n".join(f"  - {t}" for t in sorted(supervisor_diff))
            + "\n\nEither handle these types in ToTriggerSupervisor or update in_supervisor_but_not_in_trigger_supervisor list."
        )

        assert not task_diff, (
            f"New message types in ToTask not handled in ToTriggerRunner: "
            f"{len(task_diff)} types found:\n"
            + "\n".join(f"  - {t}" for t in sorted(task_diff))
            + "\n\nEither handle these types in ToTriggerRunner or update in_task_but_not_in_trigger_runner list."
        )
