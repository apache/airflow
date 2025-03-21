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

import contextlib
import json
import os
import uuid
from collections.abc import Iterable
from datetime import datetime, timedelta
from pathlib import Path
from socket import socketpair
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import patch

import pytest
from task_sdk import FAKE_BUNDLE
from uuid6 import uuid7

from airflow.decorators import task as task_decorator
from airflow.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowSensorTimeout,
    AirflowSkipException,
    AirflowTaskTerminated,
    DownstreamTasksSkipped,
)
from airflow.listeners import hookimpl
from airflow.listeners.listener import get_listener_manager
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, BaseOperator, Connection, dag as dag_decorator, get_current_context
from airflow.sdk.api.datamodels._generated import (
    AssetEventResponse,
    AssetProfile,
    AssetResponse,
    DagRunState,
    TaskInstance,
    TerminalTIState,
)
from airflow.sdk.definitions.asset import Asset, AssetAlias, Dataset, Model
from airflow.sdk.definitions.param import DagParam
from airflow.sdk.definitions.variable import Variable
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import (
    AssetEventsResult,
    BundleInfo,
    ConnectionResult,
    DagRunStateResult,
    DeferTask,
    ErrorResponse,
    GetConnection,
    GetDagRunState,
    GetVariable,
    GetXCom,
    OKResponse,
    PrevSuccessfulDagRunResult,
    RuntimeCheckOnTask,
    SetRenderedFields,
    SetXCom,
    SkipDownstreamTasks,
    StartupDetails,
    SucceedTask,
    TaskState,
    TriggerDagRun,
    VariableResult,
    XComResult,
)
from airflow.sdk.execution_time.context import (
    ConnectionAccessor,
    InletEventsAccessors,
    MacrosAccessor,
    OutletEventAccessors,
    VariableAccessor,
)
from airflow.sdk.execution_time.task_runner import (
    CommsDecoder,
    RuntimeTaskInstance,
    TaskRunnerMarker,
    _push_xcom_if_needed,
    _xcom_push,
    finalize,
    parse,
    run,
    startup,
)
from airflow.sdk.execution_time.xcom import XCom
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState
from airflow.utils.types import NOTSET, ArgNotSet

from tests_common.test_utils.mock_operators import AirflowLink

if TYPE_CHECKING:
    from kgb import SpyAgency


def get_inline_dag(dag_id: str, task: BaseOperator) -> DAG:
    """Creates an inline dag and returns it based on dag_id and task."""
    dag = DAG(dag_id=dag_id, start_date=timezone.datetime(2024, 12, 3))
    task.dag = dag

    return dag


class CustomOperator(BaseOperator):
    def execute(self, context):
        task_id = context["task_instance"].task_id
        print(f"Hello World {task_id}!")


class TestCommsDecoder:
    """Test the communication between the subprocess and the "supervisor"."""

    @pytest.mark.usefixtures("disable_capturing")
    def test_recv_StartupDetails(self):
        r, w = socketpair()
        # Create a valid FD for the decoder to open
        _, w2 = socketpair()

        w.makefile("wb").write(
            b'{"type":"StartupDetails", "ti": {'
            b'"id": "4d828a62-a417-4936-a7a6-2b3fabacecab", "task_id": "a", "try_number": 1, "run_id": "b", '
            b'"dag_id": "c"}, "ti_context":{"dag_run":{"dag_id":"c","run_id":"b","logical_date":"2024-12-01T01:00:00Z",'
            b'"data_interval_start":"2024-12-01T00:00:00Z","data_interval_end":"2024-12-01T01:00:00Z",'
            b'"start_date":"2024-12-01T01:00:00Z","run_after":"2024-12-01T01:00:00Z","end_date":null,"run_type":"manual","conf":null},'
            b'"max_tries":0,"should_retry":false,"variables":null,"connections":null},"file": "/dev/null",'
            b'"start_date":"2024-12-01T01:00:00Z", "dag_rel_path": "/dev/null", "bundle_info": {"name": '
            b'"any-name", "version": "any-version"}, "requests_fd": '
            + str(w2.fileno()).encode("ascii")
            + b"}\n"
        )

        decoder = CommsDecoder(input=r.makefile("r"))

        msg = decoder.get_message()
        assert isinstance(msg, StartupDetails)
        assert msg.ti.id == uuid.UUID("4d828a62-a417-4936-a7a6-2b3fabacecab")
        assert msg.ti.task_id == "a"
        assert msg.ti.dag_id == "c"
        assert msg.dag_rel_path == "/dev/null"
        assert msg.bundle_info == BundleInfo(name="any-name", version="any-version")
        assert msg.start_date == timezone.datetime(2024, 12, 1, 1)

        # Since this was a StartupDetails message, the decoder should open the other socket
        assert decoder.request_socket is not None
        assert decoder.request_socket.writable()
        assert decoder.request_socket.fileno() == w2.fileno()


def test_parse(test_dags_dir: Path, make_ti_context):
    """Test that checks parsing of a basic dag with an un-mocked parse."""
    what = StartupDetails(
        ti=TaskInstance(
            id=uuid7(),
            task_id="a",
            dag_id="super_basic",
            run_id="c",
            try_number=1,
        ),
        dag_rel_path="super_basic.py",
        bundle_info=BundleInfo(name="my-bundle", version=None),
        requests_fd=0,
        ti_context=make_ti_context(),
        start_date=timezone.utcnow(),
    )

    with patch.dict(
        os.environ,
        {
            "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(
                [
                    {
                        "name": "my-bundle",
                        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                        "kwargs": {"path": str(test_dags_dir), "refresh_interval": 1},
                    }
                ]
            ),
        },
    ):
        ti = parse(what)

    assert ti.task
    assert ti.task.dag
    assert isinstance(ti.task, BaseOperator)
    assert isinstance(ti.task.dag, DAG)


def test_run_deferred_basic(time_machine, create_runtime_ti, mock_supervisor_comms):
    """Test that a task can transition to a deferred state."""
    from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync

    # Use the time machine to set the current time
    instant = timezone.datetime(2024, 11, 22)
    task = DateTimeSensorAsync(
        task_id="async",
        target_time=str(instant + timedelta(seconds=3)),
        poke_interval=60,
        timeout=600,
    )
    time_machine.move_to(instant, tick=False)

    # Expected DeferTask
    expected_defer_task = DeferTask(
        state="deferred",
        classpath="airflow.providers.standard.triggers.temporal.DateTimeTrigger",
        # Since we are in the task process here, we expect this to have not been encoded by serde yet
        trigger_kwargs={
            "end_from_trigger": False,
            "moment": instant + timedelta(seconds=3),
        },
        trigger_timeout=None,
        next_method="execute_complete",
        next_kwargs={},
    )

    # Run the task
    ti = create_runtime_ti(dag_id="basic_deferred_run", task=task)
    state, msg, err = run(ti, log=mock.MagicMock())

    # send_request will only be called when the TaskDeferred exception is raised
    mock_supervisor_comms.send_request.assert_any_call(msg=expected_defer_task, log=mock.ANY)


def test_run_downstream_skipped(mocked_parse, create_runtime_ti, mock_supervisor_comms):
    listener = TestTaskRunnerCallsListeners.CustomListener()
    get_listener_manager().add_listener(listener)

    class CustomOperator(BaseOperator):
        def execute(self, context):
            raise DownstreamTasksSkipped(tasks=["task1", "task2"])

    task = CustomOperator(
        task_id="test_task_runner_calls_listeners_skipped", do_xcom_push=True, multiple_outputs=True
    )
    ti = create_runtime_ti(task=task)

    log = mock.MagicMock()
    run(ti, log=log)
    finalize(ti, log=mock.MagicMock(), state=TerminalTIState.SUCCESS)

    assert listener.state == [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS]
    log.info.assert_called_with("Skipping downstream tasks.")
    mock_supervisor_comms.send_request.assert_any_call(
        log=mock.ANY, msg=SkipDownstreamTasks(tasks=["task1", "task2"], type="SkipDownstreamTasks")
    )


def test_resume_from_deferred(time_machine, create_runtime_ti, mock_supervisor_comms, spy_agency: SpyAgency):
    from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync

    instant_str = "2024-09-30T12:00:00Z"
    instant = timezone.parse(instant_str)
    task = DateTimeSensorAsync(
        task_id="async",
        target_time=instant + timedelta(seconds=3),
        poke_interval=60,
        timeout=600,
    )

    ti = create_runtime_ti(dag_id="basic_deferred_run", task=task)
    ti._ti_context_from_server.next_method = "execute_complete"
    ti._ti_context_from_server.next_kwargs = {
        "__type": "dict",
        "__var": {"event": {"__type": "datetime", "__var": 1727697600.0}},
    }

    spy = spy_agency.spy_on(task.execute_complete)
    state, msg, err = run(ti, log=mock.MagicMock())
    assert err is None
    assert state == TaskInstanceState.SUCCESS

    spy_agency.assert_spy_called_with(spy, mock.ANY, event=instant)


def test_run_basic_skipped(time_machine, create_runtime_ti, mock_supervisor_comms):
    """Test running a basic task that marks itself skipped."""

    task = PythonOperator(
        task_id="skip",
        python_callable=lambda: (_ for _ in ()).throw(
            AirflowSkipException("This task is being skipped intentionally."),
        ),
    )

    ti = create_runtime_ti(dag_id="basic_skipped", task=task)

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    mock_supervisor_comms.send_request.assert_called_with(
        msg=TaskState(state=TerminalTIState.SKIPPED, end_date=instant), log=mock.ANY
    )


def test_run_raises_base_exception(time_machine, create_runtime_ti, mock_supervisor_comms):
    """Test running a basic task that raises a base exception which should send fail_with_retry state."""

    task = PythonOperator(
        task_id="zero_division_error",
        python_callable=lambda: 1 / 0,
    )

    ti = create_runtime_ti(dag_id="basic_dag_base_exception", task=task)

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    mock_supervisor_comms.send_request.assert_called_with(
        msg=TaskState(
            state=TerminalTIState.FAILED,
            end_date=instant,
        ),
        log=mock.ANY,
    )


def test_run_raises_system_exit(time_machine, create_runtime_ti, mock_supervisor_comms):
    """Test running a basic task that exits with SystemExit exception."""

    task = PythonOperator(
        task_id="system_exit_task",
        python_callable=lambda: exit(10),
    )

    ti = create_runtime_ti(task=task, dag_id="basic_dag_system_exit")

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    mock_supervisor_comms.send_request.assert_called_with(
        msg=TaskState(
            state=TerminalTIState.FAILED,
            end_date=instant,
        ),
        log=mock.ANY,
    )


def test_run_raises_airflow_exception(time_machine, create_runtime_ti, mock_supervisor_comms):
    """Test running a basic task that exits with AirflowException."""

    task = PythonOperator(
        task_id="af_exception_task",
        python_callable=lambda: (_ for _ in ()).throw(
            AirflowException("Oops! I am failing with AirflowException!"),
        ),
    )

    ti = create_runtime_ti(task=task, dag_id="basic_dag_af_exception")

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    mock_supervisor_comms.send_request.assert_called_with(
        msg=TaskState(
            state=TerminalTIState.FAILED,
            end_date=instant,
        ),
        log=mock.ANY,
    )


def test_run_task_timeout(time_machine, create_runtime_ti, mock_supervisor_comms):
    """Test running a basic task that times out."""
    from time import sleep

    task = PythonOperator(
        task_id="sleep",
        execution_timeout=timedelta(milliseconds=10),
        python_callable=lambda: sleep(2),
    )

    ti = create_runtime_ti(task=task, dag_id="basic_dag_time_out")

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    # this state can only be reached if the try block passed down the exception to handler of AirflowTaskTimeout
    mock_supervisor_comms.send_request.assert_called_with(
        msg=TaskState(
            state=TerminalTIState.FAILED,
            end_date=instant,
        ),
        log=mock.ANY,
    )


def test_basic_templated_dag(mocked_parse, make_ti_context, mock_supervisor_comms, spy_agency):
    """Test running a DAG with templated task."""
    from airflow.providers.standard.operators.bash import BashOperator

    task = BashOperator(
        task_id="templated_task",
        bash_command="echo 'Logical date is {{ logical_date }}'",
    )

    what = StartupDetails(
        ti=TaskInstance(
            id=uuid7(),
            task_id="templated_task",
            dag_id="basic_templated_dag",
            run_id="c",
            try_number=1,
        ),
        bundle_info=FAKE_BUNDLE,
        dag_rel_path="",
        requests_fd=0,
        ti_context=make_ti_context(),
        start_date=timezone.utcnow(),
    )
    ti = mocked_parse(what, "basic_templated_dag", task)

    # Ensure that task is locked for execution
    spy_agency.spy_on(task.prepare_for_execution)
    assert not task._lock_for_execution

    # mock_supervisor_comms.get_message.return_value = what
    run(ti, log=mock.Mock())

    spy_agency.assert_spy_called(task.prepare_for_execution)
    assert ti.task._lock_for_execution
    assert ti.task is not task, "ti.task should be a copy of the original task"

    mock_supervisor_comms.send_request.assert_any_call(
        msg=SetRenderedFields(
            rendered_fields={
                "bash_command": "echo 'Logical date is 2024-12-01 01:00:00+00:00'",
                "cwd": None,
                "env": None,
            }
        ),
        log=mock.ANY,
    )


@pytest.mark.parametrize(
    ["task_params", "expected_rendered_fields"],
    [
        pytest.param(
            {"op_args": [], "op_kwargs": {}, "templates_dict": None},
            {"op_args": [], "op_kwargs": {}, "templates_dict": None},
            id="no_templates",
        ),
        pytest.param(
            {
                "op_args": ["arg1", "arg2", 1, 2, 3.75, {"key": "value"}],
                "op_kwargs": {"key1": "value1", "key2": 99.0, "key3": {"nested_key": "nested_value"}},
            },
            {
                "op_args": ["arg1", "arg2", 1, 2, 3.75, {"key": "value"}],
                "op_kwargs": {"key1": "value1", "key2": 99.0, "key3": {"nested_key": "nested_value"}},
            },
            id="mixed_types",
        ),
        pytest.param(
            {"my_tup": (1, 2), "my_set": {1, 2, 3}},
            {"my_tup": "(1, 2)", "my_set": "{1, 2, 3}"},
            id="tuples_and_sets",
        ),
        pytest.param(
            {"op_args": [("a", "b", "c")], "op_kwargs": {}, "templates_dict": None},
            {"op_args": [["a", "b", "c"]], "op_kwargs": {}, "templates_dict": None},
            id="nested_tuples_within_lists",
        ),
        pytest.param(
            {
                "op_args": [
                    [
                        ("t0.task_id", "t1.task_id", "branch one"),
                        ("t0.task_id", "t2.task_id", "branch two"),
                        ("t0.task_id", "t3.task_id", "branch three"),
                    ]
                ],
                "op_kwargs": {},
                "templates_dict": None,
            },
            {
                "op_args": [
                    [
                        ["t0.task_id", "t1.task_id", "branch one"],
                        ["t0.task_id", "t2.task_id", "branch two"],
                        ["t0.task_id", "t3.task_id", "branch three"],
                    ]
                ],
                "op_kwargs": {},
                "templates_dict": None,
            },
            id="nested_tuples_within_lists_higher_nesting",
        ),
    ],
)
def test_startup_and_run_dag_with_rtif(
    mocked_parse, task_params, expected_rendered_fields, make_ti_context, time_machine, mock_supervisor_comms
):
    """Test startup of a DAG with various rendered templated fields."""

    class CustomOperator(BaseOperator):
        template_fields = tuple(task_params.keys())

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            for key, value in task_params.items():
                setattr(self, key, value)

        def execute(self, context):
            for key in self.template_fields:
                print(key, getattr(self, key))

    task = CustomOperator(task_id="templated_task")
    instant = timezone.datetime(2024, 12, 3, 10, 0)

    what = StartupDetails(
        ti=TaskInstance(
            id=uuid7(),
            task_id="templated_task",
            dag_id="basic_dag",
            run_id="c",
            try_number=1,
        ),
        dag_rel_path="",
        bundle_info=FAKE_BUNDLE,
        requests_fd=0,
        ti_context=make_ti_context(),
        start_date=timezone.utcnow(),
    )
    mocked_parse(what, "basic_dag", task)

    time_machine.move_to(instant, tick=False)

    mock_supervisor_comms.get_message.return_value = what

    run(*startup())
    expected_calls = [
        mock.call.send_request(
            msg=SetRenderedFields(rendered_fields=expected_rendered_fields),
            log=mock.ANY,
        ),
        mock.call.send_request(
            msg=SucceedTask(
                end_date=instant,
                state=TerminalTIState.SUCCESS,
                task_outlets=[],
                outlet_events=[],
            ),
            log=mock.ANY,
        ),
    ]
    mock_supervisor_comms.assert_has_calls(expected_calls)


@pytest.mark.parametrize(
    ["command", "rendered_command"],
    [
        ("{{ task.task_id }}", "templated_task"),
        ("{{ run_id }}", "c"),
        ("{{ logical_date }}", "2024-12-01 01:00:00+00:00"),
    ],
)
@pytest.mark.usefixtures("mock_supervisor_comms")
def test_startup_and_run_dag_with_templated_fields(
    command, rendered_command, create_runtime_ti, time_machine
):
    """Test startup of a DAG with various templated fields."""
    from airflow.providers.standard.operators.bash import BashOperator

    task = BashOperator(task_id="templated_task", bash_command=command)

    ti = create_runtime_ti(
        task=task, dag_id="basic_dag", logical_date="2024-12-01 01:00:00+00:00", run_id="c"
    )

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)
    run(ti, log=mock.MagicMock())
    assert ti.task.bash_command == rendered_command


def test_get_context_in_task(create_runtime_ti, time_machine, mock_supervisor_comms):
    """Test that the `get_current_context` & `set_current_context` work correctly."""

    class MyContextAssertOperator(BaseOperator):
        def execute(self, context):
            # Ensure the context returned by get_current_context is the same as the
            # context passed to the operator
            assert context == get_current_context()

    task = MyContextAssertOperator(task_id="assert_context")

    ti = create_runtime_ti(task=task)

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    # Ensure the task is Successful
    mock_supervisor_comms.send_request.assert_called_once_with(
        msg=SucceedTask(state=TerminalTIState.SUCCESS, end_date=instant, task_outlets=[], outlet_events=[]),
        log=mock.ANY,
    )


@pytest.mark.parametrize(
    ["dag_id", "task_id", "fail_with_exception"],
    [
        pytest.param(
            "basic_failed", "fail-exception", AirflowFailException("Oops. Failing by AirflowFailException!")
        ),
        pytest.param(
            "basic_failed2",
            "sensor-timeout-exception",
            AirflowSensorTimeout("Oops. Failing by AirflowSensorTimeout!"),
        ),
        pytest.param(
            "basic_failed3",
            "task-terminated-exception",
            AirflowTaskTerminated("Oops. Failing by AirflowTaskTerminated!"),
        ),
    ],
)
def test_run_basic_failed(
    time_machine, create_runtime_ti, dag_id, task_id, fail_with_exception, mock_supervisor_comms
):
    """Test running a basic task that marks itself as failed by raising exception."""

    class CustomOperator(BaseOperator):
        def __init__(self, e, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.e = e

        def execute(self, context):
            print(f"raising exception {self.e}")
            raise self.e

    task = CustomOperator(task_id=task_id, e=fail_with_exception)

    ti = create_runtime_ti(task=task, dag_id=dag_id)

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    mock_supervisor_comms.send_request.assert_called_once_with(
        msg=TaskState(state=TerminalTIState.FAILED, end_date=instant), log=mock.ANY
    )


def test_dag_parsing_context(make_ti_context, mock_supervisor_comms, monkeypatch, test_dags_dir):
    """
    Test that the DAG parsing context is correctly set during the startup process.

    This test verifies that the DAG and task IDs are correctly set in the parsing context
    when a DAG is started up.
    """
    dag_id = "dag_parsing_context_test"
    task_id = "conditional_task"

    what = StartupDetails(
        ti=TaskInstance(id=uuid7(), task_id=task_id, dag_id=dag_id, run_id="c", try_number=1),
        dag_rel_path="dag_parsing_context.py",
        bundle_info=BundleInfo(name="my-bundle", version=None),
        requests_fd=0,
        ti_context=make_ti_context(dag_id=dag_id, run_id="c"),
        start_date=timezone.utcnow(),
    )

    mock_supervisor_comms.get_message.return_value = what

    # Set the environment variable for DAG bundles
    # We use the DAG defined in `task_sdk/tests/dags/dag_parsing_context.py` for this test!
    dag_bundle_val = json.dumps(
        [
            {
                "name": "my-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(test_dags_dir), "refresh_interval": 1},
            }
        ]
    )

    monkeypatch.setenv("AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST", dag_bundle_val)
    ti, _ = startup()

    # Presence of `conditional_task` below means DAG ID is properly set in the parsing context!
    # Check the dag file for the actual logic!
    assert ti.task.dag.task_dict.keys() == {"visible_task", "conditional_task"}


@pytest.mark.parametrize(
    ["task_outlets", "expected_msg"],
    [
        pytest.param(
            [Asset(name="s3://bucket/my-task", uri="s3://bucket/my-task")],
            SucceedTask(
                state="success",
                end_date=timezone.datetime(2024, 12, 3, 10, 0),
                task_outlets=[
                    AssetProfile(name="s3://bucket/my-task", uri="s3://bucket/my-task", type="Asset")
                ],
                outlet_events=[],
            ),
            id="asset",
        ),
        pytest.param(
            [Dataset(name="s3://bucket/my-task", uri="s3://bucket/my-task")],
            SucceedTask(
                state="success",
                end_date=timezone.datetime(2024, 12, 3, 10, 0),
                task_outlets=[
                    AssetProfile(name="s3://bucket/my-task", uri="s3://bucket/my-task", type="Asset")
                ],
                outlet_events=[],
            ),
            id="dataset",
        ),
        pytest.param(
            [Model(name="s3://bucket/my-task", uri="s3://bucket/my-task")],
            SucceedTask(
                state="success",
                end_date=timezone.datetime(2024, 12, 3, 10, 0),
                task_outlets=[
                    AssetProfile(name="s3://bucket/my-task", uri="s3://bucket/my-task", type="Asset")
                ],
                outlet_events=[],
            ),
            id="model",
        ),
        pytest.param(
            [Asset.ref(name="s3://bucket/my-task")],
            SucceedTask(
                state="success",
                end_date=timezone.datetime(2024, 12, 3, 10, 0),
                task_outlets=[AssetProfile(name="s3://bucket/my-task", type="AssetNameRef")],
                outlet_events=[],
            ),
            id="name-ref",
        ),
        pytest.param(
            [Asset.ref(uri="s3://bucket/my-task")],
            SucceedTask(
                state="success",
                end_date=timezone.datetime(2024, 12, 3, 10, 0),
                task_outlets=[AssetProfile(uri="s3://bucket/my-task", type="AssetUriRef")],
                outlet_events=[],
            ),
            id="uri-ref",
        ),
        pytest.param(
            [AssetAlias(name="example-alias", group="asset")],
            SucceedTask(
                state="success",
                end_date=timezone.datetime(2024, 12, 3, 10, 0),
                task_outlets=[AssetProfile(name="example-alias", type="AssetAlias")],
                outlet_events=[],
            ),
            id="asset-alias",
        ),
    ],
)
def test_run_with_asset_outlets(
    time_machine, create_runtime_ti, mock_supervisor_comms, task_outlets, expected_msg
):
    """Test running a basic task that contains asset outlets."""
    from airflow.providers.standard.operators.bash import BashOperator

    task = BashOperator(
        outlets=task_outlets,
        task_id="asset-outlet-task",
        bash_command="echo 'hi'",
    )

    ti = create_runtime_ti(task=task, dag_id="dag_with_asset_outlet_task")
    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)
    mock_supervisor_comms.get_message.return_value = OKResponse(
        ok=True,
    )

    run(ti, log=mock.MagicMock())

    mock_supervisor_comms.send_request.assert_any_call(msg=expected_msg, log=mock.ANY)


def test_run_with_asset_inlets(create_runtime_ti, mock_supervisor_comms):
    """Test running a basic task that contains asset inlets."""
    asset_event_resp = AssetEventResponse(
        id=1,
        created_dagruns=[],
        timestamp=datetime.now(),
        asset=AssetResponse(name="test", uri="test", group="asset"),
    )
    events_result = AssetEventsResult(asset_events=[asset_event_resp])
    mock_supervisor_comms.get_message.return_value = events_result

    from airflow.providers.standard.operators.bash import BashOperator

    task = BashOperator(
        inlets=[Asset(name="test", uri="test://uri"), AssetAlias(name="alias-name")],
        task_id="asset-outlet-task",
        bash_command="echo 0",
    )

    ti = create_runtime_ti(task=task, dag_id="dag_with_asset_outlet_task")
    run(ti, log=mock.MagicMock())
    inlet_events = ti.get_template_context()["inlet_events"]

    # access the asset events of Asset(name="test", uri="test://uri")
    assert inlet_events[0] == [asset_event_resp]
    assert inlet_events[-2] == [asset_event_resp]
    assert inlet_events[Asset(name="test", uri="test://uri")] == [asset_event_resp]

    # access the asset events of AssetAlias(name="alias-name")
    assert inlet_events[1] == [asset_event_resp]
    assert inlet_events[-1] == [asset_event_resp]
    assert inlet_events[AssetAlias(name="alias-name")] == [asset_event_resp]

    # access with invalid index
    with pytest.raises(IndexError):
        inlet_events[2]

    with pytest.raises(IndexError):
        inlet_events[-3]

    with pytest.raises(KeyError):
        inlet_events[Asset(name="no such asset in inlets")]


@pytest.mark.parametrize(
    ["ok", "last_expected_msg"],
    [
        pytest.param(
            True,
            SucceedTask(
                end_date=timezone.datetime(2024, 12, 3, 10, 0),
                task_outlets=[
                    AssetProfile(name="name", uri="s3://bucket/my-task", type="Asset"),
                    AssetProfile(name="new-name", uri="s3://bucket/my-task", type="Asset"),
                ],
                outlet_events=[],
            ),
            id="runtime_checks_pass",
        ),
        pytest.param(
            False,
            TaskState(
                state=TerminalTIState.FAILED,
                end_date=timezone.datetime(2024, 12, 3, 10, 0),
            ),
            id="runtime_checks_fail",
        ),
    ],
)
def test_run_with_inlets_and_outlets(
    create_runtime_ti, mock_supervisor_comms, time_machine, ok, last_expected_msg
):
    """Test running a basic tasks with inlets and outlets."""

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    from airflow.providers.standard.operators.bash import BashOperator

    task = BashOperator(
        outlets=[
            Asset(name="name", uri="s3://bucket/my-task"),
            Asset(name="new-name", uri="s3://bucket/my-task"),
        ],
        inlets=[
            Asset(name="name", uri="s3://bucket/my-task"),
            Asset(name="new-name", uri="s3://bucket/my-task"),
        ],
        task_id="inlets-and-outlets",
        bash_command="echo 'hi'",
    )

    ti = create_runtime_ti(task=task, dag_id="dag_with_inlets_and_outlets")
    mock_supervisor_comms.get_message.return_value = OKResponse(
        ok=ok,
    )

    run(ti, log=mock.MagicMock())

    expected = RuntimeCheckOnTask(
        inlets=[
            AssetProfile(name="name", uri="s3://bucket/my-task", type="Asset"),
            AssetProfile(name="new-name", uri="s3://bucket/my-task", type="Asset"),
        ],
        outlets=[
            AssetProfile(name="name", uri="s3://bucket/my-task", type="Asset"),
            AssetProfile(name="new-name", uri="s3://bucket/my-task", type="Asset"),
        ],
    )
    mock_supervisor_comms.send_request.assert_any_call(msg=expected, log=mock.ANY)
    mock_supervisor_comms.send_request.assert_any_call(msg=last_expected_msg, log=mock.ANY)


@mock.patch("airflow.sdk.execution_time.task_runner.context_to_airflow_vars")
@mock.patch.dict(os.environ, {}, clear=True)
def test_execute_task_exports_env_vars(
    mock_context_to_airflow_vars, create_runtime_ti, mock_supervisor_comms
):
    """Test that _execute_task exports airflow context to environment variables."""

    def test_function():
        return "test function"

    task = PythonOperator(
        task_id="test_task",
        python_callable=test_function,
    )

    ti = create_runtime_ti(task=task, dag_id="dag_with_env_vars")

    mock_env_vars = {"AIRFLOW_CTX_DAG_ID": "test_dag_env_vars", "AIRFLOW_CTX_TASK_ID": "test_env_task"}
    mock_context_to_airflow_vars.return_value = mock_env_vars
    run(ti, log=mock.MagicMock())

    assert os.environ["AIRFLOW_CTX_DAG_ID"] == "test_dag_env_vars"
    assert os.environ["AIRFLOW_CTX_TASK_ID"] == "test_env_task"


class TestRuntimeTaskInstance:
    def test_get_context_without_ti_context_from_server(self, mocked_parse, make_ti_context):
        """Test get_template_context without ti_context_from_server."""

        task = BaseOperator(task_id="hello")
        dag_id = "basic_task"

        # Assign task to DAG
        get_inline_dag(dag_id=dag_id, task=task)

        ti_id = uuid7()
        ti = TaskInstance(
            id=ti_id,
            task_id=task.task_id,
            dag_id=dag_id,
            run_id="test_run",
            try_number=1,
        )
        start_date = timezone.datetime(2025, 1, 1)

        # Keep the context empty
        runtime_ti = RuntimeTaskInstance.model_construct(
            **ti.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=None,
            start_date=start_date,
        )
        context = runtime_ti.get_template_context()

        # Verify the context keys and values
        assert context == {
            "params": {},
            "var": {
                "json": VariableAccessor(deserialize_json=True),
                "value": VariableAccessor(deserialize_json=False),
            },
            "conn": ConnectionAccessor(),
            "dag": runtime_ti.task.dag,
            "inlets": task.inlets,
            "inlet_events": InletEventsAccessors(inlets=[]),
            "macros": MacrosAccessor(),
            "map_index_template": task.map_index_template,
            "outlet_events": OutletEventAccessors(),
            "outlets": task.outlets,
            "run_id": "test_run",
            "task": task,
            "task_instance": runtime_ti,
            "ti": runtime_ti,
        }

    def test_get_context_with_ti_context_from_server(self, create_runtime_ti, mock_supervisor_comms):
        """Test the context keys are added when sent from API server (mocked)"""
        from airflow.utils import timezone

        task = BaseOperator(task_id="hello")

        # Assume the context is sent from the API server
        # `task_sdk/tests/api/test_client.py::test_task_instance_start` checks the context is received
        # from the API server
        runtime_ti = create_runtime_ti(task=task, dag_id="basic_task")

        dr = runtime_ti._ti_context_from_server.dag_run

        mock_supervisor_comms.get_message.return_value = PrevSuccessfulDagRunResult(
            data_interval_end=dr.logical_date - timedelta(hours=1),
            data_interval_start=dr.logical_date - timedelta(hours=2),
            start_date=dr.start_date - timedelta(hours=1),
            end_date=dr.start_date,
        )

        context = runtime_ti.get_template_context()

        assert context == {
            "params": {},
            "var": {
                "json": VariableAccessor(deserialize_json=True),
                "value": VariableAccessor(deserialize_json=False),
            },
            "conn": ConnectionAccessor(),
            "dag": runtime_ti.task.dag,
            "inlets": task.inlets,
            "inlet_events": InletEventsAccessors(inlets=[]),
            "macros": MacrosAccessor(),
            "map_index_template": task.map_index_template,
            "outlet_events": OutletEventAccessors(),
            "outlets": task.outlets,
            "prev_data_interval_end_success": timezone.datetime(2024, 12, 1, 0, 0, 0),
            "prev_data_interval_start_success": timezone.datetime(2024, 11, 30, 23, 0, 0),
            "prev_end_date_success": timezone.datetime(2024, 12, 1, 1, 0, 0),
            "prev_start_date_success": timezone.datetime(2024, 12, 1, 0, 0, 0),
            "run_id": "test_run",
            "task": task,
            "task_instance": runtime_ti,
            "ti": runtime_ti,
            "dag_run": dr,
            "data_interval_end": timezone.datetime(2024, 12, 1, 1, 0, 0),
            "data_interval_start": timezone.datetime(2024, 12, 1, 0, 0, 0),
            "logical_date": timezone.datetime(2024, 12, 1, 1, 0, 0),
            "task_reschedule_count": 0,
            "ds": "2024-12-01",
            "ds_nodash": "20241201",
            "task_instance_key_str": "basic_task__hello__20241201",
            "ts": "2024-12-01T01:00:00+00:00",
            "ts_nodash": "20241201T010000",
            "ts_nodash_with_tz": "20241201T010000+0000",
        }

    def test_lazy_loading_not_triggered_until_accessed(self, create_runtime_ti, mock_supervisor_comms):
        """Ensure lazy-loaded attributes are not resolved until accessed."""
        task = BaseOperator(task_id="hello")
        runtime_ti = create_runtime_ti(task=task, dag_id="basic_task")

        mock_supervisor_comms.get_message.return_value = PrevSuccessfulDagRunResult(
            data_interval_end=timezone.datetime(2025, 1, 1, 2, 0, 0),
            data_interval_start=timezone.datetime(2025, 1, 1, 1, 0, 0),
            start_date=timezone.datetime(2025, 1, 1, 1, 0, 0),
            end_date=timezone.datetime(2025, 1, 1, 2, 0, 0),
        )

        context = runtime_ti.get_template_context()

        # Assert lazy attributes are not resolved initially
        mock_supervisor_comms.get_message.assert_not_called()

        # Access a lazy-loaded attribute to trigger computation
        assert context["prev_data_interval_start_success"] == timezone.datetime(2025, 1, 1, 1, 0, 0)

        # Now the lazy attribute should trigger the call
        mock_supervisor_comms.get_message.assert_called_once()

    def test_get_connection_from_context(self, create_runtime_ti, mock_supervisor_comms):
        """Test that the connection is fetched from the API server via the Supervisor lazily when accessed"""

        task = BaseOperator(task_id="hello")

        conn = ConnectionResult(
            conn_id="test_conn",
            conn_type="mysql",
            host="mysql",
            schema="airflow",
            login="root",
            password="password",
            port=1234,
            extra='{"extra_key": "extra_value"}',
        )

        runtime_ti = create_runtime_ti(task=task, dag_id="test_get_connection_from_context")
        mock_supervisor_comms.get_message.return_value = conn

        context = runtime_ti.get_template_context()

        # Assert that the connection is not fetched from the API server yet!
        # The connection should be only fetched connection is accessed
        mock_supervisor_comms.send_request.assert_not_called()
        mock_supervisor_comms.get_message.assert_not_called()

        # Access the connection from the context
        conn_from_context = context["conn"].test_conn

        mock_supervisor_comms.send_request.assert_called_once_with(
            log=mock.ANY, msg=GetConnection(conn_id="test_conn")
        )
        mock_supervisor_comms.get_message.assert_called_once_with()

        assert conn_from_context == Connection(
            conn_id="test_conn",
            conn_type="mysql",
            description=None,
            host="mysql",
            schema="airflow",
            login="root",
            password="password",
            port=1234,
            extra='{"extra_key": "extra_value"}',
        )

        dejson_from_conn = conn_from_context.extra_dejson
        assert dejson_from_conn == {"extra_key": "extra_value"}

    def test_template_render(self, create_runtime_ti):
        task = BaseOperator(task_id="test_template_render_task")

        runtime_ti = create_runtime_ti(task=task, dag_id="test_template_render")
        template_context = runtime_ti.get_template_context()
        result = runtime_ti.task.render_template(
            "Task: {{ dag.dag_id }} -> {{ task.task_id }}", template_context
        )
        assert result == "Task: test_template_render -> test_template_render_task"

    @pytest.mark.parametrize(
        ["content", "expected_output"],
        [
            ('{{ conn.get("a_connection").host }}', "hostvalue"),
            ('{{ conn.get("a_connection", "unused_fallback").host }}', "hostvalue"),
            ("{{ conn.a_connection.host }}", "hostvalue"),
            ("{{ conn.a_connection.login }}", "loginvalue"),
            ("{{ conn.a_connection.schema }}", "schemavalues"),
            ("{{ conn.a_connection.password }}", "passwordvalue"),
            ('{{ conn.a_connection.extra_dejson["extra__asana__workspace"] }}', "extra1"),
            ("{{ conn.a_connection.extra_dejson.extra__asana__workspace }}", "extra1"),
        ],
    )
    def test_template_with_connection(
        self, content, expected_output, create_runtime_ti, mock_supervisor_comms
    ):
        """
        Test the availability of connections in templates
        """
        task = BaseOperator(task_id="hello")
        runtime_ti = create_runtime_ti(task=task, dag_id="test_template_with_connection")

        conn = ConnectionResult(
            conn_id="a_connection",
            conn_type="a_type",
            host="hostvalue",
            login="loginvalue",
            password="passwordvalue",
            schema="schemavalues",
            extra='{"extra__asana__workspace": "extra1"}',
        )

        mock_supervisor_comms.get_message.return_value = conn

        context = runtime_ti.get_template_context()
        result = runtime_ti.task.render_template(content, context)
        assert result == expected_output

    @pytest.mark.parametrize(
        ["accessor_type", "var_value", "expected_value"],
        [
            pytest.param("value", "test_value", "test_value"),
            pytest.param(
                "json",
                '{\r\n  "key1": "value1",\r\n  "key2": "value2",\r\n  "enabled": true,\r\n  "threshold": 42\r\n}',
                {"key1": "value1", "key2": "value2", "enabled": True, "threshold": 42},
            ),
        ],
    )
    def test_get_variable_from_context(
        self, create_runtime_ti, mock_supervisor_comms, accessor_type, var_value: str, expected_value
    ):
        """Test that the variable is fetched from the API server via the Supervisor lazily when accessed"""

        task = BaseOperator(task_id="hello")
        runtime_ti = create_runtime_ti(task=task)

        var = VariableResult(key="test_key", value=var_value)

        mock_supervisor_comms.get_message.return_value = var

        context = runtime_ti.get_template_context()

        # Assert that the variable is not fetched from the API server yet!
        # The variable should be only fetched connection is accessed
        mock_supervisor_comms.send_request.assert_not_called()
        mock_supervisor_comms.get_message.assert_not_called()

        # Access the variable from the context
        var_from_context = context["var"][accessor_type].test_key

        mock_supervisor_comms.send_request.assert_called_once_with(
            log=mock.ANY, msg=GetVariable(key="test_key")
        )
        mock_supervisor_comms.get_message.assert_called_once_with()

        assert var_from_context == Variable(key="test_key", value=expected_value)

    @pytest.mark.parametrize(
        "map_indexes",
        [
            pytest.param(-1, id="not_mapped_index"),
            pytest.param(1, id="single_map_index"),
            pytest.param([0, 1], id="multiple_map_indexes"),
            pytest.param((0, 1), id="any_iterable_multi_indexes"),
            pytest.param(None, id="index_none"),
            pytest.param(NOTSET, id="index_not_set"),
        ],
    )
    @pytest.mark.parametrize(
        "task_ids",
        [
            pytest.param("push_task", id="single_task"),
            pytest.param(["push_task1", "push_task2"], id="tid_multiple_tasks"),
            pytest.param({"push_task1", "push_task2"}, id="tid_any_iterable"),
            pytest.param(None, id="tid_none"),
            pytest.param(NOTSET, id="tid_not_set"),
        ],
    )
    def test_xcom_pull(
        self,
        create_runtime_ti,
        mock_supervisor_comms,
        spy_agency,
        task_ids,
        map_indexes,
    ):
        """
        Test that a task makes an expected call to the Supervisor to pull XCom values
        based on various task_ids and map_indexes configurations.
        """
        map_indexes_kwarg = {} if map_indexes is NOTSET else {"map_indexes": map_indexes}
        task_ids_kwarg = {} if task_ids is NOTSET else {"task_ids": task_ids}

        class CustomOperator(BaseOperator):
            def execute(self, context):
                value = context["ti"].xcom_pull(key="key", **task_ids_kwarg, **map_indexes_kwarg)
                print(f"Pulled XCom Value: {value}")

        test_task_id = "pull_task"
        task = CustomOperator(task_id=test_task_id)

        # In case of the specific map_index or None we should check it is passed to TI
        extra_for_ti = {"map_index": map_indexes} if map_indexes in (1, None) else {}
        runtime_ti = create_runtime_ti(task=task, **extra_for_ti)

        mock_supervisor_comms.get_message.return_value = XComResult(key="key", value='"value"')

        run(runtime_ti, log=mock.MagicMock())

        if not isinstance(task_ids, Iterable) or isinstance(task_ids, str):
            task_ids = [task_ids]

        if not isinstance(map_indexes, Iterable):
            map_indexes = [map_indexes]

        for task_id in task_ids:
            # Without task_ids (or None) expected behavior is to pull with calling task_id
            if task_id is None or isinstance(task_id, ArgNotSet):
                task_id = test_task_id
            for map_index in map_indexes:
                if map_index == NOTSET:
                    map_index = -1
                mock_supervisor_comms.send_request.assert_any_call(
                    log=mock.ANY,
                    msg=GetXCom(
                        key="key",
                        dag_id="test_dag",
                        run_id="test_run",
                        task_id=task_id,
                        map_index=map_index,
                    ),
                )

    def test_get_param_from_context(
        self, mocked_parse, make_ti_context, mock_supervisor_comms, create_runtime_ti
    ):
        """Test that a params can be retrieved from context."""

        class CustomOperator(BaseOperator):
            def execute(self, context):
                value = context["params"]
                print("The dag params are", value)

        task = CustomOperator(task_id="print-params")
        runtime_ti = create_runtime_ti(
            dag_id="basic_param_dag",
            task=task,
            conf={
                "x": 3,
                "text": "Hello World!",
                "flag": False,
                "a_simple_list": ["one", "two", "three", "actually one value is made per line"],
            },
        )
        run(runtime_ti, log=mock.MagicMock())

        assert runtime_ti.task.dag.params == {
            "x": 3,
            "text": "Hello World!",
            "flag": False,
            "a_simple_list": ["one", "two", "three", "actually one value is made per line"],
        }

    @pytest.mark.parametrize(
        ("logical_date", "check"),
        (
            pytest.param(None, pytest.raises(KeyError), id="no-logical-date"),
            pytest.param(timezone.datetime(2024, 12, 3), contextlib.nullcontext(), id="with-logical-date"),
        ),
    )
    def test_no_logical_date_key_error(
        self, mocked_parse, make_ti_context, mock_supervisor_comms, create_runtime_ti, logical_date, check
    ):
        """Test that a params can be retrieved from context."""

        class CustomOperator(BaseOperator):
            def execute(self, context):
                for key in ("ds", "ds_nodash", "ts", "ts_nodash", "ts_nodash_with_tz"):
                    with check:
                        context[key]
                # We should always be able to get this
                assert context["task_instance_key_str"]

        task = CustomOperator(task_id="print-params")
        runtime_ti = create_runtime_ti(
            dag_id="basic_param_dag",
            logical_date=logical_date,
            task=task,
            conf={
                "x": 3,
                "text": "Hello World!",
                "flag": False,
                "a_simple_list": ["one", "two", "three", "actually one value is made per line"],
            },
        )
        _, msg, _ = run(runtime_ti, log=mock.MagicMock())
        assert isinstance(msg, SucceedTask)

    def test_task_run_with_operator_extra_links(self, create_runtime_ti, mock_supervisor_comms, time_machine):
        """Test that a task can run with operator extra links defined and can set an xcom."""
        instant = timezone.datetime(2024, 12, 3, 10, 0)
        time_machine.move_to(instant, tick=False)

        class DummyTestOperator(BaseOperator):
            operator_extra_links = (AirflowLink(),)

            def execute(self, context):
                print("Hello from custom operator", self.operator_extra_links)

        task = DummyTestOperator(task_id="task_with_operator_extra_links")

        runtime_ti = create_runtime_ti(task=task)

        run(runtime_ti, log=mock.MagicMock())

        mock_supervisor_comms.send_request.assert_called_once_with(
            msg=SucceedTask(
                state=TerminalTIState.SUCCESS, end_date=instant, task_outlets=[], outlet_events=[]
            ),
            log=mock.ANY,
        )

        finalize(runtime_ti, log=mock.MagicMock(), state=TerminalTIState.SUCCESS)

        mock_supervisor_comms.send_request.assert_any_call(
            msg=SetXCom(
                key="_link_AirflowLink",
                value="https://airflow.apache.org",
                dag_id="test_dag",
                run_id="test_run",
                task_id="task_with_operator_extra_links",
                map_index=-1,
                mapped_length=None,
                type="SetXCom",
            ),
            log=mock.ANY,
        )

    def test_overwrite_rtif_after_execution_sets_rtif(self, create_runtime_ti, mock_supervisor_comms):
        """Test that the RTIF is overwritten after execution for certain operators."""

        class CustomOperator(BaseOperator):
            overwrite_rtif_after_execution = True
            template_fields = ["bash_command"]

            def __init__(self, bash_command, *args, **kwargs):
                self.bash_command = bash_command
                super().__init__(*args, **kwargs)

        task = CustomOperator(task_id="hello", bash_command="echo 'hi'")
        runtime_ti = create_runtime_ti(task=task)

        finalize(runtime_ti, log=mock.MagicMock(), state=TerminalTIState.SUCCESS)

        mock_supervisor_comms.send_request.assert_called_with(
            msg=SetRenderedFields(rendered_fields={"bash_command": "echo 'hi'"}),
            log=mock.ANY,
        )


class TestXComAfterTaskExecution:
    @pytest.mark.parametrize(
        ["do_xcom_push", "should_push_xcom", "expected_xcom_value"],
        [
            pytest.param(False, False, None, id="do_xcom_push_false"),
            pytest.param(True, True, "Hello World!", id="do_xcom_push_true"),
        ],
    )
    def test_xcom_push_flag(
        self,
        create_runtime_ti,
        mock_supervisor_comms,
        spy_agency,
        do_xcom_push: bool,
        should_push_xcom: bool,
        expected_xcom_value,
    ):
        """Test that the do_xcom_push flag controls whether the task pushes to XCom."""

        class CustomOperator(BaseOperator):
            def execute(self, context):
                return "Hello World!"

        task = CustomOperator(task_id="hello", do_xcom_push=do_xcom_push)

        runtime_ti = create_runtime_ti(task=task)

        spy_agency.spy_on(_push_xcom_if_needed, call_original=True)
        spy_agency.spy_on(_xcom_push, call_original=False)

        run(runtime_ti, log=mock.MagicMock())

        spy_agency.assert_spy_called(_push_xcom_if_needed)

        if should_push_xcom:
            spy_agency.assert_spy_called_with(_xcom_push, runtime_ti, "return_value", expected_xcom_value)
        else:
            spy_agency.assert_spy_not_called(_xcom_push)

    def test_xcom_with_multiple_outputs(self, create_runtime_ti, spy_agency):
        """Test that the task pushes to XCom when multiple outputs are returned."""
        result = {"key1": "value1", "key2": "value2"}

        class CustomOperator(BaseOperator):
            def execute(self, context):
                return result

        task = CustomOperator(
            task_id="test_xcom_push_with_multiple_outputs", do_xcom_push=True, multiple_outputs=True
        )

        runtime_ti = create_runtime_ti(task=task)

        spy_agency.spy_on(_xcom_push, call_original=False)
        _push_xcom_if_needed(result=result, ti=runtime_ti, log=mock.MagicMock())

        expected_calls = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("return_value", result),
        ]
        spy_agency.assert_spy_call_count(_xcom_push, len(expected_calls))
        for key, value in expected_calls:
            spy_agency.assert_spy_called_with(_xcom_push, runtime_ti, key, value, mapped_length=None)

    def test_xcom_with_mapped_length(self, create_runtime_ti):
        """Test that the task pushes to XCom with mapped length."""
        result = {"key1": "value1", "key2": "value2"}

        class CustomOperator(BaseOperator):
            def execute(self, context):
                return result

        task = CustomOperator(
            task_id="test_xcom_push_with_mapped_length",
            do_xcom_push=True,
        )

        runtime_ti = create_runtime_ti(task=task)

        with mock.patch.object(XCom, "set") as mock_xcom_set:
            _xcom_push(runtime_ti, "return_value", result, 7)
            mock_xcom_set.assert_called_once_with(
                key="return_value",
                value=result,
                dag_id=runtime_ti.dag_id,
                task_id=runtime_ti.task_id,
                run_id=runtime_ti.run_id,
                map_index=runtime_ti.map_index,
                _mapped_length=7,
            )

    def test_xcom_with_multiple_outputs_and_no_mapping_result(self, create_runtime_ti, spy_agency):
        """Test that error is raised when multiple outputs are returned without mapping."""
        result = "value1"

        class CustomOperator(BaseOperator):
            def execute(self, context):
                return result

        task = CustomOperator(
            task_id="test_xcom_push_with_multiple_outputs", do_xcom_push=True, multiple_outputs=True
        )

        runtime_ti = create_runtime_ti(task=task)

        spy_agency.spy_on(runtime_ti.xcom_push, call_original=False)
        with pytest.raises(
            TypeError,
            match=f"Returned output was type {type(result)} expected dictionary for multiple_outputs",
        ):
            _push_xcom_if_needed(result=result, ti=runtime_ti, log=mock.MagicMock())

    def test_xcom_with_multiple_outputs_and_key_is_not_string(self, create_runtime_ti, spy_agency):
        """Test that error is raised when multiple outputs are returned and key isn't string."""
        result = {2: "value1", "key2": "value2"}

        class CustomOperator(BaseOperator):
            def execute(self, context):
                return result

        task = CustomOperator(
            task_id="test_xcom_push_with_multiple_outputs", do_xcom_push=True, multiple_outputs=True
        )

        runtime_ti = create_runtime_ti(task=task)

        spy_agency.spy_on(runtime_ti.xcom_push, call_original=False)

        with pytest.raises(TypeError) as exc_info:
            _push_xcom_if_needed(result=result, ti=runtime_ti, log=mock.MagicMock())

        assert str(exc_info.value) == (
            f"Returned dictionary keys must be strings when using multiple_outputs, found 2 ({int}) instead"
        )

    def test_xcom_push_to_custom_xcom_backend(
        self, create_runtime_ti, mock_supervisor_comms, mock_xcom_backend
    ):
        """Test that a task pushes a xcom to the custom xcom backend."""

        class CustomOperator(BaseOperator):
            def execute(self, context):
                return "pushing to xcom backend!"

        task = CustomOperator(task_id="pull_task")
        runtime_ti = create_runtime_ti(task=task)

        run(runtime_ti, log=mock.MagicMock())

        mock_xcom_backend.set.assert_called_once_with(
            key="return_value",
            value="pushing to xcom backend!",
            dag_id="test_dag",
            task_id="pull_task",
            run_id="test_run",
            map_index=-1,
            _mapped_length=None,
        )

        # assert that we didn't call the API when XCom backend is configured
        assert not any(
            x
            == mock.call(
                log=mock.ANY,
                msg=SetXCom(
                    key="key",
                    value="pushing to xcom backend!",
                    dag_id="test_dag",
                    run_id="test_run",
                    task_id="pull_task",
                    map_index=-1,
                ),
            )
            for x in mock_supervisor_comms.send_request.call_args_list
        )

    def test_xcom_pull_from_custom_xcom_backend(
        self, create_runtime_ti, mock_supervisor_comms, mock_xcom_backend
    ):
        """Test that a task pulls the expected XCom value if it exists, but from custom xcom backend."""

        class CustomOperator(BaseOperator):
            def execute(self, context):
                value = context["ti"].xcom_pull(task_ids="pull_task", key="key")
                print(f"Pulled XCom Value: {value}")

        task = CustomOperator(task_id="pull_task")
        runtime_ti = create_runtime_ti(task=task)
        run(runtime_ti, log=mock.MagicMock())

        mock_xcom_backend.get_one.assert_called_once_with(
            key="key",
            dag_id="test_dag",
            task_id="pull_task",
            run_id="test_run",
            map_index=-1,
        )

        assert not any(
            x
            == mock.call(
                log=mock.ANY,
                msg=GetXCom(
                    key="key",
                    dag_id="test_dag",
                    run_id="test_run",
                    task_id="pull_task",
                    map_index=-1,
                ),
            )
            for x in mock_supervisor_comms.send_request.call_args_list
        )


class TestDagParamRuntime:
    DEFAULT_ARGS = {
        "owner": "test",
        "depends_on_past": True,
        "start_date": datetime.now(tz=timezone.utc),
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    }
    VALUE = 42

    def test_dag_param_resolves_from_task(self, create_runtime_ti, mock_supervisor_comms, time_machine):
        """Test dagparam resolves on operator execution"""
        instant = timezone.datetime(2024, 12, 3, 10, 0)
        time_machine.move_to(instant, tick=False)

        dag = DAG(dag_id="dag_with_dag_params", start_date=timezone.datetime(2024, 12, 3))
        dag.param("value", default="NOTSET")

        class CustomOperator(BaseOperator):
            def execute(self, context):
                assert dag.params["value"] == "NOTSET"

        task = CustomOperator(task_id="task_with_dag_params")
        runtime_ti = create_runtime_ti(task=task, dag_id="dag_with_dag_params")

        run(runtime_ti, log=mock.MagicMock())

        mock_supervisor_comms.send_request.assert_called_once_with(
            msg=SucceedTask(
                state=TerminalTIState.SUCCESS, end_date=instant, task_outlets=[], outlet_events=[]
            ),
            log=mock.ANY,
        )

    def test_dag_param_dag_overwrite(self, create_runtime_ti, mock_supervisor_comms, time_machine):
        """Test dag param is overwritten from dagrun config"""
        instant = timezone.datetime(2024, 12, 3, 10, 0)
        time_machine.move_to(instant, tick=False)

        dag = DAG(dag_id="dag_with_dag_params_overwrite", start_date=timezone.datetime(2024, 12, 3))
        dag.param("value", default="NOTSET")

        class CustomOperator(BaseOperator):
            def execute(self, context):
                # important to use self.dag here
                assert self.dag.params["value"] == "new_value"

        # asserting on the default value when not set in dag run
        assert dag.params["value"] == "NOTSET"
        task = CustomOperator(task_id="task_with_dag_params_overwrite")

        # we reparse the dag here, and if conf passed, added as params
        runtime_ti = create_runtime_ti(
            task=task, dag_id="dag_with_dag_params_overwrite", conf={"value": "new_value"}
        )
        run(runtime_ti, log=mock.MagicMock())
        mock_supervisor_comms.send_request.assert_called_once_with(
            msg=SucceedTask(
                state=TerminalTIState.SUCCESS, end_date=instant, task_outlets=[], outlet_events=[]
            ),
            log=mock.ANY,
        )

    def test_dag_param_dag_default(self, create_runtime_ti, mock_supervisor_comms, time_machine):
        """Test that dag param is correctly resolved by operator"""
        instant = timezone.datetime(2024, 12, 3, 10, 0)
        time_machine.move_to(instant, tick=False)

        dag = DAG(
            dag_id="dag_with_dag_params_default",
            start_date=timezone.datetime(2024, 12, 3),
            params={"value": "test"},
        )

        class CustomOperator(BaseOperator):
            def execute(self, context):
                assert dag.params["value"] == "test"

        assert dag.params["value"] == "test"
        task = CustomOperator(task_id="task_with_dag_params_default")
        runtime_ti = create_runtime_ti(task=task, dag_id="dag_with_dag_params_default")

        run(runtime_ti, log=mock.MagicMock())
        mock_supervisor_comms.send_request.assert_called_once_with(
            msg=SucceedTask(
                state=TerminalTIState.SUCCESS, end_date=instant, task_outlets=[], outlet_events=[]
            ),
            log=mock.ANY,
        )

    def test_dag_param_resolves(
        self, create_runtime_ti, mock_supervisor_comms, time_machine, make_ti_context
    ):
        """Test that dag param is correctly resolved by operator"""

        instant = timezone.datetime(2024, 12, 3, 10, 0)
        time_machine.move_to(instant, tick=False)

        @dag_decorator(schedule=None, start_date=timezone.datetime(2024, 12, 3))
        def dag_with_dag_params(value="NOTSET"):
            @task_decorator
            def dummy_task(val):
                return val

            class CustomOperator(BaseOperator):
                def execute(self, context):
                    assert self.dag.params["value"] == "NOTSET"

            _ = dummy_task(value)
            custom_task = CustomOperator(task_id="task_with_dag_params")
            self.operator = custom_task

        dag_with_dag_params()

        runtime_ti = create_runtime_ti(task=self.operator, dag_id="dag_with_dag_params")

        run(runtime_ti, log=mock.MagicMock())

        mock_supervisor_comms.send_request.assert_called_once_with(
            msg=SucceedTask(
                state=TerminalTIState.SUCCESS, end_date=instant, task_outlets=[], outlet_events=[]
            ),
            log=mock.ANY,
        )

    def test_dag_param_dagrun_parameterized(
        self, create_runtime_ti, mock_supervisor_comms, time_machine, make_ti_context
    ):
        """Test that dag param is correctly overwritten when set in dag run"""

        instant = timezone.datetime(2024, 12, 3, 10, 0)
        time_machine.move_to(instant, tick=False)

        @dag_decorator(schedule=None, start_date=timezone.datetime(2024, 12, 3))
        def dag_with_dag_params(value=self.VALUE):
            @task_decorator
            def dummy_task(val):
                return val

            assert isinstance(value, DagParam)

            class CustomOperator(BaseOperator):
                def execute(self, context):
                    assert self.dag.params["value"] == "new_value"

            _ = dummy_task(value)
            custom_task = CustomOperator(task_id="task_with_dag_params")
            self.operator = custom_task

        dag_with_dag_params()

        runtime_ti = create_runtime_ti(
            task=self.operator, dag_id="dag_with_dag_params", conf={"value": "new_value"}
        )

        run(runtime_ti, log=mock.MagicMock())

        mock_supervisor_comms.send_request.assert_called_once_with(
            msg=SucceedTask(
                state=TerminalTIState.SUCCESS, end_date=instant, task_outlets=[], outlet_events=[]
            ),
            log=mock.ANY,
        )

    @pytest.mark.parametrize("value", [VALUE, 0])
    def test_set_params_for_dag(
        self, create_runtime_ti, mock_supervisor_comms, time_machine, make_ti_context, value
    ):
        """Test that dag param is correctly set when using dag decorator"""

        instant = timezone.datetime(2024, 12, 3, 10, 0)
        time_machine.move_to(instant, tick=False)

        @dag_decorator(schedule=None, start_date=timezone.datetime(2024, 12, 3))
        def dag_with_param(value=value):
            @task_decorator
            def return_num(num):
                return num

            xcom_arg = return_num(value)
            self.operator = xcom_arg.operator

        dag_with_param()

        runtime_ti = create_runtime_ti(task=self.operator, dag_id="dag_with_param", conf={"value": value})

        run(runtime_ti, log=mock.MagicMock())

        mock_supervisor_comms.send_request.assert_any_call(
            msg=SucceedTask(
                state=TerminalTIState.SUCCESS, end_date=instant, task_outlets=[], outlet_events=[]
            ),
            log=mock.ANY,
        )


class TestTaskRunnerCallsListeners:
    class CustomListener:
        def __init__(self):
            self.state = []
            self.component = None
            self.error = None

        @hookimpl
        def on_starting(self, component):
            self.component = component

        @hookimpl
        def on_task_instance_running(self, previous_state, task_instance):
            self.state.append(TaskInstanceState.RUNNING)

        @hookimpl
        def on_task_instance_success(self, previous_state, task_instance):
            self.state.append(TaskInstanceState.SUCCESS)

        @hookimpl
        def on_task_instance_failed(self, previous_state, task_instance, error):
            self.state.append(TaskInstanceState.FAILED)
            self.error = error

        @hookimpl
        def before_stopping(self, component):
            self.component = component

    @pytest.fixture(autouse=True)
    def clean_listener_manager(self):
        lm = get_listener_manager()
        lm.clear()
        yield
        lm = get_listener_manager()
        lm.clear()

    def test_task_runner_calls_on_startup_before_stopping(
        self, make_ti_context, mocked_parse, mock_supervisor_comms
    ):
        listener = self.CustomListener()
        get_listener_manager().add_listener(listener)

        class CustomOperator(BaseOperator):
            def execute(self, context):
                self.value = "something"

        task = CustomOperator(
            task_id="test_task_runner_calls_listeners", do_xcom_push=True, multiple_outputs=True
        )
        what = StartupDetails(
            ti=TaskInstance(
                id=uuid7(),
                task_id="templated_task",
                dag_id="basic_dag",
                run_id="c",
                try_number=1,
            ),
            dag_rel_path="",
            bundle_info=FAKE_BUNDLE,
            requests_fd=0,
            ti_context=make_ti_context(),
            start_date=timezone.utcnow(),
        )

        mock_supervisor_comms.get_message.return_value = what
        mocked_parse(what, "basic_dag", task)

        runtime_ti, log = startup()
        assert isinstance(listener.component, TaskRunnerMarker)
        del listener.component

        state, _, _ = run(runtime_ti, log)
        finalize(runtime_ti, state, log)
        assert isinstance(listener.component, TaskRunnerMarker)

    def test_task_runner_calls_listeners_success(self, mocked_parse, mock_supervisor_comms):
        listener = self.CustomListener()
        get_listener_manager().add_listener(listener)

        class CustomOperator(BaseOperator):
            def execute(self, context):
                self.value = "something"

        task = CustomOperator(
            task_id="test_task_runner_calls_listeners", do_xcom_push=True, multiple_outputs=True
        )
        dag = get_inline_dag(dag_id="test_dag", task=task)
        ti = TaskInstance(
            id=uuid7(),
            task_id=task.task_id,
            dag_id=dag.dag_id,
            run_id="test_run",
            try_number=1,
        )

        runtime_ti = RuntimeTaskInstance.model_construct(
            **ti.model_dump(exclude_unset=True), task=task, start_date=timezone.utcnow()
        )
        log = mock.MagicMock()

        state, _, _ = run(runtime_ti, log)
        finalize(runtime_ti, state, log)

        assert listener.state == [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS]

    @pytest.mark.parametrize(
        "exception",
        [
            ValueError("oops"),
            SystemExit("oops"),
            AirflowException("oops"),
        ],
    )
    def test_task_runner_calls_listeners_failed(self, mocked_parse, mock_supervisor_comms, exception):
        listener = self.CustomListener()
        get_listener_manager().add_listener(listener)

        class CustomOperator(BaseOperator):
            def execute(self, context):
                raise exception

        task = CustomOperator(
            task_id="test_task_runner_calls_listeners_failed", do_xcom_push=True, multiple_outputs=True
        )
        dag = get_inline_dag(dag_id="test_dag", task=task)
        ti = TaskInstance(
            id=uuid7(),
            task_id=task.task_id,
            dag_id=dag.dag_id,
            run_id="test_run",
            try_number=1,
        )

        runtime_ti = RuntimeTaskInstance.model_construct(
            **ti.model_dump(exclude_unset=True), task=task, start_date=timezone.utcnow()
        )
        log = mock.MagicMock()

        state, _, error = run(runtime_ti, log)
        finalize(runtime_ti, state, log, error)

        assert listener.state == [TaskInstanceState.RUNNING, TaskInstanceState.FAILED]
        assert listener.error == error


@pytest.mark.usefixtures("mock_supervisor_comms")
class TestTaskRunnerCallsCallbacks:
    def test_task_runner_calls_execute_callback(self, create_runtime_ti):
        results = []

        def custom_callback(context):
            results.append("callback")

        class CustomOperator(BaseOperator):
            def execute(self, context):
                results.append("execute")

        task = CustomOperator(task_id="task", on_execute_callback=custom_callback)
        runtime_ti = create_runtime_ti(dag_id="dag", task=task)
        log = mock.MagicMock()
        state, _, _ = run(runtime_ti, log)

        assert state == TerminalTIState.SUCCESS
        assert results == ["callback", "execute"]

    def test_task_runner_not_fail_on_failed_execute_callback(self, create_runtime_ti):
        results = []

        def custom_callback_1(context):
            results.append("callback 1")

        def custom_callback_2(context):
            raise Exception("sorry!")

        class CustomOperator(BaseOperator):
            def execute(self, context):
                results.append("execute")

        task = CustomOperator(task_id="task", on_execute_callback=[custom_callback_1, custom_callback_2])
        runtime_ti = create_runtime_ti(dag_id="dag", task=task)
        log = mock.MagicMock()
        state, _, _ = run(runtime_ti, log)

        assert state == TerminalTIState.SUCCESS
        assert results == ["callback 1", "execute"]
        assert log.exception.mock_calls == [
            mock.call("Failed to run on-execute callback", index=1, callback=custom_callback_2),
        ]


class TestTriggerDagRunOperator:
    """Tests to verify various aspects of TriggerDagRunOperator"""

    def test_handle_trigger_dag_run(self, create_runtime_ti, mock_supervisor_comms):
        """Test that TriggerDagRunOperator (with default args) sends the correct message to the Supervisor"""
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

        task = TriggerDagRunOperator(
            task_id="test_task",
            trigger_dag_id="test_dag",
            trigger_run_id="test_run_id",
        )
        ti = create_runtime_ti(dag_id="test_handle_trigger_dag_run", run_id="test_run", task=task)

        log = mock.MagicMock()
        mock_supervisor_comms.get_message.return_value = OKResponse(ok=True)
        state, msg, _ = run(ti, log=log)

        assert state == TaskInstanceState.SUCCESS
        assert msg.state == TaskInstanceState.SUCCESS

        expected_calls = [
            mock.call.send_request(
                msg=TriggerDagRun(
                    dag_id="test_dag",
                    run_id="test_run_id",
                    reset_dag_run=False,
                ),
                log=mock.ANY,
            ),
            mock.call.get_message(),
            mock.call.send_request(
                msg=SetXCom(
                    key="trigger_run_id",
                    value="test_run_id",
                    dag_id="test_handle_trigger_dag_run",
                    task_id="test_task",
                    run_id="test_run",
                    map_index=-1,
                ),
                log=mock.ANY,
            ),
        ]
        mock_supervisor_comms.assert_has_calls(expected_calls)

    @pytest.mark.parametrize(
        ["skip_when_already_exists", "expected_state"],
        [
            (True, TaskInstanceState.SKIPPED),
            (False, TaskInstanceState.FAILED),
        ],
    )
    def test_handle_trigger_dag_run_conflict(
        self, skip_when_already_exists, expected_state, create_runtime_ti, mock_supervisor_comms
    ):
        """Test that TriggerDagRunOperator (when dagrun already exists) sends the correct message to the Supervisor"""
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

        task = TriggerDagRunOperator(
            task_id="test_task",
            trigger_dag_id="test_dag",
            trigger_run_id="test_run_id",
            skip_when_already_exists=skip_when_already_exists,
        )
        ti = create_runtime_ti(dag_id="test_handle_trigger_dag_run_conflict", run_id="test_run", task=task)

        log = mock.MagicMock()
        mock_supervisor_comms.get_message.return_value = ErrorResponse(error=ErrorType.DAGRUN_ALREADY_EXISTS)
        state, msg, _ = run(ti, log=log)

        assert state == expected_state
        assert msg.state == expected_state

        expected_calls = [
            mock.call.send_request(
                msg=TriggerDagRun(
                    dag_id="test_dag",
                    run_id="test_run_id",
                    reset_dag_run=False,
                ),
                log=mock.ANY,
            ),
            mock.call.get_message(),
        ]
        mock_supervisor_comms.assert_has_calls(expected_calls)

    @pytest.mark.parametrize(
        ["allowed_states", "failed_states", "target_dr_state", "expected_task_state"],
        [
            (None, None, DagRunState.FAILED, TaskInstanceState.FAILED),
            (None, None, DagRunState.SUCCESS, TaskInstanceState.SUCCESS),
            ([DagRunState.FAILED], [], DagRunState.FAILED, DagRunState.SUCCESS),
            ([DagRunState.FAILED], None, DagRunState.FAILED, DagRunState.FAILED),
            ([DagRunState.SUCCESS], None, DagRunState.FAILED, DagRunState.FAILED),
        ],
    )
    def test_handle_trigger_dag_run_wait_for_completion(
        self,
        allowed_states,
        failed_states,
        target_dr_state,
        expected_task_state,
        create_runtime_ti,
        mock_supervisor_comms,
    ):
        """
        Test that TriggerDagRunOperator (with wait_for_completion) sends the correct message to the Supervisor

        It also polls the Supervisor for the DagRun state until it completes execution.
        """
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

        task = TriggerDagRunOperator(
            task_id="test_task",
            trigger_dag_id="test_dag",
            trigger_run_id="test_run_id",
            poke_interval=5,
            wait_for_completion=True,
            allowed_states=allowed_states,
            failed_states=failed_states,
        )
        ti = create_runtime_ti(
            dag_id="test_handle_trigger_dag_run_wait_for_completion", run_id="test_run", task=task
        )

        log = mock.MagicMock()
        mock_supervisor_comms.get_message.side_effect = [
            # Successful Dag Run trigger
            OKResponse(ok=True),
            # Dag Run is still running
            DagRunStateResult(state=DagRunState.RUNNING),
            # Dag Run completes execution on the next poll
            DagRunStateResult(state=target_dr_state),
        ]
        with mock.patch("time.sleep", return_value=None):
            state, msg, _ = run(ti, log=log)

        assert state == expected_task_state
        assert msg.state == expected_task_state

        expected_calls = [
            mock.call.send_request(
                msg=TriggerDagRun(
                    dag_id="test_dag",
                    run_id="test_run_id",
                ),
                log=mock.ANY,
            ),
            mock.call.get_message(),
            mock.call.send_request(
                msg=SetXCom(
                    key="trigger_run_id",
                    value="test_run_id",
                    dag_id="test_handle_trigger_dag_run_wait_for_completion",
                    task_id="test_task",
                    run_id="test_run",
                    map_index=-1,
                ),
                log=mock.ANY,
            ),
            mock.call.send_request(
                msg=GetDagRunState(
                    dag_id="test_dag",
                    run_id="test_run_id",
                ),
                log=mock.ANY,
            ),
            mock.call.get_message(),
            mock.call.send_request(
                msg=GetDagRunState(
                    dag_id="test_dag",
                    run_id="test_run_id",
                ),
                log=mock.ANY,
            ),
        ]
        mock_supervisor_comms.assert_has_calls(expected_calls)
