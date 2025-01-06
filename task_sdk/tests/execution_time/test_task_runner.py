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

import uuid
from datetime import timedelta
from pathlib import Path
from socket import socketpair
from unittest import mock

import pytest
from uuid6 import uuid7

from airflow.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowSensorTimeout,
    AirflowSkipException,
    AirflowTaskTerminated,
)
from airflow.sdk import DAG, BaseOperator, Connection
from airflow.sdk.api.datamodels._generated import TaskInstance, TerminalTIState
from airflow.sdk.execution_time.comms import (
    ConnectionResult,
    DeferTask,
    GetConnection,
    SetRenderedFields,
    StartupDetails,
    TaskState,
)
from airflow.sdk.execution_time.context import ConnectionAccessor
from airflow.sdk.execution_time.task_runner import (
    CommsDecoder,
    RuntimeTaskInstance,
    _push_xcom_if_needed,
    parse,
    run,
    startup,
)
from airflow.utils import timezone


def get_inline_dag(dag_id: str, task: BaseOperator) -> DAG:
    """Creates an inline dag and returns it based on dag_id and task."""
    dag = DAG(dag_id=dag_id, start_date=timezone.datetime(2024, 12, 3))
    task.dag = dag

    return dag


@pytest.fixture
def mocked_parse(spy_agency):
    """
    Fixture to set up an inline DAG and use it in a stubbed `parse` function. Use this fixture if you
    want to isolate and test `parse` or `run` logic without having to define a DAG file.

    This fixture returns a helper function `set_dag` that:
    1. Creates an in line DAG with the given `dag_id` and `task` (limited to one task)
    2. Constructs a `RuntimeTaskInstance` based on the provided `StartupDetails` and task.
    3. Stubs the `parse` function using `spy_agency`, to return the mocked `RuntimeTaskInstance`.

    After adding the fixture in your test function signature, you can use it like this ::

            mocked_parse(
                StartupDetails(
                    ti=TaskInstance(id=uuid7(), task_id="hello", dag_id="super_basic_run", run_id="c", try_number=1),
                    file="",
                    requests_fd=0,
                ),
                "example_dag_id",
                CustomOperator(task_id="hello"),
            )
    """

    def set_dag(what: StartupDetails, dag_id: str, task: BaseOperator) -> RuntimeTaskInstance:
        dag = get_inline_dag(dag_id, task)
        t = dag.task_dict[task.task_id]
        ti = RuntimeTaskInstance.model_construct(**what.ti.model_dump(exclude_unset=True), task=t)
        spy_agency.spy_on(parse, call_fake=lambda _: ti)
        return ti

    return set_dag


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
            b'"id": "4d828a62-a417-4936-a7a6-2b3fabacecab", "task_id": "a", "try_number": 1, "run_id": "b", "dag_id": "c" }, '
            b'"ti_context":{"dag_run":{"dag_id":"c","run_id":"b","logical_date":"2024-12-01T01:00:00Z",'
            b'"data_interval_start":"2024-12-01T00:00:00Z","data_interval_end":"2024-12-01T01:00:00Z",'
            b'"start_date":"2024-12-01T01:00:00Z","end_date":null,"run_type":"manual","conf":null},'
            b'"variables":null,"connections":null},"file": "/dev/null", "requests_fd": '
            + str(w2.fileno()).encode("ascii")
            + b"}\n"
        )

        decoder = CommsDecoder(input=r.makefile("r"))

        msg = decoder.get_message()
        assert isinstance(msg, StartupDetails)
        assert msg.ti.id == uuid.UUID("4d828a62-a417-4936-a7a6-2b3fabacecab")
        assert msg.ti.task_id == "a"
        assert msg.ti.dag_id == "c"
        assert msg.file == "/dev/null"

        # Since this was a StartupDetails message, the decoder should open the other socket
        assert decoder.request_socket is not None
        assert decoder.request_socket.writable()
        assert decoder.request_socket.fileno() == w2.fileno()


def test_parse(test_dags_dir: Path, make_ti_context):
    """Test that checks parsing of a basic dag with an un-mocked parse."""
    what = StartupDetails(
        ti=TaskInstance(id=uuid7(), task_id="a", dag_id="super_basic", run_id="c", try_number=1),
        file=str(test_dags_dir / "super_basic.py"),
        requests_fd=0,
        ti_context=make_ti_context(),
    )

    ti = parse(what)

    assert ti.task
    assert ti.task.dag
    assert isinstance(ti.task, BaseOperator)
    assert isinstance(ti.task.dag, DAG)


def test_run_basic(time_machine, mocked_parse, make_ti_context, spy_agency, mock_supervisor_comms):
    """Test running a basic task."""
    what = StartupDetails(
        ti=TaskInstance(id=uuid7(), task_id="hello", dag_id="super_basic_run", run_id="c", try_number=1),
        file="",
        requests_fd=0,
        ti_context=make_ti_context(),
    )

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    ti = mocked_parse(what, "super_basic_run", CustomOperator(task_id="hello"))

    # Ensure that task is locked for execution
    spy_agency.spy_on(ti.task.prepare_for_execution)
    assert not ti.task._lock_for_execution

    run(ti, log=mock.MagicMock())

    spy_agency.assert_spy_called(ti.task.prepare_for_execution)
    assert ti.task._lock_for_execution

    mock_supervisor_comms.send_request.assert_called_once_with(
        msg=TaskState(state=TerminalTIState.SUCCESS, end_date=instant), log=mock.ANY
    )


def test_run_deferred_basic(time_machine, mocked_parse, make_ti_context, mock_supervisor_comms):
    """Test that a task can transition to a deferred state."""
    import datetime

    from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync

    # Use the time machine to set the current time
    instant = timezone.datetime(2024, 11, 22)
    task = DateTimeSensorAsync(
        task_id="async",
        target_time=str(instant + datetime.timedelta(seconds=3)),
        poke_interval=60,
        timeout=600,
    )
    time_machine.move_to(instant, tick=False)
    what = StartupDetails(
        ti=TaskInstance(id=uuid7(), task_id="async", dag_id="basic_deferred_run", run_id="c", try_number=1),
        file="",
        requests_fd=0,
        ti_context=make_ti_context(),
    )

    # Expected DeferTask
    expected_defer_task = DeferTask(
        state="deferred",
        classpath="airflow.providers.standard.triggers.temporal.DateTimeTrigger",
        trigger_kwargs={
            "end_from_trigger": False,
            "moment": instant + timedelta(seconds=3),
        },
        next_method="execute_complete",
        trigger_timeout=None,
    )

    # Run the task
    ti = mocked_parse(what, "basic_deferred_run", task)
    run(ti, log=mock.MagicMock())

    # send_request will only be called when the TaskDeferred exception is raised
    mock_supervisor_comms.send_request.assert_called_once_with(msg=expected_defer_task, log=mock.ANY)


def test_run_basic_skipped(time_machine, mocked_parse, make_ti_context, mock_supervisor_comms):
    """Test running a basic task that marks itself skipped."""
    from airflow.providers.standard.operators.python import PythonOperator

    task = PythonOperator(
        task_id="skip",
        python_callable=lambda: (_ for _ in ()).throw(
            AirflowSkipException("This task is being skipped intentionally."),
        ),
    )

    what = StartupDetails(
        ti=TaskInstance(id=uuid7(), task_id="skip", dag_id="basic_skipped", run_id="c", try_number=1),
        file="",
        requests_fd=0,
        ti_context=make_ti_context(),
    )

    ti = mocked_parse(what, "basic_skipped", task)

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    mock_supervisor_comms.send_request.assert_called_once_with(
        msg=TaskState(state=TerminalTIState.SKIPPED, end_date=instant), log=mock.ANY
    )


def test_run_raises_base_exception(time_machine, mocked_parse, make_ti_context, mock_supervisor_comms):
    """Test running a basic task that raises a base exception which should send fail_with_retry state."""
    from airflow.providers.standard.operators.python import PythonOperator

    task = PythonOperator(
        task_id="zero_division_error",
        python_callable=lambda: 1 / 0,
    )

    what = StartupDetails(
        ti=TaskInstance(
            id=uuid7(),
            task_id="zero_division_error",
            dag_id="basic_dag_base_exception",
            run_id="c",
            try_number=1,
        ),
        file="",
        requests_fd=0,
        ti_context=make_ti_context(),
    )

    ti = mocked_parse(what, "basic_dag_base_exception", task)

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    mock_supervisor_comms.send_request.assert_called_once_with(
        msg=TaskState(
            state=TerminalTIState.FAILED,
            end_date=instant,
        ),
        log=mock.ANY,
    )


def test_run_raises_system_exit(time_machine, mocked_parse, make_ti_context, mock_supervisor_comms):
    """Test running a basic task that exits with SystemExit exception."""
    from airflow.providers.standard.operators.python import PythonOperator

    task = PythonOperator(
        task_id="system_exit_task",
        python_callable=lambda: exit(10),
    )

    what = StartupDetails(
        ti=TaskInstance(
            id=uuid7(),
            task_id="system_exit_task",
            dag_id="basic_dag_system_exit",
            run_id="c",
            try_number=1,
        ),
        file="",
        requests_fd=0,
        ti_context=make_ti_context(),
    )

    ti = mocked_parse(what, "basic_dag_system_exit", task)

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    mock_supervisor_comms.send_request.assert_called_once_with(
        msg=TaskState(
            state=TerminalTIState.FAILED,
            end_date=instant,
        ),
        log=mock.ANY,
    )


def test_run_raises_airflow_exception(time_machine, mocked_parse, make_ti_context, mock_supervisor_comms):
    """Test running a basic task that exits with AirflowException."""
    from airflow.providers.standard.operators.python import PythonOperator

    task = PythonOperator(
        task_id="af_exception_task",
        python_callable=lambda: (_ for _ in ()).throw(
            AirflowException("Oops! I am failing with AirflowException!"),
        ),
    )

    what = StartupDetails(
        ti=TaskInstance(
            id=uuid7(),
            task_id="af_exception_task",
            dag_id="basic_dag_af_exception",
            run_id="c",
            try_number=1,
        ),
        file="",
        requests_fd=0,
        ti_context=make_ti_context(),
    )

    ti = mocked_parse(what, "basic_dag_af_exception", task)

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    mock_supervisor_comms.send_request.assert_called_once_with(
        msg=TaskState(
            state=TerminalTIState.FAILED,
            end_date=instant,
        ),
        log=mock.ANY,
    )


def test_run_task_timeout(time_machine, mocked_parse, make_ti_context, mock_supervisor_comms):
    """Test running a basic task that times out."""
    from time import sleep

    from airflow.providers.standard.operators.python import PythonOperator

    task = PythonOperator(
        task_id="sleep",
        execution_timeout=timedelta(milliseconds=10),
        python_callable=lambda: sleep(2),
    )

    what = StartupDetails(
        ti=TaskInstance(
            id=uuid7(),
            task_id="sleep",
            dag_id="basic_dag_time_out",
            run_id="c",
            try_number=1,
        ),
        file="",
        requests_fd=0,
        ti_context=make_ti_context(),
    )

    ti = mocked_parse(what, "basic_dag_time_out", task)

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    # this state can only be reached if the try block passed down the exception to handler of AirflowTaskTimeout
    mock_supervisor_comms.send_request.assert_called_once_with(
        msg=TaskState(
            state=TerminalTIState.FAILED,
            end_date=instant,
        ),
        log=mock.ANY,
    )


def test_startup_basic_templated_dag(mocked_parse, make_ti_context, mock_supervisor_comms):
    """Test running a DAG with templated task."""
    from airflow.providers.standard.operators.bash import BashOperator

    task = BashOperator(
        task_id="templated_task",
        bash_command="echo 'Logical date is {{ logical_date }}'",
    )

    what = StartupDetails(
        ti=TaskInstance(
            id=uuid7(), task_id="templated_task", dag_id="basic_templated_dag", run_id="c", try_number=1
        ),
        file="",
        requests_fd=0,
        ti_context=make_ti_context(),
    )
    mocked_parse(what, "basic_templated_dag", task)

    mock_supervisor_comms.get_message.return_value = what
    startup()

    mock_supervisor_comms.send_request.assert_called_once_with(
        msg=SetRenderedFields(
            rendered_fields={
                "bash_command": "echo 'Logical date is {{ logical_date }}'",
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
    ],
)
def test_startup_and_run_dag_with_templated_fields(
    mocked_parse, task_params, expected_rendered_fields, make_ti_context, time_machine, mock_supervisor_comms
):
    """Test startup of a DAG with various templated fields."""

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

    what = StartupDetails(
        ti=TaskInstance(id=uuid7(), task_id="templated_task", dag_id="basic_dag", run_id="c", try_number=1),
        file="",
        requests_fd=0,
        ti_context=make_ti_context(),
    )
    ti = mocked_parse(what, "basic_dag", task)

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    mock_supervisor_comms.get_message.return_value = what

    startup()
    run(ti, log=mock.MagicMock())
    expected_calls = [
        mock.call.send_request(
            msg=SetRenderedFields(rendered_fields=expected_rendered_fields),
            log=mock.ANY,
        ),
        mock.call.send_request(
            msg=TaskState(end_date=instant, state=TerminalTIState.SUCCESS),
            log=mock.ANY,
        ),
    ]
    mock_supervisor_comms.assert_has_calls(expected_calls)


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
    time_machine, mocked_parse, dag_id, task_id, fail_with_exception, make_ti_context, mock_supervisor_comms
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

    what = StartupDetails(
        ti=TaskInstance(id=uuid7(), task_id=task_id, dag_id=dag_id, run_id="c", try_number=1),
        file="",
        requests_fd=0,
        ti_context=make_ti_context(),
    )

    ti = mocked_parse(what, dag_id, task)

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    run(ti, log=mock.MagicMock())

    mock_supervisor_comms.send_request.assert_called_once_with(
        msg=TaskState(state=TerminalTIState.FAIL_WITHOUT_RETRY, end_date=instant), log=mock.ANY
    )


class TestRuntimeTaskInstance:
    def test_get_context_without_ti_context_from_server(self, mocked_parse, make_ti_context):
        """Test get_template_context without ti_context_from_server."""

        task = BaseOperator(task_id="hello")

        ti_id = uuid7()
        ti = TaskInstance(
            id=ti_id, task_id=task.task_id, dag_id="basic_task", run_id="test_run", try_number=1
        )

        what = StartupDetails(ti=ti, file="", requests_fd=0, ti_context=make_ti_context())
        runtime_ti = mocked_parse(what, ti.dag_id, task)
        context = runtime_ti.get_template_context()

        # Verify the context keys and values
        assert context == {
            "conn": ConnectionAccessor(),
            "dag": runtime_ti.task.dag,
            "inlets": task.inlets,
            "map_index_template": task.map_index_template,
            "outlets": task.outlets,
            "run_id": "test_run",
            "task": task,
            "task_instance": runtime_ti,
            "ti": runtime_ti,
        }

    def test_get_context_with_ti_context_from_server(self, mocked_parse, make_ti_context):
        """Test the context keys are added when sent from API server (mocked)"""
        from airflow.utils import timezone

        ti = TaskInstance(id=uuid7(), task_id="hello", dag_id="basic_task", run_id="test_run", try_number=1)

        task = BaseOperator(task_id=ti.task_id)

        ti_context = make_ti_context(dag_id=ti.dag_id, run_id=ti.run_id)
        what = StartupDetails(ti=ti, file="", requests_fd=0, ti_context=ti_context)

        runtime_ti = mocked_parse(what, ti.dag_id, task)

        # Assume the context is sent from the API server
        # `task_sdk/tests/api/test_client.py::test_task_instance_start` checks the context is received
        # from the API server
        runtime_ti._ti_context_from_server = ti_context
        dr = ti_context.dag_run

        context = runtime_ti.get_template_context()

        assert context == {
            "conn": ConnectionAccessor(),
            "dag": runtime_ti.task.dag,
            "inlets": task.inlets,
            "map_index_template": task.map_index_template,
            "outlets": task.outlets,
            "run_id": "test_run",
            "task": task,
            "task_instance": runtime_ti,
            "ti": runtime_ti,
            "dag_run": dr,
            "data_interval_end": timezone.datetime(2024, 12, 1, 1, 0, 0),
            "data_interval_start": timezone.datetime(2024, 12, 1, 0, 0, 0),
            "logical_date": timezone.datetime(2024, 12, 1, 1, 0, 0),
            "ds": "2024-12-01",
            "ds_nodash": "20241201",
            "task_instance_key_str": "basic_task__hello__20241201",
            "ts": "2024-12-01T01:00:00+00:00",
            "ts_nodash": "20241201T010000",
            "ts_nodash_with_tz": "20241201T010000+0000",
        }

    def test_get_connection_from_context(self, mocked_parse, make_ti_context, mock_supervisor_comms):
        """Test that the connection is fetched from the API server via the Supervisor lazily when accessed"""

        task = BaseOperator(task_id="hello")

        ti_id = uuid7()
        ti = TaskInstance(
            id=ti_id, task_id=task.task_id, dag_id="basic_task", run_id="test_run", try_number=1
        )
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

        what = StartupDetails(ti=ti, file="", requests_fd=0, ti_context=make_ti_context())
        runtime_ti = mocked_parse(what, ti.dag_id, task)
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
        mocked_parse,
        make_ti_context,
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

        ti = TaskInstance(
            id=uuid7(), task_id=task.task_id, dag_id="xcom_push_flag", run_id="test_run", try_number=1
        )

        what = StartupDetails(ti=ti, file="", requests_fd=0, ti_context=make_ti_context())
        runtime_ti = mocked_parse(what, ti.dag_id, task)

        spy_agency.spy_on(_push_xcom_if_needed, call_original=True)
        spy_agency.spy_on(runtime_ti.xcom_push, call_original=False)

        run(runtime_ti, log=mock.MagicMock())

        spy_agency.assert_spy_called(_push_xcom_if_needed)

        if should_push_xcom:
            spy_agency.assert_spy_called_with(runtime_ti.xcom_push, "return_value", expected_xcom_value)
        else:
            spy_agency.assert_spy_not_called(runtime_ti.xcom_push)

    def test_xcom_with_multiple_outputs(self, mocked_parse, spy_agency):
        """Test that the task pushes to XCom when multiple outputs are returned."""
        result = {"key1": "value1", "key2": "value2"}

        class CustomOperator(BaseOperator):
            def execute(self, context):
                return result

        task = CustomOperator(
            task_id="test_xcom_push_with_multiple_outputs", do_xcom_push=True, multiple_outputs=True
        )
        dag = get_inline_dag(dag_id="test_dag", task=task)
        ti = TaskInstance(
            id=uuid7(), task_id=task.task_id, dag_id=dag.dag_id, run_id="test_run", try_number=1
        )

        runtime_ti = RuntimeTaskInstance.model_construct(**ti.model_dump(exclude_unset=True), task=task)

        spy_agency.spy_on(runtime_ti.xcom_push, call_original=False)
        _push_xcom_if_needed(result=result, ti=runtime_ti)

        expected_calls = [
            ("key1", "value1"),
            ("key2", "value2"),
            ("return_value", result),
        ]
        spy_agency.assert_spy_call_count(runtime_ti.xcom_push, len(expected_calls))
        for key, value in expected_calls:
            spy_agency.assert_spy_called_with(runtime_ti.xcom_push, key, value)

    def test_xcom_with_multiple_outputs_and_no_mapping_result(self, mocked_parse, spy_agency):
        """Test that error is raised when multiple outputs are returned without mapping."""
        result = "value1"

        class CustomOperator(BaseOperator):
            def execute(self, context):
                return result

        task = CustomOperator(
            task_id="test_xcom_push_with_multiple_outputs", do_xcom_push=True, multiple_outputs=True
        )
        dag = get_inline_dag(dag_id="test_dag", task=task)
        ti = TaskInstance(
            id=uuid7(), task_id=task.task_id, dag_id=dag.dag_id, run_id="test_run", try_number=1
        )

        runtime_ti = RuntimeTaskInstance.model_construct(**ti.model_dump(exclude_unset=True), task=task)

        spy_agency.spy_on(runtime_ti.xcom_push, call_original=False)
        with pytest.raises(
            TypeError,
            match=f"Returned output was type {type(result)} expected dictionary for multiple_outputs",
        ):
            _push_xcom_if_needed(result=result, ti=runtime_ti)

    def test_xcom_with_multiple_outputs_and_key_is_not_string(self, mocked_parse, spy_agency):
        """Test that error is raised when multiple outputs are returned and key isn't string."""
        result = {2: "value1", "key2": "value2"}

        class CustomOperator(BaseOperator):
            def execute(self, context):
                return result

        task = CustomOperator(
            task_id="test_xcom_push_with_multiple_outputs", do_xcom_push=True, multiple_outputs=True
        )
        dag = get_inline_dag(dag_id="test_dag", task=task)
        ti = TaskInstance(
            id=uuid7(), task_id=task.task_id, dag_id=dag.dag_id, run_id="test_run", try_number=1
        )

        runtime_ti = RuntimeTaskInstance.model_construct(**ti.model_dump(exclude_unset=True), task=task)

        spy_agency.spy_on(runtime_ti.xcom_push, call_original=False)

        with pytest.raises(TypeError) as exc_info:
            _push_xcom_if_needed(result=result, ti=runtime_ti)

        assert str(exc_info.value) == (
            f"Returned dictionary keys must be strings when using multiple_outputs, found 2 ({int}) instead"
        )
