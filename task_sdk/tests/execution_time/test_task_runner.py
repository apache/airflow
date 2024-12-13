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

from airflow.exceptions import AirflowSkipException
from airflow.sdk import DAG, BaseOperator
from airflow.sdk.api.datamodels._generated import TaskInstance, TerminalTIState
from airflow.sdk.execution_time.comms import DeferTask, SetRenderedFields, StartupDetails, TaskState
from airflow.sdk.execution_time.task_runner import CommsDecoder, RuntimeTaskInstance, parse, run, startup
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
            b'"file": "/dev/null", "requests_fd": ' + str(w2.fileno()).encode("ascii") + b"}\n"
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


def test_parse(test_dags_dir: Path):
    """Test that checks parsing of a basic dag with an un-mocked parse."""
    what = StartupDetails(
        ti=TaskInstance(id=uuid7(), task_id="a", dag_id="super_basic", run_id="c", try_number=1),
        file=str(test_dags_dir / "super_basic.py"),
        requests_fd=0,
    )

    ti = parse(what)

    assert ti.task
    assert ti.task.dag
    assert isinstance(ti.task, BaseOperator)
    assert isinstance(ti.task.dag, DAG)


def test_run_basic(time_machine, mocked_parse):
    """Test running a basic task."""
    what = StartupDetails(
        ti=TaskInstance(id=uuid7(), task_id="hello", dag_id="super_basic_run", run_id="c", try_number=1),
        file="",
        requests_fd=0,
    )

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    with mock.patch(
        "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
    ) as mock_supervisor_comms:
        ti = mocked_parse(what, "super_basic_run", CustomOperator(task_id="hello"))
        run(ti, log=mock.MagicMock())

        mock_supervisor_comms.send_request.assert_called_once_with(
            msg=TaskState(state=TerminalTIState.SUCCESS, end_date=instant), log=mock.ANY
        )


def test_run_deferred_basic(time_machine, mocked_parse):
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
    with mock.patch(
        "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
    ) as mock_supervisor_comms:
        ti = mocked_parse(what, "basic_deferred_run", task)
        run(ti, log=mock.MagicMock())

        # send_request will only be called when the TaskDeferred exception is raised
        mock_supervisor_comms.send_request.assert_called_once_with(msg=expected_defer_task, log=mock.ANY)


def test_run_basic_skipped(time_machine, mocked_parse):
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
    )

    ti = mocked_parse(what, "basic_skipped", task)

    instant = timezone.datetime(2024, 12, 3, 10, 0)
    time_machine.move_to(instant, tick=False)

    with mock.patch(
        "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
    ) as mock_supervisor_comms:
        run(ti, log=mock.MagicMock())

        mock_supervisor_comms.send_request.assert_called_once_with(
            msg=TaskState(state=TerminalTIState.SKIPPED, end_date=instant), log=mock.ANY
        )


def test_startup_basic_templated_dag(mocked_parse):
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
    )
    mocked_parse(what, "basic_templated_dag", task)

    with mock.patch(
        "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
    ) as mock_supervisor_comms:
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
