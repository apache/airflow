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

from airflow.sdk import DAG, BaseOperator
from airflow.sdk.api.datamodels._generated import TaskInstance
from airflow.sdk.execution_time.comms import DeferTask, StartupDetails
from airflow.sdk.execution_time.task_runner import CommsDecoder, parse, run
from airflow.utils import timezone


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


def test_run_basic(test_dags_dir: Path):
    """Test running a basic task."""
    what = StartupDetails(
        ti=TaskInstance(id=uuid7(), task_id="hello", dag_id="super_basic_run", run_id="c", try_number=1),
        file=str(test_dags_dir / "super_basic_run.py"),
        requests_fd=0,
    )

    ti = parse(what)
    run(ti, log=mock.MagicMock())


def test_run_deferred_basic(test_dags_dir: Path, time_machine):
    """Test that a task can transition to a deferred state."""
    what = StartupDetails(
        ti=TaskInstance(
            id=uuid7(), task_id="async", dag_id="super_basic_deferred_run", run_id="c", try_number=1
        ),
        file=str(test_dags_dir / "super_basic_deferred_run.py"),
        requests_fd=0,
    )

    # Use the time machine to set the current time
    instant = timezone.datetime(2024, 11, 22)
    time_machine.move_to(instant, tick=False)

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
        ti = parse(what)
        run(ti, log=mock.MagicMock())

        # send_request will only be called when the TaskDeferred exception is raised
        mock_supervisor_comms.send_request.assert_called_once_with(msg=expected_defer_task, log=mock.ANY)
