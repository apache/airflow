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
"""
Exercise the async connection helpers against the real supervisor comms of the installed Airflow.

The helpers exist because the Task SDK's supervisor channel behaves differently from one Airflow
version to the next (``asend()``, thread and async locks, ``Connection.aextra_dejson()``). Mocking
that channel would only encode assumptions about it, so these tests fork a real task process with
the real supervisor, as the Task SDK's own supervisor tests do: only the Execution API client is
stubbed. Run against each supported Airflow version, they exercise that version's comms as is.

They need Airflow 3.2+, the first version whose task process can call the supervisor from async
code. Before that, task-process ``asend()`` is not implemented (3.1) or does not exist (3.0), and
async code only runs in the triggerer, whose channel serializes every request, including a
worker thread's synchronous send, through ``asend()`` and an ``asyncio.Lock``.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from unittest import mock

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

pytestmark = pytest.mark.skipif(
    not AIRFLOW_V_3_2_PLUS,
    reason="A task process can only call the supervisor from async code from Airflow 3.2 on.",
)

CONN_ID = "compat_async_extra"
EXTRA = {"api_key": "s3cr3t-value", "timeout": 30}
# Many concurrent calls, a few rounds: a send interleaving with an in-flight asend() only shows
# up when the event loop has several requests in flight at once.
CONCURRENCY = 50
ROUNDS = 3


@pytest.fixture
def disable_capturing():
    """The forked task process redirects the real file descriptors, not pytest's capture objects."""
    old_in, old_out, old_err = sys.stdin, sys.stdout, sys.stderr
    sys.stdin, sys.stdout, sys.stderr = sys.__stdin__, sys.__stdout__, sys.__stderr__
    yield
    sys.stdin, sys.stdout, sys.stderr = old_in, old_out, old_err


def _ti_context():
    from airflow.sdk.api.datamodels._generated import DagRun, TIRunContext

    dag_run = {
        "dag_id": "compat_async_extra_dag",
        "run_id": "compat_async_extra_run",
        "logical_date": "2026-01-01T00:00:00Z",
        "data_interval_start": "2026-01-01T00:00:00Z",
        "data_interval_end": "2026-01-01T00:00:00Z",
        "start_date": "2026-01-01T00:00:00Z",
        "run_after": "2026-01-01T00:00:00Z",
        "run_type": "manual",
        "conf": None,
        "consumed_asset_events": [],
    }
    # Fields added in later Task SDK versions, some nullable-but-required.
    optional = {"clear_number": 0, "state": "running", "end_date": None, "partition_key": None}
    dag_run.update({field: value for field, value in optional.items() if field in DagRun.model_fields})
    return TIRunContext(
        dag_run=DagRun.model_validate(dag_run),
        task_reschedule_count=0,
        max_tries=0,
        should_retry=False,
    )


def _task_instance():
    from airflow.sdk.api.datamodels._generated import TaskInstance

    fields = {
        "id": uuid.uuid4(),
        "task_id": "compat_async_extra_task",
        "dag_id": "compat_async_extra_dag",
        "run_id": "compat_async_extra_run",
        "try_number": 1,
        "map_index": -1,
        "dag_version_id": uuid.uuid4(),
        "queue": "default",
        "pool_slots": 1,
        "priority_weight": 1,
    }
    return TaskInstance.model_validate(
        {field: value for field, value in fields.items() if field in TaskInstance.model_fields}
    )


def _connection_response():
    from airflow.sdk.api.datamodels._generated import ConnectionResponse

    return ConnectionResponse.model_validate(
        {
            "conn_id": CONN_ID,
            "conn_type": "http",
            "host": None,
            "schema": None,
            "login": None,
            "password": None,
            "port": None,
            "extra": json.dumps(EXTRA),
        }
    )


def _read_extras_concurrently(result_path: str) -> None:
    """
    Run in the forked task process: real ``CommsDecoder`` to the real supervisor.

    Many coroutines resolve the connection (``asend()`` on the event loop) while others read
    its extra, which is where ``get_async_extra_dejson()`` may call the supervisor: a synchronous
    call there, while another coroutine's request is in flight, is the defect under test.
    """
    from concurrent.futures import ThreadPoolExecutor

    from asgiref.sync import SyncToAsync

    from airflow.providers.common.compat.connection import get_async_connection, get_async_extra_dejson
    from airflow.sdk.execution_time import task_runner
    from airflow.sdk.execution_time.comms import CommsDecoder, ToSupervisor, ToTask

    # This process was forked from pytest, which may already run asgiref's shared sync_to_async
    # thread (earlier tests): the copy here would wait forever for a thread that does not exist
    # in the fork. A real task process is forked from a supervisor that never started it.
    SyncToAsync.single_thread_executor = ThreadPoolExecutor(max_workers=1)

    comms: CommsDecoder[ToTask, ToSupervisor] = CommsDecoder()
    # Follow the protocol: the supervisor sends the startup details first.
    comms._get_response()
    task_runner.SUPERVISOR_COMMS = comms

    async def read_extra() -> dict:
        conn = await get_async_connection(CONN_ID)
        # A hook does other I/O between resolving the connection and reading its extra: yield, so
        # other coroutines' requests are in flight on the channel when the extra is read.
        await asyncio.sleep(0)
        return await get_async_extra_dejson(conn)

    async def main() -> list[dict]:
        extras: list[dict] = []
        for _ in range(ROUNDS):
            extras.extend(await asyncio.gather(*(read_extra() for _ in range(CONCURRENCY))))
        return extras

    extras = asyncio.run(main())
    with open(result_path, "w") as result:
        json.dump(extras, result)


# A version without the Task SDK's deadlock guard (before 3.3) blocks forever instead of raising:
# fail rather than hang the job.
@pytest.mark.execution_timeout(60)
@pytest.mark.usefixtures("disable_capturing")
def test_get_async_extra_dejson_with_the_real_supervisor_comms(tmp_path):
    from airflow.sdk.api.client import Client
    from airflow.sdk.execution_time.comms import BundleInfo
    from airflow.sdk.execution_time.supervisor import ActivitySubprocess

    client = mock.MagicMock(spec=Client)
    client.task_instances.start.return_value = _ti_context()
    client.connections.get.return_value = _connection_response()

    masked: list = []
    handle_request = ActivitySubprocess._handle_request

    def recording_handle_request(self, msg, log, req_id):
        # The real handling still runs: only record what the task process asked to mask.
        if type(msg).__name__ == "MaskSecret":
            masked.append(msg.value)
        return handle_request(self, msg, log, req_id)

    result_path = tmp_path / "extras.json"
    with mock.patch.object(ActivitySubprocess, "_handle_request", recording_handle_request):
        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=BundleInfo(name="compat", version="test"),
            what=_task_instance(),
            client=client,
            target=lambda: _read_extras_concurrently(str(result_path)),
        )
        exit_code = proc.wait()

    assert exit_code == 0, "the task process failed, see its output above"
    extras = json.loads(result_path.read_text())
    assert len(extras) == CONCURRENCY * ROUNDS
    assert all(extra == EXTRA for extra in extras)

    # The helper masks the decoded extra (a dict) through the supervisor; retrieving the
    # connection only masks the raw string.
    assert EXTRA in [value for value in masked if isinstance(value, dict)]
