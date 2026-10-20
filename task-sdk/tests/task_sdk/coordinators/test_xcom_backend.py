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

import os
import selectors
import signal
import socket
import time
from typing import cast
from unittest.mock import MagicMock, create_autospec, patch

import httpx2
import msgspec
import pytest
import structlog
from task_sdk import make_client
from uuid6 import uuid7

from airflow.sdk import Connection
from airflow.sdk.api.client import Client
from airflow.sdk.bases.xcom import BaseXCom, XComCoordinate
from airflow.sdk.coordinators._xcom_backend import (
    AwaitXComBackendCall,
    XComBackendCall,
    XComBackendProcess,
    XComBackendResult,
    _HookRunner,
    is_hook_overridden,
    run_xcom_backend,
)
from airflow.sdk.execution_time.supervisor import ProcessTracker, WatchedSubprocess

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_3_PLUS:
    pytest.skip("Coordinator is only compatible with Airflow >= 3.3.0", allow_module_level=True)


class _StoreXCom(BaseXCom):
    @classmethod
    def store_value(cls, coordinate, key, value):
        return {"ti_id": str(coordinate.ti_id), "value": value}


class _ConnectionXCom(BaseXCom):
    """Stores the host of a Connection with each value, so a test sees the Connection lookup."""

    @classmethod
    def store_value(cls, coordinate, key, value):
        return {"host": Connection.get("xcom_store").host, "value": value}

    @classmethod
    def load_value(cls, stored):
        return tuple(stored["value"])

    @classmethod
    def purge(cls, xcom, *args):
        raise PermissionError("purge refused")


class _LoadXCom(BaseXCom):
    loaded: object = None

    @classmethod
    def load_value(cls, stored):
        return cls.loaded


class _FailingLoadXCom(BaseXCom):
    @classmethod
    def load_value(cls, stored):
        raise ImportError("cannot import the class of the value")


class _PurgeOnlyXCom(BaseXCom):
    @classmethod
    def purge(cls, xcom, *args):
        pass


class _SerializeOnlyXCom(BaseXCom):
    @staticmethod
    def serialize_value(value, *, key=None, task_id=None, dag_id=None, run_id=None, map_index=None):
        return value


class _DeserializeOnlyXCom(BaseXCom):
    @staticmethod
    def deserialize_value(result):
        return result.value


@pytest.mark.parametrize(
    ("xcom", "hooks", "expected"),
    [
        pytest.param(BaseXCom, ("store_value", "serialize_value"), False, id="default-backend"),
        pytest.param(_StoreXCom, ("store_value", "serialize_value"), True, id="store_value"),
        pytest.param(_SerializeOnlyXCom, ("store_value", "serialize_value"), True, id="serialize_value"),
        pytest.param(_DeserializeOnlyXCom, ("load_value", "deserialize_value"), True, id="deserialize_value"),
        pytest.param(_PurgeOnlyXCom, ("store_value", "serialize_value"), False, id="other-hook"),
        pytest.param(_PurgeOnlyXCom, ("purge",), True, id="purge"),
    ],
)
def test_is_hook_overridden(xcom, hooks, expected):
    assert is_hook_overridden(xcom, *hooks) is expected


def test_store_call_passes_the_coordinate_to_the_backend():
    ti_id = uuid7()
    call = XComBackendCall.store(
        XComCoordinate("dag", "run", "task", ti_id), -1, "return_value", {"orders": 12}
    )

    assert _HookRunner(_StoreXCom).run(call) == XComBackendResult(
        value={"ti_id": str(ti_id), "value": {"orders": 12}}
    )


@pytest.mark.parametrize(
    ("xcom", "loaded", "row"),
    [
        pytest.param(_FailingLoadXCom, None, "s3://bucket/xcom.json", id="cannot-import"),
        pytest.param(
            BaseXCom,
            None,
            {"__classname__": "builtins.tuple", "__version__": 99, "__data__": [1]},
            id="cannot-decode",
        ),
        pytest.param(_LoadXCom, object(), "s3://bucket/xcom.json", id="cannot-encode"),
    ],
)
def test_load_call_sends_the_row_when_the_value_cannot_be_sent(xcom, loaded, row):
    _LoadXCom.loaded = loaded

    result = _HookRunner(xcom).run(XComBackendCall(hook="load", key="return_value", value=row))

    assert result == XComBackendResult(value=row)


def test_failed_call_reports_the_error():
    result = _HookRunner(_ConnectionXCom).run(XComBackendCall(hook="purge", key="return_value", value={}))

    assert result == XComBackendResult(error="PermissionError: purge refused")


@pytest.mark.parametrize("uses_exec", [True, False])
@patch.object(WatchedSubprocess, "start", autospec=True)
def test_starts_like_a_task_process(mock_start, uses_exec):
    task_id = uuid7()
    client = create_autospec(Client, instance=True)
    logger = structlog.get_logger(logger_name="task")
    selector = selectors.DefaultSelector()
    on_result = MagicMock()
    with patch(
        "airflow.sdk.coordinators._xcom_backend.task_process_uses_exec", autospec=True, return_value=uses_exec
    ):
        XComBackendProcess.start(
            id=task_id, client=client, logger=logger, selector=selector, on_result=on_result
        )
    selector.close()

    assert mock_start.call_args.kwargs["target"] is run_xcom_backend
    assert mock_start.call_args.kwargs["use_exec"] is uses_exec
    assert mock_start.call_args.kwargs["id"] == task_id
    assert mock_start.call_args.kwargs["client"] is client
    assert mock_start.call_args.kwargs["selector"] is selector
    assert mock_start.call_args.kwargs["on_result"] is on_result


def _run_call(
    proc: XComBackendProcess, results: list[XComBackendResult], call: XComBackendCall, timeout: float = 10.0
) -> XComBackendResult:
    """Send *call* and service the process until it answers, the way the task's request loop does."""
    proc.send_call(call)
    deadline = time.monotonic() + timeout
    while not results and time.monotonic() < deadline:
        proc._service_subprocess(max_wait_time=0.1)
    assert results, "the XCom backend process did not answer the call"
    return results.pop()


def _decode_sent_call(stdin: socket.socket) -> dict:
    """Decode the body of the frame most recently sent to the helper process's ``stdin`` mock."""
    frame = cast("MagicMock", stdin).sendall.call_args.args[0]
    return msgspec.msgpack.decode(frame[4:])[1]


class TestXComBackendProcess:
    @pytest.fixture
    def results(self) -> list[XComBackendResult]:
        return []

    @pytest.fixture
    def backend_process(self, results):
        def handle_request(request: httpx2.Request) -> httpx2.Response:
            if request.url.path == "/connections/xcom_store":
                return httpx2.Response(
                    200,
                    json={
                        "conn_id": "xcom_store",
                        "conn_type": "http",
                        "host": "store.example.com",
                        "schema": None,
                        "login": None,
                        "password": None,
                        "port": None,
                        "extra": None,
                    },
                )
            return httpx2.Response(404, json={"detail": "not found"})

        # Forking lets the child inherit the backend set here.
        client = make_client(transport=httpx2.MockTransport(handle_request))
        with (
            patch("airflow.sdk.execution_time.xcom.XCom", _ConnectionXCom),
            patch(
                "airflow.sdk.coordinators._xcom_backend.task_process_uses_exec",
                autospec=True,
                return_value=False,
            ),
        ):
            proc = XComBackendProcess.start(
                id=uuid7(),
                client=client,
                logger=structlog.get_logger(logger_name="task"),
                selector=selectors.DefaultSelector(),
                on_result=results.append,
            )
        yield proc
        proc.stop()
        proc.selector.close()

    def test_runs_the_calls_and_exits(self, backend_process, results):
        coordinate = XComCoordinate("dag", "run", "task", uuid7())

        stored = _run_call(
            backend_process, results, XComBackendCall.store(coordinate, -1, "return_value", [12, 3402.0])
        )
        loaded = _run_call(
            backend_process, results, XComBackendCall(hook="load", key="return_value", value=stored.value)
        )
        backend_process.stop()

        assert stored.value == {"host": "store.example.com", "value": [12, 3402.0]}
        assert loaded.value == {"__classname__": "builtins.tuple", "__version__": 1, "__data__": [12, 3402.0]}
        assert backend_process._exit_code == 0

    def test_reports_the_error_of_a_failed_call(self, backend_process, results):
        result = _run_call(
            backend_process, results, XComBackendCall(hook="purge", key="return_value", value={})
        )

        assert result.error == "PermissionError: purge refused"

    def test_stops_a_process_that_died_while_waiting_for_a_call(self, backend_process):
        deadline = time.monotonic() + 10
        while backend_process._call_request_id is None:
            assert time.monotonic() < deadline, "The XCom backend process did not ask for a call"
            backend_process._service_subprocess(max_wait_time=1)
        os.kill(backend_process.pid, signal.SIGKILL)
        os.waitid(os.P_PID, backend_process.pid, os.WEXITED | os.WNOWAIT)

        backend_process.stop()

        assert backend_process._exit_code == -signal.SIGKILL

    def test_reports_an_error_when_the_process_exits_before_it_answers(self, backend_process, results):
        os.kill(backend_process.pid, signal.SIGKILL)

        result = _run_call(
            backend_process, results, XComBackendCall(hook="purge", key="return_value", value={})
        )

        assert result.error == "The XCom backend process exited before it answered"


class TestXComBackendProcessSendCall:
    """Deterministic, non-forking tests for the behaviour of ``send_call`` itself."""

    @pytest.fixture
    def on_result(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def backend(self, on_result):
        proc = XComBackendProcess(
            id=uuid7(),
            pid=12345,
            process=MagicMock(spec=ProcessTracker),
            process_log=structlog.get_logger(logger_name="task"),
            stdin=MagicMock(spec=socket.socket),
            client=create_autospec(Client, instance=True),
            on_result=on_result,
        )
        yield proc
        proc.selector.close()

    def test_a_call_sent_before_the_process_asks_is_held_until_it_does(self, backend):
        call = XComBackendCall(hook="purge", key="return_value", value={})

        backend.send_call(call)

        assert cast("MagicMock", backend.stdin).sendall.call_count == 0

        backend._handle_await_call(AwaitXComBackendCall(), structlog.get_logger(logger_name="task"), req_id=1)

        assert cast("MagicMock", backend.stdin).sendall.call_count == 1
        assert _decode_sent_call(backend.stdin)["hook"] == "purge"

    def test_a_closed_requests_socket_reports_that_the_process_exited(self, backend, on_result):
        other_socket = MagicMock(spec=socket.socket)

        backend._on_socket_closed(other_socket)

        on_result.assert_not_called()

        backend._on_socket_closed(backend.stdin)

        on_result.assert_called_once_with(
            XComBackendResult(error="The XCom backend process exited before it answered")
        )
