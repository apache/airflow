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

import json
import uuid
from unittest import mock

import httpx2
import pytest
import time_machine
from structlog.typing import FilteringBoundLogger
from task_sdk import make_client

from airflow._shared.timezones import timezone
from airflow.sdk import Asset, AssetAlias, AssetEventReceipt, BaseOperator, emit_asset_event
from airflow.sdk.api.client import AssetEventOperations
from airflow.sdk.exceptions import (
    AirflowRuntimeError,
    AssetEmissionError,
    AssetEmissionUnavailableError,
    ErrorType,
)
from airflow.sdk.execution_time.asset_events import set_asset_emission_scope
from airflow.sdk.execution_time.comms import (
    AssetEventEmissionResult,
    EmitAssetEvent,
    ErrorResponse,
    SucceedTask,
    TaskState,
)
from airflow.sdk.execution_time.task_runner import finalize, run

DATE = timezone.datetime(2026, 10, 1)
ASSET = Asset(name="immediate", uri="example://immediate")


def _sent_messages(comms) -> list:
    return [call.args[0] if call.args else call.kwargs["msg"] for call in comms.send.call_args_list]


@pytest.mark.parametrize(
    ("asset", "name", "uri"),
    [
        pytest.param(ASSET, ASSET.name, ASSET.uri, id="asset"),
        pytest.param(Asset.ref(name=ASSET.name), ASSET.name, None, id="name-ref"),
        pytest.param(Asset.ref(uri=ASSET.uri), None, ASSET.uri, id="uri-ref"),
    ],
)
@pytest.mark.parametrize("callback_index", [None, 2])
@pytest.mark.parametrize("partition_key", [None, "region:eu"])
def test_emit(mock_supervisor_comms, asset, name, uri, callback_index, partition_key):
    mock_supervisor_comms.send.return_value = AssetEventEmissionResult(id=12, timestamp=DATE, replayed=True)
    with set_asset_emission_scope(callback_index=callback_index):
        receipt = emit_asset_event(
            asset, extra={"rows": 42}, partition_key=partition_key, idempotency_key="batch-1"
        )
    assert receipt == AssetEventReceipt(id=12, timestamp=DATE, replayed=True)
    mock_supervisor_comms.send.assert_called_once_with(
        EmitAssetEvent(
            name=name,
            uri=uri,
            extra={"rows": 42},
            partition_key=partition_key,
            idempotency_key="batch-1",
            callback_index=callback_index,
        )
    )


def test_each_call_without_a_key_gets_its_own_key(mock_supervisor_comms):
    mock_supervisor_comms.send.return_value = AssetEventEmissionResult(id=12, timestamp=DATE)
    with set_asset_emission_scope():
        emit_asset_event(ASSET)
        emit_asset_event(ASSET)
    keys = [message.idempotency_key for message in _sent_messages(mock_supervisor_comms)]
    assert len(set(keys)) == 2
    assert all(uuid.UUID(key) for key in keys)


def test_no_emission_outside_a_runtime_scope(mock_supervisor_comms):
    with pytest.raises(AssetEmissionUnavailableError) as error:
        emit_asset_event(ASSET)
    assert error.value.reason == "no_runtime"
    mock_supervisor_comms.send.assert_not_called()


def test_scope_is_reset_after_an_error(mock_supervisor_comms):
    with pytest.raises(ValueError, match="callback failed"), set_asset_emission_scope(callback_index=0):
        raise ValueError("callback failed")
    with pytest.raises(AssetEmissionUnavailableError):
        emit_asset_event(ASSET)
    mock_supervisor_comms.send.assert_not_called()


def test_rejects_an_alias(mock_supervisor_comms):
    with set_asset_emission_scope(), pytest.raises(TypeError, match="AssetAlias"):
        emit_asset_event(AssetAlias("alias"))  # type: ignore[arg-type]
    mock_supervisor_comms.send.assert_not_called()


@pytest.mark.parametrize("extra", [{"rows": float("nan")}, {"rows": float("inf")}, {"when": DATE}])
def test_extra_must_be_strict_json(mock_supervisor_comms, extra):
    with set_asset_emission_scope(), pytest.raises(AssetEmissionError) as error:
        emit_asset_event(ASSET, extra=extra)
    assert error.value.reason == "invalid_json"
    mock_supervisor_comms.send.assert_not_called()


def test_unexpected_response_is_not_a_receipt(mock_supervisor_comms):
    mock_supervisor_comms.send.return_value = None
    with set_asset_emission_scope(), pytest.raises(RuntimeError, match="Unexpected asset emission response"):
        emit_asset_event(ASSET)


@pytest.mark.parametrize(
    ("error", "expected_type", "reason", "status_code"),
    [
        pytest.param(
            ErrorResponse(
                error=ErrorType.API_SERVER_ERROR,
                detail={
                    "status_code": 409,
                    "message": "Conflict",
                    "detail": {"detail": {"reason": "idempotency_conflict", "message": "Key reused"}},
                },
            ),
            AssetEmissionError,
            "idempotency_conflict",
            409,
            id="server-reason",
        ),
        pytest.param(
            ErrorResponse(
                error=ErrorType.API_SERVER_ERROR,
                detail={"status_code": 404, "message": "Not Found", "detail": {"detail": "Not Found"}},
            ),
            AssetEmissionUnavailableError,
            "unsupported_server",
            404,
            id="server-without-endpoint",
        ),
        pytest.param(
            ErrorResponse(error=ErrorType.PERMISSION_DENIED),
            AssetEmissionUnavailableError,
            "forbidden",
            None,
            id="supervisor-refusal",
        ),
    ],
)
def test_server_errors_become_emission_errors(
    mock_supervisor_comms, error, expected_type, reason, status_code
):
    mock_supervisor_comms.send.side_effect = AirflowRuntimeError(error)
    with set_asset_emission_scope(), pytest.raises(expected_type) as raised:
        emit_asset_event(ASSET)
    assert type(raised.value) is expected_type
    assert raised.value.reason == reason
    assert raised.value.status_code == status_code


def test_task_emits_before_failure(create_runtime_ti, mock_supervisor_comms):
    class PublishThenFailOperator(BaseOperator):
        def execute(self, context):
            emit_asset_event(ASSET, partition_key="eu")
            raise ValueError("Failure after emission")

    mock_supervisor_comms.send.return_value = AssetEventEmissionResult(id=12, timestamp=DATE)
    ti = create_runtime_ti(task=PublishThenFailOperator(task_id="publish"))
    run(ti, context=ti.get_template_context(), log=mock.MagicMock(spec=FilteringBoundLogger))
    messages = _sent_messages(mock_supervisor_comms)
    emission_index = next(i for i, message in enumerate(messages) if isinstance(message, EmitAssetEvent))
    failure_index = next(i for i, message in enumerate(messages) if isinstance(message, TaskState))
    assert emission_index < failure_index
    assert messages[failure_index].state == "failed"


def test_success_callback_emits_after_the_terminal_state(create_runtime_ti, mock_supervisor_comms):
    class NoopOperator(BaseOperator):
        def execute(self, context):
            pass

    def announce(context):
        emit_asset_event(ASSET, idempotency_key="from-callback")

    mock_supervisor_comms.send.return_value = AssetEventEmissionResult(id=12, timestamp=DATE)
    ti = create_runtime_ti(task=NoopOperator(task_id="publish", on_success_callback=announce))
    context = ti.get_template_context()
    log = mock.MagicMock(spec=FilteringBoundLogger)
    state, _, error = run(ti, context=context, log=log)
    finalize(ti, state, context, log, error)

    messages = _sent_messages(mock_supervisor_comms)
    success_index = next(i for i, message in enumerate(messages) if isinstance(message, SucceedTask))
    emission_index = next(i for i, message in enumerate(messages) if isinstance(message, EmitAssetEvent))
    assert success_index < emission_index
    assert messages[emission_index].idempotency_key == "from-callback"


def test_emission_list_is_not_an_outlet(create_runtime_ti, mock_supervisor_comms):
    class NoopOperator(BaseOperator):
        def execute(self, context):
            pass

    mock_supervisor_comms.send.return_value = AssetEventEmissionResult(id=12, timestamp=DATE)
    task = NoopOperator(task_id="publish", emits=[ASSET])
    assert (task.emits, task.outlets) == ([ASSET], [])
    ti = create_runtime_ti(task=task)
    run(ti, context=ti.get_template_context(), log=mock.MagicMock(spec=FilteringBoundLogger))

    success = next(m for m in _sent_messages(mock_supervisor_comms) if isinstance(m, SucceedTask))
    assert (success.task_outlets, success.outlet_events) == ([], [])


def test_client_sends_the_key_and_retries_with_it():
    bodies: list[dict] = []
    responses = iter(
        [
            httpx2.Response(503, text="Service Unavailable"),
            httpx2.Response(200, json={"id": 12, "timestamp": "2026-10-01T00:00:00Z", "replayed": True}),
        ]
    )

    def handle_request(request: httpx2.Request) -> httpx2.Response:
        bodies.append(json.loads(request.read()))
        return next(responses)

    message = EmitAssetEvent(name=ASSET.name, uri=ASSET.uri, idempotency_key="k", callback_index=2)
    with time_machine.travel("2026-10-01T00:00:00Z", tick=False):
        client = make_client(httpx2.MockTransport(handle_request))
        result = AssetEventOperations(client).emit(message, source_dag_id="producer", source_run_id="run")

    assert result.replayed is True
    assert len(bodies) == 2
    assert (
        bodies[0]
        == bodies[1]
        == {
            "name": ASSET.name,
            "uri": ASSET.uri,
            "extra": {},
            "partition_key": None,
            "idempotency_key": "k",
            "source_dag_id": "producer",
            "source_run_id": "run",
        }
    )
