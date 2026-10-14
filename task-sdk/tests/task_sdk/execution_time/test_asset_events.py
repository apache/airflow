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

from unittest import mock

import pytest
from structlog.typing import FilteringBoundLogger

from airflow._shared.timezones import timezone
from airflow.sdk import Asset, BaseOperator
from airflow.sdk.api.client import AssetEventOperations, Client
from airflow.sdk.definitions.asset import AssetEventReceipt
from airflow.sdk.execution_time.asset_events import set_asset_emission_callback
from airflow.sdk.execution_time.comms import AssetEventEmissionResult, EmitAssetEvent, TaskState
from airflow.sdk.execution_time.context import set_current_context
from airflow.sdk.execution_time.task_runner import run

DATE = timezone.datetime(2026, 10, 1)
ASSET = Asset(name="immediate", uri="example://immediate")


@pytest.mark.parametrize("scope", ["task", "callback"])
@pytest.mark.parametrize("partition_key", [None, "2026-10-01", "region:eu"])
def test_emit(mock_supervisor_comms, scope, partition_key):
    send_comms = mock_supervisor_comms
    send_comms.send.return_value = AssetEventEmissionResult(id=12, timestamp=DATE)
    runtime = set_current_context({"ti": object()}) if scope == "task" else set_asset_emission_callback(2)
    partition_date = DATE if partition_key else None
    with runtime:
        receipt = ASSET.emit(extra={"rows": 42}, partition_key=partition_key, partition_date=partition_date)
    assert receipt == AssetEventReceipt(id=12, timestamp=DATE)
    send_comms.send.assert_called_once_with(
        EmitAssetEvent(
            name=ASSET.name,
            uri=ASSET.uri,
            extra={"rows": 42},
            partition_key=partition_key,
            partition_date=partition_date,
            callback_index=2 if scope == "callback" else None,
        )
    )


def test_no_parse_time_emission(mock_supervisor_comms):
    send_comms = mock_supervisor_comms
    with pytest.raises(RuntimeError):
        ASSET.emit()
    send_comms.send.assert_not_called()


def test_callback_context_cleared_after_error(mock_supervisor_comms):
    send_comms = mock_supervisor_comms
    with pytest.raises(ValueError, match="callback failed"), set_asset_emission_callback(0):
        raise ValueError("callback failed")
    with pytest.raises(RuntimeError):
        ASSET.emit()
    send_comms.send.assert_not_called()


def test_task_context_requires_ti(mock_supervisor_comms):
    with set_current_context({}), pytest.raises(RuntimeError, match="requires a running task"):
        ASSET.emit()
    mock_supervisor_comms.send.assert_not_called()


def test_unexpected_response_is_not_a_receipt(mock_supervisor_comms):
    mock_supervisor_comms.send.return_value = None
    with (
        set_asset_emission_callback(0),
        pytest.raises(RuntimeError, match="Unexpected asset emission response"),
    ):
        ASSET.emit()


def test_task_emits_before_failure(create_runtime_ti, mock_supervisor_comms):
    class PublishThenFailOperator(BaseOperator):
        def execute(self, context):
            ASSET.emit(partition_key="eu", partition_date=DATE)
            raise ValueError("Failure after emission")

    mock_supervisor_comms.send.return_value = AssetEventEmissionResult(id=12, timestamp=DATE)
    ti = create_runtime_ti(task=PublishThenFailOperator(task_id="publish"))
    run(ti, context=ti.get_template_context(), log=mock.MagicMock(spec=FilteringBoundLogger))
    messages = [
        call.args[0] if call.args else call.kwargs["msg"]
        for call in mock_supervisor_comms.send.call_args_list
    ]
    emission_index = next(i for i, message in enumerate(messages) if isinstance(message, EmitAssetEvent))
    failure_index = next(i for i, message in enumerate(messages) if isinstance(message, TaskState))
    assert emission_index < failure_index
    assert messages[failure_index].state == "failed"


def test_client_does_not_retry_or_forward_callback_selector():
    client = mock.create_autospec(Client, instance=True)
    client.request.return_value.read.return_value = b'{"id": 12, "timestamp": "2026-10-01T00:00:00Z"}'
    message = EmitAssetEvent(name=ASSET.name, uri=ASSET.uri, callback_index=2, partition_key="eu")
    result = AssetEventOperations(client).emit(message, source_dag_id="producer", source_run_id="run")
    assert result.id == 12
    client.request.assert_called_once_with(
        "POST",
        "asset-events/emit",
        json={
            "name": ASSET.name,
            "uri": ASSET.uri,
            "extra": {},
            "partition_key": "eu",
            "partition_date": None,
            "source_dag_id": "producer",
            "source_run_id": "run",
        },
        retry=False,
    )
