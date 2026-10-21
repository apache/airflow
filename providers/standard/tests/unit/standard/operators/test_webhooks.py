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

import pytest

from airflow.providers.standard.version_compat import BaseOperator

if not hasattr(BaseOperator, "wait_for_webhook"):
    pytest.skip("Webhook waits need Airflow 3.5+", allow_module_level=True)

import importlib
import sys
from datetime import timedelta
from unittest import mock

import structlog
from uuid6 import uuid7

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.standard.exceptions import WebhookWaitTimeoutError
from airflow.providers.standard.operators.webhooks import WaitForWebhookOperator
from airflow.sdk import DAG, task, timezone
from airflow.sdk.api.datamodels._generated import TaskInstanceState, WaitStatus
from airflow.sdk.exceptions import ErrorType, WebhookWaitUnsupported
from airflow.sdk.execution_time.comms import (
    AwaitInputTask,
    DeferTask,
    ErrorResponse,
    GetXCom,
    InboundWaitResult,
    RegisterInboundWait,
    SetXCom,
    XComResult,
)
from airflow.sdk.execution_time.task_runner import run
from airflow.sdk.serde import serialize

NOW = timezone.datetime(2026, 10, 20, 12)
WAIT = InboundWaitResult(id=uuid7(), status=WaitStatus.REGISTERED, deadline=NOW + timedelta(days=3))
RESUME_EVENT = {
    "webhook": "github",
    "event": "pull_request",
    "delivery_id": "72d3162e-cc78-11e3-81ab-4c9367dc0958",
    "received_at": "2026-10-21T09:30:00+00:00",
    "attributes": {
        "event": "pull_request",
        "action": "closed",
        "repository": "my-org/configs",
        "number": 4242,
    },
    "payload": {"action": "closed", "number": 4242, "pull_request": {"merged": True}},
}


def run_until_done(ti):
    return run(ti, context=ti.get_template_context(), log=structlog.get_logger(logger_name="task"))


def resume_with(create_runtime_ti, event):
    ti = create_runtime_ti(
        task=WaitForWebhookOperator(
            task_id="wait_for_merge", webhook="github", match={"repository": "my-org/configs"}
        )
    )
    ti._ti_context_from_server.next_method = "execute_complete"
    ti._ti_context_from_server.next_kwargs = serialize({"event": event})
    return ti, run_until_done(ti)


@mock.patch.dict(sys.modules)
def test_import_without_wait_for_webhook_is_refused(monkeypatch):
    monkeypatch.delattr(BaseOperator, "wait_for_webhook")
    del sys.modules["airflow.providers.standard.operators.webhooks"]

    with pytest.raises(
        AirflowOptionalProviderFeatureException, match=r"^Webhook waits need Airflow 3\.5\+\.$"
    ):
        importlib.import_module("airflow.providers.standard.operators.webhooks")


def test_templated_wait_is_registered_and_parks(create_runtime_ti, mock_supervisor_comms, time_machine):
    """Templates in webhook, events and match are rendered when the task runs, then the task parks."""
    time_machine.move_to(NOW, tick=False)
    mock_supervisor_comms.send.return_value = WAIT
    ti = create_runtime_ti(
        task=WaitForWebhookOperator(
            task_id="wait_for_merge",
            webhook="{{ params.webhook }}",
            events=["{{ params.event }}"],
            match={
                "repository": "{{ params.repository }}",
                "number": 4242,
                "action": ["{{ params.action }}"],
            },
            timeout=timedelta(days=3),
            params={
                "webhook": "github",
                "event": "pull_request",
                "repository": "my-org/configs",
                "action": "closed",
            },
        )
    )

    state, _, error = run_until_done(ti)

    assert error is None
    assert state == TaskInstanceState.AWAITING_INPUT
    mock_supervisor_comms.send.assert_any_call(
        RegisterInboundWait(
            webhook="github",
            events=["pull_request"],
            match={"repository": "my-org/configs", "number": 4242, "action": ["closed"]},
            timeout=timedelta(days=3),
        )
    )
    mock_supervisor_comms.send.assert_any_call(
        AwaitInputTask(timeout=timedelta(days=3), next_method="execute_complete", next_kwargs={})
    )


def test_xcom_arg_in_match_sets_upstream_and_is_resolved(create_runtime_ti, mock_supervisor_comms):
    with DAG(dag_id="merge_gate", schedule=None, start_date=NOW):

        @task
        def open_pr() -> int:
            return 4242

        wait_for_merge = WaitForWebhookOperator(
            task_id="wait_for_merge",
            webhook="github",
            events=["pull_request"],
            match={"repository": "my-org/configs", "number": open_pr(), "action": "closed"},
        )

    assert wait_for_merge.upstream_task_ids == {"open_pr"}

    def answer(msg):
        if isinstance(msg, GetXCom):
            return XComResult(key=msg.key, value=serialize(4242))
        if isinstance(msg, RegisterInboundWait):
            return WAIT
        return None

    mock_supervisor_comms.send.side_effect = answer
    ti = create_runtime_ti(task=wait_for_merge)

    state, _, error = run_until_done(ti)

    assert error is None
    assert state == TaskInstanceState.AWAITING_INPUT
    mock_supervisor_comms.send.assert_any_call(
        RegisterInboundWait(
            webhook="github",
            events=["pull_request"],
            match={"repository": "my-org/configs", "number": 4242, "action": "closed"},
            timeout=None,
        )
    )


def test_server_without_webhook_waits_fails_the_task_without_another_way_to_wait(
    create_runtime_ti, mock_supervisor_comms
):
    mock_supervisor_comms.send.return_value = ErrorResponse(
        error=ErrorType.WEBHOOK_WAIT_UNSUPPORTED,
        detail={"message": "The API server does not support webhook waits"},
    )
    ti = create_runtime_ti(
        task=WaitForWebhookOperator(
            task_id="wait_for_merge", webhook="github", match={"repository": "my-org/configs"}
        )
    )

    state, _, error = run_until_done(ti)

    assert state == TaskInstanceState.FAILED
    assert isinstance(error, WebhookWaitUnsupported)
    assert str(error) == "The API server does not support webhook waits"
    sent = [
        msg
        for call in mock_supervisor_comms.send.call_args_list
        for msg in (*call.args, *call.kwargs.values())
    ]
    assert not [msg for msg in sent if isinstance(msg, (AwaitInputTask, DeferTask))]


def test_resumed_task_returns_the_delivery_as_its_xcom(create_runtime_ti, mock_supervisor_comms):
    ti, (state, _, error) = resume_with(create_runtime_ti, RESUME_EVENT)

    assert error is None
    assert state == TaskInstanceState.SUCCESS
    mock_supervisor_comms.send.assert_any_call(
        SetXCom(
            key="return_value",
            value=serialize(RESUME_EVENT),
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            task_id="wait_for_merge",
            map_index=-1,
        )
    )


def test_timed_out_wait_fails_the_task(create_runtime_ti, mock_supervisor_comms):
    timeout_event = {
        "error": "No matching delivery arrived before 2026-10-23T12:00:00+00:00",
        "error_type": "timeout",
    }

    _, (state, _, error) = resume_with(create_runtime_ti, timeout_event)

    assert state == TaskInstanceState.FAILED
    assert isinstance(error, WebhookWaitTimeoutError)
    assert str(error) == "No matching delivery arrived before 2026-10-23T12:00:00+00:00"
