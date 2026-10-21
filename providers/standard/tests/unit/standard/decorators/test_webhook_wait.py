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

from datetime import timedelta

import structlog
from uuid6 import uuid7

from airflow.providers.standard.decorators.webhook_wait import webhook_wait_task
from airflow.sdk import DAG, task, timezone
from airflow.sdk.api.datamodels._generated import TaskInstanceState, WaitStatus
from airflow.sdk.execution_time.comms import (
    AwaitInputTask,
    GetXCom,
    InboundWaitResult,
    RegisterInboundWait,
    SetXCom,
    XComResult,
)
from airflow.sdk.execution_time.task_runner import run
from airflow.sdk.serde import serialize

NOW = timezone.datetime(2026, 10, 20, 12)
REPOSITORY = "my-org/configs"
WAIT = InboundWaitResult(id=uuid7(), status=WaitStatus.REGISTERED, deadline=NOW + timedelta(days=3))
RESUME_EVENT = {
    "webhook": "github",
    "event": "pull_request",
    "delivery_id": "72d3162e-cc78-11e3-81ab-4c9367dc0958",
    "received_at": "2026-10-21T09:30:00+00:00",
    "attributes": {"event": "pull_request", "action": "closed", "repository": REPOSITORY, "number": 4242},
    "payload": {"action": "closed", "number": 4242, "pull_request": {"merged": True}},
}


def run_until_done(ti):
    return run(ti, context=ti.get_template_context(), log=structlog.get_logger(logger_name="task"))


def get_sent_messages(mock_supervisor_comms):
    return [
        msg
        for call in mock_supervisor_comms.send.call_args_list
        for msg in (*call.args, *call.kwargs.values())
    ]


def test_match_is_built_from_the_function_arguments_and_the_task_parks(
    create_runtime_ti, mock_supervisor_comms, time_machine
):
    """XComArgs, templates and context keys reach the function; webhook and events resolve templates."""
    time_machine.move_to(NOW, tick=False)
    with DAG(dag_id="merge_gate", schedule=None, start_date=NOW):

        @task
        def open_pr() -> int:
            return 4242

        @task.webhook_wait(
            webhook="{{ params.webhook }}",
            events=["{{ params.event }}"],
            timeout=timedelta(days=3),
            params={
                "webhook": "github",
                "event": "pull_request",
                "action": "closed",
                "repository": REPOSITORY,
            },
        )
        def wait_for_merge(number: int, action: str, params: dict) -> dict:
            return {"repository": params["repository"], "number": str(number), "action": action}

        wait = wait_for_merge(open_pr(), action="{{ params.action }}").operator

    assert wait.upstream_task_ids == {"open_pr"}

    def answer(msg):
        if isinstance(msg, GetXCom):
            return XComResult(key=msg.key, value=serialize(4242))
        if isinstance(msg, RegisterInboundWait):
            return WAIT
        return None

    mock_supervisor_comms.send.side_effect = answer
    ti = create_runtime_ti(task=wait)

    state, _, error = run_until_done(ti)

    assert error is None
    assert state == TaskInstanceState.AWAITING_INPUT
    mock_supervisor_comms.send.assert_any_call(
        RegisterInboundWait(
            webhook="github",
            events=["pull_request"],
            match={"repository": REPOSITORY, "number": "4242", "action": "closed"},
            timeout=timedelta(days=3),
        )
    )
    mock_supervisor_comms.send.assert_any_call(
        AwaitInputTask(timeout=timedelta(days=3), next_method="execute_complete", next_kwargs={})
    )


@pytest.mark.parametrize(
    ("returned", "type_name"),
    [
        pytest.param(None, "NoneType", id="none"),
        pytest.param([("repository", REPOSITORY)], "list", id="list"),
    ],
)
def test_function_that_returns_no_dict_fails_the_task_without_a_wait(
    create_runtime_ti, mock_supervisor_comms, returned, type_name
):
    @webhook_wait_task(webhook="github")
    def wait_for_merge():
        return returned

    ti = create_runtime_ti(task=wait_for_merge().operator)

    state, _, error = run_until_done(ti)

    assert state == TaskInstanceState.FAILED
    assert isinstance(error, TypeError)
    assert str(error) == f"The @task.webhook_wait function must return the match as a dict, not {type_name}"
    assert not [
        msg
        for msg in get_sent_messages(mock_supervisor_comms)
        if isinstance(msg, (RegisterInboundWait, AwaitInputTask))
    ]


def test_resumed_task_returns_the_delivery_as_its_xcom_without_running_the_function(
    create_runtime_ti, mock_supervisor_comms
):
    calls = []

    @webhook_wait_task(webhook="github", events=["pull_request"])
    def wait_for_merge(number: int) -> dict:
        calls.append(number)
        return {"repository": REPOSITORY, "number": str(number), "action": "closed"}

    ti = create_runtime_ti(task=wait_for_merge(4242).operator)
    ti._ti_context_from_server.next_method = "execute_complete"
    ti._ti_context_from_server.next_kwargs = serialize({"event": RESUME_EVENT})

    state, _, error = run_until_done(ti)

    assert error is None
    assert state == TaskInstanceState.SUCCESS
    assert calls == []
    # The dict return annotation describes the match, so the delivery is one XCom, not one per key.
    assert [msg for msg in get_sent_messages(mock_supervisor_comms) if isinstance(msg, SetXCom)] == [
        SetXCom(
            key="return_value",
            value=serialize(RESUME_EVENT),
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            task_id="wait_for_merge",
            map_index=-1,
        )
    ]


def test_match_argument_is_refused():
    @webhook_wait_task(webhook="github", match={"repository": REPOSITORY})
    def wait_for_merge(number: int) -> dict:
        return {"number": str(number)}

    with pytest.raises(
        TypeError,
        match=r"^@task\.webhook_wait takes its match from what the function returns, not from a match argument$",
    ):
        wait_for_merge(4242)
