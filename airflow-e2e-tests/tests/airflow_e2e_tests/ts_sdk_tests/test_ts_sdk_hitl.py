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
"""E2E tests for human-in-the-loop tasks in a native TypeScript Dag.

Run with::

    E2E_TEST_MODE=ts_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/ts_sdk_tests/test_ts_sdk_hitl.py -xvs

What is verified
----------------
``conftest._setup_ts_sdk_integration`` packs ``ts-sdk/example/src/ai-approval.ts`` and drops the bundle where the
Dag processor and the worker both see it. ``ts_hitl_ai_approval`` is a native TypeScript Dag with no Python Dag
file, so the Dag processor parses the ``.min.mjs`` itself. It is the agent-approval pattern, where a stand-in
model drafts a refund and a person decides before it is issued::

    agent_step >> review >> agent_continue

* ``agent_step`` calls the model, which drafts an ``issue_refund`` tool call and stops for approval.
* ``review`` is a ``hitl`` task. It writes the request, with the drafted call in the body and an editable
  ``amount`` field, and parks.
* ``agent_continue`` appends the decision to the history and calls the model a second time.

The Dag is triggered once and the module-scoped fixtures walk it in order: wait for ``review`` to park, then
answer it with a different amount and wait for the run to finish. Together the tests confirm:

1. A task of a native Dag parks in ``awaiting_input`` and the request is readable through the REST API, with the
   drafted call in the body and the ``amount`` param.
2. Parking needs no triggerer. ``airflow-triggerer`` is part of the base compose stack and is left running, so a
   passing run alone proves nothing. The evidence is in the data: a deferral creates a Trigger row and sets the
   task instance's ``trigger_id``, while a task parked in ``awaiting_input`` has none. The REST API exposes the
   row as ``trigger``, which is null exactly when ``trigger_id`` is.
3. The answer resumes the run, and the edited amount reaches ``agent_continue``'s model call and not the drafted
   one. ``agent_step``, which runs the model first, is not run again.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

_DAG_ID = "ts_hitl_ai_approval"

# A native Dag is parsed by running node in the Dag processor, so allow room for the first parse.
_DAG_PARSE_TIMEOUT = 300
# Scheduling and coordinator startup for `agent_step`, then the first run of `review`.
_PARK_TIMEOUT = 600
# Scheduling the resumed `review`, then `agent_continue`.
_RESUME_TIMEOUT = 600

# Match `DRAFTED_AMOUNT` in ts-sdk/example/src/ai-approval.ts.
_DRAFTED_AMOUNT = 120
_EDITED_AMOUNT = 80


@dataclass
class _ParkedReview:
    """The triggered run, as it was when ``review`` parked."""

    client: AirflowClient
    run_id: str
    task_instance: dict
    hitl_detail: dict


def _states(task_instances: dict[str, dict]) -> dict[str, str | None]:
    return {task_id: ti.get("state") for task_id, ti in task_instances.items()}


def _wait_for_review_to_park(client: AirflowClient, run_id: str) -> dict:
    """Poll until ``review`` is ``awaiting_input`` and return its task instance."""
    deadline = time.monotonic() + _PARK_TIMEOUT
    task_instances: dict[str, dict] = {}
    while time.monotonic() < deadline:
        resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
        task_instances = {ti["task_id"]: ti for ti in resp.get("task_instances", [])}
        review = task_instances.get("review")
        if review is not None and review.get("state") == "awaiting_input":
            return review
        states = _states(task_instances)
        assert "failed" not in states.values(), f"a task failed before 'review' parked: {states}"
        time.sleep(5)
    raise TimeoutError(
        f"'review' did not reach awaiting_input within {_PARK_TIMEOUT}s. task states: {_states(task_instances)}"
    )


@pytest.fixture(scope="module")
def parked_review() -> _ParkedReview:
    """Trigger ``ts_hitl_ai_approval`` once and wait for ``review`` to park."""
    client = AirflowClient()
    # Triggering waits only 120s for the Dag, which is short for a first parse by node.
    client.wait_for_dag(_DAG_ID, timeout=_DAG_PARSE_TIMEOUT)
    resp = client.trigger_dag(_DAG_ID, json={"logical_date": datetime.now(UTC).isoformat()})
    run_id = resp["dag_run_id"]
    task_instance = _wait_for_review_to_park(client, run_id)
    hitl_detail = client.get_hitl_detail(dag_id=_DAG_ID, run_id=run_id, task_id="review")
    return _ParkedReview(client=client, run_id=run_id, task_instance=task_instance, hitl_detail=hitl_detail)


@dataclass
class _AnsweredRun:
    client: AirflowClient
    run_id: str
    state: str
    ti_attrs: dict[str, dict]

    def xcom(self, task_id: str, key: str = "return_value"):
        return self.client.get_xcom_value(dag_id=_DAG_ID, task_id=task_id, run_id=self.run_id, key=key).get(
            "value"
        )


@pytest.fixture(scope="module")
def answered_run(parked_review: _ParkedReview) -> _AnsweredRun:
    """Approve ``review`` with an edited amount and wait for the run to finish."""
    client = parked_review.client
    run_id = parked_review.run_id
    client.respond_to_hitl(
        dag_id=_DAG_ID,
        run_id=run_id,
        task_id="review",
        chosen_options=["Approve"],
        params_input={"amount": _EDITED_AMOUNT},
    )
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=run_id, timeout=_RESUME_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
    ti_attrs = {ti["task_id"]: ti for ti in ti_resp.get("task_instances", [])}
    return _AnsweredRun(client=client, run_id=run_id, state=state, ti_attrs=ti_attrs)


def test_review_parks_without_a_trigger(parked_review: _ParkedReview):
    """``review`` is ``awaiting_input`` with no Trigger, though a triggerer is running.

    A deferral sets ``trigger_id``; the REST API shows that row as ``trigger``. A parked HITL task has none,
    so nothing for the triggerer to pick up.
    """
    ti = parked_review.task_instance
    assert ti["state"] == "awaiting_input", ti
    assert ti["trigger"] is None, f"'review' parked with a Trigger, so it deferred: {ti['trigger']!r}"


def test_hitl_detail_carries_the_draft(parked_review: _ParkedReview):
    """The request names the tool, shows the drafted input, and offers the amount to edit."""
    detail = parked_review.hitl_detail
    assert detail["subject"] == "Approve issue_refund?"
    assert detail["options"] == ["Approve", "Reject"]
    assert detail["response_received"] is False

    body = detail["body"]
    assert "issue_refund" in body, body
    assert f'"amount": {_DRAFTED_AMOUNT}' in body, body
    assert '"orderId": "ord_1042"' in body, body

    amount = detail["params"]["amount"]
    assert amount["value"] == _DRAFTED_AMOUNT, amount
    assert amount["schema"] == {"type": "number"}, amount
    assert amount["description"] == "Amount to refund", amount


def test_answer_resumes_the_run(answered_run: _AnsweredRun):
    assert answered_run.state == "success", (
        f"expected the run to succeed; got {answered_run.state!r}. "
        f"task states: {_states(answered_run.ti_attrs)}"
    )


def test_review_xcom_holds_the_edited_answer(answered_run: _AnsweredRun):
    value = answered_run.xcom("review")
    assert value["chosenOptions"] == ["Approve"], value
    assert value["paramsInput"] == {"amount": _EDITED_AMOUNT}, value
    assert value["timedout"] is False, value


def test_agent_continue_quotes_the_edited_amount(answered_run: _AnsweredRun):
    """The second model call saw the reviewer's figure, not the drafted one, and ran once."""
    value = answered_run.xcom("agent_continue")
    assert value == {
        "text": f"Refunded ${_EDITED_AMOUNT} on order ord_1042.",
        "modelCalls": 1,
        "approvedAmount": _EDITED_AMOUNT,
    }, f"unexpected 'agent_continue' return_value: {value!r}"


def test_model_step_did_not_run_again(answered_run: _AnsweredRun):
    """Parking and resuming ``review`` left ``agent_step``, the first model call, on its one attempt."""
    agent_step = answered_run.ti_attrs["agent_step"]
    assert agent_step["state"] == "success", agent_step
    assert agent_step["try_number"] == 1, agent_step
