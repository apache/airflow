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

import datetime
from typing import TYPE_CHECKING, Any

import pytest
import structlog

from tests_common.test_utils.version_compat import AIRFLOW_V_3_4_PLUS

if not AIRFLOW_V_3_4_PLUS:
    pytest.skip("request_input() needs Airflow 3.4+", allow_module_level=True)

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, ParamValidationError
from airflow.providers.standard.exceptions import HITLTimeoutError
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import HITLOperator, request_input
from airflow.providers.standard.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.providers.standard.utils.hitl import answer_log_var
from airflow.sdk import DAG, BaseNotifier, Param, task
from airflow.sdk.exceptions import TaskAwaitingInput
from airflow.sdk.execution_time.comms import (
    AwaitInputTask,
    CreateHITLDetailPayload,
    SetXCom,
    SkipDownstreamTasks,
)
from airflow.sdk.execution_time.context import set_current_context
from airflow.sdk.execution_time.task_runner import run
from airflow.sdk.serde import deserialize, serialize
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

RESPONDED_AT = datetime.datetime(2026, 10, 21, 12, 0, tzinfo=datetime.UTC)
REVIEWER = {"id": "alice", "name": "Alice"}


def _answer(chosen: str, *, reason: str = "", timedout: bool = False) -> dict[str, Any]:
    """The event the Core API response handler puts in next_kwargs (HITLDetail.as_resume_event_payload)."""
    return {
        "chosen_options": [chosen],
        "params_input": {"reason": reason},
        "responded_at": RESPONDED_AT,
        "responded_by_user": None if timedout else REVIEWER,
        "timedout": timedout,
    }


def _plain_answer(chosen: str) -> dict[str, Any]:
    """An answer to a request that has no params."""
    return {**_answer(chosen), "params_input": {}}


def _resume(ti: RuntimeTaskInstance, park_msg: AwaitInputTask, event: dict[str, Any]) -> None:
    """Set up ``ti`` the way the server hands back a task whose request was answered."""
    server_context = ti._ti_context_from_server
    parked_kwargs = deserialize(park_msg.next_kwargs)
    assert server_context is not None
    assert isinstance(parked_kwargs, dict)
    server_context.next_method = park_msg.next_method
    # handle_event_submit keeps the parked kwargs and adds the response as ``event``.
    server_context.next_kwargs = serialize({**parked_kwargs, "event": event})


def _sent(mock_supervisor_comms, msg_type: type) -> list[Any]:
    # Some callers pass the message positionally, others as msg=.
    messages = (
        c.kwargs.get("msg", c.args[0] if c.args else None) for c in mock_supervisor_comms.send.call_args_list
    )
    return [msg for msg in messages if isinstance(msg, msg_type)]


def _hitl_requests(mock_supervisor_comms) -> list[CreateHITLDetailPayload]:
    return _sent(mock_supervisor_comms, CreateHITLDetailPayload)


def _skipped_task_ids(mock_supervisor_comms) -> list[str]:
    return [
        t if isinstance(t, str) else t[0]
        for msg in _sent(mock_supervisor_comms, SkipDownstreamTasks)
        for t in msg.tasks
    ]


def _return_values(mock_supervisor_comms) -> list[Any]:
    return [x.value for x in _sent(mock_supervisor_comms, SetXCom) if x.key == "return_value"]


def _noop():
    pass


def _run(ti: RuntimeTaskInstance):
    return run(ti, context=ti.get_template_context(), log=structlog.get_logger())


def _answer_and_run(create_runtime_ti, op, ti_id, park_msg: AwaitInputTask, event: dict[str, Any]):
    """Resume the task with ``event`` as the answer to the request it parked on, and run it."""
    resumed = create_runtime_ti(task=op, ti_id=ti_id)
    _resume(resumed, park_msg, event)
    return _run(resumed)


class _RecordingNotifier(BaseNotifier):
    def __init__(self) -> None:
        super().__init__()
        self.task_ids: list[str] = []

    def notify(self, context) -> None:
        self.task_ids.append(context["ti"].task_id)


class TestRequestInputFromTask:
    @pytest.fixture
    def seen(self) -> list[Any]:
        return []

    @pytest.fixture
    def refund_op(self, seen):
        with DAG(dag_id="refund_dag"):

            @task
            def refund(order_id: str):
                answer = request_input(
                    subject=f"Refund {order_id}?",
                    body="The agent wants to call `refund_order`.",
                    options=["Approve", "Reject"],
                    params={"reason": Param("", type=["string", "null"])},
                    timeout=datetime.timedelta(hours=4),
                )
                seen.append(answer)
                if answer["chosen_options"] == ["Approve"]:
                    return f"refunded {order_id}"
                return f"refused {order_id}: {answer['params_input']['reason']}"

            xcom_arg = refund("o-1")
        return xcom_arg.operator

    def test_first_run_writes_the_request_and_waits_in_awaiting_input(
        self, refund_op, seen, create_runtime_ti, mock_supervisor_comms
    ):
        ti = create_runtime_ti(task=refund_op)

        state, msg, err = _run(ti)

        assert err is None
        assert state == TaskInstanceState.AWAITING_INPUT
        assert isinstance(msg, AwaitInputTask)
        assert msg.next_method == "resume_with_input"
        (request,) = _hitl_requests(mock_supervisor_comms)
        # The resume checks the answer against the options and params the reviewer was shown.
        assert deserialize(msg.next_kwargs) == {
            "key": "Refund o-1?",
            "options": ["Approve", "Reject"],
            "params": request.params,
            "answered": [],
        }
        assert msg.timeout == datetime.timedelta(hours=4)
        assert request.ti_id == ti.id
        assert request.subject == "Refund o-1?"
        assert request.options == ["Approve", "Reject"]
        assert request.body == "The agent wants to call `refund_order`."
        assert set(request.params) == {"reason"}
        assert seen == []

    @pytest.mark.parametrize(
        ("chosen", "reason", "expected"),
        [
            pytest.param("Approve", "", "refunded o-1", id="approve"),
            pytest.param("Reject", "over limit", "refused o-1: over limit", id="reject"),
        ],
    )
    def test_resumed_run_returns_the_answer_from_the_same_call(
        self, refund_op, seen, create_runtime_ti, mock_supervisor_comms, chosen, reason, expected
    ):
        ti = create_runtime_ti(task=refund_op)
        _, park_msg, _ = _run(ti)

        state, _, err = _answer_and_run(
            create_runtime_ti, refund_op, ti.id, park_msg, _answer(chosen, reason=reason)
        )

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert seen == [
            {
                "chosen_options": [chosen],
                "params_input": {"reason": reason},
                "responded_at": RESPONDED_AT,
                "responded_by_user": REVIEWER,
                "timedout": False,
            }
        ]
        assert _return_values(mock_supervisor_comms) == [expected]
        assert len(_hitl_requests(mock_supervisor_comms)) == 1

    def test_timeout_without_defaults_fails_the_task(
        self, refund_op, seen, create_runtime_ti, mock_supervisor_comms
    ):
        ti = create_runtime_ti(task=refund_op)
        _, park_msg, _ = _run(ti)

        state, _, err = _answer_and_run(
            create_runtime_ti,
            refund_op,
            ti.id,
            park_msg,
            {
                "error": "The Human-in-the-loop response timeout has passed without a response.",
                "error_type": "timeout",
            },
        )

        assert state == TaskInstanceState.FAILED
        assert isinstance(err, HITLTimeoutError)
        assert seen == []

    def test_answer_outside_options_fails(self, refund_op, create_runtime_ti, mock_supervisor_comms):
        ti = create_runtime_ti(task=refund_op)
        _, park_msg, _ = _run(ti)

        state, _, err = _answer_and_run(create_runtime_ti, refund_op, ti.id, park_msg, _answer("Maybe"))

        assert state == TaskInstanceState.FAILED
        assert isinstance(err, ValueError)

    def test_params_input_is_checked_against_the_request_params(
        self, refund_op, seen, create_runtime_ti, mock_supervisor_comms
    ):
        ti = create_runtime_ti(task=refund_op)
        _, park_msg, _ = _run(ti)

        state, _, err = _answer_and_run(
            create_runtime_ti,
            refund_op,
            ti.id,
            park_msg,
            {**_answer("Approve"), "params_input": {"reason": 5}},
        )

        assert state == TaskInstanceState.FAILED
        assert isinstance(err, ParamValidationError)
        assert "Invalid input for param reason" in str(err)
        assert seen == []


class TestRequestFields:
    def test_every_argument_reaches_the_request_and_defaults_apply_at_timeout(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        notifier = _RecordingNotifier()
        seen: list[Any] = []
        with DAG(dag_id="fields_dag"):

            @task
            def ask():
                answer = request_input(
                    subject="Pick sizes",
                    body="**Which** sizes?",
                    options=["S", "M", "L"],
                    defaults="M",
                    multiple=True,
                    params={"note": Param("", type=["string", "null"])},
                    notifiers=notifier,
                    assigned_users={"id": "alice", "name": "Alice"},
                    timeout=datetime.timedelta(minutes=7),
                )
                seen.append(answer)
                return answer["chosen_options"]

            op = ask().operator
        ti = create_runtime_ti(task=op)

        _, park_msg, _ = _run(ti)

        (request,) = _hitl_requests(mock_supervisor_comms)
        assert request.subject == "Pick sizes"
        assert request.body == "**Which** sizes?"
        assert request.options == ["S", "M", "L"]
        assert request.defaults == ["M"]
        assert request.multiple is True
        assert set(request.params) == {"note"}
        assert [(u.id, u.name) for u in request.assigned_users] == [("alice", "Alice")]
        assert park_msg.timeout == datetime.timedelta(minutes=7)
        assert notifier.task_ids == ["ask"]

        # The scheduler's timeout sweep resumes a request that has defaults with them applied.
        state, _, err = _answer_and_run(
            create_runtime_ti,
            op,
            ti.id,
            park_msg,
            {
                "chosen_options": ["M"],
                "params_input": {"note": ""},
                "responded_at": RESPONDED_AT,
                "responded_by_user": None,
                "timedout": True,
            },
        )

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert seen[0]["timedout"] is True
        assert seen[0]["responded_by_user"] is None
        assert seen[0]["responded_at"] == RESPONDED_AT

    def test_a_required_param_without_a_default_survives_the_pause(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        with DAG(dag_id="required_param_dag"):

            @task
            def ask():
                answer = request_input(
                    subject="Why?",
                    options=["Approve", "Reject"],
                    params={"reason": Param(type="string", minLength=1)},
                )
                return answer["params_input"]["reason"]

            op = ask().operator
        ti = create_runtime_ti(task=op)
        _, park_msg, _ = _run(ti)

        state, _, err = _answer_and_run(
            create_runtime_ti, op, ti.id, park_msg, {**_answer("Approve"), "params_input": {"reason": "ok"}}
        )

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert _return_values(mock_supervisor_comms) == ["ok"]

    def test_hitl_operator_parks_with_its_own_resume_method_and_timeout(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        op = HITLOperator(
            task_id="hitl",
            subject="Go?",
            options=["Yes", "No"],
            response_timeout=datetime.timedelta(minutes=3),
        )
        context = create_runtime_ti(task=op).get_template_context()

        with set_current_context(context):
            with pytest.raises(TaskAwaitingInput) as exc_info:
                op.execute(context)

        assert exc_info.value.method_name == "execute_complete"
        assert exc_info.value.timeout == datetime.timedelta(minutes=3)
        assert exc_info.value.kwargs is None


class TestSeveralQuestions:
    def test_each_run_replays_earlier_answers_and_stops_at_the_next_question(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        refund_answers: list[Any] = []
        with DAG(dag_id="two_questions_dag"):

            @task
            def refund_then_email():
                refund = request_input(subject="Refund the order?", options=["Approve", "Reject"])
                refund_answers.append(refund)
                email = request_input(subject="Email the customer?", options=["Yes", "No"])
                return [refund["chosen_options"][0], email["chosen_options"][0]]

            op = refund_then_email().operator
        ti = create_runtime_ti(task=op)
        _, first_park, _ = _run(ti)

        state, second_park, err = _answer_and_run(
            create_runtime_ti, op, ti.id, first_park, _plain_answer("Approve")
        )

        assert err is None
        assert state == TaskInstanceState.AWAITING_INPUT
        assert isinstance(second_park, AwaitInputTask)
        parked = deserialize(second_park.next_kwargs)
        assert parked["key"] == "Email the customer?"
        assert [answered_key for answered_key, _ in parked["answered"]] == ["Refund the order?"]

        state, _, err = _answer_and_run(create_runtime_ti, op, ti.id, second_park, _plain_answer("No"))

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert _return_values(mock_supervisor_comms) == [["Approve", "No"]]
        # The third run replayed the first answer instead of asking again.
        assert [r.subject for r in _hitl_requests(mock_supervisor_comms)] == [
            "Refund the order?",
            "Email the customer?",
        ]
        # The first answer went through the parked kwargs twice and is still the same answer.
        assert refund_answers[-1] == refund_answers[0]
        assert type(refund_answers[-1]["responded_at"]) is datetime.datetime

    def test_a_task_that_skips_an_answered_question_gets_the_answer_to_the_one_it_asks(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        # Stands in for state the task restores from task_state_store, the way an agent restores
        # its saved run and goes straight to the question it is waiting on.
        progress: list[str] = []
        with DAG(dag_id="restoring_dag"):

            @task
            def agent():
                if not progress:
                    first = request_input(subject="Call refund_order?", options=["Yes", "No"])
                    progress.append(first["chosen_options"][0])
                second = request_input(subject="Call email_customer?", options=["Yes", "No"])
                return [*progress, second["chosen_options"][0]]

            op = agent().operator
        ti = create_runtime_ti(task=op)
        _, first_park, _ = _run(ti)
        _, second_park, _ = _answer_and_run(create_runtime_ti, op, ti.id, first_park, _plain_answer("Yes"))

        state, _, err = _answer_and_run(create_runtime_ti, op, ti.id, second_park, _plain_answer("No"))

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert _return_values(mock_supervisor_comms) == [["Yes", "No"]]

    def test_asking_the_same_key_twice_in_one_run_fails(self, create_runtime_ti, mock_supervisor_comms):
        with DAG(dag_id="loop_dag"):

            @task
            def rounds():
                count = 0
                # Bounded so that replaying the same answer forever fails the test instead of hanging it.
                while count < 5 and request_input(subject="Another round?", options=["Yes", "No"])[
                    "chosen_options"
                ] == ["Yes"]:
                    count += 1
                return count

            op = rounds().operator
        ti = create_runtime_ti(task=op)
        _, park, _ = _run(ti)

        state, _, err = _answer_and_run(create_runtime_ti, op, ti.id, park, _plain_answer("Yes"))

        assert state == TaskInstanceState.FAILED
        assert isinstance(err, ValueError)
        assert "'Another round?'" in str(err)
        assert len(_hitl_requests(mock_supervisor_comms)) == 1

    def test_a_restored_task_asking_an_answered_subject_again_fails_instead_of_reusing_the_answer(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        # An agent approving two calls to the same tool: each run restores the saved state and asks
        # about the call it is waiting on, under the same subject both times.
        progress: list[str] = []
        with DAG(dag_id="repeated_subject_dag"):

            @task
            def agent():
                if not progress:
                    first = request_input(subject="Approve refund_order?", options=["Yes", "No"])
                    progress.append(first["chosen_options"][0])
                second = request_input(subject="Approve refund_order?", options=["Yes", "No"])
                return [*progress, second["chosen_options"][0]]

            op = agent().operator
        ti = create_runtime_ti(task=op)
        _, park, _ = _run(ti)

        state, _, err = _answer_and_run(create_runtime_ti, op, ti.id, park, _plain_answer("Yes"))

        assert state == TaskInstanceState.FAILED
        assert isinstance(err, ValueError)
        assert "pass a key that is unique to each question" in str(err)
        assert len(_hitl_requests(mock_supervisor_comms)) == 1

    def test_a_subject_that_changes_between_runs_fails_instead_of_asking_again(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        runs: list[int] = []
        with DAG(dag_id="drifting_subject_dag"):

            @task
            def refund():
                runs.append(len(runs) + 1)
                answer = request_input(subject=f"Refund (asked on run {runs[-1]})?", options=["Yes", "No"])
                return answer["chosen_options"][0]

            op = refund().operator
        ti = create_runtime_ti(task=op)
        _, park, _ = _run(ti)

        state, _, err = _answer_and_run(create_runtime_ti, op, ti.id, park, _plain_answer("Yes"))

        assert state == TaskInstanceState.FAILED
        assert isinstance(err, RuntimeError)
        assert "'Refund (asked on run 1)?' was never read" in str(err)
        assert len(_hitl_requests(mock_supervisor_comms)) == 1

    def test_finishing_without_reading_the_answer_fails(self, create_runtime_ti, mock_supervisor_comms):
        runs: list[int] = []
        with DAG(dag_id="unread_answer_dag"):

            @task
            def refund():
                runs.append(len(runs) + 1)
                if runs[-1] == 1:
                    request_input(subject="Refund?", options=["Yes", "No"])
                return "done"

            op = refund().operator
        ti = create_runtime_ti(task=op)
        _, park, _ = _run(ti)

        state, _, err = _answer_and_run(create_runtime_ti, op, ti.id, park, _plain_answer("Yes"))

        assert state == TaskInstanceState.FAILED
        assert isinstance(err, RuntimeError)
        assert "'Refund?' was never read" in str(err)

    def test_key_matches_an_answer_when_the_subject_changes(self, create_runtime_ti, mock_supervisor_comms):
        runs: list[int] = []
        with DAG(dag_id="changing_subject_dag"):

            @task
            def refund():
                runs.append(len(runs) + 1)
                answer = request_input(
                    subject=f"Refund (asked on run {runs[-1]})?", options=["Approve", "Reject"], key="refund"
                )
                return answer["chosen_options"][0]

            op = refund().operator
        ti = create_runtime_ti(task=op)
        _, park, _ = _run(ti)

        state, _, err = _answer_and_run(create_runtime_ti, op, ti.id, park, _plain_answer("Approve"))

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert _return_values(mock_supervisor_comms) == ["Approve"]

    def test_answer_is_refused_when_the_options_changed_between_runs(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        options = [["Approve", "Reject"], ["Approve", "Hold"]]
        with DAG(dag_id="changing_options_dag"):

            @task
            def refund():
                return request_input(subject="Refund?", options=options.pop(0))["chosen_options"]

            op = refund().operator
        ti = create_runtime_ti(task=op)
        _, park, _ = _run(ti)

        state, _, err = _answer_and_run(create_runtime_ti, op, ti.id, park, _plain_answer("Reject"))

        assert state == TaskInstanceState.FAILED
        assert isinstance(err, ValueError)
        assert "not in ['Approve', 'Hold']" in str(err)


class TestTaskflowVariants:
    def test_async_task_parks_and_resumes(self, create_runtime_ti, mock_supervisor_comms):
        with DAG(dag_id="async_dag"):

            @task
            async def ask():
                return request_input(subject="Go?", options=["Yes", "No"])["chosen_options"][0]

            op = ask().operator
        ti = create_runtime_ti(task=op)
        state, park_msg, err = _run(ti)
        assert err is None
        assert state == TaskInstanceState.AWAITING_INPUT

        state, _, err = _answer_and_run(create_runtime_ti, op, ti.id, park_msg, _answer("Yes"))

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert _return_values(mock_supervisor_comms) == ["Yes"]

    @pytest.mark.parametrize(
        ("chosen", "skipped"),
        [
            pytest.param("Approve", "reject_path", id="approve"),
            pytest.param("Reject", "approve_path", id="reject"),
        ],
    )
    def test_branch_applies_the_decision_made_after_resume(
        self, create_runtime_ti, mock_supervisor_comms, chosen, skipped
    ):
        with DAG(dag_id="branch_dag"):

            @task.branch
            def decide():
                answer = request_input(subject="Which way?", options=["Approve", "Reject"])
                return "approve_path" if answer["chosen_options"] == ["Approve"] else "reject_path"

            branch = decide()
            branch >> [EmptyOperator(task_id="approve_path"), EmptyOperator(task_id="reject_path")]
            op = branch.operator
        ti = create_runtime_ti(task=op)
        state, park_msg, _ = _run(ti)
        assert state == TaskInstanceState.AWAITING_INPUT

        state, _, err = _answer_and_run(create_runtime_ti, op, ti.id, park_msg, _answer(chosen))

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert _skipped_task_ids(mock_supervisor_comms) == [skipped]

    def test_short_circuit_applies_the_condition_returned_after_resume(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        with DAG(dag_id="short_circuit_dag"):

            @task.short_circuit
            def go_on():
                return request_input(subject="Continue?", options=["Yes", "No"])["chosen_options"] == ["Yes"]

            gate = go_on()
            gate >> EmptyOperator(task_id="downstream")
            op = gate.operator
        ti = create_runtime_ti(task=op)
        _, park_msg, _ = _run(ti)

        state, _, err = _answer_and_run(create_runtime_ti, op, ti.id, park_msg, _answer("No"))

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert _skipped_task_ids(mock_supervisor_comms) == ["downstream"]

    def test_classic_python_operator_parks_and_resumes(self, create_runtime_ti, mock_supervisor_comms):
        def ask():
            return request_input(subject="Go?", options=["Yes", "No"])["chosen_options"][0]

        op = PythonOperator(task_id="classic", python_callable=ask)
        ti = create_runtime_ti(task=op)
        _, park_msg, _ = _run(ti)

        state, _, err = _answer_and_run(create_runtime_ti, op, ti.id, park_msg, _answer("No"))

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert _return_values(mock_supervisor_comms) == ["No"]

    @pytest.mark.parametrize("map_index", [0, 1])
    def test_mapped_task_parks_and_resumes_its_own_index(
        self, create_runtime_ti, mock_supervisor_comms, map_index
    ):
        with DAG(dag_id="mapped_dag"):

            @task
            def ask(order_id: str):
                request_input(subject=f"Refund {order_id}?", options=["Yes", "No"])
                return order_id

            # The worker runs one unmapped copy of a mapped task per index, with that index's arguments.
            unmapped = ask(f"o-{map_index}").operator
        ti = create_runtime_ti(task=unmapped, map_index=map_index)
        state, park_msg, _ = _run(ti)
        assert state == TaskInstanceState.AWAITING_INPUT
        (request,) = _hitl_requests(mock_supervisor_comms)
        assert request.ti_id == ti.id
        assert request.subject == f"Refund o-{map_index}?"

        resumed = create_runtime_ti(task=unmapped, ti_id=ti.id, map_index=map_index)
        _resume(resumed, park_msg, _answer("Yes"))
        state, _, err = _run(resumed)

        assert err is None
        assert state == TaskInstanceState.SUCCESS


class TestAnswerLogOutsideTheTaskRunner:
    """
    ``run()`` executes each task in a copied context, so a leaked answer log could not outlive it there.

    These call ``resume_with_input`` directly, the way a caller outside the task runner would.
    """

    @staticmethod
    def _resume_directly(op: PythonOperator, create_runtime_ti):
        context = create_runtime_ti(task=op).get_template_context()
        with set_current_context(context):
            return op.resume_with_input(
                context,
                event=_plain_answer("Approve"),
                key="Go?",
                options=["Approve", "Reject"],
                params={},
                answered=[],
            )

    def test_answers_are_cleared_after_the_callable_returns(self, create_runtime_ti, mock_supervisor_comms):
        op = PythonOperator(
            task_id="t",
            python_callable=lambda: request_input(subject="Go?", options=["Approve", "Reject"])[
                "chosen_options"
            ],
        )

        result = self._resume_directly(op, create_runtime_ti)

        assert result == ["Approve"]
        assert answer_log_var.get() is None

    def test_answers_are_cleared_after_the_callable_raises(self, create_runtime_ti, mock_supervisor_comms):
        def fail_after_reading():
            answer = request_input(subject="Go?", options=["Approve", "Reject"])
            raise RuntimeError(f"failed after {answer['chosen_options']}")

        op = PythonOperator(task_id="t", python_callable=fail_after_reading)

        with pytest.raises(RuntimeError, match=r"failed after \['Approve'\]"):
            self._resume_directly(op, create_runtime_ti)

        assert answer_log_var.get() is None


class TestRequestInputRefusals:
    @pytest.mark.parametrize(
        "make_task",
        [
            pytest.param(
                lambda: PythonVirtualenvOperator(task_id="venv", python_callable=_noop),
                id="virtualenv",
            ),
            pytest.param(lambda: BashOperator(task_id="bash", bash_command="true"), id="bash"),
            pytest.param(lambda: EmptyOperator(task_id="empty"), id="not-python"),
        ],
    )
    def test_refuses_tasks_that_cannot_resume_before_writing_a_request(
        self, create_runtime_ti, mock_supervisor_comms, make_task
    ):
        ti = create_runtime_ti(task=make_task())

        with set_current_context(ti.get_template_context()):
            with pytest.raises(TypeError, match="cannot resume it"):
                request_input(subject="Go?", options=["Yes", "No"])

        assert _hitl_requests(mock_supervisor_comms) == []

    def test_refuses_before_airflow_3_4(self, create_runtime_ti, mock_supervisor_comms, monkeypatch):
        monkeypatch.setattr("airflow.providers.standard.operators.hitl.AIRFLOW_V_3_4_PLUS", False)
        ti = create_runtime_ti(task=PythonOperator(task_id="t", python_callable=lambda: None))

        with set_current_context(ti.get_template_context()):
            with pytest.raises(AirflowOptionalProviderFeatureException, match="needs Airflow 3.4"):
                request_input(subject="Go?", options=["Yes", "No"])

        assert _hitl_requests(mock_supervisor_comms) == []

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            pytest.param({"options": []}, "cannot be empty", id="no-options"),
            pytest.param({"options": ["Yes"], "defaults": "No"}, "subset", id="default-not-an-option"),
            pytest.param(
                {"options": ["Yes", "No"], "defaults": ["Yes", "No"]},
                "More than one defaults",
                id="two-defaults-single-choice",
            ),
            pytest.param(
                {"options": ["Yes"], "params": {"_options": 1}}, "_options", id="reserved-param-name"
            ),
        ],
    )
    def test_refuses_invalid_requests(self, create_runtime_ti, mock_supervisor_comms, kwargs, match):
        ti = create_runtime_ti(task=PythonOperator(task_id="t", python_callable=lambda: None))

        with set_current_context(ti.get_template_context()):
            with pytest.raises(ValueError, match=match):
                request_input(subject="Go?", **kwargs)

        assert _hitl_requests(mock_supervisor_comms) == []
