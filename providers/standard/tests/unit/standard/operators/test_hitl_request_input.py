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
from airflow.providers.standard.operators.hitl import HITLOperator, get_input_response, request_input
from airflow.providers.standard.operators.python import PythonOperator, PythonVirtualenvOperator
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


def _resume(ti: RuntimeTaskInstance, park_msg: AwaitInputTask, event: dict[str, Any]) -> None:
    """Set up ``ti`` the way the server hands back a task whose request was answered."""
    ti._ti_context_from_server.next_method = park_msg.next_method
    # handle_event_submit keeps the parked kwargs and adds the response as ``event``.
    ti._ti_context_from_server.next_kwargs = serialize({**deserialize(park_msg.next_kwargs), "event": event})


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
                answer = get_input_response()
                seen.append(answer)
                if answer is None:
                    request_input(
                        subject=f"Refund {order_id}?",
                        body="The agent wants to call `refund_order`.",
                        options=["Approve", "Reject"],
                        params={"reason": Param("", type=["string", "null"])},
                        timeout=datetime.timedelta(hours=4),
                    )
                if answer["chosen_options"] == ["Approve"]:
                    return f"refunded {order_id}"
                return f"refused {order_id}: {answer['params_input']['reason']}"

            xcom_arg = refund("o-1")
        return xcom_arg.operator

    def test_park_writes_the_request_and_waits_in_awaiting_input(
        self, refund_op, seen, create_runtime_ti, mock_supervisor_comms
    ):
        ti = create_runtime_ti(task=refund_op)

        state, msg, err = _run(ti)

        assert err is None
        assert state == TaskInstanceState.AWAITING_INPUT
        assert isinstance(msg, AwaitInputTask)
        assert msg.next_method == "resume_with_input"
        # The resume checks the answer against the same options and params the reviewer was shown.
        (request,) = _hitl_requests(mock_supervisor_comms)
        assert deserialize(msg.next_kwargs) == {"options": ["Approve", "Reject"], "params": request.params}
        assert msg.timeout == datetime.timedelta(hours=4)
        assert request.ti_id == ti.id
        assert request.subject == "Refund o-1?"
        assert request.options == ["Approve", "Reject"]
        assert request.body == "The agent wants to call `refund_order`."
        assert set(request.params) == {"reason"}
        assert seen == [None]

    @pytest.mark.parametrize(
        ("chosen", "reason", "expected"),
        [
            pytest.param("Approve", "", "refunded o-1", id="approve"),
            pytest.param("Reject", "over limit", "refused o-1: over limit", id="reject"),
        ],
    )
    def test_resume_runs_the_callable_again_with_the_answer(
        self, refund_op, seen, create_runtime_ti, mock_supervisor_comms, chosen, reason, expected
    ):
        ti = create_runtime_ti(task=refund_op)
        _, park_msg, _ = _run(ti)
        resumed = create_runtime_ti(task=refund_op, ti_id=ti.id)
        _resume(resumed, park_msg, _answer(chosen, reason=reason))

        state, _, err = _run(resumed)

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert seen[1] == {
            "chosen_options": [chosen],
            "params_input": {"reason": reason},
            "responded_at": RESPONDED_AT,
            "responded_by_user": REVIEWER,
            "timedout": False,
        }
        assert _return_values(mock_supervisor_comms) == [expected]

    def test_timeout_without_defaults_fails_the_task(
        self, refund_op, seen, create_runtime_ti, mock_supervisor_comms
    ):
        ti = create_runtime_ti(task=refund_op)
        _, park_msg, _ = _run(ti)
        resumed = create_runtime_ti(task=refund_op, ti_id=ti.id)
        _resume(
            resumed,
            park_msg,
            {
                "error": "The Human-in-the-loop response timeout has passed without a response.",
                "error_type": "timeout",
            },
        )

        state, _, err = _run(resumed)

        assert state == TaskInstanceState.FAILED
        assert isinstance(err, HITLTimeoutError)
        assert seen == [None]

    def test_answer_outside_options_fails(self, refund_op, create_runtime_ti, mock_supervisor_comms):
        ti = create_runtime_ti(task=refund_op)
        _, park_msg, _ = _run(ti)
        resumed = create_runtime_ti(task=refund_op, ti_id=ti.id)
        _resume(resumed, park_msg, _answer("Maybe"))

        state, _, err = _run(resumed)

        assert state == TaskInstanceState.FAILED
        assert isinstance(err, ValueError)

    @pytest.mark.parametrize(
        ("params_input", "error", "match"),
        [
            pytest.param({"note": "x"}, ValueError, "does not match params", id="unknown-field"),
            pytest.param(
                {"reason": 5}, ParamValidationError, "Invalid input for param reason", id="fails-schema"
            ),
        ],
    )
    def test_params_input_is_checked_against_the_request_params(
        self, refund_op, seen, create_runtime_ti, mock_supervisor_comms, params_input, error, match
    ):
        ti = create_runtime_ti(task=refund_op)
        _, park_msg, _ = _run(ti)
        resumed = create_runtime_ti(task=refund_op, ti_id=ti.id)
        _resume(resumed, park_msg, {**_answer("Approve"), "params_input": params_input})

        state, _, err = _run(resumed)

        assert state == TaskInstanceState.FAILED
        assert isinstance(err, error)
        assert match in str(err)
        assert seen == [None]


class TestRequestFields:
    def test_every_argument_reaches_the_request_and_defaults_apply_at_timeout(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        notifier = _RecordingNotifier()
        seen: list[Any] = []
        with DAG(dag_id="fields_dag"):

            @task
            def ask():
                answer = get_input_response()
                seen.append(answer)
                if answer is None:
                    request_input(
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
        resumed = create_runtime_ti(task=op, ti_id=ti.id)
        _resume(
            resumed,
            park_msg,
            {
                "chosen_options": ["M"],
                "params_input": {"note": ""},
                "responded_at": RESPONDED_AT,
                "responded_by_user": None,
                "timedout": True,
            },
        )
        state, _, err = _run(resumed)

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert seen[1]["timedout"] is True
        assert seen[1]["responded_by_user"] is None
        assert seen[1]["responded_at"] == RESPONDED_AT

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


class TestAskingAgain:
    def test_second_ask_after_resume_writes_a_new_request_and_parks_again(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        # Stands in for the task_state_store a real task would record its question in.
        asked: list[str] = []
        with DAG(dag_id="two_step_dag"):

            @task
            def two_approvals():
                if get_input_response() is None:
                    asked.append("step 1")
                    request_input(subject="Approve step 1", options=["Approve", "Reject"])
                if asked[-1] == "step 1":
                    asked.append("step 2")
                    request_input(subject="Approve step 2", options=["Approve", "Reject"])
                return "both approved"

            op = two_approvals().operator
        ti = create_runtime_ti(task=op)
        _, first_park, _ = _run(ti)
        resumed = create_runtime_ti(task=op, ti_id=ti.id)
        _resume(resumed, first_park, {**_answer("Approve"), "params_input": {}})

        state, second_park, err = _run(resumed)

        assert err is None
        assert state == TaskInstanceState.AWAITING_INPUT
        assert isinstance(second_park, AwaitInputTask)
        assert second_park.next_method == "resume_with_input"
        first_request, second_request = _hitl_requests(mock_supervisor_comms)
        assert first_request.subject == "Approve step 1"
        assert second_request.subject == "Approve step 2"
        assert second_request.ti_id == ti.id

        final = create_runtime_ti(task=op, ti_id=ti.id)
        _resume(final, second_park, {**_answer("Approve"), "params_input": {}})
        state, _, err = _run(final)

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert _return_values(mock_supervisor_comms) == ["both approved"]


class TestTaskflowVariants:
    def test_async_task_parks_and_resumes(self, create_runtime_ti, mock_supervisor_comms):
        seen: list[Any] = []
        with DAG(dag_id="async_dag"):

            @task
            async def ask():
                answer = get_input_response()
                seen.append(answer)
                if answer is None:
                    request_input(subject="Go?", options=["Yes", "No"])
                return answer["chosen_options"][0]

            op = ask().operator
        ti = create_runtime_ti(task=op)
        state, park_msg, err = _run(ti)
        assert err is None
        assert state == TaskInstanceState.AWAITING_INPUT

        resumed = create_runtime_ti(task=op, ti_id=ti.id)
        _resume(resumed, park_msg, _answer("Yes"))
        state, _, err = _run(resumed)

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert seen[0] is None
        assert seen[1]["chosen_options"] == ["Yes"]

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
                answer = get_input_response()
                if answer is None:
                    request_input(subject="Which way?", options=["Approve", "Reject"])
                return "approve_path" if answer["chosen_options"] == ["Approve"] else "reject_path"

            branch = decide()
            branch >> [EmptyOperator(task_id="approve_path"), EmptyOperator(task_id="reject_path")]
            op = branch.operator
        ti = create_runtime_ti(task=op)
        state, park_msg, _ = _run(ti)
        assert state == TaskInstanceState.AWAITING_INPUT
        resumed = create_runtime_ti(task=op, ti_id=ti.id)
        _resume(resumed, park_msg, _answer(chosen))

        state, _, err = _run(resumed)

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert _skipped_task_ids(mock_supervisor_comms) == [skipped]

    def test_short_circuit_applies_the_condition_returned_after_resume(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        with DAG(dag_id="short_circuit_dag"):

            @task.short_circuit
            def go_on():
                answer = get_input_response()
                if answer is None:
                    request_input(subject="Continue?", options=["Yes", "No"])
                return answer["chosen_options"] == ["Yes"]

            gate = go_on()
            gate >> EmptyOperator(task_id="downstream")
            op = gate.operator
        ti = create_runtime_ti(task=op)
        _, park_msg, _ = _run(ti)
        resumed = create_runtime_ti(task=op, ti_id=ti.id)
        _resume(resumed, park_msg, _answer("No"))

        state, _, err = _run(resumed)

        assert err is None
        assert state == TaskInstanceState.SUCCESS
        assert _skipped_task_ids(mock_supervisor_comms) == ["downstream"]

    def test_classic_python_operator_parks_and_resumes(self, create_runtime_ti, mock_supervisor_comms):
        def ask():
            if (answer := get_input_response()) is None:
                request_input(subject="Go?", options=["Yes", "No"])
            return answer["chosen_options"][0]

        op = PythonOperator(task_id="classic", python_callable=ask)
        ti = create_runtime_ti(task=op)
        _, park_msg, _ = _run(ti)
        resumed = create_runtime_ti(task=op, ti_id=ti.id)
        _resume(resumed, park_msg, _answer("No"))

        state, _, err = _run(resumed)

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
                if get_input_response() is None:
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


class TestInputResponseOutsideTheTaskRunner:
    """
    ``run()`` executes each task in a copied context, so a leaked answer could not outlive it there.

    These call ``resume_with_input`` directly, the way a caller outside the task runner would.
    """

    def test_answer_is_cleared_after_the_callable_returns(self, create_runtime_ti, mock_supervisor_comms):
        op = PythonOperator(task_id="t", python_callable=lambda: get_input_response()["chosen_options"])
        context = create_runtime_ti(task=op).get_template_context()

        with set_current_context(context):
            result = op.resume_with_input(
                context, event=_answer("Approve"), options=["Approve", "Reject"], params={}
            )

        assert result == ["Approve"]
        assert get_input_response() is None

    def test_answer_is_cleared_after_the_callable_raises(self, create_runtime_ti, mock_supervisor_comms):
        def fail_after_reading():
            raise RuntimeError(f"failed after {get_input_response()['chosen_options']}")

        op = PythonOperator(task_id="t", python_callable=fail_after_reading)
        context = create_runtime_ti(task=op).get_template_context()

        with set_current_context(context):
            with pytest.raises(RuntimeError, match=r"failed after \['Approve'\]"):
                op.resume_with_input(
                    context, event=_answer("Approve"), options=["Approve", "Reject"], params={}
                )

        assert get_input_response() is None


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
