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

from dataclasses import dataclass

import pytest
from pydantic import BaseModel, Field, computed_field

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_3_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("Human in the loop is only compatible with Airflow >= 3.1.0", allow_module_level=True)

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

from airflow.providers.common.ai.exceptions import HITLMaxIterationsError
from airflow.providers.common.ai.mixins.hitl_review import (
    _MAX_CONSECUTIVE_XCOM_PULL_FAILURES,
    HITLReviewMixin,
)
from airflow.providers.common.ai.utils.hitl_review import (
    XCOM_AGENT_OUTPUT_PREFIX,
    XCOM_AGENT_SESSION,
    XCOM_HUMAN_ACTION,
    XCOM_HUMAN_FEEDBACK_PREFIX,
    AgentSessionData,
    HumanActionData,
    SessionStatus,
)
from airflow.providers.standard.exceptions import (
    HITLRejectException,
    HITLTimeoutError,
    HITLTriggerEventError,
)
from airflow.sdk.execution_time.comms import GetXCom, XComResult

if AIRFLOW_V_3_3_PLUS:
    from airflow.sdk.execution_time.comms import CreateHITLDetailPayload

MIXIN = "airflow.providers.common.ai.mixins.hitl_review"


class FakeAgenticOperator(HITLReviewMixin):
    """Concrete operator for testing HITLReviewMixin."""

    def __init__(
        self,
        *,
        prompt: str = "Summarize this",
        task_id: str = "test_task",
        hitl_timeout: timedelta | None = None,
        hitl_poll_interval: float = 0.1,
    ):
        self.prompt = prompt
        self.task_id = task_id
        self.enable_hitl_review = True
        self.max_hitl_iterations = 5
        self.hitl_timeout = hitl_timeout or timedelta(seconds=30)
        self.hitl_poll_interval = hitl_poll_interval
        self.log = MagicMock(spec=["info", "warning", "error"])

    def regenerate_with_feedback(self, *, feedback: str, message_history):
        return f"Revised: {feedback}", message_history


@pytest.fixture
def fake_op():
    return FakeAgenticOperator()


def _make_mock_ti_with_xcom_from_supervisor(mock_supervisor_comms, map_index: int = -1):
    """Create a mock TI whose xcom_pull delegates to mock_supervisor_comms."""
    ti = MagicMock(spec=["xcom_push", "xcom_pull", "dag_id", "run_id", "task_id", "map_index"])
    ti.dag_id = "test_dag"
    ti.run_id = "run_1"
    ti.task_id = "test_task"
    ti.map_index = map_index
    ti.xcom_push = MagicMock()

    def xcom_pull(key=XCOM_HUMAN_ACTION, task_ids=None, map_indexes=-1, **kwargs):
        if mock_supervisor_comms is None:
            return None
        result = mock_supervisor_comms.send(
            GetXCom(
                key=key,
                dag_id=ti.dag_id,
                run_id=ti.run_id,
                task_id=task_ids or ti.task_id,
                map_index=map_indexes if map_indexes is not None else ti.map_index,
            ),
        )
        if isinstance(result, XComResult):
            return result.value
        return None

    ti.xcom_pull = MagicMock(side_effect=xcom_pull)
    return ti


@pytest.fixture
def mock_ti(mock_supervisor_comms):
    return _make_mock_ti_with_xcom_from_supervisor(mock_supervisor_comms)


@pytest.fixture
def context(mock_ti):
    return {"task_instance": mock_ti}


class _SerializationAliased(BaseModel):
    user_name: str = Field(serialization_alias="userName")


class _Computed(BaseModel):
    a: int

    @computed_field  # type: ignore[prop-decorator]
    @property
    def double(self) -> int:
        return self.a * 2


class _NoSchema:
    def __repr__(self):
        return "no-schema"


@dataclass
class _UnresolvedForwardReference:
    value: Missing  # type: ignore[name-defined]  # noqa: F821

    def __repr__(self):
        return "unresolved"


class TestToString:
    @pytest.mark.parametrize(
        ("output", "expected"),
        [
            (["tag-a", "tag-b"], '["tag-a","tag-b"]'),
            ([_SerializationAliased(user_name="kax")], '[{"user_name":"kax"}]'),
            (_Computed(a=1), '{"a":1,"double":2}'),
            (_NoSchema(), "no-schema"),
            ([_NoSchema()], "[no-schema]"),
            (_UnresolvedForwardReference(value=1), "unresolved"),
        ],
        ids=[
            "list",
            "serialization-aliased-list",
            "computed-field-shown",
            "no-schema-falls-back-to-str",
            "unserializable-falls-back-to-str",
            "unresolved-schema-falls-back-to-str",
        ],
    )
    def test_to_string_dumps_json_or_falls_back_to_str(self, output, expected):
        assert HITLReviewMixin._to_string(output) == expected

    def test_warns_when_it_falls_back_to_str(self, caplog):
        HITLReviewMixin._to_string(_NoSchema())

        assert any("Cannot serialize a _NoSchema" in message for message in caplog.messages)


class TestHITLReviewMixin:
    """The review on Airflow before 3.3, which polls the human action XCom inside ``execute()``."""

    @pytest.fixture(autouse=True)
    def _poll_instead_of_parking(self):
        with patch(f"{MIXIN}.AIRFLOW_V_3_3_PLUS", False):
            yield

    @patch("airflow.providers.common.ai.mixins.hitl_review.time.sleep", autospec=True)
    def test_pushes_session_and_output_on_start(
        self, mock_sleep, fake_op, mock_ti, context, mock_supervisor_comms
    ):
        mock_supervisor_comms.send.side_effect = [
            XComResult(key=XCOM_HUMAN_ACTION, value=None),
            XComResult(
                key=XCOM_HUMAN_ACTION,
                value=HumanActionData(action="approve", iteration=1).model_dump(mode="json"),
            ),
        ]

        result = fake_op.run_hitl_review(context, "Initial output")

        assert result == "Initial output"
        push_calls = mock_ti.xcom_push.call_args_list
        assert len(push_calls) >= 2
        keys = [c[1]["key"] for c in push_calls]
        assert XCOM_AGENT_SESSION in keys
        assert f"{XCOM_AGENT_OUTPUT_PREFIX}1" in keys

    @patch("airflow.providers.common.ai.mixins.hitl_review.time.sleep", autospec=True)
    def test_approve_returns_current_output(
        self, mock_sleep, fake_op, mock_ti, context, mock_supervisor_comms
    ):
        mock_supervisor_comms.send.side_effect = [
            XComResult(key=XCOM_HUMAN_ACTION, value=None),
            XComResult(
                key=XCOM_HUMAN_ACTION,
                value=HumanActionData(action="approve", iteration=1).model_dump(mode="json"),
            ),
        ]

        result = fake_op.run_hitl_review(context, "Approved output")

        assert result == "Approved output"
        assert mock_supervisor_comms.send.call_count >= 2
        get_xcom_calls = [
            c[0][0]
            for c in mock_supervisor_comms.send.call_args_list
            if c[0] and isinstance(c[0][0], GetXCom)
        ]
        assert any(m.key == XCOM_HUMAN_ACTION for m in get_xcom_calls)

    @patch("airflow.providers.common.ai.mixins.hitl_review.time.sleep", autospec=True)
    def test_reject_raises_hitl_reject_error(
        self, mock_sleep, fake_op, mock_ti, context, mock_supervisor_comms
    ):

        mock_supervisor_comms.send.side_effect = [
            XComResult(key=XCOM_HUMAN_ACTION, value=None),
            XComResult(
                key=XCOM_HUMAN_ACTION,
                value=HumanActionData(action="reject", iteration=1).model_dump(mode="json"),
            ),
        ]

        with pytest.raises(HITLRejectException, match="Output rejected"):
            fake_op.run_hitl_review(context, "Rejected output")

    @patch.object(FakeAgenticOperator, "regenerate_with_feedback")
    @patch("airflow.providers.common.ai.mixins.hitl_review.time.sleep", autospec=True)
    def test_changes_requested_calls_regenerate_and_pushes_new_output(
        self, mock_sleep, mock_regenerate, fake_op, mock_ti, context, mock_supervisor_comms
    ):
        mock_regenerate.return_value = ("Revised: Add more detail", None)
        mock_supervisor_comms.send.side_effect = [
            XComResult(key=XCOM_HUMAN_ACTION, value=None),
            XComResult(
                key=XCOM_HUMAN_ACTION,
                value=HumanActionData(
                    action="changes_requested",
                    feedback="Add more detail",
                    iteration=1,
                ).model_dump(mode="json"),
            ),
            XComResult(key=XCOM_HUMAN_ACTION, value=None),
            XComResult(
                key=XCOM_HUMAN_ACTION,
                value=HumanActionData(action="approve", iteration=2).model_dump(mode="json"),
            ),
        ]

        result = fake_op.run_hitl_review(context, "Initial")

        assert result == "Revised: Add more detail"
        mock_regenerate.assert_called_once_with(
            feedback="Add more detail",
            message_history=None,
        )
        push_calls = [c[1] for c in mock_ti.xcom_push.call_args_list]
        output_pushes = {
            c["key"]: c["value"] for c in push_calls if c["key"].startswith(XCOM_AGENT_OUTPUT_PREFIX)
        }
        assert "airflow_hitl_review_agent_output_1" in output_pushes
        assert "airflow_hitl_review_agent_output_2" in output_pushes
        assert output_pushes["airflow_hitl_review_agent_output_2"] == "Revised: Add more detail"

    @patch("airflow.providers.common.ai.mixins.hitl_review.time.monotonic", autospec=True)
    @patch("airflow.providers.common.ai.mixins.hitl_review.time.sleep", autospec=True)
    def test_timeout_raises_hitl_timeout_error(
        self, mock_sleep, mock_monotonic, fake_op, mock_ti, context, mock_supervisor_comms
    ):

        mock_monotonic.side_effect = [0.0, 0.1, 35.0]
        mock_supervisor_comms.send.return_value = XComResult(key=XCOM_HUMAN_ACTION, value=None)

        with pytest.raises(HITLTimeoutError, match="Task exceeded timeout"):
            fake_op.run_hitl_review(context, "Output")

    @patch("airflow.providers.common.ai.mixins.hitl_review.time.sleep", autospec=True)
    def test_xcom_pull_failures_raise_after_limit(self, mock_sleep, fake_op, mock_ti, context):
        fake_op.hitl_timeout = None

        def failing_pull(*args, **kwargs):
            if mock_ti.xcom_pull.call_count > _MAX_CONSECUTIVE_XCOM_PULL_FAILURES + 2:
                pytest.fail("XCom pull retried past the limit")
            raise ConnectionError("api server down")

        mock_ti.xcom_pull.side_effect = failing_pull

        with pytest.raises(ConnectionError, match="api server down"):
            fake_op.run_hitl_review(context, "Output")

        assert mock_ti.xcom_pull.call_count == _MAX_CONSECUTIVE_XCOM_PULL_FAILURES
        assert fake_op.log.warning.call_count == _MAX_CONSECUTIVE_XCOM_PULL_FAILURES - 1
        fake_op.log.error.assert_called_once_with(
            "Giving up HITL review after %d consecutive XCom pull failures",
            _MAX_CONSECUTIVE_XCOM_PULL_FAILURES,
        )

    @patch("airflow.providers.common.ai.mixins.hitl_review.time.sleep", autospec=True)
    def test_xcom_pull_failure_counter_resets_on_success(self, mock_sleep, fake_op, mock_ti, context):
        fake_op.hitl_timeout = None
        approve = HumanActionData(action="approve", iteration=1).model_dump(mode="json")
        failures = [ConnectionError("blip")] * (_MAX_CONSECUTIVE_XCOM_PULL_FAILURES - 1)
        mock_ti.xcom_pull.side_effect = [*failures, None, *failures, approve]

        assert fake_op.run_hitl_review(context, "Output") == "Output"

    @patch.object(FakeAgenticOperator, "regenerate_with_feedback")
    @patch("airflow.providers.common.ai.mixins.hitl_review.time.sleep", autospec=True)
    def test_empty_feedback_with_changes_requested_treats_as_approve(
        self, mock_sleep, mock_regenerate, fake_op, mock_ti, context, mock_supervisor_comms
    ):
        mock_supervisor_comms.send.side_effect = [
            XComResult(
                key=XCOM_HUMAN_ACTION,
                value=HumanActionData(
                    action="changes_requested",
                    feedback="",
                    iteration=1,
                ).model_dump(mode="json"),
            ),
        ]

        result = fake_op.run_hitl_review(context, "Initial output")

        assert result == "Initial output"
        mock_regenerate.assert_not_called()
        # Empty feedback as approve and returned early
        push_keys = [c[1]["key"] for c in mock_ti.xcom_push.call_args_list]
        assert f"{XCOM_AGENT_OUTPUT_PREFIX}2" not in push_keys

    @patch("airflow.providers.common.ai.mixins.hitl_review.time.sleep", autospec=True)
    def test_malformed_human_action_ignored_and_continues(
        self, mock_sleep, fake_op, mock_ti, context, mock_supervisor_comms
    ):
        mock_supervisor_comms.send.side_effect = [
            XComResult(key=XCOM_HUMAN_ACTION, value="not valid json"),
            XComResult(key=XCOM_HUMAN_ACTION, value=None),
            XComResult(key=XCOM_HUMAN_ACTION, value=None),
            XComResult(
                key=XCOM_HUMAN_ACTION,
                value=HumanActionData(action="approve", iteration=1).model_dump(mode="json"),
            ),
        ]

        result = fake_op.run_hitl_review(context, "Output")

        assert result == "Output"
        fake_op.log.warning.assert_called_once()
        fake_op.log.warning.assert_called_with("Malformed human action XCom: %r", "not valid json")
        assert mock_supervisor_comms.send.call_count >= 4

    @patch.object(FakeAgenticOperator, "regenerate_with_feedback")
    @patch("airflow.providers.common.ai.mixins.hitl_review.time.sleep", autospec=True)
    def test_older_iteration_ignored(
        self, mock_sleep, mock_regenerate, fake_op, mock_ti, context, mock_supervisor_comms
    ):
        mock_regenerate.return_value = ("Revised output", None)
        mock_supervisor_comms.send.side_effect = [
            XComResult(
                key=XCOM_HUMAN_ACTION,
                value=HumanActionData(
                    action="changes_requested",
                    feedback="Improve it",
                    iteration=1,
                ).model_dump(mode="json"),
            ),
            XComResult(
                key=XCOM_HUMAN_ACTION,
                value=HumanActionData(action="approve", iteration=2).model_dump(mode="json"),
            ),
        ]

        result = fake_op.run_hitl_review(context, "Initial")

        assert result == "Revised output"

    def test_max_iterations_reached_raises(self, time_machine):
        """When max iterations is exceeded after changes_requested, raise HITLMaxIterationsError."""
        time_machine.move_to("2025-01-01 12:00:00Z")
        op = FakeAgenticOperator()
        op.max_hitl_iterations = 1

        ti = MagicMock()
        ti.dag_id = "d"
        ti.run_id = "r"
        ti.task_id = "t"
        ti.map_index = -1

        session = AgentSessionData(
            status=SessionStatus.PENDING_REVIEW,
            iteration=1,
            max_iterations=1,
            prompt="p",
            current_output="initial",
        )

        # First poll: changes_requested → regenerate → new_iteration=2 > max → raise
        ti.xcom_pull.return_value = HumanActionData(
            action="changes_requested",
            feedback="Add more",
            iteration=1,
        ).model_dump(mode="json")

        with pytest.raises(HITLMaxIterationsError, match="Task exceeded max iterations"):
            op._poll_loop(
                ti=ti,
                session=session,
                message_history=[],
                deadline=None,
            )

    def test_approve_before_max_returns_output(self, time_machine):
        """When human approves before max iterations, return output."""
        time_machine.move_to("2025-01-01 12:00:00Z")
        op = FakeAgenticOperator()
        op.max_hitl_iterations = 5

        ti = MagicMock()
        ti.dag_id = "d"
        ti.run_id = "r"
        ti.task_id = "t"
        ti.map_index = -1

        session = AgentSessionData(
            status=SessionStatus.PENDING_REVIEW,
            iteration=1,
            max_iterations=5,
            prompt="p",
            current_output="approved output",
        )

        ti.xcom_pull.return_value = HumanActionData(
            action="approve",
            iteration=1,
        ).model_dump(mode="json")

        result = op._poll_loop(
            ti=ti,
            session=session,
            message_history=[],
            deadline=None,
        )

        assert result == "approved output"

    def test_reject_raises(self, time_machine):
        """When human rejects, raise HITLRejectException."""
        time_machine.move_to("2025-01-01 12:00:00Z")
        op = FakeAgenticOperator()

        ti = MagicMock()
        ti.dag_id = "d"
        ti.run_id = "r"
        ti.task_id = "t"
        ti.map_index = -1

        session = AgentSessionData(
            status=SessionStatus.PENDING_REVIEW,
            iteration=1,
            max_iterations=5,
            prompt="p",
            current_output="output",
        )

        ti.xcom_pull.return_value = HumanActionData(
            action="reject",
            iteration=1,
        ).model_dump(mode="json")

        with pytest.raises(HITLRejectException, match="rejected at iteration 1"):
            op._poll_loop(
                ti=ti,
                session=session,
                message_history=[],
                deadline=None,
            )


class ParkingOperator(FakeAgenticOperator):
    """Saves its conversation as JSON, the way an operator must to park between review rounds."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.regenerate_calls: list[tuple[str, Any]] = []
        self.on_regenerate = lambda: None

    def regenerate_with_feedback(self, *, feedback: str, message_history):
        self.on_regenerate()
        self.regenerate_calls.append((feedback, message_history))
        return f"Revised: {feedback}", [*message_history, feedback]

    def dump_review_state(self, message_history) -> str:
        return json.dumps(message_history)

    def load_review_state(self, context, state: str):
        return json.loads(state)

    def complete_hitl_review(self, context, approved_output: str, *, state: str | None):
        return {"approved": approved_output, "state": state}


def _answer(option: str, feedback: Any = None) -> dict[str, Any]:
    """The event core adds to ``next_kwargs`` when the review request is answered."""
    params_input = {} if feedback is None else {"feedback": feedback}
    return {"chosen_options": [option], "params_input": params_input, "responded_by_user": None}


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="Parking in awaiting_input needs Airflow 3.3+")
class TestParkedReview:
    """The review on Airflow 3.3+, which parks the task between rounds instead of polling."""

    @pytest.fixture
    def op(self):
        return ParkingOperator(hitl_timeout=timedelta(hours=2))

    @pytest.fixture
    def ti(self, xcom_backed_ti):
        return xcom_backed_ti

    @pytest.fixture
    def ctx(self, review_context):
        return review_context

    @staticmethod
    def _review_requests(comms) -> list[CreateHITLDetailPayload]:
        """The Human-in-the-loop requests the task sent to the supervisor, one per parked round."""
        return [
            c.kwargs["msg"]
            for c in comms.send.call_args_list
            if isinstance(c.kwargs.get("msg"), CreateHITLDetailPayload)
        ]

    def test_first_round_parks_instead_of_polling(
        self, park, op, ti, ctx, mock_supervisor_comms, time_machine
    ):
        time_machine.move_to(datetime(2026, 1, 1, 12, 0, tzinfo=UTC), tick=False)

        paused = park(lambda: op.run_hitl_review(ctx, "Initial", message_history=["prompt"]))

        assert paused.method_name == "resume_hitl_review"
        assert paused.timeout == timedelta(hours=2)
        assert paused.kwargs["iteration"] == 1
        assert datetime.fromisoformat(paused.kwargs["deadline"]) == datetime(2026, 1, 1, 14, 0, tzinfo=UTC)
        state = ctx["task_state_store"].get("common_ai_hitl_review_state")
        assert json.loads(state) == ["prompt"]
        assert paused.kwargs["state_sha256"] == hashlib.sha256(state.encode()).hexdigest()
        assert ctx["task_state_store"].set.call_args.kwargs["retention"] == timedelta(hours=2, days=1)
        assert AgentSessionData.model_validate(ti.xcoms[XCOM_AGENT_SESSION]).parked is True
        assert ti.xcoms[f"{XCOM_AGENT_OUTPUT_PREFIX}1"] == "Initial"
        ti.xcom_pull.assert_not_called()
        (request,) = self._review_requests(mock_supervisor_comms)
        assert request.ti_id == ti.id
        assert request.options == ["Approve", "Request changes", "Reject"]
        assert request.defaults is None
        assert request.params["feedback"]["schema"] == {"type": ["string", "null"]}

    def test_request_changes_regenerates_and_parks_the_next_round(
        self, park, op, ti, ctx, mock_supervisor_comms, time_machine
    ):
        time_machine.move_to(datetime(2026, 1, 1, 12, 0, tzinfo=UTC), tick=False)
        first = park(lambda: op.run_hitl_review(ctx, "Initial", message_history=["prompt"]))
        time_machine.move_to(datetime(2026, 1, 1, 13, 30, tzinfo=UTC), tick=False)
        resumed = ParkingOperator(hitl_timeout=timedelta(hours=2))
        status_while_regenerating = []
        resumed.on_regenerate = lambda: status_while_regenerating.append(
            ti.xcoms[XCOM_AGENT_SESSION]["status"]
        )

        second = park(
            lambda: resumed.resume_hitl_review(
                ctx, **first.kwargs, event=_answer("Request changes", " Shorter ")
            )
        )

        assert resumed.regenerate_calls == [("Shorter", ["prompt"])]
        # Pushed before regenerating, so the plugin refuses a second answer for the round meanwhile.
        assert status_while_regenerating == [SessionStatus.CHANGES_REQUESTED]
        assert ti.xcoms[f"{XCOM_HUMAN_FEEDBACK_PREFIX}1"] == "Shorter"
        assert ti.xcoms[f"{XCOM_AGENT_OUTPUT_PREFIX}2"] == "Revised: Shorter"
        assert AgentSessionData.model_validate(ti.xcoms[XCOM_AGENT_SESSION]).iteration == 2
        assert second.kwargs["iteration"] == 2
        assert second.kwargs["deadline"] == first.kwargs["deadline"]
        assert second.timeout == timedelta(minutes=30)
        assert json.loads(ctx["task_state_store"].get("common_ai_hitl_review_state")) == ["prompt", "Shorter"]
        # Each round asks again: on Airflow 3.3 the new request is what clears the earlier answer.
        first_request, second_request = self._review_requests(mock_supervisor_comms)
        assert first_request == second_request

    def test_approve_completes_with_the_saved_state(self, park, op, ti, ctx):
        paused = park(lambda: op.run_hitl_review(ctx, "Initial", message_history=["prompt"]))

        result = op.resume_hitl_review(ctx, **paused.kwargs, event=_answer("Approve"))

        assert result == {"approved": "Initial", "state": json.dumps(["prompt"])}
        assert op.regenerate_calls == []
        assert ti.xcoms[XCOM_AGENT_SESSION]["status"] == SessionStatus.APPROVED
        assert ctx["task_state_store"].get("common_ai_hitl_review_state") is None

    def test_reject_raises_and_drops_the_conversation(self, park, op, ti, ctx):
        paused = park(lambda: op.run_hitl_review(ctx, "Initial", message_history=[]))

        with pytest.raises(HITLRejectException, match="rejected at iteration 1"):
            op.resume_hitl_review(ctx, **paused.kwargs, event=_answer("Reject"))

        assert ti.xcoms[XCOM_AGENT_SESSION]["status"] == SessionStatus.REJECTED
        assert ctx["task_state_store"].get("common_ai_hitl_review_state") is None

    def test_timeout_event_raises_hitl_timeout_error(self, park, op, ti, ctx):
        paused = park(lambda: op.run_hitl_review(ctx, "Initial", message_history=[]))
        timeout_event = {"error": "No response before the deadline.", "error_type": "timeout"}

        with pytest.raises(HITLTimeoutError, match="Task exceeded timeout"):
            op.resume_hitl_review(ctx, **paused.kwargs, event=timeout_event)

        assert ti.xcoms[XCOM_AGENT_SESSION]["status"] == SessionStatus.TIMEOUT_EXCEEDED
        assert ctx["task_state_store"].get("common_ai_hitl_review_state") is None

    def test_deadline_passing_during_regeneration_fails_instead_of_parking(
        self, park, op, ti, ctx, mock_supervisor_comms, time_machine
    ):
        time_machine.move_to(datetime(2026, 1, 1, 12, 0, tzinfo=UTC), tick=False)
        paused = park(lambda: op.run_hitl_review(ctx, "Initial", message_history=[]))
        time_machine.move_to(datetime(2026, 1, 1, 14, 0, 1, tzinfo=UTC), tick=False)

        with pytest.raises(HITLTimeoutError, match="Task exceeded timeout"):
            op.resume_hitl_review(ctx, **paused.kwargs, event=_answer("Request changes", "Shorter"))

        assert len(self._review_requests(mock_supervisor_comms)) == 1
        assert ti.xcoms[XCOM_AGENT_SESSION]["status"] == SessionStatus.TIMEOUT_EXCEEDED
        assert ctx["task_state_store"].get("common_ai_hitl_review_state") is None

    @pytest.mark.parametrize(
        "feedback", [None, "", "   ", {"value": "the form echoed the param spec"}], ids=repr
    )
    def test_request_changes_without_feedback_waits_again_on_the_same_round(
        self, park, op, ti, ctx, mock_supervisor_comms, feedback
    ):
        paused = park(lambda: op.run_hitl_review(ctx, "Initial", message_history=[]))

        again = park(
            lambda: op.resume_hitl_review(ctx, **paused.kwargs, event=_answer("Request changes", feedback))
        )

        assert op.regenerate_calls == []
        assert again.kwargs == paused.kwargs
        assert ti.xcoms[XCOM_AGENT_SESSION]["status"] == SessionStatus.PENDING_REVIEW
        first_request, second_request = self._review_requests(mock_supervisor_comms)
        assert first_request == second_request

    def test_approve_ignores_a_feedback_param_that_is_not_text(self, park, op, ctx):
        paused = park(lambda: op.run_hitl_review(ctx, "Initial", message_history=[]))

        result = op.resume_hitl_review(ctx, **paused.kwargs, event=_answer("Approve", {"value": ""}))

        assert result["approved"] == "Initial"

    def test_request_changes_on_the_last_iteration_raises_without_regenerating(self, park, ti, ctx):
        op = ParkingOperator()
        op.max_hitl_iterations = 1
        paused = park(lambda: op.run_hitl_review(ctx, "Initial", message_history=[]))

        with pytest.raises(HITLMaxIterationsError, match="exceeded max iterations"):
            op.resume_hitl_review(ctx, **paused.kwargs, event=_answer("Request changes", "Shorter"))

        assert op.regenerate_calls == []
        assert ti.xcoms[XCOM_AGENT_SESSION]["status"] == SessionStatus.MAX_ITERATIONS_EXCEEDED

    def test_refuses_a_conversation_that_changed_while_parked(self, park, op, ctx):
        paused = park(lambda: op.run_hitl_review(ctx, "Initial", message_history=["prompt"]))
        ctx["task_state_store"].set("common_ai_hitl_review_state", json.dumps(["edited"]))

        with pytest.raises(HITLTriggerEventError, match="missing or changed"):
            op.resume_hitl_review(ctx, **paused.kwargs, event=_answer("Request changes", "Shorter"))

        assert op.regenerate_calls == []
        assert ctx["task_state_store"].get("common_ai_hitl_review_state") is None

    @patch(f"{MIXIN}.time.sleep", autospec=True)
    def test_operator_that_cannot_save_its_conversation_keeps_polling(self, mock_sleep, ti, ctx):
        ti.xcom_pull.side_effect = lambda **kwargs: HumanActionData(action="approve", iteration=1).model_dump(
            mode="json"
        )

        result = FakeAgenticOperator().run_hitl_review(ctx, "Initial")

        assert result == "Initial"
        assert AgentSessionData.model_validate(ti.xcoms[XCOM_AGENT_SESSION]).parked is False
        assert ctx["task_state_store"].get("common_ai_hitl_review_state") is None
