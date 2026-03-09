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

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("Human in the loop is only compatible with Airflow >= 3.1.0", allow_module_level=True)

from datetime import timedelta
from unittest.mock import MagicMock, patch

from airflow.providers.common.ai.exceptions import HITLMaxIterationsError
from airflow.providers.common.ai.mixins.hitl_review import HITLReviewMixin
from airflow.providers.common.ai.utils.hitl_review import (
    XCOM_AGENT_OUTPUT_PREFIX,
    XCOM_AGENT_SESSION,
    XCOM_HUMAN_ACTION,
    AgentSessionData,
    HumanActionData,
    SessionStatus,
)
from airflow.providers.standard.exceptions import HITLRejectException, HITLTimeoutError
from airflow.sdk.execution_time.comms import GetXCom, XComResult


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
        self.log = MagicMock(spec=["info", "warning"])

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


class TestHITLReviewMixin:
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
