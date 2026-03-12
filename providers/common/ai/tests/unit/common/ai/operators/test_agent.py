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

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from airflow.providers.common.ai.operators.agent import AgentOperator, HITLReviewLink
from airflow.providers.common.ai.toolsets.logging import LoggingToolset

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS


def _make_mock_run_result(output):
    """Create a mock AgentRunResult compatible with log_run_summary."""
    mock_result = MagicMock()
    mock_result.output = output
    mock_result.usage.return_value = MagicMock(
        requests=1, tool_calls=0, input_tokens=0, output_tokens=0, total_tokens=0
    )
    mock_result.response = MagicMock(model_name="test-model")
    mock_result.all_messages.return_value = []
    return mock_result


def _make_mock_agent(output):
    """Create a mock agent that returns the given output."""
    mock_agent = MagicMock(spec=["run_sync"])
    mock_agent.run_sync.return_value = _make_mock_run_result(output)
    return mock_agent


class TestAgentOperatorValidation:
    def test_requires_llm_conn_id(self):
        with pytest.raises(TypeError):
            AgentOperator(task_id="test", prompt="hello")

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
    def test_hitl_params_stored(self):
        """HITL parameters are stored on the operator."""
        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
            enable_hitl_review=True,
            max_hitl_iterations=3,
            hitl_timeout=timedelta(minutes=15),
            hitl_poll_interval=5.0,
        )
        assert op.enable_hitl_review is True
        assert op.max_hitl_iterations == 3
        assert op.hitl_timeout == timedelta(minutes=15)
        assert op.hitl_poll_interval == 5.0


class TestAgentOperatorTemplateFields:
    def test_template_fields(self):
        expected = {"prompt", "llm_conn_id", "model_id", "system_prompt", "agent_params"}
        assert set(AgentOperator.template_fields) == expected


class TestAgentOperatorExecute:
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_creates_agent_from_hook(self, mock_hook_cls):
        mock_agent = _make_mock_agent("The answer is 42.")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(
            task_id="test",
            prompt="What is the answer?",
            llm_conn_id="my_llm",
            system_prompt="You are helpful.",
        )
        result = op.execute(context=MagicMock())

        assert result == "The answer is 42."
        mock_hook_cls.get_hook.assert_called_once_with("my_llm", hook_params={"model_id": None})
        mock_hook_cls.get_hook.return_value.create_agent.assert_called_once_with(
            output_type=str, instructions="You are helpful."
        )
        mock_agent.run_sync.assert_called_once_with("What is the answer?")

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_passes_toolsets_in_agent_kwargs(self, mock_hook_cls):
        """Toolsets are passed through to the agent constructor."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent("done")

        mock_toolset = MagicMock()
        op = AgentOperator(
            task_id="test",
            prompt="Do something",
            llm_conn_id="my_llm",
            toolsets=[mock_toolset],
        )
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.get_hook.return_value.create_agent.call_args
        passed_toolsets = create_call[1]["toolsets"]
        assert len(passed_toolsets) == 1
        assert isinstance(passed_toolsets[0], LoggingToolset)
        assert passed_toolsets[0].wrapped is mock_toolset

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_enable_tool_logging_false_skips_wrapping(self, mock_hook_cls):
        """enable_tool_logging=False passes toolsets through unwrapped."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent("done")

        mock_toolset = MagicMock()
        op = AgentOperator(
            task_id="test",
            prompt="Do something",
            llm_conn_id="my_llm",
            toolsets=[mock_toolset],
            enable_tool_logging=False,
        )
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.get_hook.return_value.create_agent.call_args
        assert create_call[1]["toolsets"] == [mock_toolset]

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_passes_agent_params(self, mock_hook_cls):
        """agent_params are unpacked into create_agent."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent("ok")

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
            agent_params={"retries": 3, "model_settings": {"temperature": 0}},
        )
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.get_hook.return_value.create_agent.call_args
        assert create_call[1]["retries"] == 3
        assert create_call[1]["model_settings"] == {"temperature": 0}

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_structured_output(self, mock_hook_cls):
        """Structured output via BaseModel is serialized with model_dump."""

        class Summary(BaseModel):
            text: str
            score: float

        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent(
            Summary(text="Great", score=0.95)
        )

        op = AgentOperator(
            task_id="test",
            prompt="Analyze this",
            llm_conn_id="my_llm",
            output_type=Summary,
        )
        result = op.execute(context=MagicMock())

        assert result == {"text": "Great", "score": 0.95}

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_with_model_id(self, mock_hook_cls):
        """model_id is passed to PydanticAIHook."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent("ok")

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
            model_id="openai:gpt-5",
        )
        op.execute(context=MagicMock())

        mock_hook_cls.get_hook.assert_called_once_with("my_llm", hook_params={"model_id": "openai:gpt-5"})

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
    @patch("airflow.providers.common.ai.operators.agent.AgentOperator.run_hitl_review", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_with_enable_hitl_review_delegates_to_run_hitl_review(self, mock_hook_cls, mock_run_hitl):
        """When enable_hitl_review=True, execute delegates to run_hitl_review with output and message_history."""
        msg_history = [MagicMock()]
        mock_result = _make_mock_run_result("Initial output")
        mock_result.all_messages.return_value = msg_history
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = mock_result
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        mock_run_hitl.return_value = "Approved output"

        op = AgentOperator(
            task_id="test",
            prompt="Summarize",
            llm_conn_id="my_llm",
            enable_hitl_review=True,
            hitl_timeout=timedelta(minutes=5),
        )
        context = MagicMock()
        result = op.execute(context=context)

        assert result == "Approved output"
        mock_run_hitl.assert_called_once_with(op, context, "Initial output", message_history=msg_history)

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
    @patch("airflow.providers.common.ai.operators.agent.AgentOperator.run_hitl_review", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_with_hitl_deserializes_base_model_to_dict(self, mock_hook_cls, mock_run_hitl):
        """When enable_hitl_review=True and output_type is BaseModel, execute deserializes JSON to dict."""

        class Summary(BaseModel):
            text: str
            score: float

        mock_result = _make_mock_run_result(Summary(text="Approved summary", score=0.9))
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = mock_result
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        # run_hitl_review returns JSON string (as stored in session.current_output)
        mock_run_hitl.return_value = '{"text": "Approved summary", "score": 0.9}'

        op = AgentOperator(
            task_id="test",
            prompt="Summarize",
            llm_conn_id="my_llm",
            output_type=Summary,
            enable_hitl_review=True,
            hitl_timeout=timedelta(minutes=5),
        )
        context = MagicMock()
        result = op.execute(context=context)

        assert result == {"text": "Approved summary", "score": 0.9}

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
    @patch("airflow.providers.common.ai.operators.agent.AgentOperator.run_hitl_review", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_with_hitl_returns_string_unchanged(self, mock_hook_cls, mock_run_hitl):
        """When enable_hitl_review=True and output_type is str, execute returns string as-is."""
        mock_result = _make_mock_run_result("Initial output")
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = mock_result
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        mock_run_hitl.return_value = "Approved output"

        op = AgentOperator(
            task_id="test",
            prompt="Summarize",
            llm_conn_id="my_llm",
            output_type=str,
            enable_hitl_review=True,
            hitl_timeout=timedelta(minutes=5),
        )
        context = MagicMock()
        result = op.execute(context=context)

        assert result == "Approved output"

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
    @patch("airflow.providers.common.ai.operators.agent.AgentOperator.run_hitl_review", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_propagates_hitl_max_iterations_error(self, mock_hook_cls, mock_run_hitl):
        """When run_hitl_review raises HITLMaxIterationsError, execute propagates it."""
        from airflow.providers.common.ai.exceptions import HITLMaxIterationsError

        mock_result = _make_mock_run_result("Initial output")
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = mock_result
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        mock_run_hitl.side_effect = HITLMaxIterationsError("Task exceeded max iterations.")

        op = AgentOperator(
            task_id="test",
            prompt="Summarize",
            llm_conn_id="my_llm",
            enable_hitl_review=True,
            max_hitl_iterations=5,
            hitl_timeout=timedelta(minutes=5),
        )
        context = MagicMock()

        with pytest.raises(HITLMaxIterationsError, match="Task exceeded max iterations"):
            op.execute(context=context)


@pytest.mark.skipif(
    not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
)
class TestHITLReviewLink:
    def test_get_link_returns_empty_when_hitl_disabled(self):
        """HITLReviewLink returns empty string when operator has enable_hitl_review=False."""
        op = AgentOperator(
            task_id="task",
            prompt="test",
            llm_conn_id="my_llm",
            enable_hitl_review=False,
        )
        ti_key = MagicMock()
        ti_key.dag_id = "my_dag"
        ti_key.run_id = "run_1"
        ti_key.task_id = "task"
        ti_key.map_index = -1

        link = HITLReviewLink()
        result = link.get_link(op, ti_key=ti_key)

        assert result == ""

    def test_get_link_returns_url_with_params_when_hitl_enabled(self):
        """HITLReviewLink returns plugin URL with dag_id, run_id, task_id, map_index when HITL enabled."""
        op = AgentOperator(
            task_id="my_task",
            prompt="test",
            llm_conn_id="my_llm",
            enable_hitl_review=True,
        )
        ti_key = MagicMock()
        ti_key.dag_id = "my_dag"
        ti_key.run_id = "run_1"
        ti_key.task_id = "my_task"
        ti_key.map_index = 2

        link = HITLReviewLink()
        result = link.get_link(op, ti_key=ti_key)

        assert result == "/dags/my_dag/runs/run_1/tasks/my_task/mapped/2/plugin/hitl-review"


@pytest.mark.skipif(
    not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
)
class TestAgentOperatorRegenerateWithFeedback:
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_regenerate_with_feedback_calls_agent_with_feedback_and_history(self, mock_hook_cls):
        """regenerate_with_feedback builds agent and calls run_sync with feedback and message_history."""
        msg_history = [MagicMock()]
        mock_result = _make_mock_run_result("Revised output")
        mock_result.all_messages.return_value = msg_history + [MagicMock()]
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = mock_result
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(
            task_id="test",
            prompt="Summarize",
            llm_conn_id="my_llm",
        )
        output, new_history = op.regenerate_with_feedback(
            feedback="Add more detail",
            message_history=msg_history,
        )

        assert output == "Revised output"
        assert new_history == mock_result.all_messages.return_value
        mock_agent.run_sync.assert_called_once_with(
            "Add more detail",
            message_history=msg_history,
        )

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_regenerate_with_feedback_serializes_base_model_output(self, mock_hook_cls):
        """regenerate_with_feedback returns JSON string for BaseModel output."""

        class Summary(BaseModel):
            text: str

        mock_result = _make_mock_run_result(Summary(text="Revised"))
        mock_result.all_messages.return_value = []
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = mock_result
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
            output_type=Summary,
        )
        output, _ = op.regenerate_with_feedback(
            feedback="Expand",
            message_history=[],
        )

        assert output == '{"text":"Revised"}'
