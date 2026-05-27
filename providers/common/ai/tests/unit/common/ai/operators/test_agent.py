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
from pydantic_ai.usage import UsageLimits

from airflow.providers.common.ai.hooks.base_ai import AgentRunRequest, AgentRunResult, AgentUsage, BaseAIHook
from airflow.providers.common.ai.operators.agent import AgentOperator, HITLReviewLink

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

try:
    from airflow.sdk.serde import allow_class

    _allow_class: object | None = allow_class
except ImportError:
    _allow_class = None

requires_allow_class = pytest.mark.skipif(
    _allow_class is None,
    reason="Requires airflow.sdk.serde.allow_class (Airflow with typed-XCom support).",
)


class Summary(BaseModel):
    text: str
    score: float = 0.0


def _make_agent_run_result(output, *, message_history=None) -> AgentRunResult:
    return AgentRunResult(
        output=output,
        message_history=[] if message_history is None else message_history,
        model_name="test-model",
        usage=AgentUsage(requests=1, tool_calls=0, input_tokens=0, output_tokens=0, total_tokens=0),
    )


def _make_mock_hook(output, *, message_history=None):
    """Return (mock_hook, mock_agent) wired for AgentOperator.execute."""
    mock_hook = MagicMock(spec=BaseAIHook)
    mock_hook.llm_conn_id = "my_llm"
    mock_hook.supports_toolsets = True
    mock_hook.supports_durable = True
    mock_hook.supports_usage_limits = True
    mock_agent = MagicMock()
    mock_hook.create_agent.return_value = mock_agent
    mock_hook.run_agent.return_value = _make_agent_run_result(output, message_history=message_history)
    return mock_hook, mock_agent


class TestAgentOperatorHookCapabilities:
    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_rejects_toolsets_when_hook_does_not_support_them(self, mock_hook_cls):
        mock_hook = MagicMock(spec=BaseAIHook)
        mock_hook.llm_conn_id = "strands_conn"
        mock_hook.supports_toolsets = False
        mock_hook.supports_durable = False
        mock_hook.supports_usage_limits = False

        def create_agent(request):
            BaseAIHook.validate_run_request(mock_hook, request)
            return MagicMock()

        mock_hook.create_agent.side_effect = create_agent
        mock_hook_cls.get_agent_hook.return_value = mock_hook

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="strands_conn",
            toolsets=[MagicMock()],
        )
        with pytest.raises(ValueError, match="toolsets are not supported"):
            op.execute(context=MagicMock())


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
    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_forwards_usage_limits_to_run_sync(self, mock_hook_cls):
        """``usage_limits`` is forwarded in the AgentRunRequest on the non-durable path."""
        mock_hook, mock_agent = _make_mock_hook("ok")
        mock_hook_cls.get_agent_hook.return_value = mock_hook

        limits = UsageLimits(request_limit=3, tool_calls_limit=5)
        op = AgentOperator(
            task_id="test",
            prompt="run",
            llm_conn_id="my_llm",
            usage_limits=limits,
        )
        op.execute(context=MagicMock())

        request = mock_hook.run_agent.call_args[0][1]
        assert isinstance(request, AgentRunRequest)
        assert request.usage_limits is limits

    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_regenerate_with_feedback_forwards_usage_limits(self, mock_hook_cls):
        """``usage_limits`` is also forwarded by ``regenerate_with_feedback``."""
        mock_hook, mock_agent = _make_mock_hook("revised")
        mock_hook_cls.get_agent_hook.return_value = mock_hook

        limits = UsageLimits(request_limit=1)
        op = AgentOperator(
            task_id="test",
            prompt="run",
            llm_conn_id="my_llm",
            usage_limits=limits,
        )
        op.regenerate_with_feedback(feedback="Add detail", message_history=[])

        request = mock_hook.run_agent.call_args[0][1]
        assert isinstance(request, AgentRunRequest)
        assert request.prompt == "Add detail"
        assert request.message_history == []
        assert request.usage_limits is limits

    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_creates_agent_from_hook(self, mock_hook_cls):
        mock_hook, mock_agent = _make_mock_hook("The answer is 42.")
        mock_hook_cls.get_agent_hook.return_value = mock_hook

        op = AgentOperator(
            task_id="test",
            prompt="What is the answer?",
            llm_conn_id="my_llm",
            system_prompt="You are helpful.",
        )
        result = op.execute(context=MagicMock())

        assert result == "The answer is 42."
        mock_hook_cls.get_agent_hook.assert_called_once_with("my_llm", hook_params={"model_id": None})

        create_request = mock_hook.create_agent.call_args[0][0]
        assert isinstance(create_request, AgentRunRequest)
        assert create_request.output_type is str
        assert create_request.instructions == "You are helpful."
        assert create_request.prompt == "What is the answer?"
        assert create_request.usage_limits is None

        run_args = mock_hook.run_agent.call_args[0]
        assert run_args[0] is mock_agent
        assert run_args[1] is create_request

    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_passes_toolsets_in_agent_kwargs(self, mock_hook_cls):
        """Toolsets are passed through to create_agent in the request."""
        mock_hook, _ = _make_mock_hook("done")
        mock_hook_cls.get_agent_hook.return_value = mock_hook

        mock_toolset = MagicMock()
        op = AgentOperator(
            task_id="test",
            prompt="Do something",
            llm_conn_id="my_llm",
            toolsets=[mock_toolset],
        )
        op.execute(context=MagicMock())

        request = mock_hook.create_agent.call_args[0][0]
        assert request.toolsets == [mock_toolset]
        assert request.enable_tool_logging is True

    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_enable_tool_logging_false_skips_wrapping(self, mock_hook_cls):
        """enable_tool_logging=False is set on the request."""
        mock_hook, _ = _make_mock_hook("done")
        mock_hook_cls.get_agent_hook.return_value = mock_hook

        mock_toolset = MagicMock()
        op = AgentOperator(
            task_id="test",
            prompt="Do something",
            llm_conn_id="my_llm",
            toolsets=[mock_toolset],
            enable_tool_logging=False,
        )
        op.execute(context=MagicMock())

        request = mock_hook.create_agent.call_args[0][0]
        assert request.toolsets == [mock_toolset]
        assert request.enable_tool_logging is False

    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_passes_agent_params(self, mock_hook_cls):
        """agent_params are included in the request."""
        mock_hook, _ = _make_mock_hook("ok")
        mock_hook_cls.get_agent_hook.return_value = mock_hook

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
            agent_params={"retries": 3, "model_settings": {"temperature": 0}},
        )
        op.execute(context=MagicMock())

        request = mock_hook.create_agent.call_args[0][0]
        assert request.agent_params == {"retries": 3, "model_settings": {"temperature": 0}}

    @requires_allow_class
    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_structured_output(self, mock_hook_cls):
        """Structured output keeps the Pydantic instance so downstream tasks can type-hint it."""
        mock_hook, _ = _make_mock_hook(Summary(text="Great", score=0.95))
        mock_hook_cls.get_agent_hook.return_value = mock_hook

        op = AgentOperator(
            task_id="test",
            prompt="Analyze this",
            llm_conn_id="my_llm",
            output_type=Summary,
        )
        result = op.execute(context=MagicMock())

        assert isinstance(result, Summary)
        assert result.text == "Great"
        assert result.score == 0.95

    @requires_allow_class
    def test_init_rejects_nested_output_type(self):
        """A BaseModel defined inside a function carries ``<locals>`` and can't survive XCom."""

        def _build():
            class Nested(BaseModel):
                v: int

            return AgentOperator(task_id="t", prompt="p", llm_conn_id="c", output_type=Nested)

        with pytest.raises(ValueError, match="defined inside a function"):
            _build()

    @requires_allow_class
    def test_init_registers_output_type_in_extra_allowed(self):
        from airflow.sdk.module_loading import qualname
        from airflow.sdk.serde import _extra_allowed

        AgentOperator(task_id="t", prompt="p", llm_conn_id="c", output_type=Summary)
        assert qualname(Summary) in _extra_allowed

    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_with_model_id(self, mock_hook_cls):
        """model_id is passed to the agent hook."""
        mock_hook, _ = _make_mock_hook("ok")
        mock_hook_cls.get_agent_hook.return_value = mock_hook

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
            model_id="openai:gpt-5",
        )
        op.execute(context=MagicMock())

        mock_hook_cls.get_agent_hook.assert_called_once_with(
            "my_llm", hook_params={"model_id": "openai:gpt-5"}
        )

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
    @patch("airflow.providers.common.ai.operators.agent.AgentOperator.run_hitl_review", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_with_enable_hitl_review_delegates_to_run_hitl_review(self, mock_hook_cls, mock_run_hitl):
        """When enable_hitl_review=True, execute delegates to run_hitl_review with output and message_history."""
        msg_history = [MagicMock()]
        mock_hook, _ = _make_mock_hook("Initial output", message_history=msg_history)
        mock_hook_cls.get_agent_hook.return_value = mock_hook
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

    @requires_allow_class
    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
    @patch("airflow.providers.common.ai.operators.agent.AgentOperator.run_hitl_review", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_with_hitl_rehydrates_base_model(self, mock_hook_cls, mock_run_hitl):
        """When enable_hitl_review=True and output_type is BaseModel, execute returns the model instance."""
        mock_hook, _ = _make_mock_hook(Summary(text="Approved summary", score=0.9))
        mock_hook_cls.get_agent_hook.return_value = mock_hook
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

        assert isinstance(result, Summary)
        assert result.text == "Approved summary"
        assert result.score == 0.9

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
    @patch("airflow.providers.common.ai.operators.agent.AgentOperator.run_hitl_review", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_with_hitl_returns_string_unchanged(self, mock_hook_cls, mock_run_hitl):
        """When enable_hitl_review=True and output_type is str, execute returns string as-is."""
        mock_hook, _ = _make_mock_hook("Initial output")
        mock_hook_cls.get_agent_hook.return_value = mock_hook
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
    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_propagates_hitl_max_iterations_error(self, mock_hook_cls, mock_run_hitl):
        """When run_hitl_review raises HITLMaxIterationsError, execute propagates it."""
        from airflow.providers.common.ai.exceptions import HITLMaxIterationsError

        mock_hook, _ = _make_mock_hook("Initial output")
        mock_hook_cls.get_agent_hook.return_value = mock_hook
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
    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_regenerate_with_feedback_calls_agent_with_feedback_and_history(self, mock_hook_cls):
        """regenerate_with_feedback builds request with feedback and message_history."""
        msg_history = [MagicMock()]
        new_history = msg_history + [MagicMock()]
        mock_hook, mock_agent = _make_mock_hook("Revised output", message_history=new_history)
        mock_hook_cls.get_agent_hook.return_value = mock_hook

        op = AgentOperator(
            task_id="test",
            prompt="Summarize",
            llm_conn_id="my_llm",
        )
        output, returned_history = op.regenerate_with_feedback(
            feedback="Add more detail",
            message_history=msg_history,
        )

        assert output == "Revised output"
        assert returned_history == new_history

        request = mock_hook.create_agent.call_args[0][0]
        assert isinstance(request, AgentRunRequest)
        assert request.prompt == "Add more detail"
        assert request.message_history is msg_history
        assert request.usage_limits is None

        run_args = mock_hook.run_agent.call_args[0]
        assert run_args[0] is mock_agent
        assert run_args[1] is request

    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_regenerate_with_feedback_serializes_base_model_output(self, mock_hook_cls):
        """regenerate_with_feedback returns JSON string for BaseModel output."""
        mock_hook, _ = _make_mock_hook(Summary(text="Revised"))
        mock_hook_cls.get_agent_hook.return_value = mock_hook

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

        assert output == '{"text":"Revised","score":0.0}'


class TestAgentOperatorDurable:
    def test_durable_param_stored(self):
        op = AgentOperator(task_id="test", prompt="test", llm_conn_id="my_llm", durable=True)
        assert op.durable is True

    def test_durable_default_false(self):
        op = AgentOperator(task_id="test", prompt="test", llm_conn_id="my_llm")
        assert op.durable is False

    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_durable_passes_durable_context_in_request(self, mock_hook_cls):
        """durable=True builds DurableContext from task instance and passes it in request."""
        mock_hook, _ = _make_mock_hook("ok")
        mock_hook_cls.get_agent_hook.return_value = mock_hook

        ti = MagicMock()
        ti.dag_id = "my_dag"
        ti.task_id = "my_task"
        ti.run_id = "my_run"
        ti.map_index = -1
        context = MagicMock()
        context.__getitem__ = MagicMock(return_value=ti)

        op = AgentOperator(task_id="test", prompt="test", llm_conn_id="my_llm", durable=True)
        result = op.execute(context=context)

        assert result == "ok"
        request = mock_hook.create_agent.call_args[0][0]
        assert request.durable_context is not None
        assert request.durable_context.dag_id == "my_dag"
        assert request.durable_context.task_id == "my_task"
        assert request.durable_context.run_id == "my_run"
        assert request.durable_context.map_index == -1

    @patch("airflow.providers.common.ai.operators.agent.BaseAIHook", autospec=True)
    def test_execute_non_durable_does_not_wrap(self, mock_hook_cls):
        """Default (durable=False) sets durable_context to None in request."""
        mock_hook, _ = _make_mock_hook("ok")
        mock_hook_cls.get_agent_hook.return_value = mock_hook

        op = AgentOperator(task_id="test", prompt="test", llm_conn_id="my_llm")
        op.execute(context=MagicMock())

        request = mock_hook.run_agent.call_args[0][1]
        assert request.prompt == "test"
        assert request.durable_context is None


@pytest.mark.skipif(
    not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
)
class TestAgentOperatorMultimodalPromptGuard:
    """AgentOperator.execute raises before run_agent when enable_hitl_review=True
    and self.prompt is not a string."""

    def test_execute_rejects_sequence_prompt_with_hitl_review(self):
        op = AgentOperator(
            task_id="t",
            prompt="placeholder",
            llm_conn_id="c",
            enable_hitl_review=True,
        )
        op.prompt = ["x", object()]  # simulate post-template-render value

        with pytest.raises(TypeError, match="enable_hitl_review=True"):
            op.execute(context=MagicMock())
