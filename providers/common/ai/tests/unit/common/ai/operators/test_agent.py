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

import sys
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.capabilities import Toolset
from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.toolsets.function import FunctionToolset
from pydantic_ai.usage import UsageLimits

from airflow.providers.common.ai.durable.base import DurableStorageProtocol
from airflow.providers.common.ai.durable.caching_toolset import CachingToolset
from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
from airflow.providers.common.ai.durable.storage import DurableStorage
from airflow.providers.common.ai.operators.agent import AgentOperator, HITLReviewLink, _build_code_mode
from airflow.providers.common.ai.toolsets.logging import LoggingToolset
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_3_PLUS

try:
    from airflow.sdk.serde import SUPPORTS_OPERATOR_DESERIALIZATION_WALKER as _CORE_WALKER
except ImportError:
    _CORE_WALKER = False

requires_typed_xcom = pytest.mark.skipif(
    not _CORE_WALKER,
    reason="Requires a core with the worker-side deserialization-class walk.",
)


class Summary(BaseModel):
    text: str
    score: float = 0.0


def _make_mock_run_result(output):
    """Create a mock AgentRunResult compatible with log_run_summary."""
    mock_result = MagicMock()
    mock_result.output = output
    mock_result.usage = MagicMock(requests=1, tool_calls=0, input_tokens=0, output_tokens=0, total_tokens=0)
    mock_result.response = MagicMock(model_name="test-model")
    mock_result.all_messages.return_value = []
    return mock_result


def _make_mock_agent(output):
    """Create a mock agent that returns the given output."""
    mock_agent = MagicMock(spec=["run_sync"])
    mock_agent.run_sync.return_value = _make_mock_run_result(output)
    return mock_agent


class _InMemoryDurableStorage:
    """In-memory DurableStorageProtocol backend for exercising real replay in tests."""

    def __init__(self):
        self.models: dict = {}
        self.tools: dict = {}

    def save_model_response(self, key, response, *, fingerprint):
        self.models[key] = (response, fingerprint)

    def load_model_response(self, key):
        return self.models.get(key, (None, None))

    def save_tool_result(self, key, result, *, fingerprint):
        self.tools[key] = (result, fingerprint)

    def load_tool_result(self, key):
        if key in self.tools:
            value, fingerprint = self.tools[key]
            return True, value, fingerprint
        return False, None, None

    def cleanup(self):
        self.models.clear()
        self.tools.clear()


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
        expected = {
            "prompt",
            "llm_conn_id",
            "model_id",
            "system_prompt",
            "agent_params",
            "message_history",
        }
        assert set(AgentOperator.template_fields) == expected


class TestAgentOperatorExecute:
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_forwards_usage_limits_to_run_sync(self, mock_hook_cls):
        """``usage_limits`` is forwarded to ``agent.run_sync`` on the non-durable path."""
        mock_agent = _make_mock_agent("ok")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        limits = UsageLimits(request_limit=3, tool_calls_limit=5)
        op = AgentOperator(
            task_id="test",
            prompt="run",
            llm_conn_id="my_llm",
            usage_limits=limits,
        )
        op.execute(context=MagicMock())

        mock_agent.run_sync.assert_called_once_with("run", usage_limits=limits)

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_regenerate_with_feedback_forwards_usage_limits(self, mock_hook_cls):
        """``usage_limits`` is also forwarded by ``regenerate_with_feedback``."""
        mock_agent = _make_mock_agent("revised")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        limits = UsageLimits(request_limit=1)
        op = AgentOperator(
            task_id="test",
            prompt="run",
            llm_conn_id="my_llm",
            usage_limits=limits,
        )
        op.regenerate_with_feedback(feedback="Add detail", message_history=[])

        mock_agent.run_sync.assert_called_once_with(
            "Add detail",
            message_history=[],
            usage_limits=limits,
        )

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
        mock_agent.run_sync.assert_called_once_with("What is the answer?", usage_limits=None)

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
    def test_code_mode_default_off_no_capabilities(self, mock_hook_cls):
        """code_mode defaults to False, so no capabilities are injected."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent("ok")

        op = AgentOperator(task_id="t", prompt="hi", llm_conn_id="my_llm", toolsets=[MagicMock()])
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.get_hook.return_value.create_agent.call_args
        assert "capabilities" not in create_call[1]

    @patch("airflow.providers.common.ai.operators.agent._build_code_mode", return_value="CM")
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_code_mode_injects_capability(self, mock_hook_cls, mock_build):
        """code_mode=True appends a CodeMode capability passed to create_agent."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent("ok")

        op = AgentOperator(
            task_id="t", prompt="hi", llm_conn_id="my_llm", toolsets=[MagicMock()], code_mode=True
        )
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.get_hook.return_value.create_agent.call_args
        assert create_call[1]["capabilities"] == ["CM"]
        mock_build.assert_called_once()

    @patch("airflow.providers.common.ai.operators.agent._build_code_mode", return_value="CM")
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_code_mode_appends_to_existing_capabilities(self, mock_hook_cls, mock_build):
        """A user-supplied capability via agent_params is preserved alongside CodeMode."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent("ok")

        op = AgentOperator(
            task_id="t",
            prompt="hi",
            llm_conn_id="my_llm",
            code_mode=True,
            agent_params={"capabilities": ["existing"]},
        )
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.get_hook.return_value.create_agent.call_args
        assert create_call[1]["capabilities"] == ["existing", "CM"]

    def test_build_code_mode_missing_harness_raises(self):
        """_build_code_mode raises the optional-feature error when harness is absent."""
        with patch.dict(sys.modules, {"pydantic_ai_harness": None}):
            with pytest.raises(AirflowOptionalProviderFeatureException, match="code-mode"):
                _build_code_mode()

    def test_build_code_mode_reraises_unrelated_import_error(self):
        """A broken transitive import inside the harness is re-raised, not masked as 'extra missing'."""
        real_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "pydantic_ai_harness":
                raise ModuleNotFoundError("No module named 'a_broken_dep'", name="a_broken_dep")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            with pytest.raises(ModuleNotFoundError, match="a_broken_dep"):
                _build_code_mode()

    @patch("airflow.providers.common.ai.operators.agent._build_code_mode")
    def test_code_mode_not_built_at_init(self, mock_build):
        """code_mode is serialization-safe: the CodeMode capability is built lazily in
        _build_agent, never at construction time (so nothing non-serializable is stored)."""
        op = AgentOperator(task_id="t", prompt="hi", llm_conn_id="my_llm", code_mode=True)
        mock_build.assert_not_called()
        assert op.code_mode is True

    def test_durable_and_code_mode_rejected(self):
        """durable and code_mode cannot be combined (durable replay assumes stable step order)."""
        with pytest.raises(ValueError, match="durable=True and code_mode=True"):
            AgentOperator(task_id="t", prompt="hi", llm_conn_id="my_llm", durable=True, code_mode=True)

    @requires_typed_xcom
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_structured_output(self, mock_hook_cls):
        """Structured output keeps the Pydantic instance so downstream tasks can type-hint it."""
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

        assert isinstance(result, Summary)
        assert result.text == "Great"
        assert result.score == 0.95

    def test_declares_output_type_for_deserialization(self):
        """Declares ``output_type`` so the worker-side DAG walk registers it for deserialization."""
        assert "output_type" in AgentOperator.deserialization_allowed_class_fields

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

    @requires_typed_xcom
    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
    @patch("airflow.providers.common.ai.operators.agent.AgentOperator.run_hitl_review", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_with_hitl_rehydrates_base_model(self, mock_hook_cls, mock_run_hitl):
        """When enable_hitl_review=True and output_type is BaseModel, execute returns the model instance."""
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

        assert isinstance(result, Summary)
        assert result.text == "Approved summary"
        assert result.score == 0.9

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
            usage_limits=None,
        )

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_regenerate_with_feedback_serializes_base_model_output(self, mock_hook_cls):
        """regenerate_with_feedback returns JSON string for BaseModel output."""
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

        assert output == '{"text":"Revised","score":0.0}'


class TestAgentOperatorDurable:
    def test_durable_param_stored(self):
        op = AgentOperator(task_id="test", prompt="test", llm_conn_id="my_llm", durable=True)
        assert op.durable is True

    def test_durable_default_false(self):
        op = AgentOperator(task_id="test", prompt="test", llm_conn_id="my_llm")
        assert op.durable is False

    @pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="task state store backend requires Airflow >= 3.3")
    def test_build_durable_storage_uses_task_state_store_on_3_3(self):
        """On Airflow >= 3.3 the cache lives in the task state store -- no durable_cache_path needed."""
        # Imported inside the test: this module runs on all cores, but both symbols
        # (and ``NEVER_EXPIRE``, pulled in by ``task_state_store``) only exist on 3.3+.
        from airflow.providers.common.ai.durable.task_state_store import TaskStateStoreDurableStorage
        from airflow.sdk.execution_time.context import TaskStateStoreAccessor

        accessor = MagicMock(spec=TaskStateStoreAccessor)
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="c", durable=True)

        storage = op._build_durable_storage({"task_state_store": accessor})

        assert isinstance(storage, TaskStateStoreDurableStorage)
        assert storage._store is accessor

    @patch("airflow.providers.common.ai.operators.agent.AIRFLOW_V_3_3_PLUS", False)
    def test_build_durable_storage_falls_back_to_object_storage_below_3_3(self):
        """On Airflow < 3.3 the cache falls back to the ObjectStorage backend."""
        ti = MagicMock(spec=["dag_id", "task_id", "run_id", "map_index"])
        ti.configure_mock(dag_id="d", task_id="t", run_id="r", map_index=-1)
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="c", durable=True)

        storage = op._build_durable_storage({"task_instance": ti})

        assert isinstance(storage, DurableStorage)
        # cache_id is a stable hash of the identity components, not a raw concat.
        assert (
            storage._cache_id == DurableStorage(dag_id="d", task_id="t", run_id="r", map_index=-1)._cache_id
        )

    @patch("pydantic_ai.models.wrapper.infer_model", side_effect=lambda m: m)
    @patch("pydantic_ai.models.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.AgentOperator._build_durable_storage")
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_durable_wraps_model_and_cleans_up(
        self, mock_hook_cls, mock_build_storage, mock_infer, _
    ):
        """durable=True wraps the model with CachingModel and cleans up the cache on success."""
        from airflow.providers.common.ai.durable.base import DurableStorageProtocol

        storage = MagicMock(spec=DurableStorageProtocol)
        mock_build_storage.return_value = storage

        mock_agent = MagicMock()
        mock_agent.run_sync.return_value = _make_mock_run_result("ok")
        mock_agent.model = "test-model"
        mock_agent.override = MagicMock()
        mock_agent.override.return_value.__enter__ = MagicMock(return_value=None)
        mock_agent.override.return_value.__exit__ = MagicMock(return_value=False)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        mock_infer.return_value = MagicMock()

        op = AgentOperator(task_id="test", prompt="test", llm_conn_id="my_llm", durable=True)
        result = op.execute(context=MagicMock())

        assert result == "ok"
        mock_agent.override.assert_called_once()
        assert "model" in mock_agent.override.call_args[1]
        storage.cleanup.assert_called_once()

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_non_durable_does_not_wrap(self, mock_hook_cls):
        """Default (durable=False) does not use override."""
        mock_agent = _make_mock_agent("ok")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="test", prompt="test", llm_conn_id="my_llm")
        op.execute(context=MagicMock())

        # run_sync called directly, no override
        mock_agent.run_sync.assert_called_once_with("test", usage_limits=None)

    def test_build_durable_capabilities_wraps_toolset_capability(self):
        """A ``Toolset`` capability's inner toolset is wrapped with CachingToolset;
        capabilities that are not ``Toolset`` pass through unchanged."""
        inner = FunctionToolset()
        passthrough = object()
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="c", durable=True)

        result = op._build_durable_capabilities(
            [Toolset(inner), passthrough], MagicMock(spec=DurableStorageProtocol), DurableStepCounter()
        )

        assert isinstance(result[0], Toolset)
        assert isinstance(result[0].toolset, CachingToolset)
        assert result[0].toolset.wrapped is inner
        assert result[1] is passthrough

    def test_build_durable_capabilities_skips_callable_toolset_factory(self):
        """A ``Toolset`` holding a callable factory (resolved per run with
        RunContext) cannot be wrapped with CachingToolset, so it passes through."""

        def factory(ctx):
            return FunctionToolset()

        cap = Toolset(factory)
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="c", durable=True)

        result = op._build_durable_capabilities(
            [cap], MagicMock(spec=DurableStorageProtocol), DurableStepCounter()
        )

        assert result[0] is cap

    def test_toolset_capability_tool_replayed_on_retry(self):
        """A tool supplied via a ``Toolset`` capability is cached and replayed on a
        retry instead of re-executing. Regression: such tools bypassed the
        ``CachingToolset`` because they did not arrive via the ``toolsets=`` list."""
        calls = {"n": 0}

        def my_tool() -> str:
            calls["n"] += 1
            return "tool-result"

        def model_fn(messages, info):
            saw_return = any(isinstance(p, ToolReturnPart) for m in messages for p in getattr(m, "parts", []))
            if saw_return:
                return ModelResponse(parts=[TextPart(content="done")])
            return ModelResponse(parts=[ToolCallPart(tool_name="my_tool", args={}, tool_call_id="c1")])

        # Shared storage across two attempts; the second (a retry) must replay the
        # cached tool result rather than executing the tool a second time.
        storage = _InMemoryDurableStorage()
        for _ in range(2):
            op = AgentOperator(
                task_id="t",
                prompt="hi",
                llm_conn_id="c",
                durable=True,
                enable_tool_logging=False,
                agent_params={"capabilities": [Toolset(FunctionToolset(tools=[my_tool]))]},
            )
            op._durable_storage = storage
            op._durable_counter = DurableStepCounter()
            hook = MagicMock(spec=["create_agent"])
            hook.create_agent.side_effect = lambda **kw: Agent(FunctionModel(model_fn), **kw)
            op.llm_hook = hook
            op._build_agent().run_sync("hi")

        assert calls["n"] == 1

    @patch("pydantic_ai.models.wrapper.infer_model", side_effect=lambda m: m)
    @patch("pydantic_ai.models.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.AgentOperator._build_durable_storage")
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_cleanup_skipped_when_post_run_step_fails(self, mock_hook_cls, mock_build_storage, mock_infer, _):
        """Durable cleanup must not run if a post-run step (the message-history XCom
        push) fails, so the Airflow retry can still replay the cached steps."""
        storage = MagicMock(spec=DurableStorageProtocol)
        mock_build_storage.return_value = storage

        mock_agent = MagicMock(spec=["run_sync", "model", "override"])
        mock_agent.run_sync.return_value = _make_mock_run_result("ok")
        mock_agent.model = "test-model"
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="c", durable=True, message_history="[]")
        with patch.object(op, "_emit_message_history", side_effect=RuntimeError("xcom down")):
            with pytest.raises(RuntimeError, match="xcom down"):
                op.execute(context={})

        storage.cleanup.assert_not_called()


@pytest.mark.skipif(
    not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
)
class TestAgentOperatorMultimodalPromptGuard:
    """AgentOperator.execute raises before agent.run_sync when enable_hitl_review=True
    and self.prompt is not a string -- covering direct construction and the native
    template rendering escape (where a string template renders to a Sequence)."""

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_rejects_sequence_prompt_with_hitl_review(self, mock_hook_cls):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(
            task_id="t",
            prompt="placeholder",
            llm_conn_id="c",
            enable_hitl_review=True,
        )
        op.prompt = ["x", object()]  # simulate post-template-render value

        with pytest.raises(TypeError, match="enable_hitl_review=True"):
            op.execute(context=MagicMock())

        mock_agent.run_sync.assert_not_called()


def _sample_history():
    """A minimal two-message pydantic-ai conversation for round-trip tests."""
    return [
        ModelRequest(parts=[UserPromptPart(content="first question")]),
        ModelResponse(parts=[TextPart(content="first answer")]),
    ]


# The accepted input forms for ``message_history``, computed once at collection time.
_SAMPLE_HISTORY_JSON = ModelMessagesTypeAdapter.dump_json(_sample_history()).decode()
_SAMPLE_HISTORY_DICTS = ModelMessagesTypeAdapter.dump_python(_sample_history(), mode="json")


class TestAgentOperatorMessageHistory:
    """Multi-turn session support: seed run_sync with prior history, emit the transcript."""

    @pytest.mark.parametrize(
        ("raw", "expected_len"),
        [
            pytest.param([], 0, id="empty-list"),
            pytest.param("", 0, id="empty-str"),
            pytest.param("   ", 0, id="blank-str"),
            pytest.param(_SAMPLE_HISTORY_JSON, 2, id="json-str"),
            pytest.param(_SAMPLE_HISTORY_DICTS, 2, id="list-of-dicts"),
            pytest.param(_sample_history(), 2, id="list-of-objects"),
        ],
    )
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_message_history_seeds_run_sync(self, mock_hook_cls, raw, expected_len):
        """Every accepted input form is deserialized and passed to run_sync; blank/empty start fresh."""
        mock_agent = _make_mock_agent("ok")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="run", llm_conn_id="c", message_history=raw)
        op.execute(context=MagicMock())

        passed = mock_agent.run_sync.call_args.kwargs["message_history"]
        assert len(passed) == expected_len
        assert all(isinstance(m, (ModelRequest, ModelResponse)) for m in passed)

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_none_is_single_turn_no_history_no_emit(self, mock_hook_cls):
        """Default message_history=None passes no history and pushes no transcript XCom."""
        mock_agent = _make_mock_agent("ok")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="run", llm_conn_id="c")
        context = MagicMock()
        op.execute(context=context)

        assert "message_history" not in mock_agent.run_sync.call_args.kwargs
        context["task_instance"].xcom_push.assert_not_called()

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_transcript_emitted_to_xcom_when_history_set(self, mock_hook_cls):
        """When message_history is set, the post-run transcript is pushed to XCom and round-trips."""
        mock_agent = _make_mock_agent("ok")
        mock_agent.run_sync.return_value.all_messages.return_value = _sample_history()
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="run", llm_conn_id="c", message_history=[])
        context = MagicMock()
        op.execute(context=context)

        ti = context["task_instance"]
        ti.xcom_push.assert_called_once()
        push_kwargs = ti.xcom_push.call_args.kwargs
        assert push_kwargs["key"] == "message_history"
        restored = ModelMessagesTypeAdapter.validate_json(push_kwargs["value"])
        assert len(restored) == 2

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_usage_limits_still_forwarded_with_history(self, mock_hook_cls):
        """Adding message_history does not drop usage_limits from the run_sync call."""
        mock_agent = _make_mock_agent("ok")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        limits = UsageLimits(request_limit=2)
        op = AgentOperator(
            task_id="t", prompt="run", llm_conn_id="c", usage_limits=limits, message_history=[]
        )
        op.execute(context=MagicMock())

        kwargs = mock_agent.run_sync.call_args.kwargs
        assert kwargs["usage_limits"] is limits
        assert kwargs["message_history"] == []

    def test_message_history_with_hitl_review_raises(self):
        """message_history cannot be combined with HITL review (post-review transcript is lost)."""
        with pytest.raises(ValueError, match="message_history and enable_hitl_review"):
            AgentOperator(
                task_id="t",
                prompt="run",
                llm_conn_id="c",
                message_history=[],
                enable_hitl_review=True,
            )

    @patch("pydantic_ai.models.wrapper.infer_model", side_effect=lambda m: m)
    @patch("pydantic_ai.models.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.AgentOperator._build_durable_storage")
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_durable_path_also_seeds_message_history(self, mock_hook_cls, mock_build_storage, mock_infer, _):
        """The durable branch forwards message_history into the cached run too."""
        from airflow.providers.common.ai.durable.base import DurableStorageProtocol

        mock_build_storage.return_value = MagicMock(spec=DurableStorageProtocol)

        mock_agent = MagicMock(spec=["run_sync", "model", "override"])
        mock_agent.run_sync.return_value = _make_mock_run_result("ok")
        mock_agent.model = "test-model"
        mock_agent.override.return_value.__enter__ = MagicMock(return_value=None)
        mock_agent.override.return_value.__exit__ = MagicMock(return_value=False)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        mock_infer.return_value = MagicMock()

        history_json = ModelMessagesTypeAdapter.dump_json(_sample_history()).decode()
        op = AgentOperator(
            task_id="test", prompt="test", llm_conn_id="my_llm", durable=True, message_history=history_json
        )
        op.execute(context=MagicMock())

        passed = mock_agent.run_sync.call_args.kwargs["message_history"]
        assert len(passed) == 2
