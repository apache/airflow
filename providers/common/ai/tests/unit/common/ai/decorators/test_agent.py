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

from unittest.mock import ANY, MagicMock, patch

import pytest
from pydantic import BaseModel
from pydantic_ai.capabilities import Thinking
from pydantic_ai.messages import ImageUrl
from pydantic_ai.toolsets.function import FunctionToolset

from airflow.providers.common.ai.decorators.agent import _AgentDecoratedOperator
from airflow.providers.common.ai.toolsets.logging import ToolLoggingCapability
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.ai.utils.prompt_cache import PromptCaching
from airflow.providers.common.ai.utils.toolset_base import MaskingToolset

try:
    from airflow.sdk.serde import SUPPORTS_OPERATOR_DESERIALIZATION_WALKER as _CORE_WALKER
except ImportError:
    _CORE_WALKER = False

requires_typed_xcom = pytest.mark.skipif(
    not _CORE_WALKER,
    reason="Requires an ``apache-airflow-task-sdk`` version with the worker-side deserialization-class walk.",
)


class Summary(BaseModel):
    text: str


def _make_context():
    """A context whose ``task_instance`` carries a stable id for run-identity stamping.

    A real dict (not a mock) so the decorator's ``context_merge`` of ``op_kwargs`` works.
    """
    ti = MagicMock()
    ti.configure_mock(id="ti-1", dag_id="dag", task_id="task", run_id="run", map_index=-1, try_number=1)
    return {"task_instance": ti}


class TestAgentDecoratedOperator:
    def test_custom_operator_name(self):
        assert _AgentDecoratedOperator.custom_operator_name == "@task.agent"

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_calls_callable_and_returns_output(self, mock_hook_cls, make_mock_run_result):
        """The callable's return value becomes the agent prompt."""
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
        mock_agent.run_sync.return_value = make_mock_run_result("The top customer is Acme Corp.")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        def my_prompt():
            return "Who is our top customer?"

        op = _AgentDecoratedOperator(task_id="test", python_callable=my_prompt, llm_conn_id="my_llm")
        result = op.execute(context=_make_context())

        assert result == "The top customer is Acme Corp."
        assert op.prompt == "Who is our top customer?"
        mock_agent.run_sync.assert_called_once_with(
            "Who is our top customer?", usage_limits=None, run_id="ti-1", cancellation_token=ANY, usage=ANY
        )

    @pytest.mark.parametrize(
        "return_value",
        [42, "", "   ", None, b"bytes", bytearray(b"x"), [], ()],
        ids=["non-string", "empty", "whitespace", "none", "bytes", "bytearray", "empty-list", "empty-tuple"],
    )
    def test_execute_raises_on_invalid_prompt(self, return_value):
        """TypeError when the callable returns an unsupported prompt shape."""
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: return_value,
            llm_conn_id="my_llm",
        )
        with pytest.raises(TypeError, match="must be"):
            op.execute(context={})

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_accepts_sequence_prompt(self, mock_hook_cls, make_mock_run_result):
        """A non-empty Sequence[UserContent] return value is forwarded to run_sync as-is."""
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
        mock_agent.run_sync.return_value = make_mock_run_result("ok")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        image = ImageUrl(url="https://example.com/x.png")
        prompt = ["Describe this:", image]

        def my_prompt():
            return prompt

        op = _AgentDecoratedOperator(task_id="test", python_callable=my_prompt, llm_conn_id="my_llm")
        op.execute(context=_make_context())

        assert op.prompt == prompt
        mock_agent.run_sync.assert_called_once_with(
            prompt, usage_limits=None, run_id="ti-1", cancellation_token=ANY, usage=ANY
        )

    @patch("airflow.providers.common.ai.operators.agent.AgentOperator.run_hitl_review", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_sequence_prompt_with_hitl_review_reaches_the_review(
        self, mock_hook_cls, mock_run_hitl, make_mock_run_result
    ):
        from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

        if not AIRFLOW_V_3_1_PLUS:
            pytest.skip("enable_hitl_review requires Airflow >= 3.1.0")

        mock_agent = MagicMock(spec=["run_sync", "instrument"])
        mock_agent.run_sync.return_value = make_mock_run_result("Initial output")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        prompt = ["x", ImageUrl(url="https://example.com/x.png")]

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: prompt,
            llm_conn_id="my_llm",
            enable_hitl_review=True,
        )

        assert op.execute(context=_make_context()) == "Initial output"
        mock_run_hitl.assert_called_once()
        assert mock_agent.run_sync.call_args.args[0] == prompt

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_merges_op_kwargs_into_callable(self, mock_hook_cls, make_mock_run_result):
        """op_kwargs are resolved by the callable to build the prompt."""
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
        mock_agent.run_sync.return_value = make_mock_run_result("done")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        def my_prompt(topic):
            return f"Analyze {topic}"

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="my_llm",
            op_kwargs={"topic": "revenue trends"},
        )
        op.execute(context=_make_context())

        assert op.prompt == "Analyze revenue trends"
        mock_agent.run_sync.assert_called_once_with(
            "Analyze revenue trends", usage_limits=None, run_id="ti-1", cancellation_token=ANY, usage=ANY
        )

    @patch("airflow.providers.common.ai.operators.agent.AgentOperator.execute", autospec=True)
    def test_execute_renders_only_the_returned_prompt(self, mock_execute):
        """Fields rendered before execute() are not rendered again; only the returned value is."""
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Summarize {{ topic }}",
            llm_conn_id="my_llm",
            system_prompt="{{ note }}",
            toolsets=[SQLToolset(db_conn_id="{{ note }}")],
        )
        context = {"topic": "logs", "note": "{{ other }}", "other": "rendered twice"}

        op.render_template_fields(context)
        op.execute(context=context)

        assert op.prompt == "Summarize logs"
        assert op.system_prompt == "{{ other }}"
        assert op.toolsets[0].id == "sql-{{ other }}"

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_passes_toolsets_through(self, mock_hook_cls, make_mock_run_result):
        """Toolsets passed to the decorator are forwarded to the agent."""
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
        mock_agent.run_sync.return_value = make_mock_run_result("result")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        toolset = FunctionToolset()

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Do something",
            llm_conn_id="my_llm",
            toolsets=[toolset],
        )
        op.execute(context=_make_context())

        create_call = mock_hook_cls.get_hook.return_value.create_agent.call_args
        assert create_call[1]["toolsets"] == [MaskingToolset(wrapped=toolset)]
        assert create_call[1]["capabilities"][0] == PromptCaching()
        assert isinstance(create_call[1]["capabilities"][1], ToolLoggingCapability)

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_passes_capabilities_through(self, mock_hook_cls, make_mock_run_result):
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
        mock_agent.run_sync.return_value = make_mock_run_result("result")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        thinking = Thinking(effort="high")

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Do something",
            llm_conn_id="my_llm",
            capabilities=[thinking],
        )
        op.execute(context=_make_context())

        create_call = mock_hook_cls.get_hook.return_value.create_agent.call_args
        assert create_call.kwargs["capabilities"][:2] == [thinking, PromptCaching()]
        assert isinstance(create_call.kwargs["capabilities"][2], ToolLoggingCapability)

    @requires_typed_xcom
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_structured_output(self, mock_hook_cls, make_mock_run_result):
        """BaseModel output flows through XCom as the Pydantic instance."""
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
        mock_agent.run_sync.return_value = make_mock_run_result(Summary(text="Great results"))
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Summarize",
            llm_conn_id="my_llm",
            output_type=Summary,
        )
        result = op.execute(context=_make_context())

        assert isinstance(result, Summary)
        assert result.text == "Great results"

    def test_durable_kwarg_passes_through_to_operator(self):
        """durable=True is forwarded to AgentOperator via **kwargs."""
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "prompt",
            llm_conn_id="my_llm",
            durable=True,
        )
        assert op.durable is True

    def test_durable_default_false_through_decorator(self):
        """durable defaults to False when not specified."""
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "prompt",
            llm_conn_id="my_llm",
        )
        assert op.durable is False


class _Pause(BaseException):
    """Stands in for the pause of a run that waits for an approval or a review."""


def _describe(text: str, source: str = "") -> str:
    return f"Describe {text}"


@patch("airflow.providers.common.ai.operators.agent.AgentOperator.execute", autospec=True)
class TestAgentCallbacks:
    def test_output_processor_returns_the_result_from_the_output_and_the_arguments_it_names(
        self, mock_execute
    ):
        mock_execute.return_value = "a cat"

        def with_source(output: str, source: str) -> dict:
            return {"source": source, "answer": output}

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=_describe,
            op_kwargs={"text": "the picture", "source": "cat.png"},
            llm_conn_id="my_llm",
            output_processor=with_source,
        )

        assert op.execute(context=_make_context()) == {"source": "cat.png", "answer": "a cat"}

    def test_output_processor_also_gets_an_argument_that_was_passed_by_position(self, mock_execute):
        mock_execute.return_value = "a cat"
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=_describe,
            op_args=["the picture", "cat.png"],
            llm_conn_id="my_llm",
            output_processor=lambda output, source: (source, output),
        )

        assert op.execute(context=_make_context()) == ("cat.png", "a cat")

    def test_error_handler_returns_the_output_to_use_for_a_failed_run(self, mock_execute):
        mock_execute.side_effect = ValueError("refused")
        seen = []

        def no_answer(error: Exception, source: str) -> str:
            seen.append((error, source))
            return "no answer"

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=_describe,
            op_kwargs={"text": "the picture", "source": "cat.png"},
            llm_conn_id="my_llm",
            error_handler=no_answer,
        )

        assert op.execute(context=_make_context()) == "no answer"
        assert seen == [(mock_execute.side_effect, "cat.png")]

    def test_output_of_the_error_handler_goes_through_the_output_processor(self, mock_execute):
        mock_execute.side_effect = ValueError("refused")
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=_describe,
            op_kwargs={"text": "the picture", "source": "cat.png"},
            llm_conn_id="my_llm",
            error_handler=lambda error: "no answer",
            output_processor=lambda output, source: {"source": source, "answer": output},
        )

        assert op.execute(context=_make_context()) == {"source": "cat.png", "answer": "no answer"}

    def test_error_handler_may_raise(self, mock_execute):
        mock_execute.side_effect = ValueError("throttled")

        def reraise(error: Exception) -> str:
            raise error

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=_describe,
            op_kwargs={"text": "the picture"},
            llm_conn_id="my_llm",
            error_handler=reraise,
        )

        with pytest.raises(ValueError, match="throttled"):
            op.execute(context=_make_context())

    def test_failed_run_fails_the_task_without_error_handler(self, mock_execute):
        mock_execute.side_effect = ValueError("refused")
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=_describe,
            op_kwargs={"text": "the picture"},
            llm_conn_id="my_llm",
            output_processor=lambda output: output,
        )

        with pytest.raises(ValueError, match="refused"):
            op.execute(context=_make_context())

    def test_error_of_the_callable_is_not_handed_to_the_error_handler(self, mock_execute):
        def failing_prompt() -> str:
            raise KeyError("no such document")

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=failing_prompt,
            llm_conn_id="my_llm",
            error_handler=lambda error: "no answer",
        )

        with pytest.raises(KeyError, match="no such document"):
            op.execute(context=_make_context())
        mock_execute.assert_not_called()

    def test_pause_passes_the_error_handler_by(self, mock_execute):
        mock_execute.side_effect = _Pause()
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=_describe,
            op_kwargs={"text": "the picture"},
            llm_conn_id="my_llm",
            error_handler=lambda error: "no answer",
        )

        with pytest.raises(_Pause):
            op.execute(context=_make_context())

    @pytest.mark.parametrize(
        ("method", "kwargs"),
        [
            (
                "resume_after_tool_approval",
                {"tool_call_ids": [], "usage": {}, "transcript_sha256": "", "toolset_ids": [], "event": {}},
            ),
            ("resume_hitl_review", {"iteration": 1, "deadline": None, "state_sha256": "", "event": {}}),
        ],
    )
    def test_resumed_run_goes_through_the_callbacks_too(self, mock_execute, method, kwargs):
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=_describe,
            op_kwargs={"text": "the picture", "source": "cat.png"},
            llm_conn_id="my_llm",
            output_processor=lambda output, source: {"source": source, "answer": output},
        )

        with patch(
            f"airflow.providers.common.ai.operators.agent.AgentOperator.{method}",
            autospec=True,
            return_value="a cat",
        ) as mock_resume:
            result = getattr(op, method)(_make_context(), **kwargs)

        assert result == {"source": "cat.png", "answer": "a cat"}
        mock_resume.assert_called_once_with(op, ANY, **kwargs)
        mock_execute.assert_not_called()

    def test_error_handler_is_refused_with_durable(self, mock_execute):
        with pytest.raises(ValueError, match="error_handler is not supported with durable=True"):
            _AgentDecoratedOperator(
                task_id="test",
                python_callable=_describe,
                op_kwargs={"text": "the picture"},
                llm_conn_id="my_llm",
                durable=True,
                error_handler=lambda error: "no answer",
            )


class TestAgentCallbacksOnARealRun:
    """The callbacks around ``AgentOperator.execute`` itself, with only the model stubbed."""

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_output_processor_gets_the_output_the_task_would_return(
        self, mock_hook_cls, make_mock_run_result
    ):
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
        mock_agent.run_sync.return_value = make_mock_run_result("a cat")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=_describe,
            op_kwargs={"text": "the picture", "source": "cat.png"},
            llm_conn_id="my_llm",
            output_processor=lambda output, source: {"source": source, "answer": output},
        )

        assert op.execute(context=_make_context()) == {"source": "cat.png", "answer": "a cat"}

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_error_handler_gets_the_error_the_model_raised(self, mock_hook_cls):
        refused = ValueError("content filter")
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
        mock_agent.run_sync.side_effect = refused
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        seen = []

        def no_answer(error: Exception, source: str) -> str:
            seen.append((error, source))
            return "no answer"

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=_describe,
            op_kwargs={"text": "the picture", "source": "cat.png"},
            llm_conn_id="my_llm",
            error_handler=no_answer,
        )

        assert op.execute(context=_make_context()) == "no answer"
        assert seen == [(refused, "cat.png")]
