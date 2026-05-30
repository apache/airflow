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

import functools
import inspect
from typing import get_type_hints
from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from airflow.providers.common.ai.hooks.base import (
    AgentRunRequest,
    AgentRunResult,
    AgentUsage,
    BaseAIHook,
    BaseToolset,
    DurableContext,
    DurableStats,
    ToolSpec,
)
from airflow.providers.common.compat.sdk import BaseHook


class TestBaseAIHookGetAgentHook:
    @patch("airflow.providers.common.ai.hooks.base.BaseHook.get_hook", autospec=True)
    def test_returns_hook_when_instance_is_base_hook(self, mock_get_hook):
        mock_hook = MagicMock(spec=BaseAIHook)
        mock_get_hook.return_value = mock_hook

        result = BaseAIHook.get_agent_hook("my_conn")

        assert result is mock_hook
        mock_get_hook.assert_called_once_with("my_conn", hook_params=None)

    @patch("airflow.providers.common.ai.hooks.base.BaseHook.get_hook", autospec=True)
    def test_raises_when_hook_is_not_base_hook(self, mock_get_hook):
        mock_get_hook.return_value = MagicMock(spec=BaseHook)

        with pytest.raises(TypeError, match="not a BaseAIHook"):
            BaseAIHook.get_agent_hook("my_conn")


class TestBaseAIHookInit:
    def test_stores_model_id_and_conn_id(self):
        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec.fn

        hook = ConcreteHook(llm_conn_id="my_conn", model_id="openai:gpt-5")
        assert hook.llm_conn_id == "my_conn"
        assert hook.model_id == "openai:gpt-5"


class TestBaseAIHookAgentDurable:
    def test_bind_pop_round_trip(self):
        agent = Agent(TestModel())
        storage = MagicMock()
        counter = MagicMock()

        BaseAIHook._bind_agent_durable(agent, storage, counter)
        assert agent._airflow_durable_state == (storage, counter)
        assert BaseAIHook._pop_agent_durable(agent) == (storage, counter)
        assert BaseAIHook._pop_agent_durable(agent) is None
        assert not hasattr(agent, "_airflow_durable_state")


class TestValidateRunRequest:
    def test_rejects_toolsets_when_unsupported(self):
        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"
            supports_toolsets = False
            supports_usage_limits = True
            supports_durable = True

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec.fn

        hook = ConcreteHook(llm_conn_id="test_conn")
        request = AgentRunRequest(prompt="hi", toolsets=[MagicMock()])
        with pytest.raises(ValueError, match="toolsets are not supported"):
            hook.validate_run_request(request)

    def test_rejects_usage_limits_when_unsupported(self):
        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"
            supports_toolsets = True
            supports_usage_limits = False
            supports_durable = True

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec.fn

        hook = ConcreteHook(llm_conn_id="test_conn")
        request = AgentRunRequest(prompt="hi", usage_limits=MagicMock())
        with pytest.raises(ValueError, match="usage_limits are not supported"):
            hook.validate_run_request(request)


class TestAgentRunResult:
    def test_dataclass_fields(self):
        usage = AgentUsage(requests=1, tool_calls=2, total_tokens=10)
        result = AgentRunResult(
            output="answer",
            message_history=["msg"],
            model_name="test-model",
            usage=usage,
            tool_names=["query"],
        )
        assert result.output == "answer"
        assert result.message_history == ["msg"]
        assert result.model_name == "test-model"
        assert result.usage == usage
        assert result.tool_names == ["query"]
        assert result.durable_stats is None

    def test_durable_stats_field(self):
        stats = DurableStats(replayed_model=2, cached_model=3)
        result = AgentRunResult(output="x", durable_stats=stats)
        assert result.durable_stats is stats


class TestAgentRunRequest:
    def test_defaults(self):
        req = AgentRunRequest(prompt="hello")
        assert req.prompt == "hello"
        assert req.output_type is str
        assert req.instructions == ""
        assert req.toolsets is None
        assert req.usage_limits is None
        assert req.message_history is None
        assert req.enable_tool_logging is True
        assert req.durable_context is None
        assert req.agent_params == {}

    def test_with_all_fields(self):
        ctx = DurableContext(dag_id="d", task_id="t", run_id="r", map_index=2)
        req = AgentRunRequest(
            prompt="test",
            output_type=int,
            instructions="sys",
            toolsets=["ts"],
            usage_limits="limits",
            message_history=["h"],
            enable_tool_logging=False,
            durable_context=ctx,
            agent_params={"retries": 3},
        )
        assert req.output_type is int
        assert req.instructions == "sys"
        assert req.durable_context is ctx
        assert req.agent_params == {"retries": 3}


class TestBaseAIHookResolveTools:
    def test_resolve_tools_calls_spec_to_native(self):
        """_resolve_tools converts each ToolSpec via _tool_spec_to_native."""

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return {"name": spec.name, "fn": spec.fn}

        hook = ConcreteHook.__new__(ConcreteHook)

        def my_tool(x: int) -> str:
            return str(x)

        class MyToolset(BaseToolset):
            def as_tools(self):
                return [ToolSpec(name="my_tool", description="desc", parameters={}, fn=my_tool)]

        result = hook._resolve_tools([MyToolset()], enable_logging=False, storage=None, counter=None)

        assert len(result) == 1
        assert result[0]["name"] == "my_tool"

    def test_resolve_tools_wraps_with_logging(self):
        """When enable_logging=True, callable is wrapped."""
        mock_log = MagicMock()

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            @property
            def log(self):
                return mock_log

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec.fn

        hook = ConcreteHook.__new__(ConcreteHook)

        calls = []

        def original():
            calls.append("original")
            return "result"

        class SimpleToolset(BaseToolset):
            def as_tools(self):
                return [ToolSpec(name="original", description="", parameters={}, fn=original)]

        [wrapped_fn] = hook._resolve_tools([SimpleToolset()], enable_logging=True, storage=None, counter=None)
        wrapped_fn()

        assert calls == ["original"]
        mock_log.info.assert_called()

    def test_resolve_tools_wraps_plain_callable(self):
        """A plain function is auto-wrapped using __name__ and __doc__."""

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return {"name": spec.name, "description": spec.description, "fn": spec.fn}

        hook = ConcreteHook.__new__(ConcreteHook)

        def roll_dice() -> str:
            """Roll a six-sided die and return the result."""
            return "4"

        result = hook._resolve_tools([roll_dice], enable_logging=False, storage=None, counter=None)

        assert len(result) == 1
        assert result[0]["name"] == "roll_dice"
        assert result[0]["description"] == "Roll a six-sided die and return the result."
        assert result[0]["fn"] is roll_dice

    def test_resolve_tools_wraps_bound_method(self):
        """A bound method is auto-wrapped using __name__ and __doc__."""

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return {"name": spec.name, "description": spec.description, "fn": spec.fn}

        hook = ConcreteHook.__new__(ConcreteHook)

        class MyHelper:
            def search(self, query: str) -> str:
                """Search for data."""
                return query

        helper = MyHelper()
        bound_method = helper.search
        result = hook._resolve_tools([bound_method], enable_logging=False, storage=None, counter=None)

        assert len(result) == 1
        assert result[0]["name"] == "search"
        assert result[0]["description"] == "Search for data."
        assert result[0]["fn"] is bound_method

    def test_resolve_tools_wraps_partial(self):
        """A functools.partial is auto-wrapped using the underlying function's name and doc."""

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return {"name": spec.name, "description": spec.description, "fn": spec.fn}

        hook = ConcreteHook.__new__(ConcreteHook)

        def query_db(db: str, query: str) -> str:
            """Query the database."""
            return f"{db}: {query}"

        partial_tool = functools.partial(query_db, db="prod")
        result = hook._resolve_tools([partial_tool], enable_logging=False, storage=None, counter=None)

        assert len(result) == 1
        assert result[0]["name"] == "query_db"
        assert result[0]["description"] == "Query the database."
        assert result[0]["fn"] is partial_tool

    def test_resolve_tools_wraps_callable_object(self):
        """A callable object is auto-wrapped using the class name."""

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return {"name": spec.name, "fn": spec.fn}

        hook = ConcreteHook.__new__(ConcreteHook)

        class Searcher:
            def __call__(self, query: str) -> str:
                return query

        searcher = Searcher()
        result = hook._resolve_tools([searcher], enable_logging=False, storage=None, counter=None)

        assert len(result) == 1
        assert result[0]["name"] == "Searcher"
        assert result[0]["fn"] is searcher

    def test_resolve_tools_passes_non_function_non_toolset_through(self):
        """Items that are not BaseToolset and not plain functions are passed through unchanged."""

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec.fn

        hook = ConcreteHook.__new__(ConcreteHook)

        native_tool_obj = object()  # not a function, not a BaseToolset
        result = hook._resolve_tools([native_tool_obj], enable_logging=True, storage=None, counter=None)

        assert result == [native_tool_obj]

    def test_resolve_tools_mixes_base_toolset_and_native(self):
        """BaseToolset items are converted; non-function native items are passed through in order."""

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return f"converted:{spec.name}"

        hook = ConcreteHook.__new__(ConcreteHook)

        native_tool = object()  # not a function, passes through unchanged

        class MyToolset(BaseToolset):
            def as_tools(self):
                return [ToolSpec(name="greet", description="", parameters={}, fn=lambda: "hi")]

        result = hook._resolve_tools(
            [MyToolset(), native_tool], enable_logging=False, storage=None, counter=None
        )

        assert result == ["converted:greet", native_tool]


class TestGetWrapperMetadataSources:
    def test_plain_function_returns_self_for_both(self):
        def fn(x: int) -> str:
            return str(x)

        sig_src, ann_src = BaseAIHook._get_wrapper_metadata_sources(fn)
        assert sig_src is fn
        assert ann_src is fn

    def test_bound_method_returns_self_for_both(self):
        class MyClass:
            def method(self, x: int) -> str:
                return str(x)

        obj = MyClass()
        # Store once — each attribute access on an instance creates a new bound-method object.
        bound = obj.method
        sig_src, ann_src = BaseAIHook._get_wrapper_metadata_sources(bound)
        assert sig_src is bound
        assert ann_src is bound

    def test_simple_partial_returns_partial_and_underlying_func(self):
        def fn(x: int, y: str) -> float:
            return 1.0

        p = functools.partial(fn, x=1)
        sig_src, ann_src = BaseAIHook._get_wrapper_metadata_sources(p)
        assert sig_src is p
        assert ann_src is fn

    def test_nested_partial_unwraps_to_original_function(self):
        def fn(url: str, method: str, timeout: int) -> str:
            return ""

        p1 = functools.partial(fn, url="https://example.com")
        p2 = functools.partial(p1, method="POST")

        sig_src, ann_src = BaseAIHook._get_wrapper_metadata_sources(p2)
        assert sig_src is p2
        assert ann_src is fn

    def test_callable_object_returns_call_method(self):
        class Searcher:
            def __call__(self, query: str) -> str:
                return query

        obj = Searcher()
        sig_src, ann_src = BaseAIHook._get_wrapper_metadata_sources(obj)
        assert sig_src is ann_src
        assert tuple(inspect.signature(sig_src).parameters) == ("query",)


class TestCopyWrapperIntrospectionMetadata:
    def test_plain_function_copies_signature_and_annotations(self):
        def fn(x: int, y: str) -> float:
            return 1.0

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        BaseAIHook._copy_wrapper_introspection_metadata(fn, wrapper)

        assert inspect.signature(wrapper) == inspect.signature(fn)
        assert get_type_hints(wrapper) == {"x": int, "y": str, "return": float}

    def test_nested_partial_annotations_resolved_from_underlying_function(self):
        def fn(url: str, method: str, timeout: int) -> str:
            return ""

        p1 = functools.partial(fn, url="https://example.com")
        p2 = functools.partial(p1, method="POST")

        @functools.wraps(p2)
        def wrapper(*args, **kwargs):
            return p2(*args, **kwargs)

        BaseAIHook._copy_wrapper_introspection_metadata(p2, wrapper)

        sig = inspect.signature(wrapper)
        # inspect.signature keeps bound params with defaults rather than removing them.
        assert set(sig.parameters) == {"url", "method", "timeout"}
        # Annotations are resolved from the unwrapped underlying function, covering all params.
        assert get_type_hints(wrapper) == {"url": str, "method": str, "timeout": int, "return": str}

    def test_uninspectable_signature_returns_early_without_raising(self):
        def fn(x: int) -> str:
            return str(x)

        # Plain wrapper with no __signature__ pre-set.
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        with patch(
            "airflow.providers.common.ai.hooks.base.inspect.signature",
            side_effect=ValueError("no introspectable signature"),
        ):
            BaseAIHook._copy_wrapper_introspection_metadata(fn, wrapper)

        # Must not raise, and __signature__ must not be set on the wrapper.
        assert not hasattr(wrapper, "__signature__")

    def test_get_type_hints_failure_falls_back_to_raw_annotations(self):
        def fn(x: int) -> str:
            return str(x)

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        with patch(
            "airflow.providers.common.ai.hooks.base.get_type_hints",
            side_effect=NameError("NonExistentType"),
        ):
            BaseAIHook._copy_wrapper_introspection_metadata(fn, wrapper)

        # Falls back to raw __annotations__ from functools.wraps
        assert "x" in wrapper.__annotations__

    def test_module_falls_back_to_fn_module_when_annotation_source_lacks_it(self):
        def fn(x: int) -> int:
            return x

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        BaseAIHook._copy_wrapper_introspection_metadata(fn, wrapper)
        assert wrapper.__module__ == fn.__module__

    def test_callable_object_copies_call_signature(self):
        class Lookup:
            def __call__(self, customer_id: str) -> dict:
                return {}

        obj = Lookup()

        @functools.wraps(obj)
        def wrapper(*args, **kwargs):
            return obj(*args, **kwargs)

        BaseAIHook._copy_wrapper_introspection_metadata(obj, wrapper)

        sig = inspect.signature(wrapper)
        assert tuple(sig.parameters) == ("customer_id",)


class TestBaseAIHookLoggedCallable:
    def test_logged_callable_logs_and_returns(self):
        logger = MagicMock()
        calls = []

        def fn(x):
            calls.append(x)
            return x * 2

        wrapped = BaseAIHook._logged_callable(fn, logger)
        result = wrapped(x=5)

        assert result == 10
        assert calls == [5]
        logger.info.assert_called()

    def test_logged_callable_logs_exception(self):
        logger = MagicMock()

        def failing():
            raise RuntimeError("boom")

        wrapped = BaseAIHook._logged_callable(failing, logger)
        with pytest.raises(RuntimeError, match="boom"):
            wrapped()

        logger.exception.assert_called_once()

    def test_logged_callable_uses_explicit_name_over_introspection(self):
        logger = MagicMock()

        def fn():
            return "ok"

        wrapped = BaseAIHook._logged_callable(fn, logger, name="my_tool")
        wrapped()

        logger.info.assert_any_call("::group::Tool call: %s", "my_tool")
        logger.info.assert_any_call("Tool %s returned in %.2fs", "my_tool", pytest.approx(0.0, abs=1.0))

    def test_logged_callable_partial_logs_correct_name_without_explicit_name(self):
        """Without an explicit name, a partial falls back to type(fn).__name__ = 'partial'."""
        logger = MagicMock()

        def fetch_metric(environment: str, metric_name: str) -> float:
            return 1.0

        partial_fn = functools.partial(fetch_metric, "prod")
        wrapped = BaseAIHook._logged_callable(partial_fn, logger)
        wrapped(metric_name="cpu")

        # Without name= the fallback is type(partial).__name__ = "partial", not "fetch_metric".
        logger.info.assert_any_call("::group::Tool call: %s", "partial")

    def test_logged_callable_partial_logs_correct_name_with_explicit_name(self):
        """Passing name= fixes the 'partial' log name for functools.partial tools."""
        logger = MagicMock()

        def fetch_metric(environment: str, metric_name: str) -> float:
            return 1.0

        partial_fn = functools.partial(fetch_metric, "prod")
        wrapped = BaseAIHook._logged_callable(partial_fn, logger, name="fetch_metric")
        wrapped(metric_name="cpu")

        logger.info.assert_any_call("::group::Tool call: %s", "fetch_metric")

    def test_logged_callable_preserves_partial_introspection(self):
        logger = MagicMock()

        def fetch_metric(environment: str, metric_name: str) -> float:
            return 1.0

        wrapped = BaseAIHook._logged_callable(functools.partial(fetch_metric, "prod"), logger)

        assert inspect.signature(wrapped) == inspect.signature(functools.partial(fetch_metric, "prod"))
        assert get_type_hints(wrapped) == {
            "metric_name": str,
            "return": float,
        }

    def test_logged_callable_preserves_callable_object_introspection(self):
        logger = MagicMock()

        class CustomerLookup:
            def __call__(self, customer_id: str) -> dict[str, str]:
                return {"customer_id": customer_id}

        wrapped = BaseAIHook._logged_callable(CustomerLookup(), logger)

        signature = inspect.signature(wrapped)
        assert tuple(signature.parameters) == ("customer_id",)
        assert get_type_hints(wrapped) == {
            "customer_id": str,
            "return": dict[str, str],
        }


class TestBaseAIHookCachedCallable:
    def test_cached_callable_saves_and_returns(self):
        storage = MagicMock()
        counter = MagicMock()
        counter.next_step.return_value = 1
        storage.load_tool_result.return_value = (False, None)

        calls = []

        def fn():
            calls.append(1)
            return "computed"

        wrapped = BaseAIHook._cached_callable(fn, storage, counter)
        result = wrapped()

        assert result == "computed"
        assert calls == [1]
        storage.save_tool_result.assert_called_once_with("tool_step_1", "computed")

    def test_cached_callable_replays_on_hit(self):
        storage = MagicMock()
        counter = MagicMock()
        counter.replayed_tool = 0
        counter.next_step.return_value = 1
        storage.load_tool_result.return_value = (True, "cached_value")

        calls = []

        def fn():
            calls.append(1)
            return "computed"

        wrapped = BaseAIHook._cached_callable(fn, storage, counter)
        result = wrapped()

        assert result == "cached_value"
        assert calls == []
        assert counter.replayed_tool == 1
        storage.save_tool_result.assert_not_called()
