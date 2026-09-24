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
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch

import pytest
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.capabilities import Toolset
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.messages import (
    ModelMessage,
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.toolsets.combined import CombinedToolset
from pydantic_ai.toolsets.function import FunctionToolset
from pydantic_ai.toolsets.wrapper import WrapperToolset
from pydantic_ai.usage import RequestUsage, UsageLimits

from airflow.providers.common.ai.durable.base import DurableStorageProtocol
from airflow.providers.common.ai.durable.caching_toolset import CachingToolset
from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
from airflow.providers.common.ai.durable.storage import DurableStorage
from airflow.providers.common.ai.operators.agent import AgentOperator, HITLReviewLink, _build_code_mode
from airflow.providers.common.ai.sandbox.base import SandboxBackend
from airflow.providers.common.ai.toolsets.hook import HookToolset
from airflow.providers.common.ai.toolsets.logging import LoggingToolset
from airflow.providers.common.ai.toolsets.mcp import MCPToolset
from airflow.providers.common.ai.toolsets.sandbox import SandboxToolset
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.ai.utils.toolsets import find_toolset
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, BaseHook
from airflow.sdk import DAG, task

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


def _make_mock_agent(output, make_mock_run_result, *, cost=None):
    """Create a mock agent that returns the given output."""
    mock_agent = MagicMock(spec=["run_sync", "instrument"])
    mock_agent.run_sync.return_value = make_mock_run_result(output, cost=cost)
    return mock_agent


def _make_ti(*, id="ti-1", dag_id="dag", task_id="task", run_id="run", map_index=-1, try_number=1):
    """Return a task-instance double carrying the identity fields execute() reads."""
    ti = MagicMock()
    ti.configure_mock(
        id=id, dag_id=dag_id, task_id=task_id, run_id=run_id, map_index=map_index, try_number=try_number
    )
    return ti


def _make_context(ti=None):
    """A context whose ``task_instance`` is a configured ti. Other keys stay generic mocks."""
    ti = ti if ti is not None else _make_ti()
    ctx = MagicMock()
    ctx.__getitem__.side_effect = lambda key: ti if key == "task_instance" else MagicMock()
    return ctx


PRICED_COST = Decimal("0.10")


def _build_priced_response(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    return ModelResponse(
        parts=[TextPart(content="the answer")],
        usage=RequestUsage(input_tokens=100, output_tokens=50, cost=PRICED_COST),
    )


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


class TestAgentOperatorCoercionOrder:
    @patch.object(AgentOperator, "_build_agent", autospec=True)
    def test_unparsable_usage_limits_fails_before_agent_is_built(self, mock_build_agent):
        """Building the agent resolves the connection and every toolset, so an
        uncoercible ``usage_limits`` has to fail ahead of it."""
        op = AgentOperator(
            task_id="test",
            prompt="run",
            llm_conn_id="my_llm",
            usage_limits={"request_limit": "not-a-number"},
        )

        with pytest.raises(ValueError, match=r"usage_limits\['request_limit'\]"):
            op.execute(context={})

        mock_build_agent.assert_not_called()


class TestAgentOperatorTemplateFields:
    def test_template_fields(self):
        expected = {
            "prompt",
            "llm_conn_id",
            "model_id",
            "fallback_conn_ids",
            "system_prompt",
            "agent_params",
            "message_history",
            "usage_limits",
        }
        assert set(AgentOperator.template_fields) == expected


class _TenantHook(BaseHook):
    conn_name_attr = "tenant_conn_id"

    def __init__(self, tenant_conn_id: str):
        super().__init__()
        self.tenant_conn_id = tenant_conn_id

    def get_records(self, sql: str) -> list:
        """Run a query."""
        return []


class TestAgentOperatorToolsetTemplating:
    """Connection IDs on SQLToolset / MCPToolset / HookToolset render per task instance, on a copy."""

    CONTEXT = {"params": {"customer": "acme"}}

    def test_sql_toolset_conn_id_is_rendered_on_a_copy(self):
        toolset = SQLToolset(db_conn_id="tenant_{{ params.customer }}")
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="llm", toolsets=[toolset])

        op.render_template_fields(self.CONTEXT)

        (rendered,) = op.toolsets
        assert rendered._db_conn_id == "tenant_acme"
        assert rendered.id == "sql-tenant_acme"
        assert rendered is not toolset
        assert toolset._db_conn_id == "tenant_{{ params.customer }}"

    def test_shared_toolset_renders_independently_per_task(self):
        """Mapped task instances and dag.test() share one toolset object in a process;
        rendering it in place would hand the first customer's connection to the next."""
        shared = SQLToolset(db_conn_id="tenant_{{ params.customer }}")
        first = AgentOperator(task_id="a", prompt="p", llm_conn_id="llm", toolsets=[shared])
        second = AgentOperator(task_id="b", prompt="p", llm_conn_id="llm", toolsets=[shared])

        first.render_template_fields({"params": {"customer": "acme"}})
        second.render_template_fields({"params": {"customer": "globex"}})

        assert first.toolsets[0]._db_conn_id == "tenant_acme"
        assert second.toolsets[0]._db_conn_id == "tenant_globex"

    def test_hook_toolset_conn_id_is_rendered_on_a_copy(self):
        hook = _TenantHook(tenant_conn_id="tenant_{{ params.customer }}")
        op = AgentOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="llm",
            toolsets=[HookToolset(hook, allowed_methods=["get_records"])],
        )

        op.render_template_fields(self.CONTEXT)

        assert op.toolsets[0].id == "hook-_TenantHook-tenant_acme"
        assert hook.tenant_conn_id == "tenant_{{ params.customer }}"

    def test_mcp_toolset_conn_id_is_rendered(self):
        op = AgentOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="llm",
            toolsets=[MCPToolset(mcp_conn_id="mcp_{{ params.customer }}")],
        )

        op.render_template_fields(self.CONTEXT)

        assert op.toolsets[0].id == "mcp-mcp_acme"

    @pytest.mark.parametrize(
        "wrap",
        [
            pytest.param(lambda ts: ts.prefixed("crm"), id="prefixed"),
            pytest.param(lambda ts: ts.filtered(lambda ctx, tool: True), id="filtered"),
            pytest.param(lambda ts: CombinedToolset([FunctionToolset(), ts]), id="combined"),
        ],
    )
    def test_toolset_inside_wrapper_is_rendered(self, wrap):
        inner = SQLToolset(db_conn_id="tenant_{{ params.customer }}")
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="llm", toolsets=[wrap(inner)])

        op.render_template_fields(self.CONTEXT)

        found = find_toolset(op.toolsets, SQLToolset)
        assert found is not None
        assert found._db_conn_id == "tenant_acme"
        assert inner._db_conn_id == "tenant_{{ params.customer }}"

    def test_toolset_capability_is_rendered(self):
        capability = Toolset(SQLToolset(db_conn_id="tenant_{{ params.customer }}"))
        op = AgentOperator(
            task_id="t", prompt="p", llm_conn_id="llm", agent_params={"capabilities": [capability]}
        )

        op.render_template_fields(self.CONTEXT)

        (rendered,) = op.agent_params["capabilities"]
        assert rendered.toolset._db_conn_id == "tenant_acme"
        assert capability.toolset._db_conn_id == "tenant_{{ params.customer }}"

    def test_callable_toolset_capability_is_left_as_is(self):
        """A factory resolved per run has no toolset to render until the run starts."""
        capability = Toolset(lambda ctx: SQLToolset(db_conn_id="tenant_{{ params.customer }}"))
        op = AgentOperator(
            task_id="t", prompt="p", llm_conn_id="llm", agent_params={"capabilities": [capability]}
        )

        op.render_template_fields(self.CONTEXT)

        assert op.agent_params["capabilities"][0] is capability

    def test_only_connection_ids_are_templated(self):
        """allowed_tables is validated and canonicalised in __init__, so rendering it later
        would bypass the fail-closed empty-list check."""
        assert SQLToolset.agent_template_fields == ("_db_conn_id",)
        assert MCPToolset.agent_template_fields == ("_mcp_conn_id",)
        assert HookToolset.agent_template_fields == ("conn_id",)

    @pytest.mark.parametrize("toolset_cls", [SQLToolset, MCPToolset, HookToolset])
    def test_toolsets_do_not_opt_in_through_template_fields(self, toolset_cls):
        """Airflow's templater renders any object with ``template_fields`` in place wherever it is
        nested in a template field, which would leak one task instance's connection to the next."""
        assert not hasattr(toolset_cls, "template_fields")

    def test_toolsets_in_agent_params_render_on_a_copy_per_task(self):
        shared = SQLToolset(db_conn_id="tenant_{{ params.customer }}")
        first = AgentOperator(task_id="a", prompt="p", llm_conn_id="llm", agent_params={"toolsets": [shared]})
        second = AgentOperator(
            task_id="b", prompt="p", llm_conn_id="llm", agent_params={"toolsets": [shared]}
        )

        first.render_template_fields({"params": {"customer": "acme"}})
        second.render_template_fields({"params": {"customer": "globex"}})

        assert first.agent_params["toolsets"][0].id == "sql-tenant_acme"
        assert second.agent_params["toolsets"][0].id == "sql-tenant_globex"
        assert shared._db_conn_id == "tenant_{{ params.customer }}"

    def test_wrapped_toolset_inside_a_capability_is_rendered(self):
        capability = Toolset(SQLToolset(db_conn_id="tenant_{{ params.customer }}").prefixed("crm"))
        op = AgentOperator(
            task_id="t", prompt="p", llm_conn_id="llm", agent_params={"capabilities": [capability]}
        )

        op.render_template_fields(self.CONTEXT)

        found = find_toolset([op.agent_params["capabilities"][0].toolset], SQLToolset)
        assert found is not None
        assert found.id == "sql-tenant_acme"

    def test_rendered_toolset_id_is_logged(self, caplog):
        op = AgentOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="llm",
            toolsets=[SQLToolset(db_conn_id="tenant_{{ params.customer }}")],
        )

        with caplog.at_level("INFO"):
            op.render_template_fields(self.CONTEXT)

        assert "Rendered toolset sql-tenant_acme" in caplog.text

    def test_rendering_twice_logs_once(self, caplog):
        """@task.agent renders a second time; by then the id no longer changes."""
        op = AgentOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="llm",
            toolsets=[SQLToolset(db_conn_id="tenant_{{ params.customer }}")],
        )

        with caplog.at_level("INFO"):
            op.render_template_fields(self.CONTEXT)
            op.render_template_fields(self.CONTEXT)

        assert caplog.text.count("Rendered toolset sql-tenant_acme") == 1

    def test_toolset_without_template_fields_is_left_as_is(self):
        toolset = FunctionToolset()
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="llm", toolsets=[toolset])

        op.render_template_fields(self.CONTEXT)

        assert op.toolsets[0] is toolset

    def test_untemplated_wrapper_subclass_is_not_rebuilt(self):
        """visit_and_replace rebuilds wrappers with dataclasses.replace, which a subclass with its
        own __init__ does not survive; nothing to render means nothing to rebuild."""

        class Audited(WrapperToolset):
            def __init__(self, wrapped, *, audit_name):
                super().__init__(wrapped=wrapped)
                self.audit_name = audit_name

        toolset = Audited(FunctionToolset(), audit_name="x")
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="llm", toolsets=[toolset])

        op.render_template_fields(self.CONTEXT)

        assert op.toolsets[0] is toolset

    @pytest.mark.parametrize(
        ("form", "template"),
        [
            pytest.param("operator", "tenant_{{ task.prompt }}", id="operator"),
            pytest.param("decorator", "tenant_{{ task.op_kwargs.customer }}", id="decorator"),
        ],
    )
    def test_each_map_index_gets_its_own_connection(self, form, template):
        """Through the real MappedOperator render path, for both authoring forms."""
        shared = SQLToolset(db_conn_id=template)
        with DAG("d", schedule=None) as dag:
            if form == "operator":
                mapped = AgentOperator.partial(task_id="m", llm_conn_id="llm", toolsets=[shared]).expand(
                    prompt=["acme", "globex"]
                )
            else:

                @task.agent(llm_conn_id="llm", toolsets=[shared])
                def report(customer: str) -> str:
                    return customer

                mapped = report.expand(customer=["acme", "globex"]).operator

        ids = []
        for map_index in (0, 1):
            context: dict = {
                "ti": SimpleNamespace(map_index=map_index),
                "params": {},
                "dag": dag,
                "dag_run": SimpleNamespace(conf={}),
            }
            mapped.render_template_fields(context, dag.get_template_env())
            ids.append(context["task"].toolsets[0].id)

        assert ids == ["sql-tenant_acme", "sql-tenant_globex"]
        assert shared._db_conn_id == template


class TestAgentOperatorExecute:
    @pytest.mark.parametrize(
        "bad",
        [pytest.param([], id="list"), pytest.param("0.5", id="str"), pytest.param(5, id="int")],
    )
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_rejects_non_container_usage_limits(self, mock_hook_cls, bad, make_mock_run_result):
        """A non-UsageLimits/dict/None value (e.g. a str migrated from ``max_cost``,
        or the whole field written as a single un-rendered Jinja expression) is
        rejected by ``coerce_usage_limits`` at execute time, not at ``__init__`` --
        checking it at ``__init__`` would raise on a still-templated string before
        it is ever rendered (the ``validate-operators-init`` hook forbids exactly
        that: value-dependent validation of a template field before render)."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="c", usage_limits=bad)
        with pytest.raises(TypeError, match="usage_limits must be a UsageLimits, a dict, or None"):
            op.execute(context=MagicMock())
        mock_agent.run_sync.assert_not_called()

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_forwards_usage_limits_to_run_sync(self, mock_hook_cls, make_mock_run_result):
        """``usage_limits`` is forwarded to ``agent.run_sync`` on the non-durable path."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        limits = UsageLimits(request_limit=3, tool_calls_limit=5)
        op = AgentOperator(
            task_id="test",
            prompt="run",
            llm_conn_id="my_llm",
            usage_limits=limits,
        )
        op.execute(context=_make_context())

        mock_agent.run_sync.assert_called_once_with(
            "run", usage_limits=limits, run_id="ti-1", cancellation_token=ANY
        )

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_coerces_usage_limits_dict_before_run_sync(self, mock_hook_cls, make_mock_run_result):
        """A dict ``usage_limits`` is coerced into a real ``UsageLimits`` before ``run_sync``."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(
            task_id="test",
            prompt="run",
            llm_conn_id="my_llm",
            usage_limits={"cost_limit": "0.5"},
        )
        op.execute(context=MagicMock())

        _, kwargs = mock_agent.run_sync.call_args
        assert kwargs["usage_limits"] == UsageLimits(cost_limit=Decimal("0.5"))

    @pytest.mark.parametrize(
        ("field", "rendered", "expected"),
        [
            pytest.param("cost_limit", "0.05", Decimal("0.05"), id="decimal"),
            pytest.param("request_limit", "7", 7, id="int"),
            pytest.param("count_tokens_before_request", "true", True, id="bool"),
        ],
    )
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_renders_templated_usage_limits_dict_then_coerces(
        self, mock_hook_cls, field, rendered, expected, make_mock_run_result
    ):
        """The template chain end to end, for each ``_COERCERS`` type: Jinja renders
        the dict's string leaf (still a string -- Jinja never converts type), then
        ``execute`` coerces it to the field's real type. @amoghrajesh's review asked
        whether *every* field can be templated, not just ``cost_limit``."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(
            task_id="test",
            prompt="run",
            llm_conn_id="my_llm",
            usage_limits={field: "{{ params.value }}"},
        )
        op.render_template_fields({"params": {"value": rendered}})
        assert op.usage_limits == {field: rendered}

        op.execute(context=MagicMock())

        _, kwargs = mock_agent.run_sync.call_args
        assert getattr(kwargs["usage_limits"], field) == expected

    def test_render_template_fields_leaves_usage_limits_object_unchanged(self):
        """A ``UsageLimits`` instance has no ``resolve``/``template_fields``, so Jinja's
        nested-template-field walk is a no-op and the exact same object comes back."""
        limits = UsageLimits(cost_limit=Decimal("1"))
        op = AgentOperator(task_id="test", prompt="run", llm_conn_id="my_llm", usage_limits=limits)
        op.render_template_fields({})
        assert op.usage_limits is limits

    @pytest.mark.parametrize(
        "cap_kwargs",
        [
            pytest.param({"usage_limits": {"cost_limit": "0.05"}}, id="usage_limits_dict"),
            pytest.param({"usage_limits": UsageLimits(cost_limit=Decimal("0.05"))}, id="usage_limits_object"),
        ],
    )
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_raises_when_cost_cap_exceeded(self, mock_hook_cls, cap_kwargs):
        """A real run whose cost exceeds the configured cap raises ``UsageLimitExceeded``."""
        mock_hook_cls.get_hook.return_value.create_agent.side_effect = lambda **kw: Agent(
            FunctionModel(_build_priced_response), **kw
        )

        op = AgentOperator(task_id="test", prompt="run", llm_conn_id="my_llm", **cap_kwargs)

        with pytest.raises(UsageLimitExceeded, match=r"cost_limit.*0\.05"):
            op.execute(context=MagicMock())

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_completes_when_cost_stays_under_cap(self, mock_hook_cls):
        """A real run costing less than the configured ``cost_limit`` completes and returns the model output."""
        mock_hook_cls.get_hook.return_value.create_agent.side_effect = lambda **kw: Agent(
            FunctionModel(_build_priced_response), **kw
        )

        op = AgentOperator(
            task_id="test",
            prompt="run",
            llm_conn_id="my_llm",
            usage_limits={"cost_limit": str(PRICED_COST * 2)},
        )

        assert op.execute(context=MagicMock()) == "the answer"

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_raises_valueerror_for_unparsable_usage_limits_value(self, mock_hook_cls):
        """An unparsable templated ``usage_limits`` value surfaces as ``ValueError`` before any model call."""
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(
            task_id="test", prompt="run", llm_conn_id="my_llm", usage_limits={"cost_limit": ""}
        )
        with pytest.raises(ValueError, match=r"usage_limits\['cost_limit'\]"):
            op.execute(context=MagicMock())
        mock_agent.run_sync.assert_not_called()

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_regenerate_with_feedback_forwards_usage_limits(self, mock_hook_cls, make_mock_run_result):
        """``usage_limits`` is also forwarded by ``regenerate_with_feedback``."""
        mock_agent = _make_mock_agent("revised", make_mock_run_result)
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
            cancellation_token=ANY,
        )

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_regenerate_with_feedback_coerces_usage_limits_dict(self, mock_hook_cls, make_mock_run_result):
        """A dict ``usage_limits`` is also coerced by ``regenerate_with_feedback``."""
        mock_agent = _make_mock_agent("revised", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(
            task_id="test",
            prompt="run",
            llm_conn_id="my_llm",
            usage_limits={"cost_limit": "0.5"},
        )
        op.regenerate_with_feedback(feedback="Add detail", message_history=[])

        _, kwargs = mock_agent.run_sync.call_args
        assert kwargs["usage_limits"].cost_limit == Decimal("0.5")

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_creates_agent_from_hook(self, mock_hook_cls, make_mock_run_result):
        mock_agent = _make_mock_agent("The answer is 42.", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(
            task_id="test",
            prompt="What is the answer?",
            llm_conn_id="my_llm",
            system_prompt="You are helpful.",
        )
        result = op.execute(context=_make_context())

        assert result == "The answer is 42."
        mock_hook_cls.get_hook.assert_called_once_with(
            "my_llm", hook_params={"model_id": None, "fallback_conn_ids": None}
        )
        mock_hook_cls.get_hook.return_value.create_agent.assert_called_once_with(
            output_type=str, instructions="You are helpful."
        )
        mock_agent.run_sync.assert_called_once_with(
            "What is the answer?", usage_limits=None, run_id="ti-1", cancellation_token=ANY
        )

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_passes_toolsets_in_agent_kwargs(self, mock_hook_cls, make_mock_run_result):
        """Toolsets are passed through to the agent constructor."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent(
            "done", make_mock_run_result
        )

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
    def test_enable_tool_logging_false_skips_wrapping(self, mock_hook_cls, make_mock_run_result):
        """enable_tool_logging=False passes toolsets through unwrapped."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent(
            "done", make_mock_run_result
        )

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
    def test_execute_passes_agent_params(self, mock_hook_cls, make_mock_run_result):
        """agent_params are unpacked into create_agent."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent(
            "ok", make_mock_run_result
        )

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
    def test_code_mode_default_off_no_capabilities(self, mock_hook_cls, make_mock_run_result):
        """code_mode defaults to False, so no capabilities are injected."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent(
            "ok", make_mock_run_result
        )

        op = AgentOperator(task_id="t", prompt="hi", llm_conn_id="my_llm", toolsets=[MagicMock()])
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.get_hook.return_value.create_agent.call_args
        assert "capabilities" not in create_call[1]

    @patch("airflow.providers.common.ai.operators.agent._build_code_mode", return_value="CM")
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_code_mode_injects_capability(self, mock_hook_cls, mock_build, make_mock_run_result):
        """code_mode=True appends a CodeMode capability passed to create_agent."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent(
            "ok", make_mock_run_result
        )

        op = AgentOperator(
            task_id="t", prompt="hi", llm_conn_id="my_llm", toolsets=[MagicMock()], code_mode=True
        )
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.get_hook.return_value.create_agent.call_args
        assert create_call[1]["capabilities"] == ["CM"]
        mock_build.assert_called_once()

    @patch("airflow.providers.common.ai.operators.agent._build_code_mode", return_value="CM")
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_code_mode_appends_to_existing_capabilities(
        self, mock_hook_cls, mock_build, make_mock_run_result
    ):
        """A user-supplied capability via agent_params is preserved alongside CodeMode."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent(
            "ok", make_mock_run_result
        )

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
    def test_execute_structured_output(self, mock_hook_cls, make_mock_run_result):
        """Structured output keeps the Pydantic instance so downstream tasks can type-hint it."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent(
            Summary(text="Great", score=0.95), make_mock_run_result
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
    def test_execute_with_model_id(self, mock_hook_cls, make_mock_run_result):
        """model_id is passed to PydanticAIHook."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent(
            "ok", make_mock_run_result
        )

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
            model_id="openai:gpt-5",
        )
        op.execute(context=MagicMock())

        mock_hook_cls.get_hook.assert_called_once_with(
            "my_llm", hook_params={"model_id": "openai:gpt-5", "fallback_conn_ids": None}
        )

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_forwards_fallback_conn_ids_to_hook(self, mock_hook_cls, make_mock_run_result):
        """``fallback_conn_ids`` on the operator overrides the connection's own extra field."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent(
            "ok", make_mock_run_result
        )

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
            fallback_conn_ids=["conn_a", "conn_b"],
        )
        op.execute(context=MagicMock())

        mock_hook_cls.get_hook.assert_called_once_with(
            "my_llm", hook_params={"model_id": None, "fallback_conn_ids": ["conn_a", "conn_b"]}
        )

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_forwards_empty_fallback_conn_ids_to_hook(self, mock_hook_cls, make_mock_run_result):
        """An explicit ``[]`` disables a chain configured on the connection, not just an override."""
        mock_hook_cls.get_hook.return_value.create_agent.return_value = _make_mock_agent(
            "ok", make_mock_run_result
        )

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
            fallback_conn_ids=[],
        )
        op.execute(context=MagicMock())

        mock_hook_cls.get_hook.assert_called_once_with(
            "my_llm", hook_params={"model_id": None, "fallback_conn_ids": []}
        )

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
    @patch("airflow.providers.common.ai.operators.agent.AgentOperator.run_hitl_review", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_with_enable_hitl_review_delegates_to_run_hitl_review(
        self, mock_hook_cls, mock_run_hitl, make_mock_run_result
    ):
        """When enable_hitl_review=True, execute delegates to run_hitl_review with output and message_history."""
        msg_history = [MagicMock()]
        mock_result = make_mock_run_result("Initial output")
        mock_result.all_messages.return_value = msg_history
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
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
    def test_execute_with_hitl_rehydrates_base_model(
        self, mock_hook_cls, mock_run_hitl, make_mock_run_result
    ):
        """When enable_hitl_review=True and output_type is BaseModel, execute returns the model instance."""
        mock_result = make_mock_run_result(Summary(text="Approved summary", score=0.9))
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
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
    def test_execute_with_hitl_returns_string_unchanged(
        self, mock_hook_cls, mock_run_hitl, make_mock_run_result
    ):
        """When enable_hitl_review=True and output_type is str, execute returns string as-is."""
        mock_result = make_mock_run_result("Initial output")
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
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
    def test_execute_propagates_hitl_max_iterations_error(
        self, mock_hook_cls, mock_run_hitl, make_mock_run_result
    ):
        """When run_hitl_review raises HITLMaxIterationsError, execute propagates it."""
        from airflow.providers.common.ai.exceptions import HITLMaxIterationsError

        mock_result = make_mock_run_result("Initial output")
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
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
    def test_regenerate_with_feedback_calls_agent_with_feedback_and_history(
        self, mock_hook_cls, make_mock_run_result
    ):
        """regenerate_with_feedback builds agent and calls run_sync with feedback and message_history."""
        msg_history = [MagicMock()]
        mock_result = make_mock_run_result("Revised output")
        mock_result.all_messages.return_value = msg_history + [MagicMock()]
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
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
            cancellation_token=ANY,
        )

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_regenerate_with_feedback_serializes_base_model_output(self, mock_hook_cls, make_mock_run_result):
        """regenerate_with_feedback returns JSON string for BaseModel output."""
        mock_result = make_mock_run_result(Summary(text="Revised"))
        mock_result.all_messages.return_value = []
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
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
        self, mock_hook_cls, mock_build_storage, mock_infer, _, make_mock_run_result
    ):
        """durable=True wraps the model with CachingModel and cleans up the cache on success."""
        from airflow.providers.common.ai.durable.base import DurableStorageProtocol

        storage = MagicMock(spec=DurableStorageProtocol)
        mock_build_storage.return_value = storage

        mock_agent = MagicMock()
        mock_agent.run_sync.return_value = make_mock_run_result("ok")
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
    def test_execute_non_durable_does_not_wrap(self, mock_hook_cls, make_mock_run_result):
        """Default (durable=False) does not use override."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="test", prompt="test", llm_conn_id="my_llm")
        op.execute(context=_make_context())

        # run_sync called directly, no override
        mock_agent.run_sync.assert_called_once_with(
            "test", usage_limits=None, run_id="ti-1", cancellation_token=ANY
        )

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
    def test_cleanup_skipped_when_post_run_step_fails(
        self, mock_hook_cls, mock_build_storage, mock_infer, _, make_mock_run_result
    ):
        """Durable cleanup must not run if a post-run step (the message-history XCom
        push) fails, so the Airflow retry can still replay the cached steps."""
        storage = MagicMock(spec=DurableStorageProtocol)
        mock_build_storage.return_value = storage

        mock_agent = MagicMock(spec=["run_sync", "model", "override", "instrument"])
        mock_agent.run_sync.return_value = make_mock_run_result("ok")
        mock_agent.model = "test-model"
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="c", durable=True, message_history="[]")
        with patch.object(op, "_emit_message_history", side_effect=RuntimeError("xcom down")):
            with pytest.raises(RuntimeError, match="xcom down"):
                op.execute(context=_make_context())

        storage.cleanup.assert_not_called()

    def test_supports_durable_execution_marker(self):
        assert AgentOperator._AgentOperator__supports_durable_execution is True


@pytest.mark.skipif(
    not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
)
class TestAgentOperatorMultimodalPromptGuard:
    """AgentOperator.execute raises before agent.run_sync when enable_hitl_review=True
    and self.prompt is not a string -- covering direct construction and the native
    template rendering escape (where a string template renders to a Sequence)."""

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_rejects_sequence_prompt_with_hitl_review(self, mock_hook_cls):
        mock_agent = MagicMock(spec=["run_sync", "instrument"])
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
    def test_message_history_seeds_run_sync(self, mock_hook_cls, raw, expected_len, make_mock_run_result):
        """Every accepted input form is deserialized and passed to run_sync; blank/empty start fresh."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="run", llm_conn_id="c", message_history=raw)
        op.execute(context=MagicMock())

        passed = mock_agent.run_sync.call_args.kwargs["message_history"]
        assert len(passed) == expected_len
        assert all(isinstance(m, (ModelRequest, ModelResponse)) for m in passed)

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_none_is_single_turn_no_history_no_emit(self, mock_hook_cls, make_mock_run_result):
        """Default message_history=None passes no history and pushes no transcript XCom."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="run", llm_conn_id="c")
        context = _make_context()
        op.execute(context=context)

        assert "message_history" not in mock_agent.run_sync.call_args.kwargs
        # The transcript is not emitted without history, but run id + usage always are.
        pushed_keys = {c.kwargs["key"] for c in context["task_instance"].xcom_push.call_args_list}
        assert "message_history" not in pushed_keys
        assert pushed_keys == {"run_id", "usage"}

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_transcript_emitted_to_xcom_when_history_set(self, mock_hook_cls, make_mock_run_result):
        """When message_history is set, the post-run transcript is pushed to XCom and round-trips."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result)
        mock_agent.run_sync.return_value.all_messages.return_value = _sample_history()
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="run", llm_conn_id="c", message_history=[])
        context = _make_context()
        op.execute(context=context)

        ti = context["task_instance"]
        pushes = {c.kwargs["key"]: c.kwargs["value"] for c in ti.xcom_push.call_args_list}
        assert set(pushes) == {"run_id", "usage", "message_history"}
        restored = ModelMessagesTypeAdapter.validate_json(pushes["message_history"])
        assert len(restored) == 2

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_usage_limits_still_forwarded_with_history(self, mock_hook_cls, make_mock_run_result):
        """Adding message_history does not drop usage_limits from the run_sync call."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        limits = UsageLimits(request_limit=2)
        op = AgentOperator(
            task_id="t", prompt="run", llm_conn_id="c", usage_limits=limits, message_history=[]
        )
        op.execute(context=MagicMock())

        kwargs = mock_agent.run_sync.call_args.kwargs
        assert kwargs["usage_limits"] is limits
        assert kwargs["message_history"] == []

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
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
    def test_durable_path_also_seeds_message_history(
        self, mock_hook_cls, mock_build_storage, mock_infer, _, make_mock_run_result
    ):
        """The durable branch forwards message_history into the cached run too."""
        from airflow.providers.common.ai.durable.base import DurableStorageProtocol

        mock_build_storage.return_value = MagicMock(spec=DurableStorageProtocol)

        mock_agent = MagicMock(spec=["run_sync", "model", "override", "instrument"])
        mock_agent.run_sync.return_value = make_mock_run_result("ok")
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


class TestAgentOperatorCancellation:
    """A killed run raises RunCancelled and propagates to fail the task."""

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_run_cancelled_propagates_without_emitting_history(self, mock_hook_cls):
        """RunCancelled is not swallowed, and no partial transcript is salvaged to XCom even for a
        message_history session: a retry clears the TI's XCom before it starts, so nothing reads it."""
        from pydantic_ai import RunCancelled

        mock_agent = MagicMock(spec=["run_sync", "instrument"])
        mock_agent.run_sync.side_effect = RunCancelled("killed", messages=_sample_history())
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="run", llm_conn_id="c", message_history=[])
        context = _make_context()
        with pytest.raises(RunCancelled):
            op.execute(context=context)

        pushed_keys = {c.kwargs["key"] for c in context["task_instance"].xcom_push.call_args_list}
        assert "message_history" not in pushed_keys


class TestAgentOperatorHITLArgumentChecks:
    """The order in which __init__ reports conflicting HITL arguments."""

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
    def test_durable_with_hitl_review_raises(self):
        """Durable replay cannot be combined with HITL review."""
        with pytest.raises(ValueError, match="durable=True and enable_hitl_review"):
            AgentOperator(task_id="t", prompt="run", llm_conn_id="c", durable=True, enable_hitl_review=True)

    @pytest.mark.parametrize(
        "conflicting_kwargs",
        [
            pytest.param({"message_history": []}, id="message_history"),
            pytest.param({"durable": True}, id="durable"),
        ],
    )
    @patch("airflow.providers.common.ai.operators.agent.AIRFLOW_V_3_1_PLUS", False)
    def test_version_gate_reported_before_combination_errors(self, conflicting_kwargs):
        """On a core older than 3.1 the core version is the blocker, so it is what is reported.

        Dropping the conflicting argument would not make the operator work there, so reporting
        the combination error first sends the user to the wrong knob. This ordering is also why
        the combination tests above carry a 3.1 skipif: on an older core they raise this instead.
        """
        with pytest.raises(AirflowOptionalProviderFeatureException, match="Airflow 3.1"):
            AgentOperator(
                task_id="t",
                prompt="run",
                llm_conn_id="c",
                enable_hitl_review=True,
                **conflicting_kwargs,
            )


class _NoopBackend(SandboxBackend):
    """A backend that is never reached: these tests stop at the operator's constructor."""

    name = "noop"

    def create(self, *, spec=None):
        raise AssertionError("constructor guards must not provision")

    def run_command(self, sandbox, command, *, timeout, max_output_bytes):
        raise AssertionError("constructor guards must not run commands")

    def destroy(self, sandbox):
        pass


def _sandbox_toolset():
    return SandboxToolset(_NoopBackend())


class TestAgentOperatorSandboxContinuityGuards:
    """
    A sandbox is destroyed when the run ends, so features that replay or rerun against
    it are refused up front rather than producing answers about files that are gone.
    """

    @pytest.mark.parametrize(
        "toolsets",
        [
            pytest.param([_sandbox_toolset()], id="direct"),
            pytest.param([_sandbox_toolset().prefixed("box")], id="prefixed"),
            pytest.param([_sandbox_toolset().filtered(lambda ctx, tool: True)], id="filtered"),
            pytest.param([CombinedToolset([FunctionToolset(), _sandbox_toolset()])], id="combined"),
            pytest.param(
                [CombinedToolset([FunctionToolset(), _sandbox_toolset().prefixed("a")]).prefixed("b")],
                id="nested_twice",
            ),
        ],
    )
    def test_durable_with_a_sandbox_toolset_is_rejected_wherever_it_hides(self, toolsets):
        with pytest.raises(ValueError, match="durable=True cannot be used with a SandboxToolset"):
            AgentOperator(task_id="t", prompt="p", llm_conn_id="c", durable=True, toolsets=toolsets)

    def test_durable_with_a_sandbox_in_a_toolset_capability_is_rejected(self):
        """Tools reaching the agent through ``capabilities=[Toolset(...)]`` are durably cached
        too, so the same replay-against-nothing applies to them."""
        with pytest.raises(ValueError, match="durable=True cannot be used with a SandboxToolset"):
            AgentOperator(
                task_id="t",
                prompt="p",
                llm_conn_id="c",
                durable=True,
                agent_params={"capabilities": [Toolset(_sandbox_toolset())]},
            )

    def test_a_callable_toolset_capability_cannot_be_inspected_and_passes(self):
        """A factory resolved per run has no concrete toolset to look inside at parse time."""
        op = AgentOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            durable=True,
            agent_params={"capabilities": [Toolset(lambda ctx: _sandbox_toolset())]},
        )
        assert op.durable is True

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
    )
    def test_hitl_review_with_a_sandbox_toolset_is_rejected(self):
        with pytest.raises(ValueError, match="enable_hitl_review=True cannot be used with a SandboxToolset"):
            AgentOperator(
                task_id="t",
                prompt="p",
                llm_conn_id="c",
                enable_hitl_review=True,
                toolsets=[_sandbox_toolset().prefixed("box")],
            )

    def test_the_message_names_the_ways_out(self):
        with pytest.raises(ValueError, match="Drop durable=True, or move the sandbox work into its own task"):
            AgentOperator(
                task_id="t", prompt="p", llm_conn_id="c", durable=True, toolsets=[_sandbox_toolset()]
            )

    @pytest.mark.parametrize("flag", [{"durable": True}, {"enable_hitl_review": True}])
    def test_the_flags_stay_usable_without_a_sandbox(self, flag):
        if "enable_hitl_review" in flag and not AIRFLOW_V_3_1_PLUS:
            pytest.skip("Human in the loop is only compatible with Airflow >= 3.1.0")
        op = AgentOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            toolsets=[FunctionToolset().prefixed("fn"), CombinedToolset([FunctionToolset()])],
            **flag,
        )
        assert op.toolsets is not None

    def test_a_sandbox_toolset_without_either_flag_is_fine(self):
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="c", toolsets=[_sandbox_toolset()])
        assert isinstance(op.toolsets[0], SandboxToolset)


class TestAgentOperatorRunIdentity:
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_run_id_and_usage_pushed_to_xcom(self, mock_hook_cls, make_mock_run_result):
        """The pydantic-ai run id and token usage are exposed on XCom for downstream tasks."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result)
        mock_agent.run_sync.return_value.run_id = "the-run-id"
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="run", llm_conn_id="c")
        context = _make_context()
        op.execute(context=context)

        pushes = {
            c.kwargs["key"]: c.kwargs["value"] for c in context["task_instance"].xcom_push.call_args_list
        }
        assert pushes["run_id"] == "the-run-id"
        assert pushes["usage"] == {
            "requests": 1,
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "tool_calls": 0,
            "cost": None,
        }

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_usage_cost_is_stringified_on_xcom(self, mock_hook_cls, make_mock_run_result):
        """A non-None run cost (Decimal) is stringified before it goes to XCom."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result, cost=PRICED_COST)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="run", llm_conn_id="c")
        context = _make_context()
        op.execute(context=context)

        pushes = {
            c.kwargs["key"]: c.kwargs["value"] for c in context["task_instance"].xcom_push.call_args_list
        }
        assert pushes["usage"]["cost"] == str(PRICED_COST)

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_run_metadata_not_pushed_when_do_xcom_push_false(self, mock_hook_cls, make_mock_run_result):
        """do_xcom_push=False suppresses the run_id/usage pushes like any other operator XCom."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="run", llm_conn_id="c", do_xcom_push=False)
        context = _make_context()
        op.execute(context=context)

        pushed_keys = {c.kwargs["key"] for c in context["task_instance"].xcom_push.call_args_list}
        assert "run_id" not in pushed_keys
        assert "usage" not in pushed_keys

    @patch("airflow.providers.common.ai.operators.agent.stamp_identity_on_agent_spans", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_stamps_identity_on_agent_spans(self, mock_hook_cls, mock_stamp, make_mock_run_result):
        """execute() derives the identity from the task instance and stamps it on the agent's spans."""
        mock_agent = _make_mock_agent("ok", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="run", llm_conn_id="c")
        op.execute(context=_make_context(_make_ti(id="ti-9")))

        mock_stamp.assert_called_once()
        agent_arg, attrs = mock_stamp.call_args.args
        assert agent_arg is mock_agent
        assert attrs["airflow.task_instance.id"] == "ti-9"

    @patch("airflow.providers.common.ai.operators.agent.stamp_identity_on_agent_spans", autospec=True)
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_regenerate_with_feedback_stamps_identity(self, mock_hook_cls, mock_stamp, make_mock_run_result):
        """A HITL re-run stamps the same identity the initial run resolved."""
        mock_agent = _make_mock_agent("revised", make_mock_run_result)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(task_id="t", prompt="run", llm_conn_id="c")
        op._run_identity_attrs = {"airflow.task_instance.id": "ti-9"}
        op.regenerate_with_feedback(feedback="more", message_history=[])

        mock_stamp.assert_called_once_with(mock_agent, {"airflow.task_instance.id": "ti-9"})
