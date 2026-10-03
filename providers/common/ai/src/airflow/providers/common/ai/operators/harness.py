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
"""Operator for running a vendor's own agent loop (a "native harness") in-process."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.harness.base import HarnessRequest, HarnessRunError
from airflow.providers.common.ai.tools import collect_tools
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from collections.abc import Sequence

    from airflow.providers.common.ai.harness.base import HarnessBackend
    from airflow.providers.common.ai.tools import AirflowTool, ToolProvider
    from airflow.sdk import Context

__all__ = ["HarnessOperator"]


class HarnessOperator(BaseOperator):
    """
    Run a vendor's own agent loop (a "native harness") in a task.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Unlike ``AgentOperator``, which builds and drives a Pydantic AI agent itself, this
    operator hands the whole run to a :class:`~airflow.providers.common.ai.harness.base.HarnessBackend`
    -- the vendor's own loop decides how many turns to take and when to stop. Airflow's part
    is giving it tools, credentials and collecting the result. The default backend is the
    `Claude Agent SDK <https://platform.claude.com/docs/en/api/agent-sdk/overview>`__, which
    requires the ``claude-agent-sdk`` extra.

    Alongside the returned text, the run's ``session_id`` and ``usage`` (turns, cost,
    the vendor's own subtype and usage accounting) are pushed to XCom, pushed before the
    run is failed on an error result, so a downstream task or failure callback can read
    what a failed attempt spent.

    :param prompt: The prompt to send to the agent.
    :param llm_conn_id: Connection ID for the LLM provider.
    :param model_id: Model identifier. Overrides the model stored in the
        connection's extra field.
    :param system_prompt: System-level instructions for the agent.
    :param toolsets: Toolsets and individual tools the agent may call, collected the
        same way the Strands and ADK adapters do (see
        :func:`~airflow.providers.common.ai.tools.collect_tools`). Connection IDs on a
        toolset are used as written -- unlike ``AgentOperator``, they are not rendered
        as a template.
    :param max_turns: Maximum number of agent turns. ``None`` (default) leaves it to
        the backend's own default.
    :param harness: The backend to run the agent with. ``None`` (default) builds a
        :class:`~airflow.providers.common.ai.harness.claude_agent_sdk.ClaudeAgentSDKBackend`
        lazily, in :meth:`execute`, so importing this operator never requires the
        ``claude-agent-sdk`` extra to be installed.
    """

    template_fields: Sequence[str] = ("prompt", "llm_conn_id", "model_id", "system_prompt")

    def __init__(
        self,
        *,
        prompt: str,
        llm_conn_id: str,
        model_id: str | None = None,
        system_prompt: str = "",
        toolsets: Sequence[ToolProvider | AirflowTool] | None = None,
        max_turns: int | None = None,
        harness: HarnessBackend | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if max_turns is not None and max_turns <= 0:
            raise ValueError(f"max_turns must be a positive integer, got {max_turns!r}.")
        self.prompt = prompt
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self.system_prompt = system_prompt
        self.toolsets = toolsets
        self.max_turns = max_turns
        self.harness = harness

    def execute(self, context: Context) -> str | None:
        harness = self.harness
        if harness is None:
            from airflow.providers.common.ai.harness.claude_agent_sdk import ClaudeAgentSDKBackend

            harness = ClaudeAgentSDKBackend()

        tools = collect_tools(self.toolsets or ())
        request = HarnessRequest(
            prompt=self.prompt,
            llm_conn_id=self.llm_conn_id,
            model_id=self.model_id,
            system_prompt=self.system_prompt or None,
            max_turns=self.max_turns,
            tools=tools,
        )
        result = harness.run(request)

        if self.do_xcom_push:
            ti = context["task_instance"]
            ti.xcom_push(key="session_id", value=result.session_id)
            ti.xcom_push(
                key="usage",
                value={
                    "num_turns": result.num_turns,
                    "cost_usd": result.cost_usd,
                    "subtype": result.subtype,
                    "usage": result.usage,
                },
            )

        if result.is_error:
            raise HarnessRunError(
                f"{type(harness).__name__} reported the run as failed (subtype={result.subtype!r}).",
                subtype=result.subtype,
                session_id=result.session_id,
            )
        return result.output
