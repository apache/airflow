#
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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.snowflake.hooks.snowflake_cortex_agent import SnowflakeCortexAgentHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class SnowflakeCortexAgentOperator(BaseOperator):
    """
    Execute a Snowflake Cortex Agent.

    :param database: Database containing the Cortex Agent.
    :param schema: Schema containing the Cortex Agent.
    :param agent_name: Name of the Cortex Agent.
    :param messages: Conversation messages to send to the agent.
    :param thread_id: Existing conversation thread identifier. Optional.
        Defaults to ``None``.
    :param parent_message_id: Parent message identifier within the specified
        thread. Required when ``thread_id`` is provided. Defaults to ``None``.
    :param tool_choice: Tool selection configuration. Optional. Defaults to
        ``None``.
    :param models: Model configuration. Optional. Defaults to ``None``.
    :param instructions: Agent instruction overrides. Optional. Defaults to
        ``None``.
    :param orchestration: Orchestration configuration. Optional. Defaults to
        ``None``.
    :param tools: Additional tools available to the agent. Optional. Defaults
        to ``None``.
    :param tool_resources: Configuration for tools specified in ``tools``.
        Optional. Defaults to ``None``.
    :param timeout: Maximum time in seconds to wait for the request to
        complete. Defaults to ``600``.
    :param snowflake_conn_id: Snowflake connection ID. Defaults to
        ``snowflake_default``.
    """

    template_fields: Sequence[str] = (
        "database",
        "schema",
        "agent_name",
        "messages",
    )

    template_fields_renderers = {
        "messages": "json",
    }

    ui_color = "#29B5E8"

    def __init__(
        self,
        *,
        database: str,
        schema: str,
        agent_name: str,
        messages: list[dict[str, Any]],
        thread_id: int | None = None,
        parent_message_id: int | None = None,
        tool_choice: dict[str, Any] | None = None,
        models: dict[str, Any] | None = None,
        instructions: dict[str, Any] | None = None,
        orchestration: dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_resources: dict[str, Any] | None = None,
        timeout: int | None = 600,
        snowflake_conn_id: str = "snowflake_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.database = database
        self.schema = schema
        self.agent_name = agent_name
        self.messages = messages
        self.thread_id = thread_id
        self.parent_message_id = parent_message_id
        self.tool_choice = tool_choice
        self.models = models
        self.instructions = instructions
        self.orchestration = orchestration
        self.tools = tools
        self.tool_resources = tool_resources
        self.timeout = timeout
        self.snowflake_conn_id = snowflake_conn_id

    @cached_property
    def hook(self) -> SnowflakeCortexAgentHook:
        """Return the Snowflake Cortex Agent hook."""
        return SnowflakeCortexAgentHook(
            snowflake_conn_id=self.snowflake_conn_id,
        )

    def execute(self, context: Context) -> dict[str, Any]:
        """Execute the Snowflake Cortex Agent."""
        self.log.info(
            "Executing Snowflake Cortex Agent '%s.%s.%s'.",
            self.database,
            self.schema,
            self.agent_name,
        )

        return self.hook.run_agent(
            database=self.database,
            schema=self.schema,
            agent_name=self.agent_name,
            messages=self.messages,
            thread_id=self.thread_id,
            parent_message_id=self.parent_message_id,
            tool_choice=self.tool_choice,
            models=self.models,
            instructions=self.instructions,
            orchestration=self.orchestration,
            tools=self.tools,
            tool_resources=self.tool_resources,
            timeout=self.timeout,
        )
