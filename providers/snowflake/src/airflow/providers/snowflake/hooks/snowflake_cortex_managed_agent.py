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
"""Expose Snowflake Cortex Agents through the Common AI managed-agent contract."""

from __future__ import annotations

import json
from typing import Any

import requests

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.snowflake.hooks.snowflake_cortex_agent import SnowflakeCortexAgentHook

try:
    from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
    from airflow.providers.common.ai.managed_agents.contract import (
        BaseManagedAgentHook,
        ManagedAgentCapabilities,
        ManagedAgentRef,
        ManagedAgentRequest,
        ManagedAgentResponse,
    )
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "This feature requires the 'common.ai' provider, in a version that ships "
        "airflow.providers.common.ai.managed_agents."
    )

# The optional payload fields run_agent() accepts, beyond messages/thread_id/parent_message_id
# (which this contract does not expose -- see the class docstring).
_ALLOWED_VENDOR_OPTIONS = frozenset(
    {"tool_choice", "models", "instructions", "orchestration", "tools", "tool_resources"}
)
# Retryable at the HTTP layer; Airflow's task-level retry is the right layer for these, not a
# terminal ManagedAgentInvocationError.
_RETRYABLE_STATUS_CODES = frozenset({408, 429})


class SnowflakeCortexManagedAgentHook(SnowflakeCortexAgentHook, BaseManagedAgentHook):
    """
    Invoke a Snowflake Cortex Agent as a Common AI managed agent.

    The agent is ``DATABASE.SCHEMA.NAME`` (quoted identifiers containing their own ``.`` are not
    supported here -- use :meth:`~airflow.providers.snowflake.hooks.snowflake_cortex_agent.SnowflakeCortexAgentHook.run_agent`
    directly for those). A request is sent as ``request.as_messages()``, the same shape
    ``run_agent`` already accepts. This adoption does not support sessions: Cortex threads need a
    ``parent_message_id`` this contract has no field for (see :meth:`agent_capabilities`), so a
    request carrying ``session_id`` is refused rather than silently starting a fresh thread.

    ``vendor_options`` may carry ``tool_choice``, ``models``, ``instructions``, ``orchestration``,
    ``tools``, or ``tool_resources`` -- the optional payload fields ``run_agent`` accepts beyond
    messages; anything else is rejected. A 4xx response (other than 408 or 429, which are
    retryable) is raised as :class:`~airflow.providers.common.ai.exceptions.ManagedAgentInvocationError`;
    everything else propagates unchanged. This hook never raises
    :class:`~airflow.providers.common.ai.exceptions.ManagedAgentRejected`.

    .. code-block:: python

        from airflow.providers.snowflake.hooks.snowflake_cortex_managed_agent import (
            SnowflakeCortexManagedAgentHook,
        )
        from airflow.providers.common.ai.toolsets import ManagedAgentToolset

        claims = SnowflakeCortexManagedAgentHook(snowflake_conn_id="snowflake_default").agent(
            "MY_DB.MY_SCHEMA.CLAIMS_AGENT"
        )
        toolset = ManagedAgentToolset(claims, tool_name="ask_claims_agent", description="...")

    Authentication and other arguments are inherited from
    :class:`~airflow.providers.snowflake.hooks.snowflake_cortex_agent.SnowflakeCortexAgentHook`.
    """

    agent_platform = "snowflake.cortex_agent"

    def resolve_agent(self, agent: str) -> ManagedAgentRef:
        parts = agent.split(".")
        if len(parts) != 3 or not all(parts):
            raise ValueError(
                f"A Snowflake Cortex agent is DATABASE.SCHEMA.NAME, got {agent!r}. Quoted "
                "identifiers containing their own '.' are not supported."
            )
        return ManagedAgentRef(platform=self.agent_platform, name=agent)

    def agent_capabilities(self, agent: str) -> ManagedAgentCapabilities:
        return ManagedAgentCapabilities()

    def invoke_agent(self, agent: str, request: ManagedAgentRequest) -> ManagedAgentResponse:
        if request.session_id is not None:
            raise ValueError(
                "Snowflake Cortex Agents managed-agent adoption does not support session_id; a "
                "Cortex thread needs parent_message_id, which this contract has no field for."
            )
        database, schema, agent_name = agent.split(".")

        unknown = set(request.vendor_options) - _ALLOWED_VENDOR_OPTIONS
        if unknown:
            raise ValueError(
                f"vendor_options {sorted(unknown)} are not accepted; run_agent takes "
                f"{sorted(_ALLOWED_VENDOR_OPTIONS)}."
            )

        kwargs: dict[str, Any] = dict(request.vendor_options)
        if request.timeout is not None:
            kwargs["timeout"] = request.timeout

        try:
            response = self.run_agent(
                database=database,
                schema=schema,
                agent_name=agent_name,
                messages=request.as_messages(),
                **kwargs,
            )
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if (
                status_code is not None
                and 400 <= status_code < 500
                and status_code not in _RETRYABLE_STATUS_CODES
            ):
                raise ManagedAgentInvocationError(
                    f"Cortex agent {agent} via connection {self.snowflake_conn_id!r}: {exc}"
                ) from exc
            raise

        text = self.get_text_response(response)
        if not text:
            text = json.dumps(response.get("content", []), default=str)
        return ManagedAgentResponse(text=text, raw=response)
