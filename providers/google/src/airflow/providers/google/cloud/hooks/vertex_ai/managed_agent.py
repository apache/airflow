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
"""Expose Vertex AI Agent Engine deployments through the Common AI managed-agent contract."""

from __future__ import annotations

import json
from typing import Any

from google.api_core.exceptions import InvalidArgument, NotFound, PermissionDenied, Unauthenticated
from google.cloud.aiplatform_v1 import ReasoningEngineExecutionServiceClient
from google.cloud.aiplatform_v1.types import QueryReasoningEngineResponse

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.google.cloud.hooks.vertex_ai.agent_engine import AgentEngineHook

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

# ``vendor_options`` reach ``AgentEngineHook.query_reasoning_engine``, which accepts exactly these.
_ALLOWED_OPTIONS = frozenset({"class_method", "retry", "metadata"})


class AgentEngineManagedAgentHook(AgentEngineHook, BaseManagedAgentHook):
    """
    Query a Vertex AI Agent Engine deployment as a Common AI managed agent.

    The agent is the full resource name, ``projects/P/locations/L/reasoningEngines/ID``, so a
    single hook on one connection can reach engines in several projects and regions. A request
    carrying a ``prompt`` is sent as ``{input_key: prompt}`` to ``class_method`` (``query`` by
    default); a request carrying ``messages`` is sent as ``{"messages": [...]}``. When the
    engine returns a mapping with a string ``output``, that is the answer text; any other
    output is returned as JSON text and kept on ``ManagedAgentResponse.structured``. The wire
    type is a protobuf ``Value``, so integers come back as floats and an absent, null or empty
    output all read as an empty string.

    Agent Engine reports an author-side mistake, such as an unknown ``class_method`` or an
    input under the wrong key, as ``INVALID_ARGUMENT``. That is a configuration error, not
    something a model rephrase could fix, so it is terminal here; this hook never raises
    :class:`~airflow.providers.common.ai.exceptions.ManagedAgentRejected`.

    This hook wraps the synchronous query path. Agent Engine's query *jobs* remain the domain
    of :class:`~airflow.providers.google.cloud.operators.vertex_ai.agent_engine.RunQueryJobOperator`,
    which can defer.

    .. code-block:: python

        from airflow.providers.google.cloud.hooks.vertex_ai.managed_agent import AgentEngineManagedAgentHook
        from airflow.providers.common.ai.toolsets import ManagedAgentToolset

        analyst = AgentEngineManagedAgentHook(gcp_conn_id="google_cloud_default").agent(
            "projects/my-project/locations/us-central1/reasoningEngines/1234567890"
        )
        toolset = ManagedAgentToolset(analyst, tool_name="ask_analyst", description="...")

    ``vendor_options`` may carry ``class_method`` (per request), ``retry`` and ``metadata``, the
    arguments ``query_reasoning_engine`` accepts; anything else is rejected. Agent Engine keeps no
    conversation state on this path, so a request with a ``session_id`` is refused rather than
    silently sent as a fresh call.

    :param class_method: The engine class method a prompt is sent to. Default ``query``.
    :param input_key: The key a prompt is sent under. Default ``input``.

    Additional arguments (such as ``gcp_conn_id`` and ``impersonation_chain``) are passed down
    to :class:`~airflow.providers.google.cloud.hooks.vertex_ai.agent_engine.AgentEngineHook`.
    """

    agent_platform = "gcp.vertex_agent_engine"

    def __init__(
        self, *args: Any, class_method: str = "query", input_key: str = "input", **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self.class_method = class_method
        self.input_key = input_key

    @staticmethod
    def _parse(agent: str) -> tuple[str, str, str]:
        parts = ReasoningEngineExecutionServiceClient.parse_reasoning_engine_path(agent)
        # The SDK parser is non-greedy, so a child resource such as an operation name still matches.
        if not parts or "/" in parts["reasoning_engine"]:
            raise ValueError(
                f"An Agent Engine agent is its full resource name projects/P/locations/L/reasoningEngines/ID, "
                f"got {agent!r}."
            )
        return parts["project"], parts["location"], parts["reasoning_engine"]

    def resolve_agent(self, agent: str) -> ManagedAgentRef:
        self._parse(agent)
        return ManagedAgentRef(platform=self.agent_platform, name=agent)

    def agent_capabilities(self, agent: str) -> ManagedAgentCapabilities:
        return ManagedAgentCapabilities(structured_output=True)

    def invoke_agent(self, agent: str, request: ManagedAgentRequest) -> ManagedAgentResponse:
        project_id, location, engine_id = self._parse(agent)
        if request.session_id is not None:
            raise ValueError(
                "Agent Engine's query path keeps no conversation state; session_id is not supported."
            )
        unknown = set(request.vendor_options) - _ALLOWED_OPTIONS
        if unknown:
            raise ValueError(
                f"vendor_options {sorted(unknown)} are not accepted; query_reasoning_engine takes {sorted(_ALLOWED_OPTIONS)}."
            )
        options = dict(request.vendor_options)
        class_method = options.pop("class_method", self.class_method)
        input_data = (
            {self.input_key: request.prompt}
            if request.prompt is not None
            else {"messages": request.as_messages()}
        )
        try:
            response = self.query_reasoning_engine(
                project_id=project_id,
                location=location,
                reasoning_engine_id=engine_id,
                input_data=input_data,
                class_method=class_method,
                timeout=request.timeout,
                **options,
            )
        except InvalidArgument as exc:
            raise ManagedAgentInvocationError(
                f"Agent Engine {agent} rejected the request (class_method={class_method!r}, "
                f"input_key={self.input_key!r}): {exc}"
            ) from exc
        except (NotFound, PermissionDenied, Unauthenticated) as exc:
            raise ManagedAgentInvocationError(f"Agent Engine {agent}: {exc}") from exc
        raw = QueryReasoningEngineResponse.to_dict(response)
        output = raw.get("output")
        return ManagedAgentResponse(
            text=_text(output),
            raw=raw,
            structured=None if isinstance(output, str) else output,
        )


def _text(output: Any) -> str:
    if output is None:
        return ""
    if isinstance(output, str):
        return output
    if isinstance(output, dict) and isinstance(output.get("output"), str):
        return output["output"]
    return json.dumps(output, default=str)
