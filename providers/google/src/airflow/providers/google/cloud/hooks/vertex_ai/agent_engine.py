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
"""This module contains a Google Cloud Vertex AI Agent Engine hook."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import google.auth.transport.requests
from asgiref.sync import sync_to_async
from google.api_core.exceptions import InvalidArgument, NotFound, PermissionDenied, Unauthenticated
from google.cloud.aiplatform_v1 import ReasoningEngineExecutionServiceClient
from google.cloud.aiplatform_v1.types import QueryReasoningEngineResponse
from vertexai import Client

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)

if TYPE_CHECKING:
    from google.api_core.retry import Retry
    from vertexai._genai import types

    from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
    from airflow.providers.common.ai.managed_agents.base import (
        BaseManagedAgentHook,
        ManagedAgentCapabilities,
        ManagedAgentRef,
        ManagedAgentRequest,
        ManagedAgentResponse,
    )
else:
    try:
        from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
        from airflow.providers.common.ai.managed_agents.base import (
            BaseManagedAgentHook,
            ManagedAgentCapabilities,
            ManagedAgentRef,
            ManagedAgentResponse,
        )
    except ImportError:
        # The Common AI provider is optional. This module still imports without it, and every
        # managed-agent entry point on AgentEngineHook then says what is missing.
        def _needs_common_ai(*args: Any, **kwargs: Any) -> Any:
            raise AirflowOptionalProviderFeatureException(
                "Consulting an Agent Engine as a managed agent needs the 'common.ai' extra of the "
                "google provider: pip install 'apache-airflow-providers-google[common.ai]'."
            )

        class BaseManagedAgentHook:
            """Stand-in for the Common AI contract base; ``agent()`` names the missing extra."""

            agent = _needs_common_ai

        ManagedAgentCapabilities = ManagedAgentRef = ManagedAgentResponse = _needs_common_ai
        ManagedAgentInvocationError = _needs_common_ai


VERTEX_AI_AGENT_ENGINE_API_VERSION = "v1beta1"
VERTEX_AI_AGENT_ENGINE_OPERATION_URL = (
    "https://{location}-aiplatform.googleapis.com/{api_version}/{operation_name}"
)
DEFAULT_AGENT_ENGINE_OPERATION_REQUEST_TIMEOUT = 60.0


def extract_operation_id(operation_name: str) -> str:
    """Extract the operation ID from a fully qualified operation name."""
    return operation_name.rstrip("/").split("/")[-1]


def serialize_value(value: Any) -> Any:
    """Recursively convert SDK model objects to JSON-serializable types."""
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {key: serialize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [serialize_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(serialize_value(item) for item in value)
    return value


# ``vendor_options`` of a managed-agent request. ``class_method`` and ``input_key`` shape the
# request; ``retry`` and ``metadata`` reach ``query_reasoning_engine``, which accepts exactly those.
_AGENT_OPTIONS = frozenset({"class_method", "input_key", "retry", "metadata"})


def _agent_output_text(output: Any) -> str:
    if output is None:
        return ""
    if isinstance(output, str):
        return output
    if isinstance(output, dict) and isinstance(output.get("output"), str):
        return output["output"]
    return json.dumps(output, default=str)


class AgentEngineHook(GoogleBaseHook, BaseManagedAgentHook):
    """
    Hook for Google Cloud Vertex AI Agent Engine APIs.

    Wraps the ``agent_engines`` module of the Vertex AI SDK client and the
    Reasoning Engine Execution Service GAPIC client:
    https://docs.cloud.google.com/python/docs/reference/agentplatform/latest/vertexai._genai.agent_engines.AgentEngines

    With the ``common.ai`` extra installed, the hook also implements the Common AI
    managed-agent contract over the synchronous query path, so ``hook.agent(resource_name)``
    can be handed to a ``ManagedAgentToolset``. The agent is the engine's full resource name,
    ``projects/P/locations/L/reasoningEngines/ID``, so a single hook on one connection reaches
    engines in several projects and regions. A request carrying a ``prompt`` is sent as
    ``{input_key: prompt}`` to ``class_method`` (``query`` and ``input`` by default; both can be
    set per request in ``vendor_options``, alongside ``retry`` and ``metadata``); a request
    carrying ``messages`` is sent as ``{"messages": [...]}``. When the engine returns a mapping
    with a string ``output``, that is the answer text; any other output is returned as JSON text
    and kept on ``ManagedAgentResponse.structured``. The wire type is a protobuf ``Value``, so
    integers come back as floats and an absent, null or empty output all read as an empty string.

    Agent Engine reports an author-side mistake, such as an unknown ``class_method`` or an
    input under the wrong key, as ``INVALID_ARGUMENT``. That is a configuration error, not
    something a model rephrase could fix, so it is terminal here; the hook never raises
    :class:`~airflow.providers.common.ai.exceptions.ManagedAgentRejected`. The query path keeps
    no conversation state, so a request with a ``session_id`` is refused rather than silently
    sent as a fresh call. Query *jobs* remain the domain of
    :class:`~airflow.providers.google.cloud.operators.vertex_ai.agent_engine.RunQueryJobOperator`,
    which can defer.

    .. code-block:: python

        from airflow.providers.common.ai.toolsets import ManagedAgentToolset
        from airflow.providers.google.cloud.hooks.vertex_ai.agent_engine import AgentEngineHook

        analyst = AgentEngineHook(gcp_conn_id="google_cloud_default").agent(
            "projects/my-project/locations/us-central1/reasoningEngines/1234567890"
        )
        toolset = ManagedAgentToolset(analyst, tool_name="ask_analyst", description="...")
    """

    agent_platform = "gcp.vertex_agent_engine"

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )
        # One execution client per location for managed-agent calls: a GAPIC client opens its gRPC
        # channel in __init__, and a model may consult the same agent several times in one run.
        self._agent_clients: dict[str, ReasoningEngineExecutionServiceClient] = {}

    @staticmethod
    def _parse_agent(agent: str) -> tuple[str, str, str]:
        parts = ReasoningEngineExecutionServiceClient.parse_reasoning_engine_path(agent)
        # The SDK parser is non-greedy, so a child resource such as an operation name still matches.
        if not parts or "/" in parts["reasoning_engine"]:
            raise ValueError(
                f"An Agent Engine agent is its full resource name projects/P/locations/L/reasoningEngines/ID, "
                f"got {agent!r}."
            )
        return parts["project"], parts["location"], parts["reasoning_engine"]

    def resolve_agent(self, agent: str) -> ManagedAgentRef:
        self._parse_agent(agent)
        return ManagedAgentRef(platform=self.agent_platform, name=agent)

    def agent_capabilities(self, agent: str) -> ManagedAgentCapabilities:
        return ManagedAgentCapabilities(structured_output=True)

    def invoke_agent(self, agent: str, request: ManagedAgentRequest) -> ManagedAgentResponse:
        _, location, _ = self._parse_agent(agent)
        if request.session_id is not None:
            raise ValueError(
                "Agent Engine's query path keeps no conversation state; session_id is not supported."
            )
        unknown = set(request.vendor_options) - _AGENT_OPTIONS
        if unknown:
            raise ValueError(
                f"vendor_options {sorted(unknown)} are not accepted; an Agent Engine request takes "
                f"{sorted(_AGENT_OPTIONS)}."
            )
        options = dict(request.vendor_options)
        class_method = options.pop("class_method", "query")
        input_key = options.pop("input_key", "input")
        if not isinstance(class_method, str) or not isinstance(input_key, str):
            raise ValueError(
                "vendor_options['class_method'] and vendor_options['input_key'] must be strings."
            )
        input_data = (
            {input_key: request.prompt} if request.prompt is not None else {"messages": request.as_messages()}
        )
        client = self._agent_clients.get(location)
        if client is None:
            client = self._agent_clients[location] = self.get_reasoning_engine_execution_service_client(
                location
            )
        try:
            response = client.query_reasoning_engine(
                request={"name": agent, "class_method": class_method, "input": input_data},
                retry=options.get("retry"),
                timeout=request.timeout,
                metadata=options.get("metadata", ()),
            )
        except InvalidArgument as exc:
            raise ManagedAgentInvocationError(
                f"Agent Engine {agent} rejected the request (class_method={class_method!r}, "
                f"input_key={input_key!r}): {exc}"
            ) from exc
        except (NotFound, PermissionDenied, Unauthenticated) as exc:
            raise ManagedAgentInvocationError(f"Agent Engine {agent}: {exc}") from exc
        raw = QueryReasoningEngineResponse.to_dict(response)
        output = raw.get("output")
        return ManagedAgentResponse(
            text=_agent_output_text(output),
            raw=raw,
            structured=None if isinstance(output, str) else output,
        )

    def get_agent_engine_client(self, project_id: str, location: str):
        """Return the Vertex AI Agent Engine client."""
        return Client(
            project=project_id,
            location=location,
            credentials=self.get_credentials(),
        ).agent_engines

    def _get_api_endpoint(self, location: str | None = None) -> str | None:
        if location and location != "global" and self.is_default_universe():
            return f"{location}-aiplatform.googleapis.com:443"
        return None

    def get_reasoning_engine_execution_service_client(
        self, location: str | None = None
    ) -> ReasoningEngineExecutionServiceClient:
        """Return the Reasoning Engine Execution Service client."""
        return ReasoningEngineExecutionServiceClient(
            credentials=self.get_credentials(),
            client_info=CLIENT_INFO,
            client_options=self.get_client_options(
                api_endpoint_override=self._get_api_endpoint(location=location)
            ),
        )

    @staticmethod
    def build_agent_engine_name(project_id: str, location: str, agent_engine_id: str) -> str:
        """Build a fully qualified Agent Engine resource name."""
        return f"projects/{project_id}/locations/{location}/reasoningEngines/{agent_engine_id}"

    @staticmethod
    def build_operation_name(project_id: str, location: str, operation_id: str) -> str:
        """Build a fully qualified Agent Engine operation name."""
        return f"projects/{project_id}/locations/{location}/operations/{operation_id}"

    @GoogleBaseHook.fallback_to_default_project_id
    def create_agent_engine(
        self,
        location: str,
        agent: Any | None = None,
        config: types.AgentEngineConfigOrDict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.AgentEngine:
        """
        Create an Agent Engine.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param agent: Optional. The agent object to deploy.
        :param config: Optional. Configuration for the Agent Engine.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        return client.create(agent=agent, config=config)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_agent_engine(
        self,
        location: str,
        agent_engine_id: str,
        config: types.GetAgentEngineConfigOrDict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.AgentEngine:
        """
        Get an Agent Engine.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param agent_engine_id: Required. The Agent Engine ID.
        :param config: Optional. Configuration for getting the Agent Engine.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        name = self.build_agent_engine_name(project_id, location, agent_engine_id)
        return client.get(name=name, config=config)

    @GoogleBaseHook.fallback_to_default_project_id
    def query_reasoning_engine(
        self,
        location: str,
        reasoning_engine_id: str,
        input_data: dict[str, Any] | None = None,
        class_method: str = "query",
        retry: Retry | None = None,
        timeout: float | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> QueryReasoningEngineResponse:
        """
        Query a Reasoning Engine synchronously.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param reasoning_engine_id: Required. The Reasoning Engine resource ID.
        :param input_data: Optional. Input for the Reasoning Engine class method in JSON object format.
            Defaults to ``None``.
        :param class_method: Optional. The Reasoning Engine class method to invoke. Defaults to ``query``.
        :param retry: Designation of what errors, if any, should be retried. Defaults to ``None``.
        :param timeout: The timeout for this request. Defaults to ``None``.
        :param metadata: Strings which should be sent along with the request as metadata. Defaults
            to an empty tuple.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        client = self.get_reasoning_engine_execution_service_client(location=location)
        name = client.reasoning_engine_path(project_id, location, reasoning_engine_id)
        request: dict[str, Any] = {"name": name, "class_method": class_method}
        if input_data is not None:
            request["input"] = input_data
        return client.query_reasoning_engine(
            request=request,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def run_query_job(
        self,
        location: str,
        agent_engine_id: str,
        config: types.RunQueryJobAgentEngineConfigOrDict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.RunQueryJobResult:
        """
        Run a query job on an Agent Engine.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param agent_engine_id: Required. The Agent Engine ID.
        :param config: Optional. Configuration for the query job (``query``, ``output_gcs_uri``).
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        name = self.build_agent_engine_name(project_id, location, agent_engine_id)
        return client.run_query_job(name=name, config=config)

    @GoogleBaseHook.fallback_to_default_project_id
    def check_query_agent_engine_job(
        self,
        location: str,
        operation_id: str,
        config: types.CheckQueryJobAgentEngineConfigOrDict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.CheckQueryJobResult:
        """
        Check a query job on an Agent Engine.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param operation_id: Required. The query job operation ID.
        :param config: Optional. Configuration for checking the query job.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        operation_name = self.build_operation_name(project_id, location, operation_id)
        return client.check_query_job(name=operation_name, config=config)

    @GoogleBaseHook.fallback_to_default_project_id
    def wait_for_query_agent_engine_job(
        self,
        location: str,
        operation_id: str,
        config: types.CheckQueryJobAgentEngineConfigOrDict | None = None,
        poll_interval: float = 30,
        timeout: float | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.CheckQueryJobResult:
        """
        Wait until an Agent Engine query job completes.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param operation_id: Required. The query job operation ID.
        :param config: Optional. Configuration for checking the query job.
        :param poll_interval: Time, in seconds, to wait between checks.
        :param timeout: Optional timeout, in seconds.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        start_time = time.monotonic()
        operation_name = self.build_operation_name(project_id, location, operation_id)
        while True:
            query_job = self.check_query_agent_engine_job(
                project_id=project_id,
                location=location,
                operation_id=operation_id,
                config=config,
            )
            status = getattr(query_job, "status", None)
            if status == "SUCCESS":
                return query_job
            if status == "FAILED":
                raise RuntimeError(f"Agent Engine query job {operation_name} failed.")
            if status not in (None, "RUNNING"):
                raise RuntimeError(
                    f"Agent Engine query job {operation_name} completed with unexpected status {status}."
                )
            if timeout is not None and time.monotonic() - start_time >= timeout:
                raise TimeoutError(f"Timed out waiting for Agent Engine query job {operation_name}")
            self.log.info("Waiting for Agent Engine query job %s to complete.", operation_name)
            time.sleep(poll_interval)

    @GoogleBaseHook.fallback_to_default_project_id
    def update_agent_engine(
        self,
        location: str,
        agent_engine_id: str,
        config: types.AgentEngineConfigOrDict,
        agent: Any | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.AgentEngine:
        """
        Update an Agent Engine.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param agent_engine_id: Required. The Agent Engine ID.
        :param config: Required. Configuration for the Agent Engine update.
        :param agent: Optional. The updated agent object to deploy.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        name = self.build_agent_engine_name(project_id, location, agent_engine_id)
        return client.update(name=name, agent=agent, config=config)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_agent_engine(
        self,
        location: str,
        agent_engine_id: str,
        force: bool | None = None,
        config: types.DeleteAgentEngineConfigOrDict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.DeleteAgentEngineOperation:
        """
        Delete an Agent Engine.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param agent_engine_id: Required. The Agent Engine ID.
        :param force: Optional. Whether to forcefully delete child resources. Defaults to ``False``
            when not specified.
        :param config: Optional. Additional deletion configuration.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        client = self.get_agent_engine_client(project_id=project_id, location=location)
        name = self.build_agent_engine_name(project_id, location, agent_engine_id)
        return client.delete(name=name, force=force, config=config)

    @GoogleBaseHook.fallback_to_default_project_id
    def get_agent_engine_operation(
        self,
        location: str,
        operation_id: str,
        request_timeout: float | None = DEFAULT_AGENT_ENGINE_OPERATION_REQUEST_TIMEOUT,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> dict[str, Any]:
        """
        Return a Vertex AI Agent Engine long-running operation.

        :param location: The ID of the Google Cloud location that the service belongs to.
        :param operation_id: The Agent Engine operation ID.
        :param request_timeout: Optional timeout, in seconds, for the operation request.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        operation_name = self.build_operation_name(project_id, location, operation_id)
        url = VERTEX_AI_AGENT_ENGINE_OPERATION_URL.format(
            location=location,
            api_version=VERTEX_AI_AGENT_ENGINE_API_VERSION,
            operation_name=operation_name,
        )
        session = google.auth.transport.requests.AuthorizedSession(self.get_credentials())
        response = session.get(url, timeout=request_timeout)
        response.raise_for_status()
        return response.json()

    @GoogleBaseHook.fallback_to_default_project_id
    def wait_for_agent_engine_operation(
        self,
        location: str,
        operation_id: str,
        poll_interval: float = 30,
        timeout: float | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> None:
        """
        Wait until an Agent Engine operation completes.

        :param location: The ID of the Google Cloud location that the service belongs to.
        :param operation_id: The Agent Engine operation ID.
        :param poll_interval: Time, in seconds, to wait between checks.
        :param timeout: Optional timeout, in seconds.
        :param project_id: Optional. The ID of the Google Cloud project. Defaults to the project
            configured in the connection.
        """
        start_time = time.monotonic()
        operation_name = self.build_operation_name(project_id, location, operation_id)
        while True:
            operation = self.get_agent_engine_operation(
                project_id=project_id,
                location=location,
                operation_id=operation_id,
            )
            if operation.get("done"):
                if operation.get("error"):
                    raise RuntimeError(
                        f"Agent Engine operation {operation_name} failed: {operation['error']}"
                    )
                return
            if timeout is not None and time.monotonic() - start_time >= timeout:
                raise TimeoutError(f"Timed out waiting for Agent Engine operation {operation_name}")
            self.log.info("Waiting for Agent Engine operation %s to complete.", operation_name)
            time.sleep(poll_interval)


class AgentEngineAsyncHook(GoogleBaseAsyncHook):
    """Async hook for Google Cloud Vertex AI Agent Engine APIs."""

    sync_hook_class = AgentEngineHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    async def check_query_agent_engine_job(
        self,
        location: str,
        operation_id: str,
        config: types.CheckQueryJobAgentEngineConfigOrDict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> types.CheckQueryJobResult:
        """Check a query job on an Agent Engine."""
        sync_hook = await self.get_sync_hook()
        return await sync_to_async(sync_hook.check_query_agent_engine_job)(
            project_id=project_id,
            location=location,
            operation_id=operation_id,
            config=config,
        )
