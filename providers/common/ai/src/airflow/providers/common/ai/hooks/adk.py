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
"""Hook for LLM access via Google Agent Development Kit (ADK)."""

from __future__ import annotations

import asyncio
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, PrivateAttr

from airflow.providers.common.ai.hooks.base import BaseAIHook

try:
    from google.adk.agents import Agent as ADKAgent, BaseAgent, LoopAgent, ParallelAgent, SequentialAgent
    from google.adk.memory import InMemoryMemoryService
    from google.adk.models.google_llm import Gemini
    from google.adk.runners import Runner as ADKRunner
    from google.adk.sessions import InMemorySessionService
    from google.genai import types as genai_types
except ImportError:
    Gemini = BaseModel  # type: ignore[assignment, misc]

if TYPE_CHECKING:
    from google.adk.sessions import BaseSessionService
    from google.genai import Client

#: Allowed values for the ``agent_type`` parameter.
AgentType = Literal["llm", "sequential", "parallel", "loop"]


def _agent_cls_for(agent_type: AgentType) -> type[BaseAgent]:
    """Resolve agent class at call-time so monkeypatching/mocking works."""
    mapping: dict[str, type] = {
        "llm": ADKAgent,
        "sequential": SequentialAgent,
        "parallel": ParallelAgent,
        "loop": LoopAgent,
    }
    cls = mapping.get(agent_type)
    if cls is None:
        raise ValueError(f"Unknown agent_type={agent_type!r}. Choose from: {', '.join(mapping)}.")
    return cls


class _GeminiWithApiKey(Gemini):  # type: ignore[misc]
    """
    Gemini variant that injects *api_key* at the Client level.

    Avoids setting ``GOOGLE_API_KEY`` in the process-wide environment, which
    would leak credentials to other tasks sharing the same worker process.
    """

    _api_key: str | None = PrivateAttr(default=None)

    @cached_property
    def api_client(self) -> Client:  # type: ignore[override]
        from google.genai import Client

        return Client(
            api_key=self._api_key,
            http_options=genai_types.HttpOptions(
                headers=self._tracking_headers(),
                retry_options=self.retry_options,
                base_url=self.base_url,
            ),
        )


class AdkHook(BaseAIHook):
    """
    Hook for LLM access via Google Agent Development Kit (ADK).

    Manages connection credentials and agent creation/execution.  Uses
    Google's ADK to orchestrate multi-turn agent interactions with Gemini
    models.

    Implements the :class:`~airflow.providers.common.ai.hooks.base.BaseAIHook`
    contract so that :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`
    can use it transparently via connection type.

    Connection fields:
        - **password**: Google API key (for Gemini API access)
        - **host**: Custom endpoint URL (optional — for custom/proxy endpoints)

    Cloud authentication via Application Default Credentials (ADC) is used
    when no API key is provided.  Configure ``GOOGLE_APPLICATION_CREDENTIALS``
    or run ``gcloud auth application-default login``.

    :param llm_conn_id: Airflow connection ID for the LLM provider.
    :param model_id: Model identifier (e.g. ``"gemini-2.5-flash"``).
        Overrides the model stored in the connection's extra field.
    :param session_service: Optional session service instance.  When set to a
        ``DatabaseSessionService`` or any custom ``BaseSessionService``, the
        runner will use it instead of the default in-memory store.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "adk_default"
    conn_type = "adk"
    hook_name = "Google ADK"

    def __init__(
        self,
        llm_conn_id: str = default_conn_name,
        model_id: str | None = None,
        session_service: BaseSessionService | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self._api_key: str | None = None
        self._session_service: BaseSessionService | None = session_service
        self._configured: bool = False

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {
                "host": "(optional — for custom/proxy endpoints)",
            },
        }

    # -----------------------------------------------------------------
    # Model resolution helpers
    # -----------------------------------------------------------------

    def get_conn(self) -> str:
        """
        Configure credentials and return the model identifier.

        Reads API key from connection password and model from (in priority
        order):

        1. ``model_id`` parameter on the hook
        2. ``extra["model"]`` on the connection

        The API key is stored on this hook instance and later injected
        directly into the ``google.genai.Client`` constructor — it is
        **never** written to the process-wide environment, preventing
        credential leakage across tasks on multi-tenant workers.

        The result is cached for the lifetime of this hook instance.

        :return: The resolved model identifier string.
        """
        if self._configured:
            return self.model_id  # type: ignore[return-value]

        if self.llm_conn_id:
            try:
                conn = self.get_connection(self.llm_conn_id)
            except Exception:
                self.log.warning("Could not retrieve connection %r, using model_id.", self.llm_conn_id)
            else:
                api_key = conn.password
                if api_key:
                    self._api_key = api_key
                model_name = self.model_id or conn.extra_dejson.get("model", "")
                if model_name:
                    self.model_id = model_name

        if not self.model_id:
            raise ValueError(
                "No model specified. Set model_id on the hook or the Model field on the connection."
            )

        self._configured = True
        return self.model_id

    def _resolve_model(self) -> str | Gemini:
        """Return a model string or a ``_GeminiWithApiKey`` with the API key baked in."""
        self.get_conn()
        if self._api_key:
            gemini = _GeminiWithApiKey(model=self.model_id)
            gemini._api_key = self._api_key
            return gemini
        return self.model_id  # type: ignore[return-value]

    # -----------------------------------------------------------------
    # Agent creation
    # -----------------------------------------------------------------

    def create_agent(
        self,
        *,
        output_type: type = str,
        instructions: str = "",
        toolsets: list | None = None,
        name: str = "airflow_agent",
        agent_type: AgentType = "llm",
        sub_agents: list[BaseAgent] | None = None,
        input_schema: type[BaseModel] | None = None,
        output_schema: type[BaseModel] | None = None,
        output_key: str | None = None,
        generate_content_config: genai_types.GenerateContentConfig | None = None,
        before_model_callback: Any | None = None,
        after_model_callback: Any | None = None,
        before_tool_callback: Any | None = None,
        after_tool_callback: Any | None = None,
        before_agent_callback: Any | None = None,
        after_agent_callback: Any | None = None,
        max_iterations: int | None = None,
        **kwargs: Any,
    ) -> BaseAgent:
        """
        Create an ADK Agent configured with this hook's model.

        Satisfies the :class:`~airflow.providers.common.ai.hooks.base.BaseAIHook`
        contract.  When ``toolsets`` are provided, they are bridged from
        pydantic-ai's ``AbstractToolset`` format to ADK-compatible callables
        using :func:`~airflow.providers.common.ai.utils.toolset_bridge.toolsets_to_adk_tools`.

        :param output_type: **Deprecated / ignored** — kept for interface
            compatibility.  Use ``output_schema`` instead.
        :param instructions: System-level instructions for the agent
            (LLM agents only).
        :param toolsets: List of pydantic-ai ``AbstractToolset`` instances
            (e.g. ``SQLToolset``, ``HookToolset``).  Bridged automatically.
        :param name: Agent name (default: ``"airflow_agent"``).
        :param agent_type: One of ``"llm"`` (default), ``"sequential"``,
            ``"parallel"``, or ``"loop"``.  Composite types (sequential,
            parallel, loop) require ``sub_agents``.
        :param sub_agents: List of pre-built ``BaseAgent`` instances for
            composite agent types.
        :param input_schema: Pydantic ``BaseModel`` subclass describing the
            expected input when the agent is used as a tool (LLM agents only).
        :param output_schema: Pydantic ``BaseModel`` subclass constraining
            the agent's output (LLM agents only).  **Note:** when set, the
            agent can only reply — it cannot use tools.
        :param output_key: Key in session state to store the agent's output.
        :param generate_content_config: ``GenerateContentConfig`` for
            temperature, safety settings, tool config, etc.
        :param before_model_callback: Called before each LLM call.
        :param after_model_callback: Called after each LLM call.
        :param before_tool_callback: Called before each tool invocation.
        :param after_tool_callback: Called after each tool invocation.
        :param before_agent_callback: Called before the agent runs.
        :param after_agent_callback: Called after the agent runs.
        :param max_iterations: Maximum loop iterations (``loop`` agent only).
        :param kwargs: Additional keyword arguments passed to the ADK
            agent constructor.  May include ``tools`` (plain callables),
            ``description``, etc.
        :return: A configured ``google.adk.agents.BaseAgent`` instance.
        """
        model = self._resolve_model()

        # Merge toolsets (bridged) and any explicit ADK tools from kwargs.
        adk_tools: list = list(kwargs.pop("tools", None) or [])
        if toolsets:
            from airflow.providers.common.ai.utils.toolset_bridge import toolsets_to_adk_tools

            adk_tools.extend(toolsets_to_adk_tools(toolsets))

        # ----- common base-agent kwargs (all agent types) -----
        agent_kwargs: dict[str, Any] = {
            "name": name,
            **kwargs,
        }
        if sub_agents:
            agent_kwargs["sub_agents"] = sub_agents
        if before_agent_callback is not None:
            agent_kwargs["before_agent_callback"] = before_agent_callback
        if after_agent_callback is not None:
            agent_kwargs["after_agent_callback"] = after_agent_callback

        # ----- agent-type-specific kwargs -----
        if agent_type == "llm":
            agent_kwargs.update(
                {
                    "model": model,
                    "instruction": instructions,
                    "tools": adk_tools,
                }
            )
            if input_schema is not None:
                agent_kwargs["input_schema"] = input_schema
            if output_schema is not None:
                agent_kwargs["output_schema"] = output_schema
            if output_key is not None:
                agent_kwargs["output_key"] = output_key
            if generate_content_config is not None:
                agent_kwargs["generate_content_config"] = generate_content_config
            if before_model_callback is not None:
                agent_kwargs["before_model_callback"] = before_model_callback
            if after_model_callback is not None:
                agent_kwargs["after_model_callback"] = after_model_callback
            if before_tool_callback is not None:
                agent_kwargs["before_tool_callback"] = before_tool_callback
            if after_tool_callback is not None:
                agent_kwargs["after_tool_callback"] = after_tool_callback

        elif agent_type == "loop":
            if max_iterations is not None:
                agent_kwargs["max_iterations"] = max_iterations

        agent_cls = _agent_cls_for(agent_type)
        return agent_cls(**agent_kwargs)

    # -----------------------------------------------------------------
    # Agent execution
    # -----------------------------------------------------------------

    def run_agent(self, *, agent: BaseAgent, prompt: str, **runner_kwargs: Any) -> str:
        """
        Run an ADK agent synchronously and return the final response text.

        Satisfies the :class:`~airflow.providers.common.ai.hooks.base.BaseAIHook`
        contract.  Creates a session (in-memory or database-backed),
        submits the prompt, iterates through the async event stream, and
        collects the final response.

        :param agent: A configured ADK ``BaseAgent`` instance.
        :param prompt: The user prompt to send to the agent.
        :param runner_kwargs: Extra keyword arguments forwarded to the
            ``Runner`` constructor (e.g. ``memory_service``, ``plugins``).
        :return: The concatenated text of the agent's final response.
        """
        session_service = self._session_service or InMemorySessionService()
        runner_kwargs.setdefault("memory_service", InMemoryMemoryService())
        runner = ADKRunner(
            agent=agent,
            app_name="airflow",
            session_service=session_service,
            **runner_kwargs,
        )

        async def _run() -> str:
            session = await session_service.create_session(app_name="airflow", user_id="airflow")
            content = genai_types.Content(
                role="user",
                parts=[genai_types.Part(text=prompt)],
            )
            final_text = ""
            async for event in runner.run_async(
                user_id="airflow",
                session_id=session.id,
                new_message=content,
            ):
                if event.is_final_response() and event.content and event.content.parts:
                    for part in event.content.parts:
                        if part.text:
                            final_text += part.text
            return final_text

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            # Already inside a running event loop (triggerer, Jupyter, etc.)
            output = loop.run_until_complete(_run())
        else:
            output = asyncio.run(_run())
        self.log.info("ADK agent run complete for model=%s", self.model_id)
        return output

    def test_connection(self) -> tuple[bool, str]:
        """
        Test connection by resolving the model.

        Validates that the model string is set and the google-adk package
        is installed.  Does NOT make an LLM API call — that would be
        expensive, flaky, and fail for reasons unrelated to connectivity.
        """
        try:
            model = self.get_conn()
            return True, f"Model resolved successfully: {model}"
        except Exception as e:
            return False, str(e)
