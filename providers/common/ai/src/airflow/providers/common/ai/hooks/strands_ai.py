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
"""Hooks for LLM agents via the Strands Agents SDK."""

from __future__ import annotations

import functools
from abc import abstractmethod
from typing import Any

from airflow.providers.common.ai.hooks.base_ai import (
    AgentRunRequest,
    AgentRunResult,
    BaseAIHook,
    SkillSpec,
    ToolSpec,
    tool_identifier,
)


class StrandsHook(BaseAIHook):
    """
    Base hook for LLM agents via `Strands Agents <https://strandsagents.com/>`__.

    Subclasses implement :meth:`get_model` to return a configured Strands model instance
    (for example :class:`strands.models.gemini.GeminiModel`). The
    :meth:`create_agent`, :meth:`run_agent`, and :meth:`_tool_spec_to_native`
    implementations are shared across all Strands model backends.
    """

    conn_name_attr = "llm_conn_id"
    default_conn_name = "strands_default"

    supports_toolsets = True
    supports_durable = False
    supports_usage_limits = False
    supports_skills = True

    def __init__(
        self,
        llm_conn_id: str | None = None,
        model_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.llm_conn_id = llm_conn_id if llm_conn_id is not None else self.default_conn_name
        self.model_id = model_id
        self._resolved_model_id: str | None = None

    @abstractmethod
    def get_model(self) -> Any:
        """Return a configured Strands model instance."""

    def _tool_spec_to_native(self, spec: ToolSpec) -> Any:
        """Convert a :class:`~airflow.providers.common.ai.hooks.base_ai.ToolSpec` to a Strands tool."""
        try:
            from strands import tool as strands_tool
        except ImportError as e:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "The 'strands-agents' package is required for StrandsHook. "
                "Install it with: pip install 'apache-airflow-providers-common-ai[strands]'"
            ) from e

        fn = spec.fn

        # Strands infers tool name from __name__ and description from __doc__.
        # functools.wraps preserves __wrapped__ so inspect.signature() follows it
        # for parameter schema inference, then we override name/doc from spec.
        @functools.wraps(fn)
        def tool_fn(*args: Any, **kwargs: Any) -> Any:
            return fn(*args, **kwargs)

        tool_fn.__name__ = tool_identifier(spec.name)
        tool_fn.__doc__ = spec.description
        return strands_tool(tool_fn)

    def _skill_spec_to_native(self, skill: str | SkillSpec) -> Any:
        """Convert a skill source to a Strands-native skill object or path."""
        if isinstance(skill, SkillSpec) and not skill.path:
            from strands import Skill

            return Skill(
                name=skill.name,
                description=skill.description,
                instructions=skill.instructions,
            )
        return super()._skill_spec_to_native(skill)

    def _build_skills_plugin(self, request: AgentRunRequest) -> Any | None:
        """Build a Strands ``AgentSkills`` plugin when skill sources are configured."""
        sources = self._resolve_skill_sources(request)
        if not sources:
            return None

        try:
            from strands import AgentSkills
        except ImportError as e:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "The 'strands-agents' package is required for Strands skills. "
                "Install it with: pip install 'apache-airflow-providers-common-ai[strands]'"
            ) from e

        skills_arg: Any = sources[0] if len(sources) == 1 else sources
        return AgentSkills(skills=skills_arg, **dict(request.skills_params or {}))

    def create_agent(self, request: AgentRunRequest) -> Any:
        """Build a Strands ``Agent`` from *request*."""
        try:
            from strands import Agent
        except ImportError as e:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "The 'strands-agents' package is required for StrandsHook. "
                "Install it with: pip install 'apache-airflow-providers-common-ai[strands]'"
            ) from e

        native_tools: list[Any] = []
        if request.toolsets:
            native_tools = self._resolve_tools(
                request.toolsets,
                request.enable_tool_logging,
                None,  # durable execution is not supported for Strands
                None,
            )

        agent_kwargs: dict[str, Any] = dict(request.agent_params or {})
        if request.instructions:
            agent_kwargs["system_prompt"] = request.instructions

        plugins: list[Any] = list(agent_kwargs.pop("plugins", []) or [])
        skills_plugin = self._build_skills_plugin(request)
        if skills_plugin is not None:
            plugins.append(skills_plugin)
        if plugins:
            agent_kwargs["plugins"] = plugins

        return Agent(model=self.get_model(), tools=native_tools or [], **agent_kwargs)

    def run_agent(self, agent: Any, request: AgentRunRequest) -> AgentRunResult:
        """Run the Strands *agent* for *request* and return a normalized :class:`AgentRunResult`."""
        response = agent(request.prompt)
        return AgentRunResult(
            output=str(response),
            model_name=self._resolved_model_id or self.model_id,
        )

    def test_connection(self) -> tuple[bool, str]:
        """
        Validate the connection by constructing the configured model client.

        Does not run an agent or send a user prompt. Remote credential checks depend
        on the installed ``strands-agents`` / ``google-genai`` version.
        """
        try:
            self.get_model()
            return True, f"{type(self).__name__} resolved successfully."
        except Exception as e:
            return False, str(e)


class StrandsGeminiHook(StrandsHook):
    """
    Hook for Strands Agents using Google Gemini as the model backend.

    Credentials are resolved in order:

    1. API key from the connection **password** field.
    2. ``GOOGLE_API_KEY`` environment variable when no password is set.

    Connection fields:
        - **password**: Google AI API key
        - **extra** JSON::

            {"model": "gemini-2.5-flash", "params": {"temperature": 0.7, "max_output_tokens": 2048}}

    :param llm_conn_id: Airflow connection ID.
    :param model_id: Gemini model identifier (e.g. ``"gemini-2.5-flash"``).
        Overrides the model stored in the connection's extra field.
    """

    conn_type = "strands-gemini"
    hook_name = "Strands (Google Gemini)"

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login", "host"],
            "relabeling": {"password": "API Key"},
            "placeholders": {
                "extra": '{"model": "gemini-2.5-flash", "params": {"temperature": 0.7}}',
            },
        }

    def get_model(self) -> Any:
        """
        Return a configured Strands ``GeminiModel``.

        Resolution order:

        1. **Explicit API key** — when the connection password is set, it is passed as
           ``client_args={"api_key": ...}``.
        2. **Default resolution** — delegates to the Google GenAI client, which reads
           ``GOOGLE_API_KEY`` from the environment when no key is provided.
        """
        try:
            from strands.models.gemini import GeminiModel
        except ImportError as e:
            from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "The 'strands-agents[gemini]' extra is required for StrandsGeminiHook. "
                "Install it with: pip install 'apache-airflow-providers-common-ai[strands]'"
            ) from e

        conn = self.get_connection(self.llm_conn_id)
        extra: dict[str, Any] = conn.extra_dejson
        model_id: str = self.model_id or extra.get("model", "")
        if not model_id:
            raise ValueError(
                "No model specified. Set model_id on the hook or the 'model' field in the connection extra."
            )

        kwargs: dict[str, Any] = {"model_id": model_id}
        self._resolved_model_id = model_id

        api_key: str | None = conn.password or None
        if api_key:
            kwargs["client_args"] = {"api_key": api_key}

        params = extra.get("params")
        if isinstance(params, dict):
            kwargs["params"] = params

        self.log.info("Creating Strands GeminiModel: model_id=%s", model_id)
        return GeminiModel(**kwargs)
