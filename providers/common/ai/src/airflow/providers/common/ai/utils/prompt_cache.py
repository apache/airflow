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
"""Prompt caching that works the same way whichever provider the connection resolves to."""

from __future__ import annotations

from dataclasses import KW_ONLY, dataclass
from typing import TYPE_CHECKING, Any

from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.settings import ModelSettings, merge_model_settings

if TYPE_CHECKING:
    from pydantic_ai import RunContext
    from pydantic_ai.agent import AgentModelSettings
    from pydantic_ai.models.anthropic import AnthropicModelSettings
    from pydantic_ai.models.bedrock import BedrockModelSettings
    from pydantic_ai.models.openrouter import OpenRouterModelSettings

# Anthropic writes a cache entry only at a breakpoint, so each of these marks a prefix a later
# request can read back: the tool schemas, then the system prompt (the one that survives a
# different user prompt per mapped task), then the conversation so far, which pydantic-ai
# re-marks on every request of the run. ``anthropic_cache_messages`` rather than the
# top-level ``anthropic_cache``: both put the moving breakpoint on the last message, but
# pydantic-ai documents the per-block form as the one for Anthropic-compatible gateways that
# lack the top-level parameter, and Bedrock and Vertex fall back to the per-block form anyway.
_ANTHROPIC_CACHE_SETTINGS: AnthropicModelSettings = {
    "anthropic_cache_tool_definitions": True,
    "anthropic_cache_instructions": True,
    "anthropic_cache_messages": True,
}
# The same three breakpoints for Bedrock's Converse API and for OpenRouter. pydantic-ai adds
# them only for models whose profile supports prompt caching, so they are a no-op for the
# rest of either catalog.
_BEDROCK_CACHE_SETTINGS: BedrockModelSettings = {
    "bedrock_cache_tool_definitions": True,
    "bedrock_cache_instructions": True,
    "bedrock_cache_messages": True,
}
_OPENROUTER_CACHE_SETTINGS: OpenRouterModelSettings = {
    "openrouter_cache_tool_definitions": True,
    "openrouter_cache_instructions": True,
    "openrouter_cache_messages": True,
}
# Each provider family's settings, keyed by the prefix every one of its cache settings shares.
# OpenAI and Gemini cache long prompts on their own, so they need nothing here, and a model
# ignores settings meant for another provider, which is what lets one set cover a fallback
# chain that spans providers.
_CACHE_SETTINGS_BY_PREFIX: tuple[tuple[str, ModelSettings], ...] = (
    ("anthropic_cache", _ANTHROPIC_CACHE_SETTINGS),
    ("bedrock_cache", _BEDROCK_CACHE_SETTINGS),
    ("openrouter_cache", _OPENROUTER_CACHE_SETTINGS),
)

# Every cache setting of those three families, including the ``anthropic_cache`` a caller may
# set in place of ours. They change what the provider keeps, never what the model answers.
PROMPT_CACHE_SETTING_NAMES = frozenset(
    {
        "anthropic_cache",
        "anthropic_cache_instructions",
        "anthropic_cache_messages",
        "anthropic_cache_tool_definitions",
        "bedrock_cache_instructions",
        "bedrock_cache_messages",
        "bedrock_cache_tool_definitions",
        "openrouter_cache_instructions",
        "openrouter_cache_messages",
        "openrouter_cache_tool_definitions",
    }
)


def _fill_cache_settings(ctx: RunContext[Any]) -> ModelSettings:
    """
    Return the cache settings for each provider family the run has not configured itself.

    ``ctx.model_settings`` already holds the model's own settings and the agent's, so a
    cache setting from either, including one read from a spec file, leaves that provider's
    caching entirely to the caller. Taking over the whole family rather than filling
    individual keys matters for Anthropic: ``anthropic_cache`` and
    ``anthropic_cache_messages`` cannot be combined, and a caller who set one of them
    would otherwise get a request pydantic-ai refuses to send.
    """
    configured = ctx.model_settings or {}
    settings: ModelSettings | None = None
    for prefix, defaults in _CACHE_SETTINGS_BY_PREFIX:
        if not any(name.startswith(prefix) for name in configured):
            settings = merge_model_settings(settings, defaults)
    return settings or ModelSettings()


@dataclass
class PromptCaching(AbstractCapability[Any]):
    """
    Ask the model's provider to cache the repeated prefix of each request.

    Backs ``AgentOperator(cache_prompt=True)``. A provider-agnostic setting does not exist in
    pydantic-ai, so this turns on each provider's own: Anthropic models (direct, Bedrock,
    Vertex), and Bedrock Converse and OpenRouter models that support caching, are marked;
    OpenAI and Gemini already cache automatically. Settings the agent or its model already
    carry for a provider win over these.
    """

    _: KW_ONLY
    id: str | None = "prompt_caching"

    def get_model_settings(self) -> AgentModelSettings[Any]:
        # A callable so it sees the settings merged before it: capability settings otherwise
        # override the agent's, and the agent's are what a caller configures.
        return _fill_cache_settings
