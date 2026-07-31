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
"""
Request fingerprints for durable replay verification.

Durable caching keys steps positionally (``model_step_{N}`` / ``tool_step_{N}``).
Position alone cannot tell whether a cached entry still corresponds to the
current request: if the prompt, model, toolset, or message history changed
between the failed attempt and the retry, replaying by position would feed the
agent responses recorded for a different conversation.

Each cache entry therefore stores a fingerprint of the request that produced
it.  On a cache hit the stored fingerprint is compared against the current
request; a mismatch is treated as a cache miss and the step re-runs live.
A divergence invalidates downstream steps too: a fresh model response carries
newly generated ``tool_call_id`` values, which are part of the tool
fingerprint, so stale tool results recorded under the old conversation no
longer match.

Fields that pydantic-ai regenerates on every attempt (message-level
``timestamp``/``run_id``/``conversation_id`` and part-level ``timestamp``)
are excluded from the fingerprint.  Requests that cannot be serialized to
JSON fingerprint as ``None``, which degrades that step to unverified
positional replay (the pre-fingerprint behavior) rather than disabling
caching.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import TypeAdapter
from pydantic_ai.messages import ModelMessagesTypeAdapter
from pydantic_ai.models import ModelRequestParameters

if TYPE_CHECKING:
    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.settings import ModelSettings

log = structlog.get_logger(logger_name="task")

_MODEL_REQUEST_PARAMETERS_ADAPTER = TypeAdapter(ModelRequestParameters)

# Message-level fields regenerated on every attempt.
_VOLATILE_MESSAGE_KEYS = ("timestamp", "run_id", "conversation_id")

# Settings that control transport, not response content. Excluded from the
# fingerprint: changing them should not invalidate a cached response, and some
# (``timeout`` can be an ``httpx.Timeout``) are not JSON-serializable, which
# would otherwise force the whole fingerprint to ``None`` and silently disable
# replay verification for every step.
_TRANSPORT_ONLY_SETTINGS = frozenset({"timeout"})


def _content_settings(model_settings: ModelSettings | None) -> dict[str, Any] | None:
    """Return the content-affecting settings, or ``None`` if there are none."""
    if not model_settings:
        return None
    content = {k: v for k, v in model_settings.items() if k not in _TRANSPORT_ONLY_SETTINGS}
    return content or None


def _strip_volatile(messages_dump: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Drop per-attempt fields from a dumped message list.

    Only the levels pydantic-ai regenerates are touched (message-level ids and
    timestamps, part-level timestamps); user data such as tool arguments is
    never recursed into, so an argument legitimately named ``run_id`` still
    affects the fingerprint.
    """
    stripped = []
    for message in messages_dump:
        cleaned = {k: v for k, v in message.items() if k not in _VOLATILE_MESSAGE_KEYS}
        if isinstance(cleaned.get("parts"), list):
            cleaned["parts"] = [
                {k: v for k, v in part.items() if k != "timestamp"} if isinstance(part, dict) else part
                for part in cleaned["parts"]
            ]
        stripped.append(cleaned)
    return stripped


def _digest(payload: Any) -> str:
    # No ``default=`` fallback: a non-JSON-serializable value must raise so the
    # callers degrade to an unverifiable (None) fingerprint instead of hashing
    # process-local reprs like ``<object at 0x...>`` that never match on retry.
    canonical = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def fingerprint_model_request(
    model_identifier: str,
    messages: list[ModelMessage],
    model_settings: ModelSettings | None,
    model_request_parameters: ModelRequestParameters,
) -> str | None:
    """
    Fingerprint a model request: model identity, message history, settings, and request parameters.

    The full ``ModelRequestParameters`` object is hashed (tool definitions,
    output mode and schema, native tools, ...) so any change to what is sent
    to the model invalidates the cached response.

    Returns ``None`` when the request cannot be serialized; ``None`` compares
    equal to ``None``, so requests that cannot be fingerprinted degrade to
    unverified positional replay rather than disabling caching.
    """
    try:
        dumped = ModelMessagesTypeAdapter.dump_python(messages, mode="json")
        params = _MODEL_REQUEST_PARAMETERS_ADAPTER.dump_python(model_request_parameters, mode="json")
        return _digest(
            {
                "model": model_identifier,
                "messages": _strip_volatile(dumped),
                "settings": _content_settings(model_settings),
                "params": params,
            }
        )
    except (TypeError, ValueError):
        # TypeError from json.dumps, ValueError covers PydanticSerializationError
        log.warning(
            "Durable: could not fingerprint model request; cached responses for this "
            "step replay without verification"
        )
        return None


def fingerprint_tool_call(name: str, tool_args: dict[str, Any], tool_call_id: str | None) -> str | None:
    """
    Fingerprint a tool call: tool name, arguments, and the model-issued call id.

    ``tool_call_id`` round-trips through the model-response cache, so it is
    stable under faithful replay but regenerated whenever a live model call
    replaces a cached response -- chaining invalidation to downstream tool steps.
    """
    try:
        return _digest({"name": name, "args": tool_args, "tool_call_id": tool_call_id})
    except (TypeError, ValueError):
        log.warning(
            "Durable: could not fingerprint tool call; cached results for this "
            "step replay without verification",
            tool=name,
        )
        return None
