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
are excluded from the fingerprint.  Payloads are canonicalized before hashing
(see ``_canonical``), so values that are not JSON types but do render the same
way on every attempt -- a ``datetime`` or ``Decimal`` tool argument, a dataclass
in ``tool_choice`` -- still produce a usable fingerprint.  Set members are
ordered by their JSON encoding, because a set of strings iterates in an order
that follows the interpreter's hash seed and every task attempt is a fresh
process.

A request that cannot be canonicalized fingerprints as ``None``: that step is
neither replayed nor cached, and re-runs live instead of replaying without
verification.  A lazily validated ``Iterable`` argument lands there deliberately,
since hashing it would consume the input the tool has not read yet, as does a
value pydantic cannot serialize at all.  On the model path this is seldom
confined to one step, because model settings and the message history are carried
into every later request, so durable execution stops contributing anything for
the rest of the run.  A tool call is fingerprinted from its name, arguments and
call id alone, so it can only lose its own step.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import BaseModel, TypeAdapter
from pydantic_ai.messages import ModelMessagesTypeAdapter
from pydantic_ai.models import ModelRequestParameters
from pydantic_core import to_jsonable_python

if TYPE_CHECKING:
    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.settings import ModelSettings

log = structlog.get_logger(logger_name="task")

_MODEL_REQUEST_PARAMETERS_ADAPTER = TypeAdapter(ModelRequestParameters)

# Message-level fields regenerated on every attempt.
_VOLATILE_MESSAGE_KEYS = ("timestamp", "run_id", "conversation_id")

# Settings that control transport, not response content. Excluded from the
# fingerprint: changing them should not invalidate a cached response, and
# ``timeout`` can be an ``httpx.Timeout``, which neither ``json`` nor pydantic
# can serialize.
#
# This frozenset is load-bearing. Model settings accompany every request, so one
# member that pydantic cannot serialize fingerprints every model step as ``None``
# -- which costs durable execution entirely for the run (nothing is cached,
# nothing is replayed), not merely the verification of a replay. Merely non-JSON
# values are fine, since ``_canonical`` renders those; a setting pydantic cannot
# serialize either has to be listed here instead.
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

    Raises ``TypeError`` if a message did not dump to a mapping. A python-mode dump
    passes an object it does not recognise straight through, and a fingerprint that
    cannot strip the volatile fields would change on every attempt, so the caller
    degrades to an unverifiable ``None`` instead.
    """
    stripped = []
    for message in messages_dump:
        if not isinstance(message, Mapping):
            raise TypeError(f"expected a dumped message mapping, got {type(message).__name__}")
        cleaned = {k: v for k, v in message.items() if k not in _VOLATILE_MESSAGE_KEYS}
        if isinstance(cleaned.get("parts"), list):
            cleaned["parts"] = [
                {k: v for k, v in part.items() if k != "timestamp"} if isinstance(part, dict) else part
                for part in cleaned["parts"]
            ]
        stripped.append(cleaned)
    return stripped


def _canonical(value: Any) -> Any:
    """
    Render ``value`` as JSON-safe data whose encoding is identical on every attempt.

    ``to_jsonable_python`` on its own is not a safe fingerprint input, for two
    reasons that both matter here because tool arguments arrive as live Python
    objects that pydantic has already validated.

    It renders a ``set`` in iteration order, and for string members that order
    follows the interpreter's hash seed. Every task attempt is a fresh process, so
    a ``set[str]`` argument would hash differently each time and never replay --
    worse than declining to cache, because the step re-runs live on every retry.
    Members are therefore ordered by their own JSON encoding.

    It also *consumes* an iterator, and pydantic validates an ``Iterable[T]``
    parameter lazily into a ``ValidatorIterator``. Normalizing the arguments would
    drain the tool's own input before the tool ran, so the tool would see an empty
    sequence and that wrong result would be cached under the fingerprint of the
    full one. Such a value is refused instead, which degrades the step to the
    not-cached path rather than corrupting it.
    """
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, Iterator):
        # Hashing this means draining it, leaving nothing for the tool to read.
        raise TypeError(f"cannot fingerprint {type(value).__name__} without consuming it")
    if isinstance(value, Mapping):
        return {key: _canonical(item) for key, item in value.items()}
    if isinstance(value, (set, frozenset)):
        members = [_canonical(item) for item in value]
        return sorted(members, key=lambda member: json.dumps(member, sort_keys=True))
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, BaseModel):
        # ``mode="python"`` leaves nested sets as sets so they reach the branch above.
        return _canonical(value.model_dump(mode="python"))
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _canonical({f.name: getattr(value, f.name) for f in dataclasses.fields(value)})
    # datetime, Decimal, UUID, Enum, bytes and friends render as scalars. A value
    # pydantic cannot serialize raises ``PydanticSerializationError`` (a
    # ``ValueError``), so the caller degrades to an unverifiable ``None``
    # fingerprint rather than hashing a process-local repr like
    # ``<object at 0x...>`` that would never match on retry.
    return to_jsonable_python(value)


def _digest(payload: Any) -> str:
    # Plain JSON values survive ``_canonical`` untouched, so fingerprints written
    # before normalization existed still match.
    canonical = json.dumps(_canonical(payload), sort_keys=True)
    return hashlib.sha256(canonical.encode()).hexdigest()


def fingerprint_model_request(
    model_identifier: str,
    messages: list[ModelMessage],
    model_settings: ModelSettings | None,
    model_request_parameters: ModelRequestParameters,
    *,
    step: int | None = None,
) -> str | None:
    """
    Fingerprint a model request: model identity, message history, settings, and request parameters.

    The full ``ModelRequestParameters`` object is hashed (tool definitions,
    output mode and schema, native tools, ...) so any change to what is sent
    to the model invalidates the cached response.

    Returns ``None`` when the request cannot be serialized even through pydantic,
    which prevents the step from being replayed or cached. Because model settings
    and message history are carried into every later request, such a value in
    either usually degrades every subsequent model step of the run the same way.
    ``step`` is attached to that warning so the log names where it began.
    """
    try:
        # ``mode="python"``, not ``mode="json"``: a json-mode dump renders a set as
        # a list in iteration order, and a tool that returned a set puts one in the
        # message history, where it would reach the hash already unstably ordered.
        # Python mode leaves it a set for ``_canonical`` to order. For values that
        # are not sets the two modes produce the same digest, so stored
        # fingerprints are unaffected.
        dumped = ModelMessagesTypeAdapter.dump_python(messages)
        params = _MODEL_REQUEST_PARAMETERS_ADAPTER.dump_python(model_request_parameters)
        return _digest(
            {
                "model": model_identifier,
                "messages": _strip_volatile(dumped),
                "settings": _content_settings(model_settings),
                "params": params,
            }
        )
    except (TypeError, ValueError, RecursionError):
        # TypeError from json.dumps and from _canonical refusing an iterator;
        # ValueError covers PydanticSerializationError.
        # RecursionError because _canonical walks the payload before json.dumps can
        # apply its own circular-reference check, so a self-referencing value hits
        # the recursion limit here instead of raising ValueError there.
        log.warning(
            "Durable: could not fingerprint model request; this step will not be cached and will "
            "execute live on retry. If the cause is in model settings or message history, every "
            "later model step of this run is affected too",
            step=step,
        )
        return None


def fingerprint_tool_call(
    name: str,
    tool_args: dict[str, Any],
    tool_call_id: str | None,
    *,
    step: int | None = None,
) -> str | None:
    """
    Fingerprint a tool call: tool name, arguments, and the model-issued call id.

    ``tool_call_id`` round-trips through the model-response cache, so it is
    stable under faithful replay but regenerated whenever a live model call
    replaces a cached response -- chaining invalidation to downstream tool steps.

    Only the name, arguments and call id are hashed, so neither model settings
    nor the message history can affect a tool fingerprint. Arguments arrive as
    live objects pydantic has already validated, which is what ``_canonical``
    exists to handle: it must neither reorder a ``set`` unpredictably nor consume
    a lazily validated ``Iterable``, since the tool has not read it yet.
    """
    try:
        return _digest({"name": name, "args": tool_args, "tool_call_id": tool_call_id})
    except (TypeError, ValueError, RecursionError):
        log.warning(
            "Durable: could not fingerprint tool call; this step will not be cached and will "
            "execute live on retry",
            tool=name,
            step=step,
        )
        return None
