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
are excluded from the fingerprint.  Every payload is rendered by pydantic in
JSON mode before hashing, so values that are not JSON types but render the same
way on every attempt -- a ``datetime`` or ``Decimal`` tool argument, a dataclass
in ``tool_choice``, bytes in a ``BinaryContent`` -- still produce a usable
fingerprint, and the message history hashes exactly as it always has.  The one
correction applied on top is that set members are ordered by their JSON encoding,
because a set of strings iterates in an order that follows the interpreter's hash
seed and every task attempt is a fresh process (see ``_order_sets``).

A request that cannot be rendered fingerprints as ``None``: that step is
neither replayed nor cached, and re-runs live instead of replaying without
verification.  A lazily validated ``Iterable`` argument lands there deliberately,
since hashing it would consume the input the tool has not read yet, as does a
value pydantic cannot serialize at all.  On the model path this is seldom
confined to one step, because model settings, the tool definitions and the
message history are carried into every later request, so durable execution stops
contributing anything for the rest of the run.  A tool call is fingerprinted from its name, arguments and
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
    from collections.abc import Iterable

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
# values are fine, since pydantic renders those; a setting pydantic cannot
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


def _is_dataclass_instance(value: Any) -> bool:
    return dataclasses.is_dataclass(value) and not isinstance(value, type)


def _refuse_iterators(value: Any) -> None:
    """
    Raise ``TypeError`` if ``value`` holds an iterator anywhere inside it.

    Rendering an iterator consumes it, and pydantic validates an ``Iterable[T]``
    tool parameter lazily into a ``ValidatorIterator``. Tool arguments are
    fingerprinted before the tool runs, so rendering them would drain the tool's
    own input: the tool would see an empty sequence, and that wrong result would be
    cached under the fingerprint of the full one. Refusing degrades the step to the
    not-cached path instead.
    """
    if value is None or isinstance(value, (str, bytes, bytearray, bool, int, float)):
        return
    if isinstance(value, Iterator):
        raise TypeError(f"cannot fingerprint {type(value).__name__} without consuming it")
    if isinstance(value, Mapping):
        children: Iterable[Any] = value.values()
    elif isinstance(value, (list, tuple, set, frozenset)):
        children = value
    elif isinstance(value, BaseModel):
        children = [getattr(value, name, None) for name in type(value).model_fields]
    elif _is_dataclass_instance(value):
        children = [getattr(value, field.name, None) for field in dataclasses.fields(value)]
    else:
        return
    for child in children:
        _refuse_iterators(child)


def _order_sets(value: Any, rendered: Any) -> Any:
    """
    Return ``rendered`` with every list that pydantic rendered from a set sorted.

    ``rendered`` is pydantic's JSON rendering of ``value`` and is otherwise kept as
    is, so pydantic stays the only renderer: bytes, dates, dict keys, ``NaN`` and
    custom serializers come out exactly as a json-mode dump renders them. The one
    thing pydantic cannot do stably is order a set. It lists the members in
    iteration order, which for strings follows the interpreter's hash seed, and
    every task attempt is a fresh process, so a ``set[str]`` would hash differently
    on each attempt and never replay. ``value`` is walked alongside ``rendered``
    only to find those lists; a branch whose rendering does not line up with the
    object, such as one with a custom serializer, is left exactly as rendered.
    """
    if isinstance(value, (set, frozenset)):
        if not isinstance(rendered, list) or len(rendered) != len(value):
            return rendered
        members = [_order_sets(member, item) for member, item in zip(value, rendered)]
        return sorted(members, key=lambda member: json.dumps(member, sort_keys=True))
    if isinstance(value, Mapping):
        if not isinstance(rendered, dict):
            return rendered
        if len(rendered) != len(value):
            # Distinct keys that render alike, such as 1 and "1": the digest could no
            # longer tell those payloads apart, so refuse rather than hash either one.
            raise TypeError("dict keys collide once rendered as JSON")
        return {
            key: _order_sets(item, rendered_item)
            for (_, item), (key, rendered_item) in zip(value.items(), rendered.items())
        }
    if isinstance(value, (list, tuple)):
        if not isinstance(rendered, list) or len(rendered) != len(value):
            return rendered
        return [_order_sets(item, rendered_item) for item, rendered_item in zip(value, rendered)]
    if isinstance(rendered, dict) and (isinstance(value, BaseModel) or _is_dataclass_instance(value)):
        return {
            key: _order_sets(getattr(value, key), rendered_item) if hasattr(value, key) else rendered_item
            for key, rendered_item in rendered.items()
        }
    return rendered


def _render(value: Any) -> Any:
    """
    Render ``value`` as a json-mode dump would, with set members in a fixed order.

    Plain JSON values come back unchanged, so their digests match the ones main
    stored. A value pydantic cannot serialize at all raises
    ``PydanticSerializationError`` (a ``ValueError``) naming its type, so the caller
    degrades to an unverifiable ``None`` rather than hashing a process-local repr
    like ``<object at 0x...>`` that would never match on retry.
    """
    _refuse_iterators(value)
    return _order_sets(value, to_jsonable_python(value, bytes_mode="base64"))


def _digest(payload: Any) -> str:
    """Hash an already rendered payload."""
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


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
    which prevents the step from being replayed or cached. Because model settings,
    the tool definitions in the request parameters and the message history are all
    carried into every later request, such a value in any of them usually degrades
    every subsequent model step of the run the same way.
    ``step`` is attached to that warning so the log names where it began.
    """
    try:
        # Messages and parameters are rendered by pydantic's json-mode dump, exactly
        # as before sets were ordered, so stored fingerprints still match. The dump
        # lists a set in iteration order (a tool can return one into the message
        # history), and ``_order_sets`` fixes only that.
        _refuse_iterators(messages)
        _refuse_iterators(model_request_parameters)
        dumped = _order_sets(messages, ModelMessagesTypeAdapter.dump_python(messages, mode="json"))
        params = _order_sets(
            model_request_parameters,
            _MODEL_REQUEST_PARAMETERS_ADAPTER.dump_python(model_request_parameters, mode="json"),
        )
        return _digest(
            {
                "model": model_identifier,
                "messages": _strip_volatile(dumped),
                "settings": _render(_content_settings(model_settings)),
                "params": params,
            }
        )
    except (TypeError, ValueError, RecursionError) as exc:
        # TypeError from json.dumps, from refusing an iterator and from colliding
        # dict keys; ValueError covers PydanticSerializationError, whose message
        # names the offending type and is the only pointer to the setting at fault.
        # RecursionError because the iterator and set walks recurse before pydantic
        # can apply its own circular-reference check, so a self-referencing value
        # hits the recursion limit there instead.
        log.warning(
            "Durable: could not fingerprint model request; this step will not be cached and will "
            "execute live on retry. If the cause is in model settings, tool definitions or message "
            "history, every later model step of this run is affected too",
            step=step,
            error=str(exc),
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
    live objects pydantic has already validated, which is what ``_render``
    exists to handle: it must neither reorder a ``set`` unpredictably nor consume
    a lazily validated ``Iterable``, since the tool has not read it yet.
    """
    try:
        return _digest({"name": name, "args": _render(tool_args), "tool_call_id": tool_call_id})
    except (TypeError, ValueError, RecursionError) as exc:
        log.warning(
            "Durable: could not fingerprint tool call; this step will not be cached and will "
            "execute live on retry",
            error=str(exc),
            tool=name,
            step=step,
        )
        return None
