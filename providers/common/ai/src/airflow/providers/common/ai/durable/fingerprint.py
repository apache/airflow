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
are excluded from the fingerprint.  Plain JSON tool arguments and settings are
hashed exactly as before.  Everything else is rendered by pydantic: the message
history and the request parameters by their json-mode dump, which is also what
they were always hashed from, and other tool arguments and settings by
``to_jsonable_python``, with bytes outside a model as base64.  So a value that is
not a JSON type but renders the same way on every attempt -- a ``datetime`` or
``Decimal`` tool argument, a dataclass in ``tool_choice`` -- still produces a
usable fingerprint.

That rendering is hashed as it is, with two exceptions.  Both are found by
dumping the payload a second time in pydantic's python mode, which has the same
shape as the JSON rendering but keeps sets as sets and dict keys as they are, and
wraps an iterator instead of reading it (see ``_check_guide``).  The members of a
set are sorted by their JSON encoding, because a set of strings iterates in an
order that follows the interpreter's hash seed and every task attempt is a fresh
process (see ``_order_sets``); a list that a serializer produced is left as the
serializer produced it.  And a payload is refused rather than hashed if it would
render through an iterator, since rendering consumes it, or if distinct dict keys
render alike, such as ``1`` and ``"1"``, since two different payloads would then
share a digest.  Sorting changes the digest of a set whose order was already
stable, such as a set of integers, so a history holding one can re-run from that
step once after upgrading.

Tool arguments are rendered from deep copies, so nothing that rendering runs -- a
computed field, a cached property, a serializer -- reaches the objects the tool is
about to receive.  An argument that cannot be copied, such as a lazily validated
``Iterable``, is refused.

A request that cannot be fingerprinted fingerprints as ``None``: that step is
neither replayed nor cached, and re-runs live instead of replaying without
verification.  On the model path this is seldom confined to one step, because
model settings, the tool definitions and the message history are usually carried
into every later request, so the model steps from that point on are lost too.  A
tool call is fingerprinted from its name, arguments and call id alone, so it
cannot stop another step from being fingerprinted, though a live re-run that
returns something different still invalidates the model steps after it, because
its result is part of the history they fingerprint.
"""

from __future__ import annotations

import copy
import enum
import hashlib
import json
from collections import deque
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter
from pydantic_ai.messages import ModelMessagesTypeAdapter
from pydantic_ai.models import ModelRequestParameters
from pydantic_core import to_jsonable_python

from airflow.providers.common.ai.utils.prompt_cache import PROMPT_CACHE_SETTING_NAMES
from airflow.providers.common.ai.utils.task_logger import get_task_logger

if TYPE_CHECKING:
    from collections.abc import Iterable

    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.settings import ModelSettings

log = get_task_logger()

_MODEL_REQUEST_PARAMETERS_ADAPTER = TypeAdapter(ModelRequestParameters)

# Renders tool arguments and settings in python mode, the way ``to_jsonable_python``
# renders them in JSON mode.
_ANY_ADAPTER: TypeAdapter[Any] = TypeAdapter(Any)

# Message-level fields regenerated on every attempt.
_VOLATILE_MESSAGE_KEYS = ("timestamp", "run_id", "conversation_id")

# Settings that control transport, not response content. Excluded from the
# fingerprint: changing them should not invalidate a cached response, and
# ``timeout`` can be an ``httpx.Timeout``, which neither ``json`` nor pydantic
# can serialize.
# Prompt cache settings only decide what the provider keeps for the next
# request, so ``cache_prompt`` can change between attempts without re-running
# the steps the previous one completed.
#
# This frozenset is load-bearing. Model settings accompany every request, so one
# member that pydantic cannot serialize fingerprints every model step as ``None``
# -- which costs durable execution entirely for the run (nothing is cached,
# nothing is replayed), not merely the verification of a replay. Merely non-JSON
# values are fine, since pydantic renders those; a setting pydantic cannot
# serialize either has to be listed here instead.
_TRANSPORT_ONLY_SETTINGS = frozenset({"timeout"}) | PROMPT_CACHE_SETTING_NAMES

# Exact types that can hold nothing else, so the walks need not visit them.
_LEAF_TYPES = frozenset({str, bytes, bytearray, bool, int, float, type(None)})

# A set member with no set inside it: its rendering is taken as it is.
_LEAF = "leaf"


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


def _check_guide(guide: Any) -> bool:
    """
    Refuse an iterator, and report whether the JSON rendering needs ``_order_sets``.

    ``guide`` is pydantic's python-mode dump of the payload. It has the shape of the
    json-mode rendering that is hashed -- the same keys, aliases, exclusions,
    computed fields, extras and serializer output -- but keeps sets as sets and dict
    keys as they are, and wraps an iterator in a ``SerializationIterator`` without
    reading it. So this walk runs no user code, and nothing has been consumed yet.

    Raises ``TypeError`` if the payload renders through an iterator anywhere.
    Rendering it to JSON would consume it, and pydantic validates an ``Iterable[T]``
    tool parameter lazily into an iterator; a tool return that holds a generator
    would reach the model empty. Returns ``True`` if the guide holds a set, or a dict
    key that is not a string, since only those can make the JSON rendering depend on
    the hash seed or lose a key.
    """
    needs_check = False
    pending = [guide]
    seen: set[int] = set()
    while pending:
        item = pending.pop()
        if id(item) in seen:
            # The dump shares this object, or left a cycle in place: walked already.
            continue
        seen.add(id(item))
        children: Iterable[Any]
        if isinstance(item, dict):
            needs_check = needs_check or any(not isinstance(key, str) for key in item)
            children = item.values()
        elif isinstance(item, (list, tuple, deque)):
            children = item
        elif isinstance(item, (set, frozenset)):
            needs_check = True
            children = item
        elif isinstance(item, Iterator):
            raise TypeError(f"cannot fingerprint a value that renders through a {type(item).__name__}")
        elif isinstance(item, enum.Enum):
            # Python mode keeps an Enum member; JSON renders its value.
            children = (item.value,)
        else:
            continue
        pending.extend(child for child in children if type(child) not in _LEAF_TYPES)
    return needs_check


def _json_order(member: Any) -> str:
    return json.dumps(member, sort_keys=True)


def _template(value: Any) -> Any:
    """
    Say where the sets are inside one set member, or ``None`` if that cannot be said.

    ``_LEAF`` for a member with no set inside it, ``("set", inner)`` for a set whose
    members all have the template ``inner``, and ``("sequence", parts)`` for a tuple
    with a set somewhere in it.
    """
    if isinstance(value, enum.Enum):
        return _template(value.value)
    if isinstance(value, (set, frozenset)):
        inner = _member_template(value)
        return None if inner is None else ("set", inner)
    if isinstance(value, (list, tuple, deque)):
        parts = tuple(_template(item) for item in value)
        if None in parts:
            return None
        return _LEAF if all(part == _LEAF for part in parts) else ("sequence", parts)
    if isinstance(value, dict):
        # No set member dumps to a dict (python mode refuses a set of models), so
        # nothing is known about one.
        return None
    return _LEAF


def _member_template(members: Iterable[Any]) -> Any:
    """Return the template every member of a set shares, or ``None`` if they differ."""
    templates = {_template(member) for member in members}
    if not templates:
        return _LEAF
    return templates.pop() if len(templates) == 1 else None


def _apply_template(template: Any, rendered: Any) -> Any:
    """Sort the lists that ``template`` puts a set at, in the rendering of one set member."""
    if template == _LEAF or not isinstance(rendered, list):
        return rendered
    kind, inner = template
    if kind == "set":
        return sorted((_apply_template(inner, item) for item in rendered), key=_json_order)
    if len(rendered) != len(inner):
        return rendered
    return [_apply_template(part, item) for part, item in zip(inner, rendered)]


def _order_sets(guide: Any, rendered: Any) -> Any:
    """
    Return ``rendered`` with every list that pydantic rendered from a set sorted.

    ``rendered`` is the json-mode rendering that is hashed, and ``guide`` the
    python-mode dump of the same payload (see ``_check_guide``). They are walked
    side by side: a dict or a sequence pairs up by position, because both dumps
    keep the same order, and a list is sorted where the guide holds a set. A set's
    members cannot pair up by position -- the dump builds a new set, which may
    iterate in another order -- so the members' shared template says where any set
    inside one of them sits (see ``_template``). Anything that does not line up is
    kept exactly as rendered, so nothing that did not come from a set is reordered.

    Raises ``TypeError`` where distinct dict keys render alike, such as ``1`` and
    ``"1"``, or ``None`` and ``nan`` in a message dump: the guide then holds more keys
    than the rendering, and hashing it would let two different payloads share a
    digest. A dict whose keys are all strings cannot collide, so one that renders
    fewer keys was reshaped by a serializer that applies only in JSON mode, and is
    kept as rendered.
    """
    if isinstance(guide, enum.Enum):
        # An Enum member renders as its value.
        return _order_sets(guide.value, rendered)
    if isinstance(guide, (set, frozenset)):
        if not isinstance(rendered, list) or len(rendered) != len(guide):
            return rendered
        template = _member_template(guide)
        if template is None:
            return rendered
        return sorted((_apply_template(template, item) for item in rendered), key=_json_order)
    if isinstance(guide, dict):
        if not isinstance(rendered, dict):
            return rendered
        if len(rendered) != len(guide):
            if len(rendered) < len(guide) and any(not isinstance(key, str) for key in guide):
                raise TypeError("dict keys collide once rendered as JSON")
            return rendered
        ordered = {}
        for (guide_key, guide_item), (key, item) in zip(guide.items(), rendered.items()):
            if isinstance(guide_key, str) and guide_key != key:
                # A string key renders as itself, so the two do not line up.
                return rendered
            ordered[key] = _order_sets(guide_item, item)
        return ordered
    if isinstance(guide, (list, tuple, deque)):
        if not isinstance(rendered, list) or len(rendered) != len(guide):
            return rendered
        return [_order_sets(guide_item, item) for guide_item, item in zip(guide, rendered)]
    return rendered


def _render(value: Any, *, copy_first: bool = False) -> Any:
    """
    Render tool arguments or settings for hashing.

    Plain JSON is returned unchanged, so it is hashed exactly as before: numeric keys
    in numeric order, ``None`` and ``inf`` keys as ``null`` and ``Infinity``, and
    nesting as deep as ``json`` allows. Anything else is rendered by
    ``to_jsonable_python``, with set members in a fixed order and bytes as base64,
    which also takes data that is not UTF-8; bytes inside a model follow that
    model's own ``ser_json_bytes``. A value pydantic cannot serialize raises
    ``PydanticSerializationError`` (a ``ValueError``) naming its type, so the caller
    degrades to an unverifiable ``None`` rather than hashing a process-local repr
    like ``<object at 0x...>`` that would never match on retry. So does a model used
    as a dict key, and a set of models: python mode dumps them to dicts, which
    cannot be hashed.

    With ``copy_first``, each rendering is taken from a deep copy of ``value``, so the
    objects themselves are left exactly as they were: rendering can run a computed
    field, fill a cached property, or read an iterator a computed field holds. A
    value that cannot be copied, such as a lazily validated iterator or a generator,
    raises ``TypeError`` before anything is rendered.
    """
    try:
        json.dumps(value, sort_keys=True)
    except (TypeError, ValueError, RecursionError):
        pass
    else:
        return value
    guide_source, rendered_source = (
        (copy.deepcopy(value), copy.deepcopy(value)) if copy_first else (value, value)
    )
    guide = _ANY_ADAPTER.dump_python(guide_source, by_alias=True)
    needs_check = _check_guide(guide)
    rendered = to_jsonable_python(rendered_source, by_alias=True, bytes_mode="base64")
    return _order_sets(guide, rendered) if needs_check else rendered


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

    Returns ``None`` when the request cannot be fingerprinted, which prevents the
    step from being replayed or cached. Model settings, the tool definitions in the
    request parameters and the message history are normally carried into every
    later request, so such a value in any of them usually degrades every
    subsequent model step of the run the same way. ``step`` is attached to the
    warning so the log names where it began.
    """
    try:
        # Messages and parameters are hashed from pydantic's json-mode dump, as they
        # always were, so stored fingerprints still match. The python-mode dumps only
        # guide ``_order_sets``, which runs where a set or a non-string key needs it.
        messages_guide = ModelMessagesTypeAdapter.dump_python(messages)
        params_guide = _MODEL_REQUEST_PARAMETERS_ADAPTER.dump_python(model_request_parameters)
        messages_need_check = _check_guide(messages_guide)
        params_need_check = _check_guide(params_guide)
        dumped = ModelMessagesTypeAdapter.dump_python(messages, mode="json")
        params = _MODEL_REQUEST_PARAMETERS_ADAPTER.dump_python(model_request_parameters, mode="json")
        if messages_need_check:
            dumped = _order_sets(messages_guide, dumped)
        if params_need_check:
            params = _order_sets(params_guide, params)
        return _digest(
            {
                "model": model_identifier,
                "messages": _strip_volatile(dumped),
                "settings": _render(_content_settings(model_settings)),
                "params": params,
            }
        )
    except Exception as exc:
        # Rendering runs user code -- computed fields, serializers, custom mappings --
        # so anything can be raised here, and a fingerprint that cannot be computed
        # must cost this step a live re-run, never the task. For a value pydantic
        # cannot serialize, the error names its type, the only pointer to the
        # setting or message at fault.
        log.warning(
            "Durable: could not fingerprint model request; this step will not be cached and will "
            "execute live on retry. If the cause is in model settings, tool definitions or message "
            "history, later model steps of this run are usually affected too",
            step=step,
            error=str(exc),
            error_type=type(exc).__name__,
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
    live objects pydantic has already validated, and the tool has not run yet,
    so they are rendered from copies (see ``_render``): hashing must neither order
    a ``set`` unpredictably nor consume a lazily validated ``Iterable`` the tool
    has not read, and must not change what the tool will see.
    """
    try:
        return _digest(
            {"name": name, "args": _render(tool_args, copy_first=True), "tool_call_id": tool_call_id}
        )
    except Exception as exc:
        # As for model requests: whatever rendering the arguments raises costs this
        # step a live re-run, never the task.
        log.warning(
            "Durable: could not fingerprint tool call; this step will not be cached and will "
            "execute live on retry",
            error=str(exc),
            error_type=type(exc).__name__,
            tool=name,
            step=step,
        )
        return None
