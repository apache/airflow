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
Capability ids in the request parameters are left out, since one without an
explicit id is random per run (see ``_normalize_params``).

That rendering is hashed as it is, with two exceptions.  Both are found by
dumping the payload a second time in pydantic's python mode, which has the same
shape as the JSON rendering but keeps sets as sets and dict keys as they are, and
wraps an iterator instead of reading it (see ``_check_guide``).  The members of a
set are sorted by their JSON encoding, because a set of strings iterates in an
order that follows the interpreter's hash seed and every task attempt is a fresh
process (see ``_order_sets``).  A list is sorted only where it holds exactly the
members of the set found at its place, so a list that a serializer produced is
left as the serializer produced it; a set that cannot be found in the rendering
is refused if its order follows the hash seed, since the digest would not be
reproduced on retry.  And a payload is refused rather than hashed if it would
render through an iterator, since rendering consumes it, or if distinct dict keys
render alike, such as ``1`` and ``"1"``, since two different payloads would then
share a digest.  Sorting changes the digest of a set whose order was already
stable, such as a set of integers, so a history holding one can re-run from that
step once after upgrading.

Python mode cannot dump a set of models or dataclass instances, or a dict keyed
by one, because it turns them into dicts, which cannot be hashed.  The message history
and the request parameters are then hashed from their JSON rendering alone, as
they were before, with nothing sorted and nothing refused.

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
import datetime
import enum
import hashlib
import json
import math
import uuid
from collections import deque
from collections.abc import Iterator
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal

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

# Exact types whose hash does not follow the interpreter's hash seed, so a set of
# them iterates in the same order in every process.
_SEED_FREE_TYPES = frozenset({bool, int, float, complex, type(None), Decimal, datetime.timedelta, uuid.UUID})

# Every way pydantic renders bytes as JSON. Bytes inside a model follow that model's
# ``ser_json_bytes``, which the python-mode dump does not record.
_BYTES_MODES: tuple[Literal["base64", "utf8", "hex"], ...] = ("base64", "utf8", "hex")


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


def _normalize_params(params_dump: dict[str, Any]) -> dict[str, Any]:
    """
    Drop capability ids and sort set-valued fields from dumped request parameters.

    A capability without an explicit ``id`` gets a random one per run (``<toolset:d0d75e>``),
    stamped on its tools' ``capability_id``, so hashing it would make every retry miss the
    cache. What the model sees of capabilities (the deferred-capability catalog in the
    instructions, tool visibility, revealed tool names) is hashed through other fields, so
    ``deferred_capability_ids`` is dropped too. A set dumps in iteration order, which differs
    between processes (``PYTHONHASHSEED``), and a retry runs in a new process.
    """
    cleaned = {k: v for k, v in params_dump.items() if k != "deferred_capability_ids"}
    cleaned["revealed_tool_names"] = sorted(cleaned["revealed_tool_names"])
    for key in ("function_tools", "output_tools"):
        cleaned[key] = [{k: v for k, v in tool.items() if k != "capability_id"} for tool in cleaned[key]]
    return cleaned


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


def _seed_free(member: Any) -> bool:
    """Whether the hash of a set member is the same in every process."""
    if type(member) in (tuple, frozenset):
        return all(_seed_free(item) for item in member)
    return type(member) in _SEED_FREE_TYPES


def _unpaired(guide: Any, rendered: Any) -> Any:
    """
    Return ``rendered`` as it is, for a part of the guide that does not line up with it.

    Raises ``TypeError`` if that part of the guide holds a set whose order follows
    the hash seed. Its members are somewhere in ``rendered`` in that order, and
    hashing them would cache a digest the next attempt cannot reproduce: the step
    would miss with a "diverged" warning on every retry instead of being reported
    as not cached. A set whose order is the same in every process is kept as
    rendered, which is what was always hashed.
    """
    pending = [guide]
    seen: set[int] = set()
    while pending:
        item = pending.pop()
        if type(item) in _LEAF_TYPES or id(item) in seen:
            continue
        seen.add(id(item))
        if isinstance(item, enum.Enum):
            pending.append(item.value)
        elif isinstance(item, dict):
            pending.extend(item.values())
        elif isinstance(item, (list, tuple, deque)):
            pending.extend(item)
        elif isinstance(item, (set, frozenset)):
            if len(item) > 1 and not all(_seed_free(member) for member in item):
                raise TypeError("cannot fingerprint a set that cannot be found in its JSON rendering")
    return rendered


def _order_set(guide: set[Any] | frozenset[Any], rendered: Any) -> Any:
    """
    Return ``rendered`` sorted, if it is the list of the members of the set ``guide``.

    That both dumps have a list and a set at the same place does not prove one came
    from the other: a serializer that applies only in JSON mode can put any list
    there, and sorting it would let two different payloads share a digest. So the
    set is rendered on its own, and ``rendered`` is sorted only where both give the
    same members once ordered. A set's members cannot pair up by position -- the
    dump builds a new set, which may iterate in another order -- so the members'
    shared template says where any set inside one of them sits (see ``_template``).
    """
    template = _member_template(guide)
    if template is None or not isinstance(rendered, list):
        return _unpaired(guide, rendered)
    ordered = sorted((_apply_template(template, item) for item in rendered), key=_json_order)
    encoded = [_json_order(member) for member in ordered]
    for bytes_mode in _BYTES_MODES:
        try:
            own = to_jsonable_python(guide, bytes_mode=bytes_mode)
        except ValueError:
            # Bytes that are not UTF-8 cannot have been rendered as UTF-8.
            continue
        if sorted(_json_order(_apply_template(template, member)) for member in own) == encoded:
            return ordered
    return _unpaired(guide, rendered)


def _order_sets(guide: Any, rendered: Any) -> Any:
    """
    Return ``rendered`` with every list that pydantic rendered from a set sorted.

    ``rendered`` is the json-mode rendering that is hashed, and ``guide`` the
    python-mode dump of the same payload (see ``_check_guide``). They are walked
    side by side: a dict or a sequence pairs up by position, because both dumps
    keep the same order, and a list is sorted where the guide holds a set with
    those members (see ``_order_set``). Only a serializer that applies in JSON mode
    alone can make the two differ, and what does not line up is kept exactly as
    rendered or refused (see ``_unpaired``), never reordered.

    Raises ``TypeError`` where distinct dict keys render alike, such as ``1`` and
    ``"1"``: hashing the rendering would let two different payloads share a digest.
    The keys are rendered on their own to tell that apart from a serializer that
    drops, renames or reorders entries, whose dict is kept as rendered. ``None``
    and ``nan`` collide only in a message dump, so a dict with a ``nan`` or ``inf``
    key that renders fewer keys is refused as well.
    """
    if isinstance(guide, enum.Enum):
        # An Enum member renders as its value.
        return _order_sets(guide.value, rendered)
    if isinstance(guide, (set, frozenset)):
        return _order_set(guide, rendered)
    if isinstance(guide, dict):
        if not isinstance(rendered, dict):
            return _unpaired(guide, rendered)
        keys = list(guide)
        if any(not isinstance(key, str) for key in keys):
            try:
                keys = list(to_jsonable_python(dict.fromkeys(keys), bytes_mode="base64"))
            except ValueError:
                # Keys only the enclosing schema can render, such as a ``PurePath``:
                # nothing says which entries the rendering kept.
                return _unpaired(guide, rendered)
            # The message dump renders ``nan`` and ``inf`` keys as ``None``, which
            # rendering the keys on their own does not show.
            unsure = len(rendered) < len(guide) and any(
                isinstance(key, float) and not math.isfinite(key) for key in guide
            )
            if len(keys) < len(guide) or unsure:
                raise TypeError("dict keys collide once rendered as JSON")
        if keys != list(rendered):
            # Not the entries the guide has, or not in its order: a value would be
            # paired with another entry's rendering.
            return _unpaired(guide, rendered)
        return {
            key: _order_sets(guide_item, item)
            for guide_item, (key, item) in zip(guide.values(), rendered.items())
        }
    if isinstance(guide, (list, tuple, deque)):
        if not isinstance(rendered, list) or len(rendered) != len(guide):
            return _unpaired(guide, rendered)
        return [_order_sets(guide_item, item) for guide_item, item in zip(guide, rendered)]
    return rendered


def _guide(adapter: TypeAdapter[Any], payload: Any) -> tuple[Any, bool]:
    """
    Dump the message history or the request parameters in python mode, and check the dump.

    Returns the guide and whether the JSON rendering needs ``_order_sets``. Python
    mode dumps a model or a dataclass to a dict even where it is a member of a set
    or a dict key, which fails with ``TypeError: unhashable type: 'dict'`` although
    JSON mode renders the same value. There is then no guide: the JSON rendering is
    hashed as it is, exactly as before, so nothing in it is sorted and an iterator
    in it is read rather than refused.
    """
    try:
        guide = adapter.dump_python(payload)
    except TypeError:
        return None, False
    return guide, _check_guide(guide)


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

    The ``ModelRequestParameters`` object is hashed (tool definitions, output
    mode and schema, native tools, ...) so any change to what is sent to the
    model invalidates the cached response; only capability ids are left out.

    Returns ``None`` when the request cannot be fingerprinted, which prevents the
    step from being replayed or cached. Model settings, the tool definitions in the
    request parameters and the message history are normally carried into every
    later request, so such a value in any of them usually degrades every
    subsequent model step of the run the same way. ``step`` is attached to the
    warning so the log names where it began.
    """
    try:
        # Messages and parameters are hashed from pydantic's json-mode dump, as they
        # always were, so stored fingerprints still match. The python-mode dumps are
        # taken first, since they refuse an iterator before the json-mode dump reads
        # it, and they guide ``_order_sets`` where a set or a non-string key needs it.
        messages_guide, messages_need_check = _guide(ModelMessagesTypeAdapter, messages)
        params_guide, params_need_check = _guide(_MODEL_REQUEST_PARAMETERS_ADAPTER, model_request_parameters)
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
                "params": _normalize_params(params),
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
