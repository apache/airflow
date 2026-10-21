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
Webhook waits: the match grammar, its evaluator, and the events a wait resumes its task with.

Nothing here touches the database, so registering a wait, a delivery looking for waits and a wait
claiming an early delivery all apply the same rules.

A wait accepts a delivery when:

* ``events`` is None, or the delivery's event is one of its values;
* for every key of ``match``, the delivery's attribute of that name equals the key's value, or one of
  its values if the value is a list.

Values are strings or integers and compare as strings, so ``42`` and ``"42"`` accept the same
deliveries. A mapping as a value is refused, as it is reserved for match operators.

.. note:: |experimental|
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any

from airflow._shared.serialization import FORBIDDEN_XCOM_KEYS

if TYPE_CHECKING:
    from datetime import datetime

#: The key a failed match names when the delivery's event is not one of the wait's ``events``.
EVENT_KEY = "event"


@dataclass(frozen=True, kw_only=True)
class MatchResult:
    """
    Whether a delivery matches a wait and, if it does not, the first key it fails.

    :param matched: Whether the delivery matches.
    :param key: ``"event"`` if the delivery's event is not one of the wait's ``events``, otherwise the
        ``match`` key that failed.
    :param expected: The values the key accepts, as strings.
    :param actual: The delivery's value for the key as a string, or None if the delivery has none.
    """

    matched: bool
    key: str | None = None
    expected: list[str] | None = None
    actual: str | None = None


def _normalize_value(value: Any, *, name: str) -> str:
    # bool is an int, but no attribute holds one, so True could never match.
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"{name} must be a string or an integer, not {type(value).__name__}")
    return str(value)


def _normalize_events(events: Any) -> list[str] | None:
    if events is None:
        return None
    if not isinstance(events, (list, tuple)):
        raise ValueError(f"events must be a list of event values or None, not {type(events).__name__}")
    if not events:
        raise ValueError("events must not be empty; pass None to accept any event")
    return sorted({_normalize_value(event, name="events") for event in events})


def _normalize_match(match: Any) -> dict[str, list[str]]:
    if match is None:
        return {}
    if not isinstance(match, Mapping):
        raise ValueError(f"match must be a mapping of attribute names to values, not {type(match).__name__}")
    normalized: dict[str, list[str]] = {}
    for key, value in match.items():
        if not isinstance(key, str) or not key:
            raise ValueError(f"match keys must be non-empty strings, not {key!r}")
        name = f"match[{key!r}]"
        if isinstance(value, Mapping):
            raise ValueError(
                f"{name} must be a value or a list of values; mappings are reserved for operators"
            )
        if isinstance(value, (list, tuple)):
            if not value:
                raise ValueError(f"{name} must not be an empty list, which no delivery matches")
            normalized[key] = sorted({_normalize_value(item, name=name) for item in value})
        else:
            normalized[key] = [_normalize_value(value, name=name)]
    return dict(sorted(normalized.items()))


def validate_wait(events: Any, match: Any) -> None:
    """
    Raise ``ValueError`` unless ``events`` and ``match`` follow the match grammar.

    :param events: None for any event, or a non-empty list of event values.
    :param match: None or a mapping of attribute names to a value or a non-empty list of values.
    """
    _normalize_events(events)
    _normalize_match(match)


def _compute_digest(value: Any) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def compute_key_digest(match: Any) -> str:
    """
    Return the SHA-256 of the attribute names ``match`` uses, the same for every wait on that key set.

    :raises ValueError: if ``match`` does not follow the match grammar.
    """
    return _compute_digest(sorted(_normalize_match(match)))


def compute_match_digest(events: Any, match: Any) -> str:
    """
    Return the SHA-256 of a wait's ``events`` and ``match`` in canonical form.

    Waits that write the same events and match differently have the same digest: key and list order,
    repeated values, ``42`` against ``"42"`` and ``"a"`` against ``["a"]`` do not change it.

    :raises ValueError: if ``events`` or ``match`` does not follow the match grammar.
    """
    return _compute_digest({"events": _normalize_events(events), "match": _normalize_match(match)})


def evaluate(
    *, events: Any, match: Any, event: str | None, attributes: Mapping[str, Any] | None
) -> MatchResult:
    """
    Return whether a delivery matches a wait.

    The events filter is checked first, then the ``match`` keys in sorted order; the result names the
    first that fails.

    :param events: The wait's ``events``.
    :param match: The wait's ``match``.
    :param event: The delivery's event, or None if it has none.
    :param attributes: The delivery's attributes.
    :raises ValueError: if ``events`` or ``match`` does not follow the match grammar, which
        ``validate_wait`` refuses when the wait is registered.
    """
    expected_events = _normalize_events(events)
    if expected_events is not None:
        actual_event = None if event is None else str(event)
        if actual_event not in expected_events:
            return MatchResult(matched=False, key=EVENT_KEY, expected=expected_events, actual=actual_event)
    attributes = attributes or {}
    for key, expected in _normalize_match(match).items():
        value = attributes.get(key)
        actual = None if value is None else str(value)
        if actual not in expected:
            return MatchResult(matched=False, key=key, expected=expected, actual=actual)
    return MatchResult(matched=True)


def _encode_non_json(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _is_reserved_key(key: str) -> bool:
    return any(
        key.endswith(reserved) and not key[: -len(reserved)].strip("_") for reserved in FORBIDDEN_XCOM_KEYS
    )


def _escape_reserved_keys(value: Any) -> None:
    # A loop rather than recursion: a payload may nest deeper than the recursion limit allows.
    pending = [value]
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            if any(_is_reserved_key(key) for key in node):
                escaped = {f"_{key}" if _is_reserved_key(key) else key: item for key, item in node.items()}
                node.clear()
                node.update(escaped)
            pending.extend(node.values())
        elif isinstance(node, list):
            pending.extend(node)


def build_resume_event(
    *,
    webhook: str,
    event: str | None,
    delivery_id: str | None,
    received_at: datetime,
    attributes: Mapping[str, Any] | None,
    payload: Any,
) -> dict[str, Any]:
    """
    Return the event a matching delivery resumes a wait's task with.

    The event is serialized into the task instance's ``next_kwargs`` with serde, and an event serde
    cannot encode fails the task instead of resuming it, so the event is made JSON that serde encodes
    and decodes unchanged:

    * ``received_at`` and other dates become ISO 8601 strings, and any other value JSON cannot hold
      becomes its string;
    * a mapping key serde reserves for its own encoding (one of ``FORBIDDEN_XCOM_KEYS``, such as
      ``__classname__``, ``__id__`` or ``__type``), which serde refuses to encode or decodes as its
      own, gets one more leading underscore. So does such a key that already has extra leading
      underscores, which keeps every key of a mapping distinct.

    :param webhook: The webhook's name.
    :param event: The delivery's event, or None if it has none.
    :param delivery_id: The sender's id for the delivery, or None.
    :param received_at: When the delivery was received.
    :param attributes: The delivery's attributes.
    :param payload: The delivery's payload as stored, which ``store_payload`` bounds, or None.
    """
    resume_event = json.loads(
        json.dumps(
            {
                "webhook": webhook,
                "event": event,
                "delivery_id": delivery_id,
                "received_at": received_at,
                "attributes": attributes,
                "payload": payload,
            },
            default=_encode_non_json,
        )
    )
    _escape_reserved_keys(resume_event)
    return resume_event


def build_timeout_event(deadline: datetime) -> dict[str, Any]:
    """Return the event the timeout sweep resumes a wait's task with when nothing matched before ``deadline``."""
    return {"error": f"No matching delivery arrived before {deadline.isoformat()}", "error_type": "timeout"}
