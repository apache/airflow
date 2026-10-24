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
The match grammar of conditions on webhook deliveries, and its evaluator.

A condition on deliveries to a webhook (a webhook wait, or a ``cancel_on`` condition) accepts a
delivery when:

* ``events`` is None, or the delivery's event is one of its values;
* for every key of ``match``, the delivery's attribute of that name equals the key's value, or one of
  its values if the value is a list.

Values are strings or integers and compare as strings, so ``42`` and ``"42"`` accept the same
deliveries. A mapping as a value is refused, as it is reserved for match operators.

Shared so the Task SDK and the API server apply the same rules. Nothing here touches the database.

.. note:: |experimental|
"""

from __future__ import annotations

import hashlib
import itertools
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Collection

#: The key a failed match names when the delivery's event is not one of the condition's ``events``.
EVENT_KEY = "event"

#: The longest conf key that a ``cancel_on`` condition's ``correlation_key_conf`` may name, shared so
#: that the Task SDK's check and the column that stores the conditions use one bound.
MAX_CORRELATION_KEY_CONF_LENGTH = 250


@dataclass(frozen=True, kw_only=True)
class MatchResult:
    """
    Whether a delivery matches a condition and, if it does not, the first key it fails.

    :param matched: Whether the delivery matches.
    :param key: ``"event"`` if the delivery's event is not one of the condition's ``events``,
        otherwise the ``match`` key that failed.
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


def normalize_events(events: Any) -> list[str] | None:
    """
    Return ``events`` as a sorted list of distinct strings, or None for any event.

    :param events: None for any event, or a non-empty list of event values.
    :raises ValueError: if ``events`` does not follow the match grammar.
    """
    if events is None:
        return None
    if not isinstance(events, (list, tuple)):
        raise ValueError(f"events must be a list of event values or None, not {type(events).__name__}")
    if not events:
        raise ValueError("events must not be empty; pass None to accept any event")
    return sorted({_normalize_value(event, name="events") for event in events})


def normalize_match(match: Any) -> dict[str, list[str]]:
    """
    Return ``match`` as a dict with sorted keys, each mapped to a sorted list of distinct strings.

    :param match: None, which becomes ``{}``, or a mapping of attribute names to a value or a
        non-empty list of values.
    :raises ValueError: if ``match`` does not follow the match grammar.
    """
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


def validate_match(events: Any, match: Any) -> None:
    """
    Raise ``ValueError`` unless ``events`` and ``match`` follow the match grammar.

    :param events: None for any event, or a non-empty list of event values.
    :param match: None or a mapping of attribute names to a value or a non-empty list of values.
    """
    normalize_events(events)
    normalize_match(match)


def _compute_digest(value: Any) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def compute_key_digest(match: Any) -> str:
    """
    Return the SHA-256 of the attribute names ``match`` uses, the same for every condition on that key set.

    :raises ValueError: if ``match`` does not follow the match grammar.
    """
    return _compute_digest(sorted(normalize_match(match)))


def compute_covered_key_digests(names: Collection[str]) -> list[str]:
    """
    Return the key digest of every set of attribute names within ``names``, the empty set included.

    A condition matches only a delivery that has every attribute its ``match`` names, so these are the
    key digests of the conditions that a delivery with attributes ``names`` can match. There are
    ``2 ** n`` of them for ``n`` names.
    """
    ordered = sorted(set(names))
    return [
        _compute_digest(list(subset))
        for size in range(len(ordered) + 1)
        for subset in itertools.combinations(ordered, size)
    ]


def compute_match_digest(events: Any, match: Any) -> str:
    """
    Return the SHA-256 of a condition's ``events`` and ``match`` in canonical form.

    Conditions that write the same events and match differently have the same digest: key and list
    order, repeated values, ``42`` against ``"42"`` and ``"a"`` against ``["a"]`` do not change it.

    :raises ValueError: if ``events`` or ``match`` does not follow the match grammar.
    """
    return _compute_digest({"events": normalize_events(events), "match": normalize_match(match)})


def evaluate(
    *, events: Any, match: Any, event: str | None, attributes: Mapping[str, Any] | None
) -> MatchResult:
    """
    Return whether a delivery matches a condition.

    The events filter is checked first, then the ``match`` keys in sorted order; the result names the
    first that fails.

    :param events: The condition's ``events``.
    :param match: The condition's ``match``.
    :param event: The delivery's event, or None if it has none.
    :param attributes: The delivery's attributes.
    :raises ValueError: if ``events`` or ``match`` does not follow the match grammar, which
        ``validate_match`` refuses when the condition is declared.
    """
    expected_events = normalize_events(events)
    if expected_events is not None:
        actual_event = None if event is None else str(event)
        if actual_event not in expected_events:
            return MatchResult(matched=False, key=EVENT_KEY, expected=expected_events, actual=actual_event)
    attributes = attributes or {}
    for key, expected in normalize_match(match).items():
        value = attributes.get(key)
        actual = None if value is None else str(value)
        if actual not in expected:
            return MatchResult(matched=False, key=key, expected=expected, actual=actual)
    return MatchResult(matched=True)
