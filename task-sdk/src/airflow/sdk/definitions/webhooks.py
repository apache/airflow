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
"""Conditions on inbound webhook deliveries that Dags and tasks declare."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from typing import Any, Literal

import attrs

from airflow.sdk._shared.webhook_naming import WEBHOOK_NAME_PATTERN
from airflow.sdk._shared.webhook_naming.matching import (
    MAX_CORRELATION_KEY_CONF_LENGTH,
    compute_match_digest,
    normalize_events,
    normalize_match,
)

__all__ = [
    "CANCEL_STATES",
    "CancelState",
    "WebhookMatch",
    "coerce_cancel_on",
    "coerce_cancel_state",
]

CancelState = Literal["failed", "skipped"]
CANCEL_STATES: tuple[CancelState, ...] = ("failed", "skipped")


def _validate_webhook(_instance: Any, _attribute: Any, webhook: Any) -> None:
    if not isinstance(webhook, str) or not WEBHOOK_NAME_PATTERN.fullmatch(webhook):
        raise ValueError(f"Webhook name must match {WEBHOOK_NAME_PATTERN.pattern!r}, got {webhook!r}")


def _convert_events(events: Sequence[str | int] | None) -> list[str] | None:
    return normalize_events(events)


def _convert_match(match: Mapping[str, str | int | Sequence[str | int]] | None) -> dict[str, list[str]]:
    return normalize_match(match)


def _validate_correlation_key_conf(_instance: Any, _attribute: Any, key: Any) -> None:
    if key is None:
        return
    if not isinstance(key, str) or not key.strip():
        raise ValueError(f"correlation_key_conf must name a top-level conf key, got {key!r}")
    if len(key) > MAX_CORRELATION_KEY_CONF_LENGTH:
        raise ValueError(
            f"correlation_key_conf cannot be longer than {MAX_CORRELATION_KEY_CONF_LENGTH} characters"
        )


@attrs.frozen
class WebhookMatch:
    """
    A condition on deliveries to an inbound webhook, which a Dag or a task declares in ``cancel_on``.

    A delivery to ``webhook`` matches when its event is one of ``events`` and its attributes match
    ``match``, with the rules of webhook waits: values compare as strings, so ``42`` and ``"42"`` match
    the same deliveries, a list accepts any of its values, and every key of ``match`` must match.

    Declared on a Dag, a matching delivery fails the runs that are about the delivery's correlation key;
    declared on a task, it ends that task in those runs. A run is about a key when a webhook event with
    that key started it, or when its conf holds the key under ``correlation_key_conf``.

    ``events`` and ``match`` are kept sorted and without repeats, with their values as strings, so
    conditions that differ only in order, in repeated values or in ``42`` against ``"42"`` are equal. A
    condition that does not follow these rules is refused when it is created.

    .. note:: |experimental|

    :param webhook: The webhook's name, as listed in ``[webhooks] names``.
    :param events: The delivery events that match, as a list of strings or integers, or None to match
        any event.
    :param match: The attributes a delivery must have, each a value or a list of values of which one
        must match.
    :param correlation_key_conf: The top-level conf key that holds, as a string, the correlation key of a
        run that no webhook event started, such as a manual run. With None, only runs that a webhook
        event started can be cancelled.
    """

    webhook: str = attrs.field(validator=_validate_webhook)
    # Lists and dicts cannot be hashed; equal conditions still hash alike, as they share the other fields.
    events: list[str] | None = attrs.field(default=None, kw_only=True, converter=_convert_events, hash=False)
    match: dict[str, list[str]] = attrs.field(
        default=None, kw_only=True, converter=_convert_match, hash=False
    )
    correlation_key_conf: str | None = attrs.field(
        default=None, kw_only=True, validator=_validate_correlation_key_conf
    )


def _get_sort_key(condition: WebhookMatch) -> tuple[str, str, str]:
    return (
        condition.webhook,
        condition.correlation_key_conf or "",
        compute_match_digest(condition.events, condition.match),
    )


def coerce_cancel_on(
    cancel_on: WebhookMatch | Collection[WebhookMatch] | None,
) -> list[WebhookMatch] | None:
    """
    Return ``cancel_on`` as a list without repeats in a fixed order, or None if it holds no condition.

    The order does not depend on how the conditions were given, so a Dag file parsed twice serializes
    them the same way.

    :raises TypeError: if ``cancel_on`` is not a ``WebhookMatch`` or a collection of them.
    """
    if cancel_on is None:
        return None
    conditions = [cancel_on] if isinstance(cancel_on, WebhookMatch) else cancel_on
    if isinstance(conditions, (str, bytes, Mapping)) or not isinstance(conditions, Collection):
        raise TypeError(f"cancel_on must be a WebhookMatch or a list of them, not {type(cancel_on).__name__}")
    for condition in conditions:
        if not isinstance(condition, WebhookMatch):
            raise TypeError(f"cancel_on takes WebhookMatch conditions, not {type(condition).__name__}")
    return sorted(set(conditions), key=_get_sort_key) or None


def coerce_cancel_state(cancel_state: str | None) -> CancelState | None:
    """
    Return ``cancel_state`` as a plain string, or None if it is not set.

    String enums such as ``TaskInstanceState.SKIPPED`` are accepted.

    :raises ValueError: if ``cancel_state`` is neither ``"failed"`` nor ``"skipped"``.
    """
    if cancel_state is None:
        return None
    for state in CANCEL_STATES:
        if cancel_state == state:
            return state
    raise ValueError(f"cancel_state must be 'failed' or 'skipped', got {cancel_state!r}")
