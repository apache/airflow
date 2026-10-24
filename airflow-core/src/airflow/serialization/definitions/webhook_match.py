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
"""The ``cancel_on`` conditions of a serialized Dag and its tasks."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import attrs

from airflow._shared.webhook_naming import WEBHOOK_NAME_PATTERN
from airflow._shared.webhook_naming.matching import (
    MAX_CORRELATION_KEY_CONF_LENGTH,
    normalize_events,
    normalize_match,
)
from airflow.utils.state import TaskInstanceState

#: The state a task with ``cancel_on`` ends in when it names no ``cancel_state``.
DEFAULT_CANCEL_STATE = TaskInstanceState.FAILED.value
#: The states a task's ``cancel_state`` can name.
CANCEL_STATES = frozenset({TaskInstanceState.FAILED.value, TaskInstanceState.SKIPPED.value})


@attrs.frozen(kw_only=True)
class SerializedWebhookMatch:
    """
    A ``cancel_on`` condition of a Dag or a task, in the canonical form of the webhook match grammar.

    ``events`` is None for any event, or its sorted distinct values. ``match`` holds its keys in sorted
    order, each with the sorted distinct values it accepts, as pairs so the condition can be hashed.

    :meta private:
    """

    webhook: str
    events: tuple[str, ...] | None = None
    match: tuple[tuple[str, tuple[str, ...]], ...] = ()
    correlation_key_conf: str | None = None

    @classmethod
    def build(
        cls,
        *,
        webhook: Any,
        events: Any = None,
        match: Any = None,
        correlation_key_conf: Any = None,
    ) -> SerializedWebhookMatch:
        """
        Return the condition in canonical form.

        :raises ValueError: if the condition does not follow the rules the Task SDK checks when a Dag
            declares it.
        """
        if not isinstance(webhook, str) or not WEBHOOK_NAME_PATTERN.fullmatch(webhook):
            raise ValueError(f"Webhook name must match {WEBHOOK_NAME_PATTERN.pattern!r}, got {webhook!r}")
        if correlation_key_conf is not None:
            if not isinstance(correlation_key_conf, str) or not correlation_key_conf.strip():
                raise ValueError(
                    f"correlation_key_conf must name a top-level conf key, got {correlation_key_conf!r}"
                )
            if len(correlation_key_conf) > MAX_CORRELATION_KEY_CONF_LENGTH:
                raise ValueError(
                    f"correlation_key_conf cannot be longer than {MAX_CORRELATION_KEY_CONF_LENGTH} characters"
                )
        normalized_events = normalize_events(events)
        return cls(
            webhook=webhook,
            events=None if normalized_events is None else tuple(normalized_events),
            match=tuple((key, tuple(values)) for key, values in normalize_match(match).items()),
            correlation_key_conf=correlation_key_conf,
        )

    def to_json(self) -> dict[str, Any]:
        """Return the condition as serialized Dags store it, without the fields it leaves unset."""
        data: dict[str, Any] = {"webhook": self.webhook}
        if self.events is not None:
            data["events"] = list(self.events)
        if self.match:
            data["match"] = {key: list(values) for key, values in self.match}
        if self.correlation_key_conf is not None:
            data["correlation_key_conf"] = self.correlation_key_conf
        return data

    @property
    def digest(self) -> str:
        """The SHA-256 of the condition, the same for every way of writing it."""
        canonical = json.dumps(self.to_json(), sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return hashlib.sha256(canonical.encode("ascii")).hexdigest()
