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
The events a webhook wait resumes its task with.

Which deliveries a wait accepts is decided by the match grammar in
``airflow._shared.webhook_naming.matching``.

.. note:: |experimental|
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from typing import TYPE_CHECKING, Any

from airflow._shared.serialization import FORBIDDEN_XCOM_KEYS

if TYPE_CHECKING:
    from datetime import datetime


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
