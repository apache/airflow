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
"""Register a running task instance's wait for a delivery to an inbound webhook."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.sdk.exceptions import ErrorType, WebhookWaitUnsupported
from airflow.sdk.execution_time.comms import ErrorResponse, InboundWaitResult, RegisterInboundWait


def _convert_sequence_to_list(value: Any) -> Any:
    # The request holds JSON, which has no tuple, range or LazyXComSequence. A string is a sequence
    # too, but of characters, never of values.
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return value


def register_inbound_wait(
    *, webhook: str, events: Any = None, match: Any = None, timeout: timedelta | None = None
) -> InboundWaitResult:
    """
    Register the running task instance's wait for a delivery to an inbound webhook.

    The API server alone checks ``events``, ``match`` and ``timeout`` against the match grammar and
    its bounds, so the worker never refuses a wait that a newer API server accepts.

    :param webhook: The webhook's name, as listed in ``[webhooks] names``.
    :param events: The delivery events the wait accepts, as a sequence, or None for any event.
    :param match: The delivery attributes the wait requires: a mapping of each name to a value, or to a
        sequence of values of which one must match.
    :param timeout: How long to wait, or None for ``[webhooks] default_wait_timeout``.
    :return: The registered wait, with its deadline.
    :raises TypeError: if ``timeout`` is neither a ``timedelta`` nor None.
    :raises ValueError: if ``events`` is a string or not a sequence, ``match`` is not a mapping, or
        either holds a value that JSON cannot carry.
    :raises WebhookWaitUnsupported: if the API server cannot hold webhook waits.
    :raises AirflowRuntimeError: if the API server refuses the wait.
    """
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    # The request would read a number as seconds and a string as an ISO 8601 duration.
    if timeout is not None and not isinstance(timeout, timedelta):
        raise TypeError(f"timeout must be a timedelta or None, not {type(timeout).__name__}")
    if isinstance(match, Mapping):
        match = {key: _convert_sequence_to_list(value) for key, value in match.items()}
    resp = SUPERVISOR_COMMS.send(
        RegisterInboundWait(
            webhook=webhook, events=_convert_sequence_to_list(events), match=match, timeout=timeout
        )
    )
    if isinstance(resp, ErrorResponse) and resp.error == ErrorType.WEBHOOK_WAIT_UNSUPPORTED:
        raise WebhookWaitUnsupported((resp.detail or {}).get("message"))
    if TYPE_CHECKING:
        assert isinstance(resp, InboundWaitResult)
    return resp
