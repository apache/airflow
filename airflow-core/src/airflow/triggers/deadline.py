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

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.module_loading import import_string, qualname

log = logging.getLogger(__name__)

PAYLOAD_STATUS_KEY = "state"
PAYLOAD_BODY_KEY = "body"


class DeadlineCallbackTrigger(BaseTrigger):
    """Trigger that executes a deadline callback function asynchronously."""

    def __init__(self, callback_path: str, callback_kwargs: dict[str, Any] | None = None):
        super().__init__()
        self.callback_path = callback_path
        self.callback_kwargs = callback_kwargs or {}

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            qualname(self),
            {attr: getattr(self, attr) for attr in ("callback_path", "callback_kwargs")},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        from airflow.models.deadline import DeadlineCallbackState  # to avoid cyclic imports

        try:
            callback = import_string(self.callback_path)
            result = await callback(**self.callback_kwargs)
            log.info("Deadline callback completed with return value: %s", result)
            yield TriggerEvent({PAYLOAD_STATUS_KEY: DeadlineCallbackState.SUCCESS, PAYLOAD_BODY_KEY: result})
        except Exception as e:
            if isinstance(e, ImportError):
                message = "Could not import deadline callback on the triggerer"
            elif isinstance(e, TypeError) and "await" in str(e):
                message = "Deadline callback not awaitable"
            else:
                message = "An error occurred while executing deadline callback"
            log.exception("%s: %s", message, e)
            yield TriggerEvent(
                {PAYLOAD_STATUS_KEY: DeadlineCallbackState.FAILED, PAYLOAD_BODY_KEY: f"{message}: {e}"}
            )
