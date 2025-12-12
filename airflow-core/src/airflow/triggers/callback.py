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
import traceback
from collections.abc import AsyncIterator
from typing import Any

from airflow._shared.module_loading import import_string, qualname
from airflow.configuration import conf
from airflow.models.callback import CallbackState
from airflow.triggers.base import BaseTrigger, TriggerEvent

log = logging.getLogger(__name__)

PAYLOAD_STATUS_KEY = "state"
PAYLOAD_BODY_KEY = "body"


class CallbackTrigger(BaseTrigger):
    """Trigger that executes a callback function asynchronously."""

    def __init__(
        self,
        callback_path: str,
        callback_kwargs: dict[str, Any] | None = None,
        trigger_queue: str | None = None,
    ):
        super().__init__()
        self.callback_path = callback_path
        self.callback_kwargs = callback_kwargs or {}
        self.trigger_queue = trigger_queue or conf.get("triggerer", "default_trigger_queue")

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            qualname(self),
            {attr: getattr(self, attr) for attr in ("callback_path", "callback_kwargs")},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            yield TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.RUNNING})
            callback = import_string(self.callback_path)

            # TODO: get full context and run template rendering. Right now, a simple context in included in `callback_kwargs`
            result = await callback(**self.callback_kwargs)
            yield TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.SUCCESS, PAYLOAD_BODY_KEY: result})

        except Exception as e:
            if isinstance(e, ImportError):
                message = "Failed to import the callable on the triggerer"
            elif isinstance(e, TypeError) and "await" in str(e):
                message = "Failed to run the callable because it's not awaitable"
            else:
                message = "An error occurred during execution of the callable"

            log.exception("%s: %s; kwargs: %s\n%s", message, self.callback_path, self.callback_kwargs, e)
            yield TriggerEvent(
                {
                    PAYLOAD_STATUS_KEY: CallbackState.FAILED,
                    PAYLOAD_BODY_KEY: f"{message}: {traceback.format_exception(e)}",
                }
            )
