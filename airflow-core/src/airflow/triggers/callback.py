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

import inspect
import logging
import traceback
from collections.abc import AsyncIterator
from typing import Any

from airflow._shared.module_loading import accepts_context, import_string, qualname
from airflow.models.callback import CallbackState
from airflow.triggers.base import BaseTrigger, TriggerEvent

log = logging.getLogger(__name__)

PAYLOAD_STATUS_KEY = "state"
PAYLOAD_BODY_KEY = "body"


def _is_notifier_class(callback: Any) -> bool:
    """
    Check if the callback is a BaseNotifier subclass (not an instance).

    Uses duck-typing (checks for ``async_notify`` and ``template_fields``)
    to avoid importing ``airflow.sdk`` in core.
    """
    return (
        inspect.isclass(callback)
        and hasattr(callback, "async_notify")
        and hasattr(callback, "template_fields")
        and hasattr(callback, "__await__")
    )


class CallbackTrigger(BaseTrigger):
    """Trigger that executes a callback function asynchronously."""

    supports_triggerer_queue: bool = False

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
        try:
            yield TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.RUNNING})
            callback = import_string(self.callback_path)

            # TODO: Fetch full context via the Execution API at execution time rather than
            #  relying on the minimal context passed from the scheduler in callback_kwargs.
            #  This will provide fresh context and use the standard Airflow Context object,
            #  avoiding DB bloat from serialized context in trigger kwargs.
            #  Tracked at: https://github.com/apache/airflow/pull/64984
            context = self.callback_kwargs.pop("context", None)

            # Render Jinja templates in kwargs for plain function callbacks.
            # Notifiers handle their own template rendering in __await__ via
            # render_template_fields(), so we skip rendering here for them.
            if context is not None and not _is_notifier_class(callback):
                self.callback_kwargs = self.render_template(self.callback_kwargs, context)

            if accepts_context(callback) and context is not None:
                result = await callback(**self.callback_kwargs, context=context)
            else:
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
