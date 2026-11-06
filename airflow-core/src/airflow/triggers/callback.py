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


class InvalidCallbackPathError(TypeError):
    """Raised when a callback path does not resolve to a usable triggerer callback."""


def _validate_callback(callback: Any, callback_path: str) -> None:
    """
    Check that the object resolved from ``callback_path`` can be used as a triggerer callback.

    It accepts exactly what ``AsyncCallback.verify_callable`` in the Task SDK accepts when the
    callback is defined, and is run again because the path is read back from the database. Core
    cannot import that check from the SDK, so keep the two in step. The path is validated before
    the callable is invoked, the same way trigger classpaths are validated before a trigger is
    instantiated.
    """
    if not callable(callback):
        raise InvalidCallbackPathError(f"The callback path {callback_path!r} does not resolve to a callable.")
    if not (inspect.iscoroutinefunction(callback) or hasattr(callback, "__await__")):
        raise InvalidCallbackPathError(
            f"The callback path {callback_path!r} does not resolve to a coroutine function or "
            f"an awaitable callable; refusing to run it on the triggerer."
        )


class CallbackTrigger(BaseTrigger):
    """Trigger that executes a callback function asynchronously."""

    # CallbackTrigger sets its own `queue` directly (see `__init__` below), so
    # `_defer_task` must not overwrite it with the deferring task's queue.
    trigger_queue_inherited_from_task: bool = False

    def __init__(
        self,
        callback_path: str,
        callback_kwargs: dict[str, Any] | None = None,
        *,
        queue: str | None = None,
    ):
        super().__init__()
        self.callback_path = callback_path
        self.callback_kwargs = callback_kwargs or {}
        # Read by Trigger.from_object() when persisting the row; unused once running.
        self.queue = queue

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            qualname(self),
            {attr: getattr(self, attr) for attr in ("callback_path", "callback_kwargs")},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            yield TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.RUNNING})
            callback = import_string(self.callback_path)
            _validate_callback(callback, self.callback_path)
            # TODO: get full context and run template rendering. Right now, a simple context is included in `callback_kwargs`
            context = self.callback_kwargs.pop("context", None)

            if accepts_context(callback) and context is not None:
                result = await callback(**self.callback_kwargs, context=context)
            else:
                result = await callback(**self.callback_kwargs)

            yield TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.SUCCESS, PAYLOAD_BODY_KEY: result})

        except Exception as e:
            if isinstance(e, ImportError):
                message = "Failed to import the callable on the triggerer"
            elif isinstance(e, InvalidCallbackPathError):
                message = "The callback path does not resolve to a valid callback"
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
