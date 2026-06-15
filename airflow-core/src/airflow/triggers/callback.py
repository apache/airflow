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

import asyncio
import logging
import sys
import traceback
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from airflow._shared.module_loading import (
    UNUSUAL_MODULE_PREFIX,
    accepts_context,
    import_string,
    load_mangled_dag_module,
    qualname,
)
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.models.callback import CallbackState
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.file import get_unique_dag_module_name

log = logging.getLogger(__name__)

PAYLOAD_STATUS_KEY = "state"
PAYLOAD_BODY_KEY = "body"


def _ensure_bundle_module_registered(callback_path: str) -> None:
    """
    Register an unusual_prefix_{hash}_{stem} module so import_string can find it.

    The triggerer event loop doesn't run the DAG processor, so DAG files with mangled
    module names must be registered manually. Walks all configured bundles to find the
    file, then delegates the actual loading to _load_mangled_module in callback_supervisor.

    The bundle directory is also added to ``sys.path`` so that a callback defined in a
    DAG file can import sibling helper modules living in the same bundle (e.g. a
    ``my_helpers.py`` next to the DAG). This mirrors the executor/sync callback path in
    ``callback_supervisor`` which appends the bundle path to ``sys.path`` before running
    the callback; without it, ``import my_helpers`` inside an async callback fails with
    ``ModuleNotFoundError`` on the triggerer.
    """
    mod_name = callback_path.split(".")[0]
    if mod_name in sys.modules:
        return

    # unusual_prefix_{hex40}_{stem}  →  {stem}.py
    parts = mod_name.split("_", 3)
    if len(parts) < 4:
        return
    stem = parts[3]

    try:
        for bundle in DagBundlesManager().get_all_dag_bundles():
            try:
                bundle.initialize()
                # Walk all .py files — stem may differ from filename due to sanitization
                # (get_unique_dag_module_name replaces . and - with _ in the stem)
                for file_path in Path(bundle.path).rglob("*.py"):
                    if get_unique_dag_module_name(str(file_path)) == mod_name:
                        # Put the bundle dir on sys.path so the callback can import sibling
                        # helper modules from the same bundle (parity with the sync path).
                        bundle_path = str(bundle.path)
                        if bundle_path not in sys.path:
                            sys.path.append(bundle_path)
                        if load_mangled_dag_module(mod_name, str(file_path)):
                            return
            except Exception:
                log.debug("Bundle %s did not contain module stem %s", bundle.name, stem)
                continue
    except Exception:
        log.warning("Failed to register unusual_prefix module %s", mod_name, exc_info=True)


class CallbackTrigger(BaseTrigger):
    """Trigger that executes a callback function asynchronously."""

    supports_triggerer_queue: bool = False

    def __init__(self, callback_path: str, callback_kwargs: dict[str, Any] | None = None):
        super().__init__()
        self.callback_path = callback_path
        self.callback_kwargs = callback_kwargs or {}
        #: Set externally by TriggerRunner from workload dag_run_data before run() is called,
        #: the same pattern as task_instance is set for task-bound triggers. Not serialized;
        #: always None on a freshly deserialized trigger.
        self._callback_context: dict | None = None

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            qualname(self),
            {attr: getattr(self, attr) for attr in ("callback_path", "callback_kwargs")},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            yield TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.RUNNING})
            if self.callback_path.startswith(UNUSUAL_MODULE_PREFIX):
                await asyncio.to_thread(_ensure_bundle_module_registered, self.callback_path)
            callback = import_string(self.callback_path)

            kwargs = dict(self.callback_kwargs)

            # ALWAYS strip any "context" stored in callback_kwargs (3.2.x back-compat, where the
            # context was serialized inside kwargs) so it is never double-passed alongside the
            # explicit ``context=`` below. ``pop`` must run unconditionally: a previous
            # ``self._callback_context or kwargs.pop(...)`` short-circuited the pop whenever the
            # runtime context was set, so a 3.2.x-serialized trigger that ALSO got a runtime
            # ``_callback_context`` (the TriggerRunner sets it unconditionally for callback triggers
            # with dag_run_data) kept the stale key and crashed with
            # "got multiple values for keyword argument 'context'". Pop first, then prefer the
            # runtime context over the stored one.
            stored_context = kwargs.pop("context", None)
            context = self._callback_context or stored_context

            if accepts_context(callback) and context is not None:
                result = await callback(**kwargs, context=context)
            else:
                result = await callback(**kwargs)

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
