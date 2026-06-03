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

from airflow._shared.module_loading import accepts_context, import_string, load_mangled_dag_module, qualname
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
                # Walk the bundle tree to handle nested DAG files (e.g. dags/team_a/my_dag.py)
                for file_path in Path(bundle.path).rglob(f"{stem}.py"):
                    if get_unique_dag_module_name(str(file_path)) == mod_name:
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
            if self.callback_path.startswith("unusual_prefix_"):
                await asyncio.to_thread(_ensure_bundle_module_registered, self.callback_path)
            callback = import_string(self.callback_path)

            kwargs = dict(self.callback_kwargs)

            # Prefer the runtime context injected by TriggerRunner; fall back to a stored
            # context from callback_kwargs (3.2.x backward compat where context was serialized
            # inside kwargs). Strip "context" from kwargs so it doesn't get passed as an
            # unexpected keyword argument to callables that don't accept it.
            context = self._callback_context or kwargs.pop("context", None)

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
