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

from airflow._shared.module_loading import accepts_context, import_string, qualname
from airflow.models.callback import CallbackState
from airflow.triggers.base import BaseTrigger, TriggerEvent

log = logging.getLogger(__name__)

PAYLOAD_STATUS_KEY = "state"
PAYLOAD_BODY_KEY = "body"


def _ensure_bundle_module_registered(callback_path: str) -> None:
    """
    Register an unusual_prefix_{hash}_{stem} module so import_string can find it.

    DAG files in bundles are loaded with a mangled module name by the DAG processor.
    The triggerer event loop doesn't run the DAG processor, so we must register the
    module manually before calling import_string.
    """
    import importlib.machinery
    import importlib.util
    import sys
    from pathlib import Path

    mod_name = callback_path.split(".")[0]
    if mod_name in sys.modules:
        return

    # Reconstruct stem: unusual_prefix_{40-char hex}_{stem} → {stem}.py
    parts = mod_name.split("_", 3)
    if len(parts) < 4:
        return
    stem = parts[3]

    try:
        from airflow.dag_processing.bundles.manager import DagBundlesManager

        for bundle in DagBundlesManager().get_all_dag_bundles():
            try:
                bundle.initialize()
                file_path = Path(bundle.path) / f"{stem}.py"
                if not file_path.exists():
                    continue
                loader = importlib.machinery.SourceFileLoader(mod_name, str(file_path))
                spec = importlib.util.spec_from_loader(mod_name, loader)
                module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
                sys.modules[mod_name] = module
                loader.exec_module(module)
                log.debug("Registered bundle module on triggerer", mod_name=mod_name, file=str(file_path))
                return
            except Exception:
                log.debug("Bundle did not contain module", bundle=bundle.name, stem=stem)
                continue
    except Exception:
        log.warning("Failed to register unusual_prefix module", mod_name=mod_name, exc_info=True)


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
        self.context: dict | None = None

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            qualname(self),
            {attr: getattr(self, attr) for attr in ("callback_path", "callback_kwargs")},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            yield TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.RUNNING})
            if self.callback_path.startswith("unusual_prefix_"):
                _ensure_bundle_module_registered(self.callback_path)
            callback = import_string(self.callback_path)

            # self.context is set by TriggerRunner from workload.dag_run_data before run() is called.
            context = self.context

            kwargs = dict(self.callback_kwargs)

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
