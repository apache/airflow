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

import sys
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar

__all__ = [
    "force_server_context",
    "should_use_task_sdk_api_path",
]

_FORCE_SERVER_CONTEXT: ContextVar[bool] = ContextVar("_AIRFLOW_FORCE_SERVER_CONTEXT", default=False)


@contextmanager
def force_server_context() -> Generator[None, None, None]:
    """Handle the active execution flow as server-side, even if ``SUPERVISOR_COMMS`` is set."""
    token = _FORCE_SERVER_CONTEXT.set(True)
    try:
        yield
    finally:
        _FORCE_SERVER_CONTEXT.reset(token)


def should_use_task_sdk_api_path() -> bool:
    """Return True when execution-context helpers should route through Task SDK APIs."""
    # Only the ContextVar, never the ``_AIRFLOW_PROCESS_CONTEXT`` env var: that env var is
    # process-wide and inherited by children (``action_cli`` sets it around the whole
    # ``airflow dags test`` body, and PythonVirtualenvOperator passes it to the venv child), so
    # letting it win here would send worker-side code straight to the metastore. ``SUPERVISOR_COMMS``
    # keeps precedence over it, matching ``ensure_secrets_backend_loaded()`` in the Task SDK.
    if _FORCE_SERVER_CONTEXT.get():
        return False

    task_runner_module = sys.modules.get("airflow.sdk.execution_time.task_runner")
    return bool(getattr(task_runner_module, "SUPERVISOR_COMMS", None))
