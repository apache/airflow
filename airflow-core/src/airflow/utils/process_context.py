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

import os
import sys
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Literal

_PROCESS_CONTEXT_OVERRIDE: ContextVar[str | None] = ContextVar(
    "_AIRFLOW_PROCESS_CONTEXT_OVERRIDE",
    default=None,
)


def get_process_context() -> str | None:
    """Return the current process context, preferring request-scoped overrides."""
    return _PROCESS_CONTEXT_OVERRIDE.get() or os.environ.get("_AIRFLOW_PROCESS_CONTEXT")


@contextmanager
def override_process_context(context: Literal["server", "client"]) -> Generator[None, None, None]:
    """Temporarily override the current process context for the active execution flow."""
    token = _PROCESS_CONTEXT_OVERRIDE.set(context)
    try:
        yield
    finally:
        _PROCESS_CONTEXT_OVERRIDE.reset(token)


def should_use_task_sdk_api_path() -> bool:
    """Return True when execution-context helpers should route through Task SDK APIs."""
    if get_process_context() == "server":
        return False

    task_runner_module = sys.modules.get("airflow.sdk.execution_time.task_runner")
    return bool(getattr(task_runner_module, "SUPERVISOR_COMMS", None))
