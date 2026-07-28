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
"""Shared interface for durable execution storage backends."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pydantic_ai.messages import ModelResponse

# Marks a stored entry as a cached tool result; lets ``load_tool_result``
# tell a cached ``None`` apart from a missing entry. Single source of truth so
# the two backends cannot drift on the envelope shape.
TOOL_RESULT_SENTINEL = "__durable_cached__"

# Prefix for durable cache keys. On the task state store backend (>= 3.3) the
# cache shares the task instance's key namespace with anything user code writes
# via ``context["task_state_store"]``; the reserved prefix keeps durable steps
# from colliding with user keys. No ``/`` -- task state store keys are a single,
# un-encoded URL path segment.
DURABLE_KEY_PREFIX = "__commonai_durable__"


@runtime_checkable
class DurableStorageProtocol(Protocol):
    """
    Persistence contract shared by the durable execution storage backends.

    Implemented by both :class:`~airflow.providers.common.ai.durable.storage.DurableStorage`
    (ObjectStorage, Airflow < 3.3) and
    :class:`~airflow.providers.common.ai.durable.task_state_store.TaskStateStoreDurableStorage`
    (AIP-103 task state store, Airflow >= 3.3). ``CachingModel`` and
    ``CachingToolset`` depend on this interface, not a concrete backend.
    """

    def save_model_response(self, key: str, response: ModelResponse, *, fingerprint: str | None) -> None: ...

    def load_model_response(self, key: str) -> tuple[ModelResponse | None, str | None]: ...

    def save_tool_result(self, key: str, result: Any, *, fingerprint: str | None) -> None: ...

    def load_tool_result(self, key: str) -> tuple[bool, Any, str | None]: ...

    def cleanup(self) -> None: ...
