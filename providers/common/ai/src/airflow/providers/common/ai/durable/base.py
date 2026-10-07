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

from collections.abc import Iterable
from typing import Any, Protocol, runtime_checkable

# Prefix for durable journal keys. On the task state store backend (>= 3.3) the
# journal shares the task instance's key namespace with anything user code writes
# via ``context["task_state_store"]``; the reserved prefix keeps durable steps
# from colliding with user keys. No ``/`` -- task state store keys are a single,
# un-encoded URL path segment.
DURABLE_KEY_PREFIX = "__commonai_durable__"


def build_step_key(run: str | int, position: int) -> str:
    """Build the journal key for the step at ``position`` of the agent run ``run``."""
    return f"{DURABLE_KEY_PREFIX}run_{run}_step_{position}"


def build_run_meta_key(run: str | int) -> str:
    """Build the journal key for what the agent run ``run`` keeps about itself across attempts."""
    return f"{DURABLE_KEY_PREFIX}run_{run}_meta"


# Holds the id the task's agent run keeps on every attempt; see ``DurableJournal.run_id``.
RUN_ID_KEY = f"{DURABLE_KEY_PREFIX}run_id"


@runtime_checkable
class DurableStorageProtocol(Protocol):
    """
    Persistence contract shared by the durable execution storage backends.

    Implemented by :class:`~airflow.providers.common.ai.durable.storage.DurableStorage`
    (ObjectStorage, Airflow < 3.3) and
    :class:`~airflow.providers.common.ai.durable.task_state_store.TaskStateStoreDurableStorage`
    (AIP-103 task state store, Airflow >= 3.3). The journal depends on this interface,
    not a concrete backend.

    An entry is a JSON-compatible dict. ``save_step`` returns whether the entry was
    written: a backend may skip a write (a value it cannot store, a store write that
    fails) without failing the step, which then runs again on retry.
    """

    def save_step(self, key: str, entry: dict[str, Any]) -> bool: ...

    def load_step(self, key: str) -> dict[str, Any] | None: ...

    def delete_steps(self, keys: Iterable[str]) -> None:
        """Delete these entries. Best-effort: runs after the work succeeded, so it must not raise."""
        ...
