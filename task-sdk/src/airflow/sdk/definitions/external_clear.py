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
"""
Cross-Dag clear propagation.

An operator that defines ``get_external_clear_target(context)`` returning an :class:`ExternalClearTarget`
asks Airflow to also clear that task, and everything downstream of it, whenever this task is cleared with
its downstream tasks. The task runner calls the method after a successful run, once templates have rendered,
and records the result so the API server can follow it without running user code.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context

__all__ = ["EXTERNAL_CLEAR_TARGET_XCOM_KEY", "ExternalClearTarget", "SupportsExternalClearTarget"]

EXTERNAL_CLEAR_TARGET_XCOM_KEY = "_external_clear_target"
"""XCom key the task runner records a resolved :class:`ExternalClearTarget` under."""


@runtime_checkable
class SupportsExternalClearTarget(Protocol):
    """An operator that names a task in another Dag to clear along with itself."""

    def get_external_clear_target(self, context: Context) -> ExternalClearTarget: ...


@dataclass(frozen=True)
class ExternalClearTarget:
    """
    A task in another Dag that is cleared together with the task that declares it.

    :param dag_id: The Dag holding the task to clear.
    :param task_id: The task to clear. Its downstream tasks in that Dag are cleared with it.
    :param logical_date: The logical date of the Dag run to clear in.
    :param recursion_depth: How many levels of these links a clear follows when it starts here, which
        stops a cycle of links from recursing forever.
    """

    dag_id: str
    task_id: str
    logical_date: datetime
    recursion_depth: int = 10

    def __post_init__(self) -> None:
        if self.logical_date.tzinfo is None:
            raise ValueError("logical_date must be timezone-aware")
        if self.recursion_depth <= 0:
            raise ValueError("recursion_depth must be a positive integer")

    def serialize(self) -> dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "logical_date": self.logical_date.isoformat(),
            "recursion_depth": self.recursion_depth,
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> ExternalClearTarget:
        return cls(
            dag_id=data["dag_id"],
            task_id=data["task_id"],
            logical_date=datetime.fromisoformat(data["logical_date"]),
            recursion_depth=data["recursion_depth"],
        )
