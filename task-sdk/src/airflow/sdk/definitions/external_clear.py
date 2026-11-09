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

An operator asks Airflow to also clear a task in another Dag, and everything downstream of it, whenever the
operator's task is cleared with its downstream tasks.

An operator whose target is known once the Dag is parsed exposes it as ``external_clear_target``
(:class:`DeclaresExternalClearTarget`), and Airflow stores it with the Dag version, so the task never has to
run for it. Otherwise the operator defines ``get_external_clear_target(context)``
(:class:`SupportsExternalClearTarget`), which the task runner calls after a successful run, once templates
have rendered, and reports with the task's success. Either way the API server follows the target without
running user code.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context

__all__ = ["DeclaresExternalClearTarget", "ExternalClearTarget", "SupportsExternalClearTarget"]


@runtime_checkable
class DeclaresExternalClearTarget(Protocol):
    """An operator that can name a task in another Dag to clear along with itself when the Dag is parsed."""

    @property
    def external_clear_target(self) -> ExternalClearTarget | None:
        """
        The target, or ``None`` when it depends on the run. Takes precedence over a reported target.

        Read by the Dag processor, so an exception here fails the Dag's import. Not read for mapped tasks.
        """
        ...


@runtime_checkable
class SupportsExternalClearTarget(Protocol):
    """An operator that names a task in another Dag to clear along with itself, resolved when it runs."""

    def get_external_clear_target(self, context: Context) -> ExternalClearTarget | None:
        """Resolve the target after a successful run. Returning ``None`` reports nothing for that run."""
        ...


@dataclass(frozen=True)
class ExternalClearTarget:
    """
    A task in another Dag that is cleared together with the task that declares it.

    :param dag_id: The Dag holding the task to clear.
    :param task_id: The task to clear. Its downstream tasks in that Dag are cleared with it.
    :param logical_date: The logical date of the Dag run to clear in. ``None`` means the logical date of the
        run the declaring task belongs to; a run without one has nothing to clear.
    :param recursion_depth: How many levels of these links a clear follows when it starts here, which
        keeps a cycle of links from being followed forever.
    """

    dag_id: str
    task_id: str
    logical_date: datetime | None = None
    recursion_depth: int = 10

    def __post_init__(self) -> None:
        if self.logical_date is not None and self.logical_date.tzinfo is None:
            raise ValueError("logical_date must be timezone-aware")
        if self.recursion_depth <= 0:
            raise ValueError("recursion_depth must be a positive integer")

    def serialize(self) -> dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "logical_date": None if self.logical_date is None else self.logical_date.isoformat(),
            "recursion_depth": self.recursion_depth,
        }
