#
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
"""Priority weight strategies for task scheduling."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


class PriorityWeightStrategy(ABC):
    """Priority weight strategy interface."""

    @abstractmethod
    def get_weight(self, ti: TaskInstance):
        """Get the priority weight of a task."""
        ...

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> PriorityWeightStrategy:
        """Deserialize a priority weight strategy from data.

        This is called when a serialized DAG is deserialized. ``data`` will be whatever
        was returned by ``serialize`` during DAG serialization. The default
        implementation constructs the priority weight strategy without any arguments.
        """
        return cls()

    def serialize(self) -> dict[str, Any]:
        """Serialize the priority weight strategy for JSON encoding.

        This is called during DAG serialization to store priority weight strategy information
        in the database. This should return a JSON-serializable dict that will be fed into
        ``deserialize`` when the DAG is deserialized. The default implementation returns
        an empty dict.
        """
        return {}

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type(self)):
            return False
        return self.serialize() == other.serialize()


class AbsolutePriorityWeightStrategy(PriorityWeightStrategy):
    """Priority weight strategy that uses the task's priority weight directly."""

    def get_weight(self, ti: TaskInstance):
        return ti.task.priority_weight


class DownstreamPriorityWeightStrategy(PriorityWeightStrategy):
    """Priority weight strategy that uses the sum of the priority weights of all downstream tasks."""

    def get_weight(self, ti: TaskInstance):
        dag = ti.task.get_dag()
        if dag is None:
            return ti.task.priority_weight
        return ti.task.priority_weight + sum(
            dag.task_dict[task_id].priority_weight
            for task_id in ti.task.get_flat_relative_ids(upstream=False)
        )


class UpstreamPriorityWeightStrategy(PriorityWeightStrategy):
    """Priority weight strategy that uses the sum of the priority weights of all upstream tasks."""

    def get_weight(self, ti: TaskInstance):
        dag = ti.task.get_dag()
        if dag is None:
            return ti.task.priority_weight
        return ti.task.priority_weight + sum(
            dag.task_dict[task_id].priority_weight for task_id in ti.task.get_flat_relative_ids(upstream=True)
        )


_airflow_priority_weight_strategies = {
    "absolute": AbsolutePriorityWeightStrategy(),
    "downstream": DownstreamPriorityWeightStrategy(),
    "upstream": UpstreamPriorityWeightStrategy(),
}


def _validate_and_load_priority_weight_strategy(
    priority_weight_strategy: str | PriorityWeightStrategy
) -> PriorityWeightStrategy:
    from airflow.serialization.serialized_objects import _get_registered_priority_weight_strategy
    from airflow.utils.module_loading import qualname

    if isinstance(priority_weight_strategy, str):
        if priority_weight_strategy in _airflow_priority_weight_strategies:
            priority_weight_strategy = _airflow_priority_weight_strategies[priority_weight_strategy]
    priority_weight_strategy_str = (
        qualname(priority_weight_strategy)
        if isinstance(priority_weight_strategy, PriorityWeightStrategy)
        else priority_weight_strategy
    )
    loaded_priority_weight_strategy = _get_registered_priority_weight_strategy(priority_weight_strategy_str)
    if loaded_priority_weight_strategy is None:
        raise AirflowException(f"Unknown priority strategy {priority_weight_strategy_str}")
    return (
        priority_weight_strategy
        if isinstance(priority_weight_strategy, PriorityWeightStrategy)
        else loaded_priority_weight_strategy()
    )
