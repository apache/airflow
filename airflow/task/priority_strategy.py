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
    """
    Priority weight strategy interface.

    This feature is experimental and subject to change at any time.

    Currently, we don't serialize the priority weight strategy parameters. This means that
    the priority weight strategy must be stateless, but you can add class attributes, and
    create multiple subclasses with different attributes values if you need to create
    different versions of the same strategy.
    """

    @abstractmethod
    def get_weight(self, ti: TaskInstance):
        """Get the priority weight of a task."""
        ...

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> PriorityWeightStrategy:
        """
        Deserialize a priority weight strategy from data.

        This is called when a serialized DAG is deserialized. ``data`` will be whatever
        was returned by ``serialize`` during DAG serialization. The default
        implementation constructs the priority weight strategy without any arguments.
        """
        return cls(**data)  # type: ignore[call-arg]

    def serialize(self) -> dict[str, Any]:
        """
        Serialize the priority weight strategy for JSON encoding.

        This is called during DAG serialization to store priority weight strategy information
        in the database. This should return a JSON-serializable dict that will be fed into
        ``deserialize`` when the DAG is deserialized. The default implementation returns
        an empty dict.
        """
        return {}

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, type(self)):
            return False
        return self.serialize() == other.serialize()


class _AbsolutePriorityWeightStrategy(PriorityWeightStrategy):
    """Priority weight strategy that uses the task's priority weight directly."""

    def get_weight(self, ti: TaskInstance):
        if TYPE_CHECKING:
            assert ti.task
        return ti.task.priority_weight


class _DownstreamPriorityWeightStrategy(PriorityWeightStrategy):
    """Priority weight strategy that uses the sum of the priority weights of all downstream tasks."""

    def get_weight(self, ti: TaskInstance) -> int:
        if TYPE_CHECKING:
            assert ti.task
        dag = ti.task.get_dag()
        if dag is None:
            return ti.task.priority_weight
        return ti.task.priority_weight + sum(
            dag.task_dict[task_id].priority_weight
            for task_id in ti.task.get_flat_relative_ids(upstream=False)
        )


class _UpstreamPriorityWeightStrategy(PriorityWeightStrategy):
    """Priority weight strategy that uses the sum of the priority weights of all upstream tasks."""

    def get_weight(self, ti: TaskInstance):
        if TYPE_CHECKING:
            assert ti.task
        dag = ti.task.get_dag()
        if dag is None:
            return ti.task.priority_weight
        return ti.task.priority_weight + sum(
            dag.task_dict[task_id].priority_weight
            for task_id in ti.task.get_flat_relative_ids(upstream=True)
        )


airflow_priority_weight_strategies: dict[str, type[PriorityWeightStrategy]] = {
    "absolute": _AbsolutePriorityWeightStrategy,
    "downstream": _DownstreamPriorityWeightStrategy,
    "upstream": _UpstreamPriorityWeightStrategy,
}


airflow_priority_weight_strategies_classes = {
    cls: name for name, cls in airflow_priority_weight_strategies.items()
}


def validate_and_load_priority_weight_strategy(
    priority_weight_strategy: str | PriorityWeightStrategy | None,
) -> PriorityWeightStrategy:
    """
    Validate and load a priority weight strategy.

    Returns the priority weight strategy if it is valid, otherwise raises an exception.

    :param priority_weight_strategy: The priority weight strategy to validate and load.

    :meta private:
    """
    from airflow.serialization.serialized_objects import (
        _get_registered_priority_weight_strategy,
    )
    from airflow.utils.module_loading import qualname

    if priority_weight_strategy is None:
        return _AbsolutePriorityWeightStrategy()

    if isinstance(priority_weight_strategy, str):
        if priority_weight_strategy in airflow_priority_weight_strategies:
            return airflow_priority_weight_strategies[priority_weight_strategy]()
        priority_weight_strategy_class = priority_weight_strategy
    else:
        priority_weight_strategy_class = qualname(priority_weight_strategy)
    loaded_priority_weight_strategy = _get_registered_priority_weight_strategy(
        priority_weight_strategy_class
    )
    if loaded_priority_weight_strategy is None:
        raise AirflowException(
            f"Unknown priority strategy {priority_weight_strategy_class}"
        )
    return loaded_priority_weight_strategy()
