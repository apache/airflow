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
from contextlib import suppress
from functools import cache
from typing import TYPE_CHECKING

from airflow.task.weight_rule import WeightRule

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
    def get_weight(self, ti: TaskInstance) -> int:
        """Get the priority weight of a task."""
        raise NotImplementedError("must be implemented by a subclass")

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        return isinstance(other, type(self))

    def __hash__(self) -> int:
        return hash(None)


class _AbsolutePriorityWeightStrategy(PriorityWeightStrategy):
    """Priority weight strategy that uses the task's priority weight directly."""

    def get_weight(self, ti: TaskInstance) -> int:
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

    def get_weight(self, ti: TaskInstance) -> int:
        if TYPE_CHECKING:
            assert ti.task
        dag = ti.task.get_dag()
        if dag is None:
            return ti.task.priority_weight
        return ti.task.priority_weight + sum(
            dag.task_dict[task_id].priority_weight for task_id in ti.task.get_flat_relative_ids(upstream=True)
        )


@cache
def get_airflow_priority_weight_strategies() -> dict[str, type[PriorityWeightStrategy]]:
    from airflow._shared.module_loading import qualname

    return {
        qualname(_AbsolutePriorityWeightStrategy): _AbsolutePriorityWeightStrategy,
        qualname(_DownstreamPriorityWeightStrategy): _DownstreamPriorityWeightStrategy,
        qualname(_UpstreamPriorityWeightStrategy): _UpstreamPriorityWeightStrategy,
        WeightRule.ABSOLUTE: _AbsolutePriorityWeightStrategy,
        WeightRule.DOWNSTREAM: _DownstreamPriorityWeightStrategy,
        WeightRule.UPSTREAM: _UpstreamPriorityWeightStrategy,
    }


@cache
def get_weight_rule_from_priority_weight_strategy(strategy: type[PriorityWeightStrategy]) -> WeightRule:
    return {
        _AbsolutePriorityWeightStrategy: WeightRule.ABSOLUTE,
        _DownstreamPriorityWeightStrategy: WeightRule.DOWNSTREAM,
        _UpstreamPriorityWeightStrategy: WeightRule.UPSTREAM,
    }[strategy]


def validate_and_load_priority_weight_strategy(
    priority_weight_strategy: str | PriorityWeightStrategy | None,
) -> PriorityWeightStrategy:
    """
    Validate and load a priority weight strategy.

    Returns the priority weight strategy if it is valid, otherwise raises an exception.

    :param priority_weight_strategy: The priority weight strategy to validate and load.

    :meta private:
    """
    from airflow._shared.module_loading import qualname
    from airflow.serialization.serialized_objects import _get_registered_priority_weight_strategy

    if priority_weight_strategy is None:
        return _AbsolutePriorityWeightStrategy()

    if isinstance(priority_weight_strategy, str):
        with suppress(KeyError):
            return get_airflow_priority_weight_strategies()[priority_weight_strategy]()
        priority_weight_strategy_class = priority_weight_strategy
    else:
        priority_weight_strategy_class = qualname(priority_weight_strategy)
    loaded_priority_weight_strategy = _get_registered_priority_weight_strategy(priority_weight_strategy_class)
    if loaded_priority_weight_strategy is None:
        raise ValueError(f"Unknown priority strategy {priority_weight_strategy_class}")
    return loaded_priority_weight_strategy()
