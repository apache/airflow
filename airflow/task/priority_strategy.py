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
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


class PriorityWeightStrategy(ABC):
    """Priority weight strategy interface."""

    @abstractmethod
    def get_weight(self, ti: TaskInstance):
        """Get the priority weight of a task."""
        ...


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


def get_priority_weight_strategy(strategy_name: str) -> PriorityWeightStrategy:
    """Get a priority weight strategy by name or class path."""
    if strategy_name not in _airflow_priority_weight_strategies:
        try:
            priority_strategy_class = import_string(strategy_name)
            if not issubclass(priority_strategy_class, PriorityWeightStrategy):
                raise AirflowException(
                    f"Priority strategy {priority_strategy_class} is not a subclass of PriorityWeightStrategy"
                )
            _airflow_priority_weight_strategies[strategy_name] = priority_strategy_class()
        except ImportError:
            raise AirflowException(f"Unknown priority strategy {strategy_name}")
    return _airflow_priority_weight_strategies[strategy_name]
