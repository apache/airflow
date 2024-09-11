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

from typing import TYPE_CHECKING, Any

from airflow.plugins_manager import AirflowPlugin
from airflow.task.priority_strategy import PriorityWeightStrategy

if TYPE_CHECKING:
    from airflow.models import TaskInstance


class StaticTestPriorityWeightStrategy(PriorityWeightStrategy):
    def get_weight(self, ti: TaskInstance):
        return 99


class FactorPriorityWeightStrategy(PriorityWeightStrategy):
    factor: int = 3

    def serialize(self) -> dict[str, Any]:
        return {"factor": self.factor}

    def get_weight(self, ti: TaskInstance):
        return max(ti.map_index, 1) * self.factor


class DecreasingPriorityStrategy(PriorityWeightStrategy):
    """A priority weight strategy that decreases the priority weight with each attempt."""

    def get_weight(self, ti: TaskInstance):
        return max(3 - ti.try_number + 1, 1)


class TestPriorityWeightStrategyPlugin(AirflowPlugin):
    # Without this import, the qualname method will not use the correct classes names
    from tests.plugins.priority_weight_strategy import (
        DecreasingPriorityStrategy,
        FactorPriorityWeightStrategy,
        StaticTestPriorityWeightStrategy,
    )

    name = "priority_weight_strategy_plugin"
    priority_weight_strategies = [
        StaticTestPriorityWeightStrategy,
        FactorPriorityWeightStrategy,
        DecreasingPriorityStrategy,
    ]


class NotRegisteredPriorityWeightStrategy(PriorityWeightStrategy):
    def get_weight(self, ti: TaskInstance):
        return 99
