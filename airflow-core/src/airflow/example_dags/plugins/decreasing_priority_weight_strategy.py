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

from typing import TYPE_CHECKING

from airflow.plugins_manager import AirflowPlugin
from airflow.task.priority_strategy import PriorityWeightStrategy

if TYPE_CHECKING:
    from airflow.models import TaskInstance


# [START custom_priority_weight_strategy]
class DecreasingPriorityStrategy(PriorityWeightStrategy):
    """A priority weight strategy that decreases the priority weight with each attempt of the DAG task."""

    def get_weight(self, ti: TaskInstance) -> int:
        try_number = ti.try_number or 0
        return max(3 - try_number + 1, 1)


class DecreasingPriorityWeightStrategyPlugin(AirflowPlugin):
    name = "decreasing_priority_weight_strategy_plugin"
    priority_weight_strategies = [DecreasingPriorityStrategy]


# [END custom_priority_weight_strategy]
