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

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any

from airflow.task.optimistic_task_selector import (
    OPTIMISTIC_SELECTOR,
    OptimisticTaskSelector,
    get_params_for_optimistic_selector,
)
from airflow.task.pessimistic_task_selector import (
    PESSIMISTIC_SELECTOR,
    PessimisticTaskSelector,
    get_params_for_pessimistic_selector,
)

if TYPE_CHECKING:
    from airflow.task.task_selector_strategy import TaskSelectorStrategy

TASK_SELECTORS: dict[str, TaskSelectorStrategy] = {}
TASK_SELECTOR_PARAMS_PROVIDERS: dict[str, Callable[[Any], Mapping[str, Any]]] = {}


def register_task_selector(
    name: str,
    task_selector_strategy: TaskSelectorStrategy,
    params_provider: Callable[[Any], Mapping[str, Any]],
):
    TASK_SELECTORS[name] = task_selector_strategy
    TASK_SELECTOR_PARAMS_PROVIDERS[name] = params_provider


register_task_selector(OPTIMISTIC_SELECTOR, OptimisticTaskSelector(), get_params_for_optimistic_selector)
register_task_selector(PESSIMISTIC_SELECTOR, PessimisticTaskSelector(), get_params_for_pessimistic_selector)
