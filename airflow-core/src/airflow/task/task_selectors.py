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
from typing import TYPE_CHECKING, Any, TypedDict

from typing_extensions import Unpack

from airflow.task.optimistic_task_selector import OptimisticTaskSelector
from airflow.task.pessimistic_task_selector import PessimisticTaskSelector

if TYPE_CHECKING:
    from airflow.configuration import AirflowConfigParser
    from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
    from airflow.task.task_selector_strategy import TaskSelectorStrategy

OPTIMISTIC_SELECTOR = "OPTIMISTIC"
PESSIMISTIC_SELECTOR = "PESSIMISTIC"

TASK_SELECTORS: dict[str, TaskSelectorStrategy] = {}
TASK_SELECTOR_PARAMS_PROVIDERS: dict[str, Callable[[ParamsProviderType], Any]] = {}

TASK_SELECTORS[OPTIMISTIC_SELECTOR] = OptimisticTaskSelector()
TASK_SELECTORS[PESSIMISTIC_SELECTOR] = PessimisticTaskSelector()


class ParamsProviderType(TypedDict):
    """Allows for better type hints."""

    conf: AirflowConfigParser
    scheduler_job_runner: SchedulerJobRunner | None


def _get_params_for_optimistic_selector(
    **kwargs: Unpack[ParamsProviderType],
) -> Mapping[str, Any]:
    conf: AirflowConfigParser = kwargs["conf"]
    scheduler_job_runner = kwargs["scheduler_job_runner"]

    params = {}

    params["max_tis"] = conf.getint("scheduler", "max_tis_per_query")

    if TYPE_CHECKING:
        # making mypy happy, though it is always passed
        assert scheduler_job_runner

    params["executor_slots_available"] = {  # type: ignore[assignment]
        str(executor.name): executor.slots_available for executor in scheduler_job_runner.job.executors
    }

    return params


def _get_params_for_pessimistic_selector(
    **kwargs: Unpack[ParamsProviderType],
) -> Mapping[str, Any]:
    conf = kwargs["conf"]

    params = {}

    params["max_tis"] = conf.getint("scheduler", "max_tis_per_query")

    return params


TASK_SELECTOR_PARAMS_PROVIDERS[OPTIMISTIC_SELECTOR] = _get_params_for_optimistic_selector  # type: ignore[assignment]
TASK_SELECTOR_PARAMS_PROVIDERS[PESSIMISTIC_SELECTOR] = _get_params_for_pessimistic_selector  # type: ignore[assignment]
