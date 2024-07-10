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

import logging
import os
from typing import TYPE_CHECKING, Callable

from tabulate import tabulate

from airflow.utils.state import DagRunState

if TYPE_CHECKING:
    from airflow.utils.context import Context

logger = logging.getLogger(__name__)


def get_test_run(dag):
    def callback(context: Context):
        ti = context["dag_run"].get_task_instances()
        if not ti:
            logger.warning("could not retrieve tasks that ran in the DAG, cannot display a summary")
            return

        ti.sort(key=lambda x: x.end_date)

        headers = ["Task ID", "Status"]
        results = []
        for t in ti:
            results.append([t.task_id, t.state])

        logger.info("EXECUTION SUMMARY:\n%s", tabulate(results, headers=headers, tablefmt="fancy_grid"))

    def add_callback(current: list[Callable] | Callable | None, new: Callable) -> list[Callable] | Callable:
        if not current:
            return new
        elif isinstance(current, list):
            current.append(new)
            return current
        else:
            return [current, new]

    def test_run():
        dag.on_failure_callback = add_callback(dag.on_failure_callback, callback)
        dag.on_success_callback = add_callback(dag.on_success_callback, callback)
        # If the env variable ``_AIRFLOW__SYSTEM_TEST_USE_EXECUTOR`` is set, then use an executor to run the
        # DAG
        dag_run = dag.test(use_executor=os.environ.get("_AIRFLOW__SYSTEM_TEST_USE_EXECUTOR") == "1")
        assert (
            dag_run.state == DagRunState.SUCCESS
        ), "The system test failed, please look at the logs to find out the underlying failed task(s)"

    return test_run


def get_test_env_id(env_var_name: str = "SYSTEM_TESTS_ENV_ID"):
    return os.environ.get(env_var_name)
