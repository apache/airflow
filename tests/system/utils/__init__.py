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
from datetime import timedelta
from typing import Callable

from tabulate import tabulate

from airflow.utils.context import Context
from airflow.utils.state import State


def get_test_run(dag):
    def callback(context: Context):
        ti = context["dag_run"].get_task_instances()
        if not ti:
            logging.warning("could not retrieve tasks that ran in the DAG, cannot display a summary")
            return

        ti.sort(key=lambda x: x.end_date)

        headers = ["Task ID", "Status", "Duration (s)"]
        results = []
        # computing time using previous task's end time
        # because task.duration only counts last poke for sensors.
        # Only produces meaningful results when running sequentially, which is the case for system tests
        prev_time = ti[0].end_date - timedelta(seconds=ti[0].duration)
        for t in ti:
            results.append([t.task_id, t.state, f"{(t.end_date - prev_time).total_seconds():.1f}"])
            prev_time = t.end_date

        logging.info("EXECUTION SUMMARY:\n" + tabulate(results, headers=headers, tablefmt="fancy_grid"))

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
        dag.clear(dag_run_state=State.QUEUED)
        dag.run()

    return test_run


def get_test_env_id(env_var_name: str = "SYSTEM_TESTS_ENV_ID"):
    return os.environ.get(env_var_name)
