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

import pendulum

from airflow.providers.common.compat.sdk import TriggerRule
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.utils.weekday import WeekDay
from airflow.sdk import chain, dag, task
from airflow.sdk.exceptions import AirflowSkipException


@dag(schedule=None, start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), catchup=False)
def example_bash_decorator():
    @task.bash
    def run_me(sleep_seconds: int, task_instance_key_str: str) -> str:
        return f"echo {task_instance_key_str} && sleep {sleep_seconds}"

    run_me_loop = [run_me.override(task_id=f"runme_{i}")(sleep_seconds=i) for i in range(3)]

    # [START howto_decorator_bash]
    @task.bash
    def run_after_loop() -> str:
        return "echo https://airflow.apache.org/"

    run_this = run_after_loop()
    # [END howto_decorator_bash]

    # [START howto_decorator_bash_template]
    @task.bash
    def also_run_this() -> str:
        return 'echo "ti_key={{ task_instance_key_str }}"'

    also_this = also_run_this()
    # [END howto_decorator_bash_template]

    # [START howto_decorator_bash_context_vars]
    @task.bash
    def also_run_this_again(task_instance_key_str) -> str:
        return f'echo "ti_key={task_instance_key_str}"'

    also_this_again = also_run_this_again()
    # [END howto_decorator_bash_context_vars]

    # [START howto_decorator_bash_skip]
    @task.bash
    def this_will_skip() -> str:
        return 'echo "hello world"; exit 99;'

    this_skips = this_will_skip()
    # [END howto_decorator_bash_skip]

    run_this_last = EmptyOperator(task_id="run_this_last", trigger_rule=TriggerRule.ALL_DONE)

    # [START howto_decorator_bash_conditional]
    @task.bash
    def sleep_in(day: str) -> str:
        if day in (WeekDay.SATURDAY, WeekDay.SUNDAY):
            return f"sleep {60 * 60}"
        raise AirflowSkipException("No sleeping in today!")

    sleep_in(day="{{ dag_run.logical_date.strftime('%A').lower() }}")
    # [END howto_decorator_bash_conditional]

    # [START howto_decorator_bash_parametrize]
    @task.bash(env={"BASE_DIR": "{{ dag_run.logical_date.strftime('%Y/%m/%d') }}"}, append_env=True)
    def make_dynamic_dirs(new_dirs: str) -> str:
        return f"mkdir -p $AIRFLOW_HOME/$BASE_DIR/{new_dirs}"

    make_dynamic_dirs(new_dirs="foo/bar/baz")
    # [END howto_decorator_bash_parametrize]

    # [START howto_decorator_bash_build_cmd]
    @task.bash
    def get_file_stats() -> str:
        from pathlib import Path
        from shlex import join

        # Get stats of the current DAG file itself
        current_file = str(Path(__file__))
        cmd = join(["stat", current_file])

        return cmd

    get_file_stats()
    # [END howto_decorator_bash_build_cmd]

    chain(run_me_loop, run_this)
    chain([also_this, also_this_again, this_skips, run_this], run_this_last)


example_bash_decorator()
