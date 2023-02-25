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
"""Example DAG demonstrating the usage of the XCom arguments in templated arguments."""
from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.models.xcom_arg import XComArg
from airflow.operators.bash import BashOperator


@dag(start_date=pendulum.datetime(2023, 1, 1, tz="UTC"))
def example_passing_xcoms_via_test_command():
    @task
    def get_python_echo_message():
        return "hello world"

    @task
    def python_echo(message: str, *args, **kwargs):
        print(f"python_echo: {message}")

    @task
    def get_bash_command():
        return "echo 'default'"

    python_echo(get_python_echo_message())
    BashOperator(task_id="run_bash", bash_command=get_bash_command())
    bash_push = BashOperator(
        task_id="manually_push_bash_command",
        bash_command='{{ ti.xcom_push(key="command", value="echo \'default\'") }}',
    )
    BashOperator(
        task_id="run_bash_with_manually_pushed_command", bash_command=XComArg(bash_push, key="command")
    )
    BashOperator(
        task_id="run_bash_with_manually_pulled_command",
        bash_command='{{ task_instance.xcom_pull(task_ids="manually_push_bash_command", key="command") }}',
    )


example_passing_xcoms_via_test_command()
