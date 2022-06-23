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

"""Example DAG demonstrating the usage of the params arguments in templated arguments."""

import datetime
import os
from textwrap import dedent

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator


@task(task_id="run_this")
def my_py_command(params, test_mode=None, task=None):
    """
    Print out the "foo" param passed in via
    `airflow tasks test example_passing_params_via_test_command run_this <date>
    -t '{"foo":"bar"}'`
    """
    if test_mode:
        print(
            f" 'foo' was passed in via test={test_mode} command : kwargs[params][foo] = {task.params['foo']}"
        )
    # Print out the value of "miff", passed in below via the Python Operator
    print(f" 'miff' was passed in via task params = {params['miff']}")
    return 1


@task(task_id="env_var_test_task")
def print_env_vars(test_mode=None):
    """
    Print out the "foo" param passed in via
    `airflow tasks test example_passing_params_via_test_command env_var_test_task <date>
    --env-vars '{"foo":"bar"}'`
    """
    if test_mode:
        print(f"foo={os.environ.get('foo')}")
        print(f"AIRFLOW_TEST_MODE={os.environ.get('AIRFLOW_TEST_MODE')}")


with DAG(
    "example_passing_params_via_test_command",
    schedule_interval='*/1 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=4),
    tags=['example'],
) as dag:
    run_this = my_py_command(params={"miff": "agg"})

    my_command = dedent(
        """
        echo "'foo' was passed in via Airflow CLI Test command with value '$FOO'"
        echo "'miff' was passed in via BashOperator with value '$MIFF'"
        """
    )

    also_run_this = BashOperator(
        task_id='also_run_this',
        bash_command=my_command,
        params={"miff": "agg"},
        env={"FOO": "{{ params.foo }}", "MIFF": "{{ params.miff }}"},
    )

    env_var_test_task = print_env_vars()

    run_this >> also_run_this
