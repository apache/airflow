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

"""Example DAG demonstrating the usage of XComs."""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

value_1 = [1, 2, 3]
value_2 = {'a': 'b'}


def push(**kwargs):
    """Pushes an XCom without a specific target"""
    kwargs['ti'].xcom_push(key='value from pusher 1', value=value_1)


def push_by_returning(**kwargs):
    """Pushes an XCom without a specific target, just by returning it"""
    return value_2


def _compare_values(pulled_value, check_value):
    if pulled_value != check_value:
        raise ValueError(f'The two values differ {pulled_value} and {check_value}')


def puller(pulled_value_1, pulled_value_2, **kwargs):
    """Pull all previously pushed XComs and check if the pushed values match the pulled values."""

    # Check pulled values from function args
    _compare_values(pulled_value_1, value_1)
    _compare_values(pulled_value_2, value_2)


def pull_value_from_bash_push(**kwargs):
    ti = kwargs['ti']
    bash_pushed_via_return_value = ti.xcom_pull(key="return_value", task_ids='bash_push')
    bash_manually_pushed_value = ti.xcom_pull(key="manually_pushed_value", task_ids='bash_push')
    print(f"The xcom value pushed by task push via return value is {bash_pushed_via_return_value}")
    print(f"The xcom value pushed by task push manually is {bash_manually_pushed_value}")


with DAG(
    'example_xcom',
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    push1 = PythonOperator(
        task_id='push',
        python_callable=push,
    )

    push2 = PythonOperator(
        task_id='push_by_returning',
        python_callable=push_by_returning,
    )

    pull = PythonOperator(
        task_id='puller',
        python_callable=puller,
        op_kwargs={
            'pulled_value_1': push1.output['value from pusher 1'],
            'pulled_value_2': push2.output,
        },
    )

    bash_push = BashOperator(
        task_id='bash_push',
        bash_command='echo "bash_push demo"  && '
        'echo "Manually set xcom value '
        '{{ ti.xcom_push(key="manually_pushed_value", value="manually_pushed_value") }}" && '
        'echo "value_by_return"',
    )

    bash_pull = BashOperator(
        task_id='bash_pull',
        bash_command='echo "bash pull demo" && '
        f'echo "The xcom pushed manually is {bash_push.output["manually_pushed_value"]}" && '
        f'echo "The returned_value xcom is {bash_push.output}" && '
        'echo "finished"',
        do_xcom_push=False,
    )

    python_pull_from_bash = PythonOperator(
        task_id='python_pull_from_bash',
        python_callable=pull_value_from_bash_push,
    )

    [bash_pull, python_pull_from_bash] << bash_push

    # Task dependencies created via `XComArgs`:
    #   push1 >> pull
    #   push2 >> pull
