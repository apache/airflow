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
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

value_1 = [1, 2, 3]
value_2 = {'a': 'b'}


def push(**kwargs):
    """Pushes an XCom without a specific target"""
    kwargs['ti'].xcom_push(key='value from pusher 1', value=value_1)


def push_by_returning(**kwargs):
    """Pushes an XCom without a specific target, just by returning it"""
    return value_2


def puller(**kwargs):
    """Pull all previously pushed XComs and check if the pushed values match the pulled values."""
    ti = kwargs['ti']

    # get value_1
    pulled_value_1 = ti.xcom_pull(key=None, task_ids='push')
    if pulled_value_1 != value_1:
        raise ValueError(f'The two values differ {pulled_value_1} and {value_1}')

    # get value_2
    pulled_value_2 = ti.xcom_pull(task_ids='push_by_returning')
    if pulled_value_2 != value_2:
        raise ValueError(f'The two values differ {pulled_value_2} and {value_2}')

    # get both value_1 and value_2
    pulled_value_1, pulled_value_2 = ti.xcom_pull(key=None, task_ids=['push', 'push_by_returning'])
    if pulled_value_1 != value_1:
        raise ValueError(f'The two values differ {pulled_value_1} and {value_1}')
    if pulled_value_2 != value_2:
        raise ValueError(f'The two values differ {pulled_value_2} and {value_2}')


def pull_value_from_bash_push(**kwargs):
    ti = kwargs['ti']
    bash_pushed_via_return_value = ti.xcom_pull(key="return_value", task_ids='bash_push')
    bash_manually_pushed_value = ti.xcom_pull(key="manually_pushed_value", task_ids='bash_push')
    print(f"The xcom value pushed by task push via return value is {bash_pushed_via_return_value}")
    print(f"The xcom value pushed by task push manually is {bash_manually_pushed_value}")


with DAG(
    'example_xcom',
    schedule_interval="@once",
    start_date=days_ago(2),
    default_args={'owner': 'airflow'},
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
    )

    pull << [push1, push2]

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
        'echo "The xcom pushed manually is '
        '"{{ ti.xcom_pull(task_ids="bash_push", key="manually_pushed_value") }}" && '
        'echo "The returned_value xcom is '
        '"{{ ti.xcom_pull(task_ids="bash_push", key="return_value") }}" && '
        'echo "finished"',
        do_xcom_push=False,
    )

    python_pull_from_bash = PythonOperator(
        task_id='python_pull_from_bash',
        python_callable=pull_value_from_bash_push,
    )

    [bash_pull, python_pull_from_bash] << bash_push
