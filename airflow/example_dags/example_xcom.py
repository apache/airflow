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
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.dates import days_ago

value_1 = [1, 2, 3]
value_2 = {'a': 'b'}
value_3 = 'some value'


def push(**kwargs):
    """Pushes an XCom without a specific target"""

    # Using the "classic" approach
    kwargs['ti'].xcom_push(key='value from pusher 1', value=value_1)

    # Using the approach available as of Airflow 2.0.  When using this approach, `**kwargs`
    # is not needed to access the execution context.
    context = get_current_context()
    context['ti'].xcom_push(key='other value from pusher 1', value=value_2)


def push_by_returning(**kwargs):
    """Pushes an XCom without a specific target, just by returning it"""
    return value_3


def _compare_values(pulled_value, check_value):
    if pulled_value != check_value:
        raise ValueError(f'The two values differ {pulled_value} and {check_value}')


def puller(pulled_value_1, pulled_value_2, pulled_value_3, **kwargs):
    """Pull all previously pushed XComs and check if the pushed values match the pulled values."""

    # Check pulled values from function args
    _compare_values(pulled_value_1, value_1)
    _compare_values(pulled_value_2, value_2)
    _compare_values(pulled_value_3, value_3)

    # Check pulled values with "classic" approach
    pulled_value_1 = kwargs['ti'].xcom_pull(key='value from pusher 1', task_ids='push')
    pulled_value_2 = kwargs['ti'].xcom_pull(key='other value from pusher 1', task_ids='push')
    _compare_values(pulled_value_1, value_1)
    _compare_values(pulled_value_2, value_2)

    # Check pulled value 3 using the approach availble as of Airflow 2.0. Again, when using
    # this approach, `**kwargs` is not needed to access the execution context.
    context = get_current_context()
    pulled_value_3 = context['ti'].xcom_pull(task_ids='push_by_returning')
    _compare_values(pulled_value_3, value_3)


with DAG(
    'example_xcom',
    schedule_interval="@once",
    start_date=days_ago(2),
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
            'pulled_value_2': push1.output['other value from pusher 1'],
            'pulled_value_3': push2.output,
        },
    )

    # Task dependencies created via `XComArgs`:
    #   push1 >> pull
    #   push2 >> pull
