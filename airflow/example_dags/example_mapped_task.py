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

from typing import Iterable

import pendulum

from airflow import DAG, XComArg
from airflow.decorators import task
from airflow.operators.bash import BashOperator

"""Example DAG demonstrating the usage of dynamically mapped tasks."""

with DAG(
    dag_id="example_mapped_task",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
) as dag:

    @task
    def make_list():
        return [1, 2, 3]

    @task
    def add_one(x: int, multiplier: int = 1):
        return (x + 1) * multiplier

    @task
    def sum_it(values: Iterable[int]):
        total = sum(values)
        print(f"Total was {total}")
        return total

    @task
    def sum_strings(values: Iterable[str]):
        total = sum(int(v) for v in values)
        print(f"Total was {total}")
        return total

    @task
    def echo_task(value: int):
        return f"echo {value}"

    added_values = add_one.partial(multiplier=2).expand(x=make_list())
    second = add_one.expand(x=make_list())
    third = add_one.expand(x=make_list(), multiplier=[2, 3, 4])
    bash_input = echo_task.expand(value=third)
    sum_it(third)

    classic = BashOperator.partial(task_id="classic").expand(bash_command=bash_input)
    sum_strings(XComArg(classic))
