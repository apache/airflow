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

"""DAG testing the use of chaining XComArgs."""
from airflow.models import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime


@task
def generate_value():
    return "Bring me a shrubbery!"


@task
def print_value(value):
    print(value)


@task
def pass_1():
    pass


@task
def pass_2():
    pass


@task
def pass_3():
    pass


with DAG(
    dag_id="test_chain_xcomargs",
    start_date=datetime(2021, 6, 30),
    schedule_interval=None,
    catchup=False,
) as dag:
    empty_func = PythonOperator(task_id="empty_func", python_callable=lambda: None)
    dummy_1 = DummyOperator(task_id="dummy_1")
    dummy_2 = DummyOperator(task_id="dummy_2")

    chain(empty_func, [pass_1(), pass_2()], dummy_1)

    chain(print_value(generate_value()), dummy_2, pass_3())
