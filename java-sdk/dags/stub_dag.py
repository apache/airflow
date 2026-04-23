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

from airflow.sdk import dag, task


@task()
def python_task_1(ti):
    print("python_task_1")
    print("Push Python Task 'python_task_1' XCom:")
    ti.xcom_push(value="value-pushed-from-python_task_1", key="return_value")


@task.stub(sdk="java")
def extract(): ...


@task.stub(sdk="java")
def transform(): ...


@task()
def python_task_2(ti):
    print("python_task_2")
    print("Pull Java Task 'transform' XCom:")
    print(ti.xcom_pull(task_ids="transform"))


@dag(dag_id="java_example")
def simple_dag():

    python_task_1() >> extract() >> transform() >> python_task_2()


simple_dag()
