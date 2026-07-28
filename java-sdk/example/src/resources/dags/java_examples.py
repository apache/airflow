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

from datetime import timedelta

from airflow.sdk import dag, task


@task()
def python_task_1():
    print("python_task_1")
    print("Push Python Task 'python_task_1' XCom:")
    return "value_from_python_task_1"


@task.stub(queue="java")
def extract(): ...


@task.stub(queue="java")
def transform(): ...


@task.stub(queue="java", retries=1, retry_delay=timedelta(seconds=5))
def load(): ...


@task.stub(queue="java")
def concurrent(): ...


@task.stub(queue="java")
def produce_number(): ...


@task.stub(queue="java")
def widen_to_long(): ...


@task.stub(queue="java")
def widen_to_double(): ...


@task.stub(queue="java")
def produce_nothing(): ...


@task.stub(queue="java")
def consume_nullable(): ...


@task.stub(queue="java")
def produce_fraction(): ...


@task.stub(queue="java")
def consume_float(): ...


@task()
def python_task_2(transformed):
    print("python_task_2")
    print("Pull Java Task 'transform' XCom:")
    print(transformed)


@dag(dag_id="java_interface_example")
def java_interface_example():
    transformed = transform()
    python_task_1() >> extract() >> transformed
    python_task_2(transformed)


@dag(dag_id="java_annotation_example")
def java_annotation_example():
    transformed = transform()
    python_task_1() >> extract() >> transformed
    python_task_2(transformed)
    transformed >> load()
    concurrent()


@dag(dag_id="java_xcom_casting_example")
def java_xcom_casting_example():
    produce_number() >> widen_to_long() >> widen_to_double()
    produce_nothing() >> consume_nullable()
    produce_fraction() >> consume_float()


java_interface_example()
java_annotation_example()
java_xcom_casting_example()
