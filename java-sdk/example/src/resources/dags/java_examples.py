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
def transform(extracted): ...


@task.stub(queue="java", retries=1, retry_delay=timedelta(seconds=5))
def load(transformed): ...


@task.stub(queue="java")
def concurrent(): ...


# Keyword arguments bind to the public fields of the Java task's TaskInput bundle.
@task.stub(queue="java")
def report(run_label, transformed): ...


@task.stub(queue="java")
def produce_number(): ...


@task.stub(queue="java")
def widen_to_long(value): ...


@task.stub(queue="java")
def widen_to_double(value): ...


@task.stub(queue="java")
def produce_nothing(): ...


@task.stub(queue="java")
def consume_nullable(value): ...


@task.stub(queue="java")
def produce_fraction(): ...


@task.stub(queue="java")
def consume_float(value): ...


@task()
def python_task_2(transformed):
    print("python_task_2")
    print("Pull Java Task 'transform' XCom:")
    print(transformed)


@dag(dag_id="java_interface_example")
def java_interface_example():
    extracted = extract()
    python_task_1() >> extracted
    transformed = transform(extracted)
    python_task_2(transformed)


@dag(dag_id="java_annotation_example")
def java_annotation_example():
    extracted = extract()
    python_task_1() >> extracted
    transformed = transform(extracted)
    python_task_2(transformed)
    load(transformed)
    report(run_label="nightly", transformed=transformed)
    concurrent()


@dag(dag_id="java_xcom_casting_example")
def java_xcom_casting_example():
    widen_to_double(widen_to_long(produce_number()))
    consume_nullable(produce_nothing())
    consume_float(produce_fraction())


java_interface_example()
java_annotation_example()
java_xcom_casting_example()
