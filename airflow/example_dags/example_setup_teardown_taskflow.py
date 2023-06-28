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
"""Example DAG demonstrating the usage of setup and teardown tasks."""
from __future__ import annotations

import pendulum

from airflow.decorators import setup, task, task_group, teardown
from airflow.models.dag import DAG

with DAG(
    dag_id="example_setup_teardown_taskflow",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:

    @task
    def task_1():
        print("Hello 1")

    @task
    def task_2():
        print("Hello 2")

    @task
    def task_3():
        print("Hello 3")

    # you can set setup / teardown relationships with the `as_teardown` method.
    t1 = task_1()
    t2 = task_2()
    t3 = task_3()
    t1 >> t2 >> t3.as_teardown(setups=t1)

    # the method `as_teadrown` will mark t3 as teardown, t1 as setup, and arrow t1 >> t3
    # now if you clear t2 (downstream), then t1 will be cleared in addition to t3

    # it's also possible to use a decorator to mark a task as setup or
    # teardown when you define it. see below.

    @setup
    def dag_setup():
        print("I am dag_setup")

    @teardown
    def dag_teardown():
        print("I am dag_teardown")

    @task
    def dag_normal_task():
        print("I am just a normal task")

    s = dag_setup()
    t = dag_teardown()

    # by using the decorators, dag_setup and dag_teardown are already marked as setup / teardown
    # now we just need to make sure they are linked directly
    # what we need to do is this::
    #     s >> t
    #     s >> dag_normal_task() >> t
    # but we can use a context manager to make it cleaner
    with s >> t:
        dag_normal_task()

    @task_group
    def section_1():
        @task
        def my_setup():
            print("I set up")

        @task
        def my_teardown():
            print("I tear down")

        @task
        def hello():
            print("I say hello")

        (s := my_setup()) >> hello() >> my_teardown().as_teardown(setups=s)

    # and let's put section 1 inside the "dag setup" and "dag teardown"
    s >> section_1() >> t
