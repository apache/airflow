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
    def my_first_task():
        print("Hello 1")

    @task
    def my_second_task():
        print("Hello 2")

    @task
    def my_third_task():
        print("Hello 3")

    # you can set setup / teardown relationships with the `as_teardown` method.
    task_1 = my_first_task()
    task_2 = my_second_task()
    task_3 = my_third_task()
    task_1 >> task_2 >> task_3.as_teardown(setups=task_1)

    # The method `as_teardown` will mark task_3 as teardown, task_1 as setup, and
    # arrow task_1 >> task_3.
    # Now if you clear task_2, then its setup task, task_1, will be cleared in
    # addition to its teardown task, task_3

    # it's also possible to use a decorator to mark a task as setup or
    # teardown when you define it. see below.

    @setup
    def outer_setup():
        print("I am outer_setup")
        return "some cluster id"

    @teardown
    def outer_teardown(cluster_id):
        print("I am outer_teardown")
        print(f"Tearing down cluster: {cluster_id}")

    @task
    def outer_work():
        print("I am just a normal task")

    @task_group
    def section_1():
        @setup
        def inner_setup():
            print("I set up")
            return "some_cluster_id"

        @task
        def inner_work(cluster_id):
            print(f"doing some work with {cluster_id=}")

        @teardown
        def inner_teardown(cluster_id):
            print(f"tearing down {cluster_id=}")

        # this passes the return value of `inner_setup` to both `inner_work` and `inner_teardown`
        inner_setup_task = inner_setup()
        inner_work(inner_setup_task) >> inner_teardown(inner_setup_task)

    # by using the decorators, outer_setup and outer_teardown are already marked as setup / teardown
    # now we just need to make sure they are linked directly.  At a low level, what we need
    # to do so is the following::
    #     s = outer_setup()
    #     t = outer_teardown()
    #     s >> t
    #     s >> outer_work() >> t
    # Thus, s and t are linked directly, and outer_work runs in between.  We can take advantage of
    # the fact that we are in taskflow, along with the context manager on teardowns, as follows:
    with outer_teardown(outer_setup()):
        outer_work()

        # and let's put section 1 inside the outer setup and teardown tasks
        section_1()
