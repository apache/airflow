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
"""Dags with task loops for the tests in ``basic_tests/test_task_loops.py``."""

from __future__ import annotations

from datetime import timedelta

from airflow.sdk import dag, task, task_group


@dag(schedule=None, catchup=False, tags=["example", "task-loop"])
def example_task_loop_test():
    @task_group
    def accumulate():
        @task
        def add_one(*, loop):
            total = (0 if loop.index == 1 else loop.previous["value"]) + 1
            return {"value": total, "trajectory": loop.trajectory}

        add_one()

    @task
    def report(results):
        return [[result["value"], result["trajectory"]] for result in results]

    accumulation = accumulate.loop(max_iterations=3)
    report(accumulation["add_one"].output)


example_task_loop_test()


@dag(schedule=None, catchup=False, tags=["example", "task-loop"])
def example_task_loop_mapped_test():
    @task_group
    def grow():
        @task
        def choose_items(*, loop):
            return list(range(loop.index))

        @task
        def square(value):
            return value * value

        square.expand(value=choose_items())

    @task
    def report(squares):
        return list(squares)

    growth = grow.loop(max_iterations=3)
    report(growth["square"].output)


example_task_loop_mapped_test()


@dag(
    schedule=None,
    catchup=False,
    tags=["example", "task-loop"],
    default_args={"retries": 1, "retry_delay": timedelta(seconds=1)},
)
def example_task_loop_not_converging_test():
    @task_group
    def poll():
        @task
        def check(*, loop):
            return loop.index

        check()

    def reached_ten(*, loop):
        return loop.result >= 10

    @task
    def after():
        pass

    poll.loop(max_iterations=2, until=reached_ten) >> after()


example_task_loop_not_converging_test()


def poll_until(dag_id: str, *, target: int, max_iterations: int):
    @dag(dag_id=dag_id, schedule=None, catchup=False, tags=["example", "task-loop"])
    def poll_until_target():
        @task_group
        def poll():
            @task
            def check(*, loop):
                return loop.index

            check()

        def reached_target(*, loop):
            return loop.result >= target

        @task
        def after():
            pass

        poll.loop(max_iterations=max_iterations, until=reached_target) >> after()

    poll_until_target()


poll_until("example_task_loop_until_met_early_test", target=2, max_iterations=5)
poll_until("example_task_loop_until_met_at_last_test", target=3, max_iterations=3)
