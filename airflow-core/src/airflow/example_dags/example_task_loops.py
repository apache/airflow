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
"""Repeat task groups with fixed counts, runtime conditions, and mapped tasks."""

from __future__ import annotations

# [START refine_estimate]
from airflow.sdk import dag, task, task_group


@dag(schedule=None, catchup=False, tags=["example"])
def refine_estimate():
    @task_group
    def refine():
        @task
        def improve(*, loop):
            estimate = 1.0 if loop.index == 0 else loop.previous["estimate"]
            return (estimate + 2.0 / estimate) / 2.0

        @task
        def evaluate(estimate):
            return {"estimate": estimate, "error": abs(estimate * estimate - 2.0)}

        evaluate(improve())

    def accurate_enough(*, loop):
        return loop.result["error"] < 0.000001

    @task
    def finished():
        print("Refinement finished.")

    refinement = refine.loop(max_iterations=10, until=accurate_enough)
    refinement >> finished()


refine_estimate()
# [END refine_estimate]


# [START fixed_loop]
@dag(schedule=None, catchup=False, tags=["example"])
def fixed_task_loop():
    @task_group
    def accumulate():
        @task
        def increment(*, loop):
            return (0 if loop.index == 0 else loop.previous) + 1

        increment()

    accumulate.loop(max_iterations=3)


fixed_task_loop()
# [END fixed_loop]


# [START mapped_loop]
@dag(schedule=None, catchup=False, tags=["example"])
def mapped_task_loop():
    @task_group
    def process_batch():
        @task
        def process(value, *, loop, ti):
            print(f"Iteration {loop.index}, mapped position {ti.map_index}")
            return value + loop.index

        process.expand(value=[1, 2])

    def batch_ready(*, loop):
        return min(loop.result) >= 2

    process_batch.loop(max_iterations=3, until=batch_ready)


mapped_task_loop()
# [END mapped_loop]


# [START partial_override_loop]
@dag(schedule=None, catchup=False, tags=["example"])
def partial_override_loop():
    @task_group
    def accumulate(increment_by):
        @task
        def increment(increment_by, *, loop):
            return (0 if loop.index == 0 else loop.previous) + increment_by

        increment(increment_by)

    accumulate.override(group_id="accumulate_more").partial(increment_by=5).loop(max_iterations=3)


partial_override_loop()
# [END partial_override_loop]
