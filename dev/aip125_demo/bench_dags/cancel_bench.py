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
"""
Dags whose ``cancel_on`` the delivery latency of ``demo.sh bench`` is measured against; never run.

Each matches ``closed`` pull requests of a repository of its own:

* ``cancel_bench_small``: one task, a condition of the Dag;
* ``cancel_bench_big_dag``: 500 tasks, a condition of the Dag, so one condition row;
* ``cancel_bench_big_tasks``: 500 tasks that each get the condition through ``default_args``, so 500
  condition rows, which a delivery evaluates once as they are the same.

``demo.sh bench`` copies this file into the Dags folder; the checks and show mode do not.
"""

from __future__ import annotations

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, WebhookMatch

TASKS = 500


def build_closed(repository: str) -> WebhookMatch:
    return WebhookMatch(
        webhook="github", events=["pull_request"], match={"repository": repository, "action": "closed"}
    )


with DAG("cancel_bench_small", schedule=None, catchup=False, cancel_on=[build_closed("my-org/small")]):
    EmptyOperator(task_id="only")

with DAG("cancel_bench_big_dag", schedule=None, catchup=False, cancel_on=[build_closed("my-org/big-dag")]):
    for index in range(TASKS):
        EmptyOperator(task_id=f"task_{index}")

with DAG(
    "cancel_bench_big_tasks",
    schedule=None,
    catchup=False,
    default_args={"cancel_on": [build_closed("my-org/big-tasks")]},
):
    for index in range(TASKS):
        EmptyOperator(task_id=f"task_{index}")
