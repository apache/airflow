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
A second Python-owned Dag served by the same TypeScript bundle.

``typescript_example`` shows the basics; this Dag exists so one bundle provides for two ``dag_id``s at once.
Its ``build_message`` stub deliberately shares a ``task_id`` with a task in ``typescript_example``:
a handler binds the ``(dag_id, task_id)`` pair, so the two are different tasks with different bodies.
See ``src/taskflow.ts``.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@task
def make_totals():
    return {"orders": 12, "revenue": 3402.0}


@task.stub(queue="typescript")
def summarize(): ...


# Same task_id as `typescript_example.build_message`, on purpose.
@task.stub(queue="typescript")
def build_message(): ...


@dag(
    dag_id="typescript_taskflow_example",
    schedule=None,
    catchup=False,
    tags=["typescript", "example", "taskflow"],
)
def typescript_taskflow_example():
    make_totals() >> summarize() >> build_message()


typescript_taskflow_example()
