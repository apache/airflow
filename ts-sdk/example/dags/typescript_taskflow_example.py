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
TaskFlow argument binding across the language boundary.

``summarize`` is called TaskFlow-style, and every argument its call passes reaches the TypeScript
handler by name, including ``make_totals``'s output, which the runtime pulls before the handler runs.
``report`` shows the one case folding cannot cover: its handler wants a name the Dag never used,
so it states that binding explicitly with ``withArgNames``.
The ``build_message`` stub shares a ``task_id`` with a task in ``typescript_example`` on purpose:
a handler binds the ``(dag_id, task_id)`` pair, so the two are different tasks.
See ``src/taskflow.ts``.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@task
def make_totals():
    return {"orders": 12, "revenue": 3402.0}


# `region_code` and `dry_run` are snake_case on purpose: they reach the handler's
# `regionCode` and `dryRun` by folding, with nothing declared on either side.
# `totals` takes an upstream task's output, which the runtime resolves from that
# task's `return_value` XCom before the handler is called.
# The call below leaves `dry_run` at its default, and the handler receives that
# value like any other.
@task.stub(queue="typescript")
def summarize(totals: dict, region_code: str, currency: str, threshold: float, dry_run: bool = False): ...


# `run_label` is not a spelling difference. The handler wants to call it
# `label`, a word this signature never uses, which is what `withArgNames` is
# for; folding would never connect the two.
@task.stub(queue="typescript")
def report(summary: dict, run_label: str): ...


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
    summary = summarize(make_totals(), "uk", "GBP", 280.0)
    report(summary, "nightly")
    summary >> build_message()


typescript_taskflow_example()
