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
"""Example DAG demonstrating the usage of the `@task.short_circuit()` TaskFlow decorator."""

from __future__ import annotations

import pendulum

from airflow.providers.common.compat.sdk import TriggerRule
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import chain, dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    doc_md="""
    ### Short-Circuit Decorator: Conditional Task Skipping

    Short-circuiting allows a task to conditionally prevent downstream execution
    by returning a falsy value, marking downstream tasks as skipped rather than failed.
    This is fundamentally different from task failure—skipped tasks follow a separate
    execution and alerting path.

    **When to use short-circuiting:**
    - Guard conditions that prevent unnecessary downstream work (cost control, resource optimization)
    - Data validation gates where non-execution is expected and not an error
    - Conditional pipelines where skipping tasks is part of normal control flow

    **Runtime behavior:**
    - When a short-circuit task returns a falsy value, all immediately downstream tasks
      are marked as skipped
    - Downstream trigger rules determine how skipped state propagates further
      (for example, `ALL_DONE` vs `ALL_SUCCESS`)
    - Skipped tasks are typically excluded from failure-based alerting and callbacks

    **Scheduling impact:**
    - Short-circuiting affects only the current DAG run’s execution path
    - Future DAG runs are scheduled normally without modification to the DAG definition
    - Useful for backfills and reprocessing scenarios without code changes

    📖 **Related documentation**  
    https://airflow.apache.org/docs/apache-airflow/stable/howto/operator.html#short-circuiting
    """,
)
def example_short_circuit_decorator():
    # [START howto_operator_short_circuit]
    @task.short_circuit()
    def check_condition(condition):
        return condition

    ds_true = [EmptyOperator(task_id=f"true_{i}") for i in [1, 2]]
    ds_false = [EmptyOperator(task_id=f"false_{i}") for i in [1, 2]]

    condition_is_true = check_condition.override(task_id="condition_is_true")(condition=True)
    condition_is_false = check_condition.override(task_id="condition_is_false")(condition=False)

    chain(condition_is_true, *ds_true)
    chain(condition_is_false, *ds_false)
    # [END howto_operator_short_circuit]

    # [START howto_operator_short_circuit_trigger_rules]
    [task_1, task_2, task_3, task_4, task_5, task_6] = [
        EmptyOperator(task_id=f"task_{i}") for i in range(1, 7)
    ]

    task_7 = EmptyOperator(task_id="task_7", trigger_rule=TriggerRule.ALL_DONE)

    short_circuit = check_condition.override(
        task_id="short_circuit", ignore_downstream_trigger_rules=False
    )(condition=False)

    chain(task_1, [task_2, short_circuit], [task_3, task_4], [task_5, task_6], task_7)
    # [END howto_operator_short_circuit_trigger_rules]


example_dag = example_short_circuit_decorator()
