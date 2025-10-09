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

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import chain, dag, task

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]


@dag(schedule=None, start_date=pendulum.datetime(2021, 1, 1, tz="UTC"), catchup=False, tags=["example"])
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

    short_circuit = check_condition.override(task_id="short_circuit", ignore_downstream_trigger_rules=False)(
        condition=False
    )

    chain(task_1, [task_2, short_circuit], [task_3, task_4], [task_5, task_6], task_7)
    # [END howto_operator_short_circuit_trigger_rules]


example_dag = example_short_circuit_decorator()
