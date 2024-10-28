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
from __future__ import annotations

import pytest
from pendulum import datetime

from airflow.decorators import task
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

pytestmark = pytest.mark.db_test


DEFAULT_DATE = datetime(2022, 8, 17)


@pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
def test_short_circuit_decorator(dag_maker):
    with dag_maker(serialized=True):

        @task
        def empty(): ...

        @task.short_circuit()
        def short_circuit(condition):
            return condition

        short_circuit_false = short_circuit.override(task_id="short_circuit_false")(
            condition=False
        )
        task_1 = empty.override(task_id="task_1")()
        short_circuit_false >> task_1

        short_circuit_true = short_circuit.override(task_id="short_circuit_true")(
            condition=True
        )
        task_2 = empty.override(task_id="task_2")()
        short_circuit_true >> task_2

        short_circuit_respect_trigger_rules = short_circuit.override(
            task_id="short_circuit_respect_trigger_rules",
            ignore_downstream_trigger_rules=False,
        )(condition=False)
        task_3 = empty.override(task_id="task_3")()
        task_4 = empty.override(task_id="task_4")()
        task_5 = empty.override(task_id="task_5", trigger_rule=TriggerRule.ALL_DONE)()
        short_circuit_respect_trigger_rules >> [task_3, task_4] >> task_5

    dr = dag_maker.create_dagrun()

    for t in dag_maker.dag.tasks:
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    task_state_mapping = {
        "short_circuit_false": State.SUCCESS,
        "task_1": State.SKIPPED,
        "short_circuit_true": State.SUCCESS,
        "task_2": State.SUCCESS,
        "short_circuit_respect_trigger_rules": State.SUCCESS,
        "task_3": State.SKIPPED,
        "task_4": State.SKIPPED,
        "task_5": State.SUCCESS,
    }

    tis = dr.get_task_instances()
    for ti in tis:
        assert ti.state == task_state_mapping[ti.task_id]


def test_short_circuit_with_multiple_outputs(dag_maker):
    @task.short_circuit(multiple_outputs=True)
    def multiple_output():
        return {"x": 1, "y": 2}

    with dag_maker(serialized=True):
        ret = multiple_output()

    dr = dag_maker.create_dagrun()
    ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
    ti = dr.get_task_instances()[0]
    assert ti.xcom_pull() == {"x": 1, "y": 2}


def test_short_circuit_with_multiple_outputs_and_empty_dict(dag_maker):
    @task.short_circuit(multiple_outputs=True)
    def empty_dict():
        return {}

    with dag_maker(serialized=True):
        ret = empty_dict()

    dr = dag_maker.create_dagrun()
    ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
    ti = dr.get_task_instances()[0]
    assert ti.xcom_pull() == {}
