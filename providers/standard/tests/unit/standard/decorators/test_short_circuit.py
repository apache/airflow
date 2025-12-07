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

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]
from airflow.utils.state import State

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_1, AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.exceptions import DownstreamTasksSkipped
    from airflow.sdk import task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]

pytestmark = pytest.mark.db_test


DEFAULT_DATE = datetime(2022, 8, 17)


@pytest.mark.skipif(AIRFLOW_V_3_0_1, reason="Test doesn't run on AF 3.0.1. Companion test below.")
def test_short_circuit_decorator(dag_maker):
    with dag_maker(serialized=True):

        @task
        def empty(): ...

        @task.short_circuit()
        def short_circuit(condition):
            return condition

        short_circuit_false = short_circuit.override(task_id="short_circuit_false")(condition=False)
        task_1 = empty.override(task_id="task_1")()
        short_circuit_false >> task_1

        short_circuit_true = short_circuit.override(task_id="short_circuit_true")(condition=True)
        task_2 = empty.override(task_id="task_2")()
        short_circuit_true >> task_2

        short_circuit_respect_trigger_rules = short_circuit.override(
            task_id="short_circuit_respect_trigger_rules", ignore_downstream_trigger_rules=False
        )(condition=False)
        task_3 = empty.override(task_id="task_3")()
        task_4 = empty.override(task_id="task_4")()
        task_5 = empty.override(task_id="task_5", trigger_rule=TriggerRule.ALL_DONE)()
        short_circuit_respect_trigger_rules >> [task_3, task_4] >> task_5

    dr = dag_maker.create_dagrun()

    for dag_task in dag_maker.dag.tasks:
        dag_maker.run_ti(dag_task.task_id, dr, ignore_ti_state=True)

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


@pytest.mark.skipif(not AIRFLOW_V_3_0_1, reason="Test only runs on AF3.0.1")
@pytest.mark.parametrize(("condition", "should_be_skipped"), [(True, False), (False, True)])
def test_short_circuit_decorator_af301(dag_maker, session, condition, should_be_skipped):
    with dag_maker(serialized=True, session=session):

        @task.short_circuit()
        def short_circuit(condition):
            return condition

        @task
        def empty(): ...

        short_circuit = short_circuit.override(task_id="short_circuit")(condition=condition)
        task_1 = empty.override(task_id="task_1")()
        short_circuit >> task_1

    dr = dag_maker.create_dagrun()

    ti_sc = dr.get_task_instance("short_circuit")

    # We only need to assert correct exception is raised for the tasks that should be skipped
    # The actual logic of task execution and setting state is already covered by Task Execution tests
    if should_be_skipped:
        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            ti_sc.run()
        assert exc_info.value.tasks == ["task_1"]
    else:
        ti_sc.run()


@pytest.mark.skipif(not AIRFLOW_V_3_0_1, reason="Test only runs on AF3.0.1")
@pytest.mark.parametrize(
    ("ignore_downstream_trigger_rules", "expected"), [(True, State.SKIPPED), (False, State.SUCCESS)]
)
def test_short_circuit_decorator__ignore_downstream_trigger_rules(
    dag_maker, session, ignore_downstream_trigger_rules, expected
):
    with dag_maker(
        dag_id="test_short_circuit_decorator__ignore_downstream_trigger_rules",
        serialized=True,
        session=session,
    ):

        @task.short_circuit()
        def short_circuit():
            return False

        @task
        def empty(): ...

        short_circuit_respect_trigger_rules = short_circuit.override(
            task_id="short_circuit_respect_trigger_rules",
            ignore_downstream_trigger_rules=ignore_downstream_trigger_rules,
        )()
        task_3 = empty.override(task_id="task_3")()
        task_4 = empty.override(task_id="task_4")()
        task_5 = empty.override(task_id="task_5", trigger_rule=TriggerRule.ALL_DONE)()
        short_circuit_respect_trigger_rules >> [task_3, task_4] >> task_5

    dr = dag_maker.create_dagrun(session=session)
    tis = dr.get_task_instances(session=session)

    # We only need to assert correct exception is raised for the tasks that should be skipped
    # The actual logic of task execution and setting state is already covered by Task Execution tests
    skipped_tasks = set()
    successful_tasks = set()

    for ti in tis:
        try:
            ti.run(session=session)

            # If the task completes successfully without exceptions, add it to the successful_tasks set
            successful_tasks.add(ti.task_id)
        except DownstreamTasksSkipped as dts:
            # `DownstreamTasksSkipped` is raised when the task completes successfully
            # and the downstream tasks are skipped
            successful_tasks.add(ti.task_id)

            # Tasks in dts.tasks are going to be skipped in the Task Execution API
            #   so simulate that here by setting the state to SKIPPED
            skipped_tasks |= set(dts.tasks)

    assert "short_circuit_respect_trigger_rules" in successful_tasks
    assert "task_3" in skipped_tasks
    assert "task_4" in skipped_tasks
    if expected == State.SUCCESS:
        assert "task_5" in successful_tasks
    else:
        assert "task_5" in skipped_tasks


def test_short_circuit_with_multiple_outputs(dag_maker):
    @task.short_circuit(multiple_outputs=True)
    def multiple_output():
        return {"x": 1, "y": 2}

    with dag_maker(serialized=True):
        multiple_output()

    dr = dag_maker.create_dagrun()
    dag_maker.run_ti("multiple_output", dr)
    ti = dr.get_task_instances()[0]
    assert ti.xcom_pull() == {"x": 1, "y": 2}


def test_short_circuit_with_multiple_outputs_and_empty_dict(dag_maker):
    @task.short_circuit(multiple_outputs=True)
    def empty_dict():
        return {}

    with dag_maker(serialized=True):
        empty_dict()

    dr = dag_maker.create_dagrun()
    dag_maker.run_ti("empty_dict", dr)
    ti = dr.get_task_instances()[0]
    assert ti.xcom_pull() == {}
