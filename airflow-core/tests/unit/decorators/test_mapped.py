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

from airflow._shared.timezones import timezone
from airflow.sdk import DAG, task

DEFAULT_DATE = timezone.datetime(2025, 1, 1)


@pytest.mark.db_test
def test_mapped_task_with_arbitrary_default_args(dag_maker, session):
    default_args = {"some": "value", "not": "in", "the": "task", "or": "dag"}
    with dag_maker(session=session, default_args=default_args, serialized=True):

        @task.python(do_xcom_push=True)
        def f(x: int, y: int) -> int:
            return x + y

        f.partial(y=10).expand(x=[1, 2, 3])

    dag_run = dag_maker.create_dagrun(session=session)
    decision = dag_run.task_instance_scheduling_decisions(session=session)
    xcoms = set()
    for ti in decision.schedulable_tis:
        dag_maker.run_ti(ti.task_id, dag_run, map_index=ti.map_index)
        xcoms.add(ti.xcom_pull(session=session, task_ids=ti.task_id, map_indexes=ti.map_index))

    assert xcoms == {11, 12, 13}


@pytest.mark.db_test
def test_fail_task_generated_mapping_with_trigger_rule_always__exapnd(dag_maker, session):
    with DAG(dag_id="d", schedule=None, start_date=DEFAULT_DATE):

        @task
        def get_input():
            return ["world", "moon"]

        @task(trigger_rule="always")
        def hello(input):
            print(f"Hello, {input}")

        with pytest.raises(
            ValueError,
            match="Task-generated mapping within a task using 'expand' is not allowed with trigger rule 'always'",
        ):
            hello.expand(input=get_input())


@pytest.mark.db_test
def test_fail_task_generated_mapping_with_trigger_rule_always__exapnd_kwargs(dag_maker, session):
    with DAG(dag_id="d", schedule=None, start_date=DEFAULT_DATE):

        @task
        def get_input():
            return ["world", "moon"]

        @task(trigger_rule="always")
        def hello(input, input2):
            print(f"Hello, {input}, {input2}")

        with pytest.raises(
            ValueError,
            match="Task-generated mapping within a task using 'expand_kwargs' is not allowed with trigger rule 'always'",
        ):
            hello.expand_kwargs([{"input": get_input(), "input2": get_input()}])
