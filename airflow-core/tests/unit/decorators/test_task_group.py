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

import pendulum
import pytest

from airflow.sdk import dag, task, task_group
from airflow.task.trigger_rule import TriggerRule


@pytest.mark.db_test
def test_fail_task_generated_mapping_with_trigger_rule_always(dag_maker, session):
    @dag(schedule=None, start_date=pendulum.datetime(2022, 1, 1))
    def pipeline():
        @task
        def get_param():
            return ["a", "b", "c"]

        @task(trigger_rule=TriggerRule.ALWAYS)
        def t1(param):
            return param

        @task_group()
        def tg(param):
            t1(param)

        with pytest.raises(
            ValueError,
            match="Task-generated mapping within a mapped task group is not allowed with trigger rule 'always'",
        ):
            tg.expand(param=get_param())


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
def test_task_group_expand_kwargs_with_upstream(dag_maker, session, caplog):
    with dag_maker() as dag:

        @dag.task
        def t1():
            return [{"a": 1}, {"a": 2}]

        @task_group("tg1")
        def tg1(a, b):
            @dag.task()
            def t2():
                return [a, b]

            t2()

        tg1.expand_kwargs(t1())

    dr = dag_maker.create_dagrun()
    dr.task_instance_scheduling_decisions()
    assert "Cannot expand" not in caplog.text
    assert "missing upstream values: ['expand_kwargs() argument']" not in caplog.text


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
def test_task_group_expand_with_upstream(dag_maker, session, caplog):
    with dag_maker() as dag:

        @dag.task
        def t1():
            return [1, 2, 3]

        @task_group("tg1")
        def tg1(a, b):
            @dag.task()
            def t2():
                return [a, b]

            t2()

        tg1.partial(a=1).expand(b=t1())

    dr = dag_maker.create_dagrun()
    dr.task_instance_scheduling_decisions()
    assert "Cannot expand" not in caplog.text
    assert "missing upstream values: ['b']" not in caplog.text
