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

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup

from tests.models import DEFAULT_DATE


def test_mapped_task_group_id_prefix_task_id():
    def f(z):
        pass

    with DAG(dag_id="d", schedule=None, start_date=DEFAULT_DATE) as dag:
        x1 = dag.task(task_id="t1")(f).expand(z=[])
        with TaskGroup("g"):
            x2 = dag.task(task_id="t2")(f).expand(z=[])

    assert x1.operator.task_id == "t1"
    assert x2.operator.task_id == "g.t2"

    dag.get_task("t1") == x1.operator
    dag.get_task("g.t2") == x2.operator


@pytest.mark.db_test
def test_mapped_task_with_arbitrary_default_args(dag_maker, session):
    default_args = {"some": "value", "not": "in", "the": "task", "or": "dag"}
    with dag_maker(session=session, default_args=default_args):

        @task.python(do_xcom_push=True)
        def f(x: int, y: int) -> int:
            return x + y

        f.partial(y=10).expand(x=[1, 2, 3])

    dag_run = dag_maker.create_dagrun(session=session)
    decision = dag_run.task_instance_scheduling_decisions(session=session)
    xcoms = set()
    for ti in decision.schedulable_tis:
        ti.run(session=session)
        xcoms.add(ti.xcom_pull(session=session, task_ids=ti.task_id, map_indexes=ti.map_index))

    assert xcoms == {11, 12, 13}
