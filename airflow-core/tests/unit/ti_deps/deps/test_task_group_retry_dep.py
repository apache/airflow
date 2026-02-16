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

from datetime import timedelta

import pytest

from airflow._shared.timezones import timezone
from airflow.models.taskgroupinstance import TaskGroupInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup
from airflow.ti_deps.deps.task_group_retry_dep import TaskGroupRetryDep
from airflow.utils.state import DagRunState

pytestmark = pytest.mark.db_test


def test_task_group_retry_dep_blocks_until_retry_time(dag_maker, session):
    with dag_maker(dag_id="tg_retry_dep", schedule=None, start_date=timezone.datetime(2024, 1, 1)) as dag:
        with TaskGroup(group_id="group", retries=1):
            task = EmptyOperator(task_id="task")

    dr = dag_maker.create_dagrun(run_id="tg_retry_dep_run", state=DagRunState.RUNNING, session=session)
    ti = dr.task_instances[0]
    ti.task = dag.get_task(task.task_id)

    tgi = TaskGroupInstance(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        task_group_id="group",
        try_number=1,
        next_retry_at=timezone.utcnow() + timedelta(minutes=10),
    )
    session.add(tgi)
    session.commit()

    assert not TaskGroupRetryDep().is_met(ti=ti, session=session)


def test_task_group_retry_dep_allows_when_ready(dag_maker, session):
    with dag_maker(
        dag_id="tg_retry_dep_ready", schedule=None, start_date=timezone.datetime(2024, 1, 1)
    ) as dag:
        with TaskGroup(group_id="group", retries=1):
            task = EmptyOperator(task_id="task")

    dr = dag_maker.create_dagrun(run_id="tg_retry_dep_ready_run", state=DagRunState.RUNNING, session=session)
    ti = dr.task_instances[0]
    ti.task = dag.get_task(task.task_id)

    tgi = TaskGroupInstance(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        task_group_id="group",
        try_number=1,
        next_retry_at=timezone.utcnow() - timedelta(minutes=1),
    )
    session.add(tgi)
    session.commit()

    assert TaskGroupRetryDep().is_met(ti=ti, session=session)
