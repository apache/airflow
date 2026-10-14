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
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.models.dagbag import DBDagBag
from airflow.models.dagrun import DagRun
from airflow.models.dynamic_region import LOOP_DECISION_KEY, DynamicRegion
from airflow.models.task_coordinates import TaskCoordinateResolver
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XComModelV2
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.utils.state import State


@pytest.fixture
def loop_run(dag_maker, session):
    @task_group
    def body():
        prepare = EmptyOperator(task_id="prepare")
        process = PythonOperator.partial(task_id="process", python_callable=list).expand(op_kwargs=[{}, {}])
        consume = EmptyOperator(task_id="consume")
        prepare >> process >> consume

    with dag_maker(serialized=True) as dag:
        loop = create_loop(body, max_iterations=5)
        loop >> EmptyOperator(task_id="outside")
    dr = dag_maker.create_dagrun()
    root = session.scalar(select(DynamicRegion).where(DynamicRegion.node_id == loop.group_id))
    for index in range(1, 5):
        child = DynamicRegion(
            dag_id=dr.dag_id,
            run_id=dr.run_id,
            node_id="body.process",
            parent_region_id=root.id,
            parent_region_index=index,
        )
        session.add(child)
        session.flush()
        for task in loop.iter_tasks():
            mapped = task.task_id == "body.process"
            for slot in range(2) if mapped else [index]:
                session.add(
                    TaskInstance(
                        task=task,
                        run_id=dr.run_id,
                        dag_version_id=dr.created_dag_version_id,
                        region_id=child.id if mapped else root.id,
                        region_index=slot,
                    )
                )
    session.flush()
    tis = list(dr.get_task_instances(session=session))
    regions = {r.id: r for r in session.scalars(select(DynamicRegion))}

    def iteration(ti):
        return regions[ti.region_id].parent_region_index if ti.task_id == "body.process" else ti.region_index

    return dr, dag, loop, root, tis, iteration


@pytest.fixture
def completed_loop(dag_maker, session):
    @task_group
    def body():
        (
            EmptyOperator(task_id="prepare")
            >> EmptyOperator(task_id="process")
            >> EmptyOperator(task_id="consume")
        )

    with dag_maker(serialized=True):
        loop = create_loop(body, max_iterations=5, until=lambda loop: True)
    dr = dag_maker.create_dagrun()
    session.scalar(select(DagRun).where(DagRun.id == dr.id).with_for_update())
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    def complete_pass(index, decision):
        members = [ti for ti in dr.get_task_instances(session=session) if ti.region_index == index]
        for ti in members:
            if ti.state != State.SUCCESS:
                ti.state = State.SUCCESS
                ti.start_date = ti.end_date = timezone.utcnow()
        gate = next(ti for ti in members if ti.task_id == loop.gate_task_id)
        session.add(XComModelV2(task_instance_id=gate.id, key=LOOP_DECISION_KEY, value=decision))
        session.flush()
        group, _ = resolver.loop_context(gate)
        dr.complete_loop_gate(gate, group, State.SUCCESS, session=session)
        session.flush()

    for index in range(5):
        complete_pass(index, "stop" if index == 4 else "continue")
    return dr, loop, complete_pass
