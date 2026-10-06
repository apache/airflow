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

from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken
from airflow.api_fastapi.execution_api.routes.xcoms import _build_xcom_read
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.task import ExecuteTask
from airflow.models.dag_version import DagVersion
from airflow.models.dagbag import DBDagBag
from airflow.models.dynamic_region import AmbiguousProducerError, DynamicRegion
from airflow.models.task_coordinates import TaskCoordinateResolver
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.utils.log.task_log_address import prepare_task_log_contexts

from tests_common.test_utils.asserts import assert_queries_count, count_loaded_task_instances

pytestmark = pytest.mark.db_test


def test_removed_task_coordinates_degrade_to_stored_region_data(dag_maker, session):
    with dag_maker("removed-coordinates", serialized=True):
        EmptyOperator(task_id="kept")
        PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1], [2]])
    dr = dag_maker.create_dagrun()
    with dag_maker(dag_id=dr.dag_id, serialized=True):
        EmptyOperator(task_id="kept")
    latest_version_id = DagVersion.get_latest_version(dr.dag_id, session=session).id
    removed = [ti for ti in dr.task_instances if ti.task_id == "mapped"]
    for ti in removed:
        ti.dag_version_id = latest_version_id
    session.flush()
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    assert sorted(resolver.public_map_index(ti) for ti in removed) == [0, 1]


@pytest.fixture
def loop_coordinates(dag_maker, session):
    @task_group
    def body():
        EmptyOperator(task_id="producer") >> EmptyOperator(task_id="consumer")

    with dag_maker(serialized=True) as dag:
        outside = EmptyOperator(task_id="outside")
        loop = create_loop(body, max_iterations=3)
        outside >> loop
    dr = dag_maker.create_dagrun()
    tis = {ti.task_id: ti for ti in dr.task_instances}
    first = DynamicRegion(dag_id=dr.dag_id, run_id=dr.run_id, node_id=loop.group_id)
    session.add(first)
    session.flush()
    replacement = DynamicRegion(
        dag_id=dr.dag_id, run_id=dr.run_id, node_id=loop.group_id, forked_from_region_id=first.id
    )
    session.add(replacement)
    session.flush()
    producer, consumer = tis["body.producer"], tis["body.consumer"]
    producer.region_id, producer.region_index = first.id, 2
    consumer.region_id, consumer.region_index = replacement.id, 2
    previous = TaskInstance(
        task=dag.get_task("body.producer"), run_id=dr.run_id, dag_version_id=producer.dag_version_id
    )
    previous.region_id, previous.region_index = first.id, 1
    session.add(previous)
    session.flush()
    return dr, producer, consumer, previous, tis["outside"]


@pytest.mark.parametrize("previous_iteration", [False, True])
def test_shared_loop_uses_pinned_graph_and_retained_producer(loop_coordinates, session, previous_iteration):
    dr, producer, consumer, previous, outside = loop_coordinates
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    selected = resolver.resolve(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        task_id=producer.task_id,
        caller=consumer,
        previous_iteration=previous_iteration,
    )

    assert selected == (previous if previous_iteration else producer,)
    assert resolver.public_map_index(producer) == -1
    assert resolver.resolve(dag_id=dr.dag_id, run_id=dr.run_id, task_id=outside.task_id, caller=consumer) == (
        outside,
    )


def test_dependency_rejects_duplicate_latest_gate_coordinates(loop_coordinates, session):
    dr, _, consumer, _, outside = loop_coordinates
    resolver = TaskCoordinateResolver(DBDagBag(), session)
    gate = next(ti for ti in dr.task_instances if ti.task_id == "body.__loop_gate")
    duplicate = TaskInstance(
        task=resolver.get_task(dr.dag_id, dr.run_id, gate.task_id, dag_version_id=gate.dag_version_id),
        run_id=dr.run_id,
        dag_version_id=gate.dag_version_id,
        region_id=consumer.region_id,
        region_index=gate.region_index,
    )
    session.add(duplicate)
    session.flush()

    with pytest.raises(AmbiguousProducerError, match="Multiple live loop gates"):
        resolver.resolve_dependency(outside, gate.task_id)


def test_dependency_on_task_missing_from_pinned_dag_falls_back_to_plain_resolution(loop_coordinates, session):
    _, _, consumer, _, _ = loop_coordinates
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    assert resolver.resolve_dependency(consumer, "removed_upstream") == ()


def test_loop_without_context_requires_explicit_scope(loop_coordinates, session):
    dr, producer, _, _, outside = loop_coordinates
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    with pytest.raises(ValueError, match="loop.*scope"):
        resolver.resolve(dag_id=dr.dag_id, run_id=dr.run_id, task_id=producer.task_id, caller=outside)
    assert resolver.resolve(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        task_id=producer.task_id,
        region_id=producer.region_id,
        region_index=producer.region_index,
    ) == (producer,)


def test_sentinel_consumer_resolves_modern_mapped_producers(dag_maker, session):
    with dag_maker(serialized=True):
        PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1], [2]])
        EmptyOperator(task_id="consumer")
    dr = dag_maker.create_dagrun()
    consumer = next(ti for ti in dr.task_instances if ti.task_id == "consumer")
    mapped = sorted((ti for ti in dr.task_instances if ti.task_id == "mapped"), key=lambda ti: ti.map_index)
    region = DynamicRegion(dag_id=dr.dag_id, run_id=dr.run_id, node_id="mapped")
    session.add(region)
    session.flush()
    for ti in mapped:
        ti.region_id = region.id
    session.flush()
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    assert resolver.resolve(dag_id=dr.dag_id, run_id=dr.run_id, task_id="mapped", caller=consumer) == tuple(
        mapped
    )
    assert [resolver.public_map_index(ti) for ti in mapped] == [0, 1]


def test_workload_batch_reuses_prepared_log_context(loop_coordinates, session):
    _, producer, consumer, previous, _ = loop_coordinates
    contexts = prepare_task_log_contexts([producer, consumer, previous], session=session)
    with assert_queries_count(0):
        workloads = [
            ExecuteTask.make(
                ti,
                log_context=contexts[ti.id],
                bundle_info=BundleInfo(name="dag_maker", version=None),
            )
            for ti in (producer, consumer, previous)
        ]

    assert [workload.ti.map_index for workload in workloads] == [-1, -1, -1]
    assert [workload.ti.region_index for workload in workloads] == [2, 2, 1]
    assert len({workload.log_path for workload in workloads}) == 3


@pytest.mark.parametrize("mixed_versions", [False, True])
def test_unversioned_run_keeps_task_definition_after_latest_graph_changes(
    loop_coordinates, dag_maker, session, mixed_versions
):
    dr, producer, consumer, _, _ = loop_coordinates
    assert dr.bundle_version is None
    with dag_maker(dag_id=dr.dag_id, serialized=True):
        PythonOperator.partial(task_id="body.producer", python_callable=str).expand(op_args=[[1], [2]])
    session.flush()
    if mixed_versions:
        mapped = TaskInstance(
            task=dag_maker.dag.get_task(producer.task_id),
            run_id=dr.run_id,
            dag_version_id=DagVersion.get_latest_version(dr.dag_id, session=session).id,
        )
        region = DynamicRegion(dag_id=dr.dag_id, run_id=dr.run_id, node_id=producer.task_id)
        session.add(region)
        session.flush()
        mapped.region_id, mapped.region_index = region.id, 0
        session.add(mapped)
        session.flush()
    bag = DBDagBag()
    assert bag.get_dag_for_run(dr, session=session).get_task(producer.task_id).get_needs_expansion()
    resolver = TaskCoordinateResolver(bag, session)

    assert resolver.public_map_index(producer) == -1
    assert resolver.resolve(
        dag_id=dr.dag_id, run_id=dr.run_id, task_id=producer.task_id, caller=consumer
    ) == (producer,)
    if mixed_versions:
        with pytest.raises(AmbiguousProducerError, match="definitions differ"):
            resolver.resolve(dag_id=dr.dag_id, run_id=dr.run_id, task_id=producer.task_id)
        assert resolver.resolve(
            dag_id=dr.dag_id,
            run_id=dr.run_id,
            task_id=producer.task_id,
            region_id=mapped.region_id,
            region_index=0,
        ) == (mapped,)


@pytest.fixture
def mapped_run(dag_maker, session):
    def create(mapped_count: int):
        with dag_maker(serialized=True):
            mapped = PythonOperator.partial(task_id="mapped", python_callable=str).expand(
                op_args=[[i] for i in range(mapped_count)]
            )
            mapped >> EmptyOperator(task_id="reduce")
        dr = dag_maker.create_dagrun()
        caller = session.scalars(
            select(TaskInstance).where(TaskInstance.run_id == dr.run_id, TaskInstance.task_id == "reduce")
        ).one()
        session.expire_all()
        return dr, caller

    return create


@pytest.mark.parametrize("mapped_count", [3, 60])
def test_xcom_read_of_one_mapped_slot_loads_no_producer_rows(mapped_run, session, mapped_count):
    dr, caller = mapped_run(mapped_count)
    token = TIToken(id=caller.id, claims=TIClaims())

    with count_loaded_task_instances("mapped") as loaded, assert_queries_count(8):
        read = _build_xcom_read(
            dag_id=dr.dag_id,
            run_id=dr.run_id,
            task_id="mapped",
            key="return_value",
            session=session,
            dag_bag=DBDagBag(),
            token=token,
            map_index=2,
        )
        session.scalars(read).all()

    assert loaded == []


def test_selected_mapped_producers_match_resolved_producers(mapped_run, session):
    dr, caller = mapped_run(6)
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    for map_indexes in (None, 4, range(1, 3), [0, 5]):
        arguments = dict(
            dag_id=dr.dag_id, run_id=dr.run_id, task_id="mapped", caller=caller, map_indexes=map_indexes
        )
        resolved = {ti.id for ti in resolver.resolve(**arguments)}
        assert set(session.scalars(resolver.select_producer_ids(**arguments))) == resolved
        assert resolved
