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

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken
from airflow.api_fastapi.execution_api.routes.xcoms import _build_xcom_read
from airflow.exceptions import TaskNotFound
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.task import ExecuteTask
from airflow.models.dag_version import DagVersion
from airflow.models.dagbag import DBDagBag
from airflow.models.dynamic_region import AmbiguousProducerError, DynamicRegion
from airflow.models.task_coordinates import (
    TaskCoordinateResolver,
    build_coordinate_filters,
    get_public_map_index,
    is_plain_expansion,
)
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.utils.log.task_log_address import prepare_task_log_contexts
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.asserts import (
    assert_queries_count,
    capture_orm_selects,
    count_loaded_task_instances,
)

pytestmark = pytest.mark.db_test


def test_loop_iterations_keep_pinned_definition_and_reuse_ancestry(loop_coordinates, dag_maker, session):
    dr, producer, consumer, previous, outside = loop_coordinates
    with dag_maker(dag_id=dr.dag_id, serialized=True):
        EmptyOperator(task_id="body.producer")
    dag_maker.sync_dag_to_db()
    session.flush()
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    assert resolver.loop_iterations(producer) == [("body", 2)]
    with assert_queries_count(0):
        assert resolver.loop_iterations(previous) == [("body", 1)]
        assert resolver.loop_iterations(outside) == []
    assert resolver.loop_iterations(consumer) == [("body", 2)]


def test_mapped_task_loop_iteration_is_separate_from_map_index(dag_maker, session):
    @task_group
    def nested():
        PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1], [2]])

    @task_group
    def body():
        nested()

    with dag_maker(serialized=True):
        create_loop(body, max_iterations=4)
    dr = dag_maker.create_dagrun()
    ti = next(ti for ti in dr.task_instances if ti.task_id == "body.nested.mapped")
    loop_region = session.scalars(
        select(DynamicRegion).where(
            DynamicRegion.dag_id == dr.dag_id,
            DynamicRegion.run_id == dr.run_id,
            DynamicRegion.node_id == "body",
        )
    ).one()
    region = DynamicRegion.get_or_create(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        node_id=ti.task_id,
        parent_region_id=loop_region.id,
        parent_region_index=3,
        session=session,
    )
    session.add(region)
    session.flush()
    ti.region_id, ti.region_index = region.id, 1
    ti.try_number = 1
    ti.state = TaskInstanceState.SUCCESS
    session.flush()
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    assert resolver.loop_iterations(ti) == [("body", 3)]
    assert resolver.public_map_index(ti) == 1


@pytest.mark.parametrize("run_count", [1, 3])
def test_prefetch_regions_walks_ancestry_once_across_runs(dag_maker, session, run_count):
    @task_group
    def body():
        EmptyOperator(task_id="work")

    with dag_maker(serialized=True):
        loop = create_loop(body, max_iterations=3)
    tis = []
    for number in range(run_count):
        dr = dag_maker.create_dagrun(
            run_id=f"run_{number}", logical_date=datetime(2024, 1, 1 + number, tzinfo=UTC)
        )
        ti = next(ti for ti in dr.task_instances if ti.task_id == "body.work")
        replacement = DynamicRegion(
            dag_id=dr.dag_id,
            run_id=dr.run_id,
            node_id=loop.group_id,
            forked_from_region_id=ti.region_id,
            resumes_from_index=1,
        )
        session.add(replacement)
        session.flush()
        ti.region_id, ti.region_index = replacement.id, 1
        tis.append(ti)
    session.flush()
    resolver = TaskCoordinateResolver(DBDagBag(), session)
    resolver.get_dag(tis[0].dag_version_id)

    with assert_queries_count(2):
        resolver.prefetch_regions(tis)
    with assert_queries_count(0):
        assert [resolver.loop_iterations(ti) for ti in tis] == [[("body", 1)]] * run_count


def test_prefetch_regions_skips_mapped_tasks_outside_loops(dag_maker, session):
    with dag_maker("mapped-only", serialized=True):
        PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1], [2]])
    dr = dag_maker.create_dagrun()
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    with capture_orm_selects("dynamic_region") as statements:
        resolver.prefetch_regions(dr.task_instances)

    assert statements == []


def test_loop_context_reuses_ancestry_loaded_by_loop_iterations(loop_coordinates, session):
    _, producer, _, previous, _ = loop_coordinates
    resolver = TaskCoordinateResolver(DBDagBag(), session)
    resolver.loop_iterations(producer)

    with assert_queries_count(0):
        group, index = resolver.loop_context(previous)

    assert (group.group_id, index) == ("body", 1)


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
    assert [resolver.loop_iterations(ti) for ti in removed] == [[], []]
    with pytest.raises(TaskNotFound):
        resolver.loop_context(removed[0])


@pytest.mark.parametrize(
    ("kind", "expected_index", "expected_plain", "expected_queries"),
    [("legacy", 2, False, 0), ("mapped", 2, True, 1), ("loop", -1, False, 1)],
)
def test_public_map_index_and_plain_expansion_of_a_persisted_task_instance(
    dag_maker, session, kind, expected_index, expected_plain, expected_queries
):
    with dag_maker("public-index", serialized=True):
        if kind == "mapped":
            PythonOperator.partial(task_id="work", python_callable=list).expand(op_kwargs=[{}, {}, {}])
        else:
            EmptyOperator(task_id="work")
    dr = dag_maker.create_dagrun()
    ti = max(dr.task_instances, key=lambda ti: ti.region_index)
    if kind != "mapped":
        ti.region_index = 2
    if kind == "loop":
        region = DynamicRegion(dag_id=dr.dag_id, run_id=dr.run_id, node_id="loop")
        session.add(region)
        session.flush()
        ti.region_id = region.id
    session.flush()

    with assert_queries_count(expected_queries):
        assert get_public_map_index(ti, session=session) == expected_index
    assert is_plain_expansion(ti, session=session) is expected_plain


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
    first = DynamicRegion.get_or_create(
        dag_id=dr.dag_id, run_id=dr.run_id, node_id=loop.group_id, session=session
    )
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


@pytest.fixture
def forked_loop_coordinates(loop_coordinates, session):
    replacement = session.scalars(
        select(DynamicRegion).where(DynamicRegion.forked_from_region_id.is_not(None))
    ).one()
    replacement.resumes_from_index = 2
    session.flush()
    return loop_coordinates


def test_outside_caller_reads_the_live_iterations_each_region_owns(forked_loop_coordinates, session):
    dr, producer, consumer, previous, outside = forked_loop_coordinates
    resolver = TaskCoordinateResolver(DBDagBag(), session)
    arguments = dict(
        dag_id=dr.dag_id, run_id=dr.run_id, task_id=producer.task_id, caller=outside, all_iterations=True
    )

    selected = resolver.resolve(**arguments)
    selected_ids = set(session.scalars(resolver.select_producer_ids(**arguments)))

    assert selected == (previous,)
    assert selected_ids == {previous.id}


@pytest.mark.parametrize("by", ["resolve", "select_producer_ids"])
def test_outside_caller_is_refused_a_loop_task_unless_it_reads_every_iteration(loop_coordinates, session, by):
    dr, producer, consumer, previous, outside = loop_coordinates
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    with pytest.raises(ValueError, match="explicit scope or a consumer inside the loop"):
        getattr(resolver, by)(dag_id=dr.dag_id, run_id=dr.run_id, task_id=producer.task_id, caller=outside)


def test_reading_every_iteration_cannot_be_combined_with_the_previous_one(loop_coordinates, session):
    dr, producer, consumer, previous, outside = loop_coordinates
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    with pytest.raises(ValueError, match="requires a shared loop scope"):
        resolver.resolve(
            dag_id=dr.dag_id,
            run_id=dr.run_id,
            task_id=producer.task_id,
            caller=outside,
            all_iterations=True,
            previous_iteration=True,
        )


def test_all_iterations_does_not_widen_the_read_of_a_caller_inside_the_loop(loop_coordinates, session):
    dr, producer, consumer, previous, outside = loop_coordinates
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    selected = resolver.resolve(
        dag_id=dr.dag_id, run_id=dr.run_id, task_id=producer.task_id, caller=consumer, all_iterations=True
    )

    assert selected == (producer,)


def test_dependency_of_an_outside_task_on_a_loop_task_is_every_live_iteration(loop_coordinates, session):
    dr, producer, consumer, previous, outside = loop_coordinates
    replacement = session.scalars(
        select(DynamicRegion).where(DynamicRegion.forked_from_region_id.is_not(None))
    ).one()
    replacement.resumes_from_index = 3
    first = TaskInstance(
        task=producer.task,
        run_id=dr.run_id,
        dag_version_id=producer.dag_version_id,
        region_id=producer.region_id,
    )
    first.region_index = 0
    session.add(first)
    session.flush()
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    assert set(resolver.resolve_dependency(outside, producer.task_id)) == {first, previous, producer}


@pytest.mark.parametrize(
    ("caller", "task_id", "run_id", "expected"),
    [
        pytest.param("outside", "body.producer", None, True, id="outside caller of a loop task"),
        pytest.param("consumer", "body.producer", None, False, id="caller inside the loop"),
        pytest.param("outside", "outside", None, False, id="task outside any loop"),
        pytest.param("outside", "removed", None, False, id="task missing from the pinned dag"),
        pytest.param("outside", "body.producer", "other_run", False, id="caller of another run"),
        pytest.param(None, "body.producer", None, False, id="no caller"),
    ],
)
def test_loop_task_read_from_outside_needs_a_caller_of_the_same_run_outside_the_loop(
    loop_coordinates, session, caller, task_id, run_id, expected
):
    dr, producer, consumer, previous, outside = loop_coordinates
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    assert (
        resolver.is_loop_task_read_from_outside(
            dag_id=dr.dag_id,
            run_id=run_id or dr.run_id,
            task_id=task_id,
            caller={"consumer": consumer, "outside": outside, None: None}[caller],
        )
        is expected
    )


def test_loop_passes_load_the_region_ancestry_once_for_all_task_instances(loop_coordinates, session):
    _, producer, consumer, previous, outside = loop_coordinates
    resolver = TaskCoordinateResolver(DBDagBag(), session)

    with capture_orm_selects("dynamic_region") as statements:
        passes = resolver.get_loop_passes([producer, consumer, previous, outside])

    assert passes == [2, 2, 1, None]
    assert len(statements) == 1


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


def test_coordinate_filters_match_the_exact_region_or_the_public_index(loop_coordinates, session):
    _, producer, _, previous, _ = loop_coordinates
    task = TaskInstance.task_id == producer.task_id

    by_public_index = session.scalars(
        select(TaskInstance.id).where(task, *build_coordinate_filters(TaskInstance, map_index=-1))
    ).all()
    exact = session.scalars(
        select(TaskInstance.id).where(
            task,
            *build_coordinate_filters(
                TaskInstance, map_index=-1, region_id=producer.region_id, region_index=producer.region_index
            ),
        )
    ).all()

    assert set(by_public_index) == {producer.id, previous.id}
    assert exact == [producer.id]


def test_coordinate_filters_require_a_region_id_with_a_region_index():
    with pytest.raises(ValueError, match="region_index requires region_id"):
        build_coordinate_filters(TaskInstance, map_index=-1, region_index=2)


def test_sentinel_consumer_resolves_modern_mapped_producers(dag_maker, session):
    with dag_maker(serialized=True):
        PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1], [2]])
        EmptyOperator(task_id="consumer")
    dr = dag_maker.create_dagrun()
    consumer = next(ti for ti in dr.task_instances if ti.task_id == "consumer")
    mapped = sorted((ti for ti in dr.task_instances if ti.task_id == "mapped"), key=lambda ti: ti.map_index)
    region = DynamicRegion.get_or_create(
        dag_id=dr.dag_id, run_id=dr.run_id, node_id="mapped", session=session
    )
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
        region = DynamicRegion.get_or_create(
            dag_id=dr.dag_id, run_id=dr.run_id, node_id=producer.task_id, session=session
        )
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
        session.scalars(read.statement).all()

    assert loaded == []


def test_loaded_task_instance_collector_keeps_only_the_requested_task(mapped_run, session):
    dr, _ = mapped_run(3)

    with (
        count_loaded_task_instances("mapped") as loaded,
        count_loaded_task_instances("other") as other_loaded,
    ):
        rows = session.scalars(
            select(TaskInstance).where(TaskInstance.run_id == dr.run_id, TaskInstance.task_id == "mapped")
        ).all()

    assert len(rows) == 3
    assert sorted(ti.id for ti in loaded) == sorted(ti.id for ti in rows)
    assert other_loaded == []


@pytest.mark.parametrize("mapped_count", [3, 60])
def test_loop_read_of_one_mapped_slot_loads_no_producer_rows(dag_maker, session, mapped_count):
    @task_group
    def body():
        PythonOperator.partial(task_id="mapped", python_callable=str).expand(
            op_args=[[i] for i in range(mapped_count)]
        ) >> EmptyOperator(task_id="reduce")

    with dag_maker(serialized=True):
        create_loop(body, max_iterations=3)
    dr = dag_maker.create_dagrun()
    caller = session.scalars(
        select(TaskInstance).where(TaskInstance.run_id == dr.run_id, TaskInstance.task_id == "body.reduce")
    ).one()
    session.expire_all()
    token = TIToken(id=caller.id, claims=TIClaims())

    with count_loaded_task_instances("body.mapped") as loaded:
        read = _build_xcom_read(
            dag_id=dr.dag_id,
            run_id=dr.run_id,
            task_id="body.mapped",
            key="return_value",
            session=session,
            dag_bag=DBDagBag(),
            token=token,
            map_index=2,
        )
        session.scalars(read.statement).all()

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
