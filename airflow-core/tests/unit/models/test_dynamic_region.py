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

import struct
from uuid import uuid4

import pytest
from sqlalchemy import event, func, insert, select, text, union_all
from sqlalchemy.exc import IntegrityError

from airflow._shared.timezones import timezone
from airflow.models.dagrun import DagRun
from airflow.models.dynamic_region import (
    _COORDINATES_CACHE_KEY,
    MAX_REGION_DEPTH,
    SENTINEL_REGION_ID,
    AmbiguousProducerError,
    DynamicRegion,
    ProducerContext,
    build_loop_family_legs,
    build_loop_sequence_order,
    get_trajectory,
    resolve_current_producers,
    select_loop_producer_ids,
)
from airflow.models.taskinstance import (
    TaskInstance,
    clear_loop_task_instances,
)
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.asserts import capture_orm_selects
from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_runs()
    yield
    clear_db_runs()


def make_region(dag_run: DagRun, node_id: str = "loop", **kwargs) -> DynamicRegion:
    return DynamicRegion(dag_id=dag_run.dag_id, run_id=dag_run.run_id, node_id=node_id, **kwargs)


def add_region(session, dag_run: DagRun, **kwargs) -> DynamicRegion:
    region = make_region(dag_run, **kwargs)
    session.add(region)
    session.flush()
    return region


@pytest.fixture
def regional_tis(dag_maker, session):
    with dag_maker(serialized=True):
        task = EmptyOperator(task_id="task")
    dr = dag_maker.create_dagrun()
    original = dr.task_instances[0]
    other = TaskInstance(task=task, run_id=dr.run_id, dag_version_id=original.dag_version_id)
    other.region_id = uuid4()
    session.add(other)
    session.flush()
    return original, other


@pytest.fixture
def producer_tis(dag_maker, session):
    with dag_maker(serialized=True) as dag:
        for task_id in ("producer", "consumer", "outside", "mapped"):
            EmptyOperator(task_id=task_id)
    dr = dag_maker.create_dagrun()
    regions = []
    for _ in range(3):
        region = DynamicRegion(dag_id=dr.dag_id, run_id=dr.run_id, node_id="loop")
        if regions:
            region.forked_from_region_id = regions[-1].id
        session.add(region)
        session.flush()
        regions.append(region)
    tis = {ti.task_id: ti for ti in dr.task_instances}
    tis["producer"].region_id = regions[0].id
    tis["producer"].region_index = 2
    tis["consumer"].region_id = regions[2].id
    tis["consumer"].region_index = 2
    previous = TaskInstance(
        dag.get_task("producer"),
        tis["producer"].dag_version_id,
        run_id=dr.run_id,
        map_index=1,
        region_id=regions[0].id,
    )
    session.add(previous)
    session.flush()
    return tis, regions, previous


@pytest.mark.parametrize("previous_iteration", [False, True])
def test_resolve_retained_producer_across_repeated_forks(producer_tis, session, previous_iteration):
    tis, regions, previous = producer_tis
    consumer = tis["consumer"]
    selected = resolve_current_producers(
        dag_id=consumer.dag_id,
        run_id=consumer.run_id,
        task_id="producer",
        is_mapped=False,
        context=ProducerContext(consumer.region_id, consumer.region_index, "loop", previous_iteration),
        session=session,
    )
    assert [ti.id for ti in selected] == [previous.id if previous_iteration else tis["producer"].id]


def test_resolve_producer_outside_the_loop(producer_tis, session):
    tis, _, _ = producer_tis
    consumer = tis["consumer"]
    selected = resolve_current_producers(
        dag_id=consumer.dag_id,
        run_id=consumer.run_id,
        task_id="outside",
        is_mapped=False,
        context=ProducerContext(consumer.region_id, consumer.region_index),
        session=session,
    )
    assert [ti.id for ti in selected] == [tis["outside"].id]


def test_resolver_never_revives_archived_producer(producer_tis, session):
    tis, _, _ = producer_tis
    producer, consumer = tis["producer"], tis["consumer"]
    producer.archive(reason="test", session=session)
    assert (
        resolve_current_producers(
            dag_id=consumer.dag_id,
            run_id=consumer.run_id,
            task_id="producer",
            is_mapped=False,
            context=ProducerContext(consumer.region_id, consumer.region_index, "loop"),
            session=session,
        )
        == ()
    )


def test_resolver_rejects_ambiguous_live_producers(producer_tis, session):
    tis, regions, _ = producer_tis
    producer, consumer = tis["producer"], tis["consumer"]
    other = TaskInstance(
        producer.task,
        producer.dag_version_id,
        run_id=producer.run_id,
        map_index=producer.map_index,
        region_id=regions[1].id,
    )
    session.add(other)
    session.flush()
    with pytest.raises(AmbiguousProducerError):
        resolve_current_producers(
            dag_id=consumer.dag_id,
            run_id=consumer.run_id,
            task_id="producer",
            is_mapped=False,
            context=ProducerContext(consumer.region_id, consumer.region_index, "loop"),
            session=session,
        )


@pytest.mark.parametrize("mapped_caller", [False, True])
def test_mapped_producer_scope_precedes_index_selection(producer_tis, session, mapped_caller):
    tis, regions, _ = producer_tis
    consumer, mapped = tis["consumer"], tis["mapped"]
    children = []
    for parent, iteration in ((regions[0], 2), (regions[2], 2), (regions[2], 3)):
        child = DynamicRegion(
            dag_id=mapped.dag_id,
            run_id=mapped.run_id,
            node_id="mapped",
            parent_region_id=parent.id,
            parent_region_index=iteration,
        )
        session.add(child)
        session.flush()
        children.append(child)
    mapped.region_id, mapped.region_index = children[0].id, 0
    second = TaskInstance(
        mapped.task, mapped.dag_version_id, run_id=mapped.run_id, map_index=1, region_id=children[1].id
    )
    wrong_iteration = TaskInstance(
        mapped.task, mapped.dag_version_id, run_id=mapped.run_id, map_index=0, region_id=children[2].id
    )
    session.add_all([second, wrong_iteration])
    if mapped_caller:
        caller_region = DynamicRegion(
            dag_id=consumer.dag_id,
            run_id=consumer.run_id,
            node_id="consumer",
            parent_region_id=regions[2].id,
            parent_region_index=2,
        )
        session.add(caller_region)
        session.flush()
        consumer.region_id, consumer.region_index = caller_region.id, 5
    session.flush()
    context = ProducerContext(consumer.region_id, consumer.region_index, "loop")
    selected = resolve_current_producers(
        dag_id=mapped.dag_id,
        run_id=mapped.run_id,
        task_id="mapped",
        is_mapped=True,
        context=context,
        session=session,
    )
    assert [ti.id for ti in selected] == [mapped.id, second.id]
    selected = resolve_current_producers(
        dag_id=mapped.dag_id,
        run_id=mapped.run_id,
        task_id="mapped",
        is_mapped=True,
        context=context,
        map_indexes=1,
        session=session,
    )
    assert [ti.id for ti in selected] == [second.id]


@pytest.mark.parametrize("depth", [1, 2, 3])
def test_loop_iteration_lookup_loads_only_the_selected_iterations_rows(producer_tis, session, depth):
    tis, regions, _ = producer_tis
    consumer, mapped = tis["consumer"], tis["mapped"]
    expected = []
    for iteration in range(4):
        parent, parent_index = regions[iteration % 3], iteration
        for level in range(depth):
            nested = DynamicRegion(
                dag_id=mapped.dag_id,
                run_id=mapped.run_id,
                node_id=f"mapped{level}",
                parent_region_id=parent.id,
                parent_region_index=parent_index,
            )
            session.add(nested)
            session.flush()
            parent, parent_index = nested, 0
        for map_index in range(3):
            ti = TaskInstance(
                mapped.task,
                mapped.dag_version_id,
                run_id=mapped.run_id,
                map_index=map_index,
                region_id=parent.id,
            )
            session.add(ti)
            if iteration == consumer.region_index:
                expected.append(ti)
    session.flush()
    expected_ids = {ti.id for ti in expected}
    session.expunge_all()
    loaded = []

    def record_loaded(_, instance):
        loaded.append(instance)

    event.listen(session, "loaded_as_persistent", record_loaded)
    try:
        selected = resolve_current_producers(
            dag_id=mapped.dag_id,
            run_id=mapped.run_id,
            task_id="mapped",
            is_mapped=True,
            context=ProducerContext(consumer.region_id, consumer.region_index, "loop"),
            session=session,
        )
    finally:
        event.remove(session, "loaded_as_persistent", record_loaded)
    assert {ti.id for ti in selected} == expected_ids
    assert {instance.id for instance in loaded if isinstance(instance, TaskInstance)} == expected_ids


def test_loop_iteration_lookup_ignores_another_loops_nested_regions(producer_tis, session):
    tis, regions, _ = producer_tis
    consumer, mapped = tis["consumer"], tis["mapped"]
    other_loop = DynamicRegion(dag_id=mapped.dag_id, run_id=mapped.run_id, node_id="other_loop")
    session.add(other_loop)
    session.flush()
    placed = {}
    for owner, parent in (("own", regions[0]), ("other", other_loop)):
        nested = DynamicRegion(
            dag_id=mapped.dag_id,
            run_id=mapped.run_id,
            node_id="mapped",
            parent_region_id=parent.id,
            parent_region_index=consumer.region_index,
        )
        session.add(nested)
        session.flush()
        placed[owner] = TaskInstance(
            mapped.task, mapped.dag_version_id, run_id=mapped.run_id, map_index=0, region_id=nested.id
        )
        session.add(placed[owner])
    session.flush()
    own_id, other_id = placed["own"].id, placed["other"].id
    session.expunge_all()
    loaded = []

    def record_loaded(_, instance):
        loaded.append(instance)

    event.listen(session, "loaded_as_persistent", record_loaded)
    try:
        selected = resolve_current_producers(
            dag_id=mapped.dag_id,
            run_id=mapped.run_id,
            task_id="mapped",
            is_mapped=True,
            context=ProducerContext(consumer.region_id, consumer.region_index, "loop"),
            session=session,
        )
    finally:
        event.remove(session, "loaded_as_persistent", record_loaded)
    assert [ti.id for ti in selected] == [own_id]
    assert other_id not in {instance.id for instance in loaded if isinstance(instance, TaskInstance)}


def test_previous_iteration_zero_is_missing(producer_tis, session):
    tis, _, _ = producer_tis
    consumer = tis["consumer"]
    assert (
        resolve_current_producers(
            dag_id=consumer.dag_id,
            run_id=consumer.run_id,
            task_id="producer",
            is_mapped=False,
            context=ProducerContext(consumer.region_id, 0, "loop", True),
            session=session,
        )
        == ()
    )


def test_explicit_producer_coordinate_is_task_and_run_scoped(regional_tis, session):
    first, second = regional_tis
    selected = resolve_current_producers(
        dag_id=second.dag_id,
        run_id=second.run_id,
        task_id=second.task_id,
        is_mapped=False,
        region_id=second.region_id,
        region_index=second.region_index,
        session=session,
    )
    assert [ti.id for ti in selected] == [second.id]
    assert (
        resolve_current_producers(
            dag_id=first.dag_id,
            run_id="missing",
            task_id=first.task_id,
            is_mapped=False,
            region_id=second.region_id,
            region_index=second.region_index,
            session=session,
        )
        == ()
    )


def test_region_exact_ti_lookup(regional_tis, session):
    first, second = regional_tis
    found = TaskInstance.get_task_instance(
        second.dag_id,
        second.run_id,
        second.task_id,
        second.map_index,
        region_id=second.region_id,
        session=session,
    )
    assert found.id == second.id
    assert (
        second.dag_run.get_task_instance(second.task_id, region_id=second.region_id, session=session).id
        == second.id
    )
    assert session.scalar(select(TaskInstance).where(TaskInstance.filter_for_tis([second]))).id == second.id
    assert session.scalar(select(TaskInstance).where(TaskInstance.filter_for_tis([first.key]))).id == first.id


def test_dependency_state_change_is_correlated_by_uuid(regional_tis, session, mocker):
    first, second = regional_tis

    def fail_dependency(ti, **kwargs):
        if ti.id == second.id:
            ti.state = TaskInstanceState.UPSTREAM_FAILED
            session.flush()
        return False

    mocker.patch.object(TaskInstance, "are_dependencies_met", autospec=True, side_effect=fail_dependency)
    ready, changed, expanded = first.dag_run._get_ready_tis(list(regional_tis), [], session=session)
    assert ready == []
    assert changed is True
    assert expanded is False
    assert first.state is None
    assert second.state == TaskInstanceState.UPSTREAM_FAILED


def test_mapping_revision_only_changes_selected_expansion(dag_maker, session):
    with dag_maker(serialized=True):
        PythonOperator.partial(task_id="mapped", python_callable=lambda: None).expand(op_kwargs=[{}, {}])
    dr = dag_maker.create_dagrun()
    task = dr.get_dag().get_task("mapped")
    version = dr.task_instances[0].dag_version_id
    ordinary = TaskInstance(task, version, run_id=dr.run_id, map_index=3)
    regional = TaskInstance(task, version, run_id=dr.run_id, map_index=3, region_id=uuid4())
    session.add_all([ordinary, regional])
    session.flush()
    added = dr._revise_map_indexes_if_mapped(regional, session=session)
    assert [(ti.region_id, ti.region_index) for ti in added] == [
        (regional.region_id, 0),
        (regional.region_id, 1),
    ]
    session.refresh(regional)
    session.refresh(ordinary)
    assert regional.state == TaskInstanceState.REMOVED
    assert ordinary.state is None


def test_get_many_orders_resolved_producers_by_logical_date(dag_maker, session):
    with dag_maker(serialized=True):
        EmptyOperator(task_id="task")
    tis = []
    for day in (1, 2):
        ti = dag_maker.create_dagrun(
            run_id=f"run-{day}", logical_date=timezone.datetime(2026, 1, day)
        ).task_instances[0]
        ti.region_id = uuid4()
        ti.region_index = 0
        session.flush()
        XComModel.set_for_attempt(task_instance_id=ti.id, key="key", value=day, session=session)
        tis.append(ti)
    rows = session.scalars(
        XComModel.get_many(
            run_id=tis[1].run_id,
            key="key",
            producer_ids=select(TaskInstance.id).where(TaskInstance.id.in_([ti.id for ti in tis])),
        )
    ).all()
    assert [row.task_instance_id for row in rows] == [tis[1].id, tis[0].id]


def test_get_many_rejects_region_filter_with_prior_dates(regional_tis):
    first, second = regional_tis
    with pytest.raises(ValueError, match="resolved separately"):
        XComModel.get_many(run_id=second.run_id, region_id=second.region_id, include_prior_dates=True)


@pytest.mark.parametrize(
    "coordinate_filter",
    [
        {"task_ids": "task"},
        {"dag_ids": "dag"},
        {"map_indexes": 0},
        {"region_id": None},
        {"region_id": uuid4()},
        {"include_prior_dates": True},
        {"try_number": 2},
    ],
)
def test_get_many_rejects_producer_ids_with_coordinate_filters(coordinate_filter):
    with pytest.raises(ValueError, match="producer_ids cannot be combined"):
        XComModel.get_many(run_id="run", producer_ids=select(TaskInstance.id), **coordinate_filter)


def test_ready_tis_revise_each_region_independently(dag_maker, session, mocker):
    mocker.patch.object(TaskInstance, "are_dependencies_met", autospec=True, return_value=True)
    with dag_maker(serialized=True):
        PythonOperator.partial(task_id="mapped", python_callable=lambda: None).expand(op_kwargs=[{}, {}])
    dr = dag_maker.create_dagrun()
    task = dr.get_dag().get_task("mapped")
    version = dr.task_instances[0].dag_version_id
    regions = [uuid4(), uuid4()]
    stale = [TaskInstance(task, version, run_id=dr.run_id, map_index=3, region_id=r) for r in regions]
    session.add_all(stale)
    session.flush()

    ready, _, _ = dr._get_ready_tis(list(stale), [], session=session)

    for ti in stale:
        session.refresh(ti)
    assert {ti.state for ti in stale} == {TaskInstanceState.REMOVED}
    stale_ids = {ti.id for ti in stale}
    assert sorted((ti.region_id, ti.region_index) for ti in ready if ti.id not in stale_ids) == sorted(
        (region, index) for region in regions for index in (0, 1)
    )


def test_xcom_reads_are_scoped_to_the_producer_region(regional_tis, session):
    first, second = regional_tis
    for ti, value in ((first, "ordinary"), (second, "regional")):
        XComModel.set_for_attempt(task_instance_id=ti.id, key="key", value=value, session=session)
    session.flush()

    def read(**kwargs):
        statement = XComModel.get_many(run_id=first.run_id, dag_ids=first.dag_id, key="key", **kwargs)
        return {row.task_instance_id for row in session.scalars(statement)}

    assert read() == {first.id}
    assert read(region_id=second.region_id) == {second.id}
    assert read(region_id=None) == {first.id, second.id}


def test_public_task_instance_lookup_addresses_a_tasks_own_region_only(dag_maker, session):
    with dag_maker(serialized=True):
        PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1], [2]])
        EmptyOperator(task_id="plain")
    dr = dag_maker.create_dagrun()
    mapped = dr.get_task_instance("mapped", map_index=1, session=session)
    plain = dr.get_task_instance("plain", session=session)
    assert mapped is not None
    assert mapped.region_id != SENTINEL_REGION_ID
    assert TaskInstance.get_task_instance(dr.dag_id, dr.run_id, "mapped", 1, session=session) == mapped
    assert dr.get_task_instance("mapped", map_index=1, region_id=SENTINEL_REGION_ID, session=session) is None
    assert dr.get_task_instance("mapped", map_index=1, region_id=mapped.region_id, session=session) == mapped

    loop = make_region(dr)
    session.add(loop)
    session.flush()
    plain.region_id, plain.region_index = loop.id, 2
    session.flush()

    assert dr.get_task_instance("plain", map_index=2, session=session) is None
    assert dr.get_task_instance("plain", map_index=2, region_id=loop.id, session=session) == plain


def _ti_search_plans(session, statements):
    plans = []
    for statement in statements:
        rows = session.execute(text(f"EXPLAIN QUERY PLAN {statement}")).all()
        plans.extend(
            row[-1]
            for row in rows
            if "task_instance" in row[-1] and ("SEARCH" in row[-1] or "SCAN" in row[-1])
        )
    return plans


def test_public_lookups_use_the_task_instance_unique_key(dag_maker, session):
    if session.get_bind().dialect.name != "sqlite":
        pytest.skip("Reads the SQLite query plan")
    with dag_maker(serialized=True):
        PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[i] for i in range(40)])
    dr = dag_maker.create_dagrun()
    key = TaskInstanceKey(dr.dag_id, "mapped", dr.run_id, map_index=7)
    lookups = {
        "dagrun": lambda: dr.get_task_instance("mapped", map_index=7, session=session),
        "ti": lambda: TaskInstance.get_task_instance(dr.dag_id, dr.run_id, "mapped", 7, session=session),
        "keys": lambda: session.scalars(select(TaskInstance).where(TaskInstance.filter_for_tis([key]))).all(),
        "xcom": lambda: session.scalars(
            XComModel.get_many(
                run_id=dr.run_id, dag_ids=dr.dag_id, task_ids="mapped", map_indexes=7, key="return_value"
            )
        ).all(),
    }

    for name, lookup in lookups.items():
        with capture_orm_selects("task_instance") as statements:
            lookup()
        plans = _ti_search_plans(session, statements)
        assert any("region_id=? AND region_index=?" in plan for plan in plans), (name, plans)
        assert not any("ANY(" in plan for plan in plans), (name, plans)

    with capture_orm_selects("task_instance") as statements:
        dr.reconcile_legacy_expansions(session=session)
    plans = _ti_search_plans(session, statements)
    assert plans, statements
    assert all("(dag_id=?" in plan for plan in plans), (statements, plans)


def test_second_original_region_for_a_slot_is_rejected(regional_tis, session):
    run, _ = regional_tis
    add_region(session, run)
    with pytest.raises(IntegrityError), session.begin_nested():
        add_region(session, run)


def test_nested_slots_are_unique_per_parent_and_index(regional_tis, session):
    run, _ = regional_tis
    parent = add_region(session, run)
    add_region(session, run, node_id="mapped", parent_region_id=parent.id, parent_region_index=0)
    add_region(session, run, node_id="mapped", parent_region_id=parent.id, parent_region_index=1)
    with pytest.raises(IntegrityError), session.begin_nested():
        add_region(session, run, node_id="mapped", parent_region_id=parent.id, parent_region_index=1)


def test_other_nodes_and_runs_do_not_share_a_slot(regional_tis, dag_maker, session):
    run, _ = regional_tis
    add_region(session, run)
    add_region(session, run, node_id="other")
    add_region(session, dag_maker.create_dagrun(run_id="other_run", logical_date=None))


def test_fork_is_exempt_but_a_source_can_only_be_forked_once(regional_tis, session):
    run, _ = regional_tis
    original = add_region(session, run)
    fork = add_region(session, run, forked_from_region_id=original.id, resumes_from_index=1)
    assert original.slot_key is not None
    assert fork.slot_key is None
    with pytest.raises(IntegrityError), session.begin_nested():
        add_region(session, run, forked_from_region_id=original.id, resumes_from_index=2)
    add_region(session, run, forked_from_region_id=fork.id, resumes_from_index=2)


def pack_index(value: int) -> bytes:
    return struct.pack(">i", value)


def test_top_level_region_is_its_own_family_with_an_empty_path(regional_tis, session):
    run, _ = regional_tis
    region = add_region(session, run)

    assert region.family_root_id == region.id
    assert region.path == b""


def test_nested_regions_extend_the_path_by_the_parents_family_and_the_index(regional_tis, session):
    run, _ = regional_tis
    outer = add_region(session, run)
    inner = add_region(session, run, parent_region_id=outer.id, parent_region_index=300)
    innermost = add_region(session, run, parent_region_id=inner.id, parent_region_index=1)

    assert inner.path == outer.id.bytes + pack_index(300)
    assert innermost.path == inner.path + inner.id.bytes + pack_index(1)


def test_forks_share_the_family_and_regions_nested_under_a_fork_use_its_family_root(regional_tis, session):
    run, _ = regional_tis
    original = add_region(session, run)
    fork = add_region(session, run, forked_from_region_id=original.id)
    forked_again = add_region(session, run, forked_from_region_id=fork.id)
    child = add_region(session, run, parent_region_id=forked_again.id, parent_region_index=2)
    grandchild = add_region(session, run, parent_region_id=child.id, parent_region_index=0)

    assert fork.family_root_id == forked_again.family_root_id == original.id
    assert child.family_root_id == child.id
    assert child.path == original.id.bytes + pack_index(2)
    assert grandchild.path == child.path + child.id.bytes + pack_index(0)


@pytest.mark.parametrize("moved", ["parent", "index"])
def test_fork_under_another_parent_than_its_source_is_rejected(regional_tis, session, moved):
    run, _ = regional_tis
    outer = add_region(session, run)
    other = add_region(session, run, node_id="other")
    original = add_region(session, run, node_id="body", parent_region_id=outer.id, parent_region_index=0)
    fork_parent = {
        "parent": {"parent_region_id": other.id, "parent_region_index": 0},
        "index": {"parent_region_id": outer.id, "parent_region_index": 1},
    }[moved]

    with pytest.raises(ValueError, match="same parent"), session.begin_nested():
        add_region(session, run, node_id="body", forked_from_region_id=original.id, **fork_parent)


def test_regions_flushed_together_resolve_a_pending_parent(regional_tis, session):
    run, _ = regional_tis
    parent = make_region(run, id=uuid4())
    child = make_region(run, node_id="child", parent_region_id=parent.id, parent_region_index=4)
    session.add_all([parent, child])
    session.flush()

    assert child.path == parent.id.bytes + pack_index(4)


def test_siblings_under_one_parent_read_the_parent_once(regional_tis, session):
    run, _ = regional_tis
    parent_id = uuid4()
    session.execute(
        insert(DynamicRegion).values(
            id=parent_id,
            dag_id=run.dag_id,
            run_id=run.run_id,
            node_id="parent",
            family_root_id=parent_id,
            path=b"",
            resumes_from_index=0,
            created_at=timezone.utcnow(),
        )
    )
    statements = []

    def record(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT") and "dynamic_region" in statement:
            statements.append(statement)

    event.listen(session.get_bind(), "before_cursor_execute", record)
    try:
        session.add_all(
            make_region(run, node_id=f"child{index}", parent_region_id=parent_id, parent_region_index=index)
            for index in range(5)
        )
        session.flush()
    finally:
        event.remove(session.get_bind(), "before_cursor_execute", record)

    assert len(statements) == 1


@pytest.mark.parametrize(
    ("kwargs", "role"),
    [
        pytest.param({"parent_region_id": uuid4(), "parent_region_index": 0}, "parent", id="parent"),
        pytest.param({"forked_from_region_id": uuid4()}, "fork source", id="fork source"),
    ],
)
def test_unknown_parent_or_fork_source_is_rejected(regional_tis, session, kwargs, role):
    run, _ = regional_tis

    with pytest.raises(ValueError, match=f"The {role} region .* does not exist"), session.begin_nested():
        add_region(session, run, **kwargs)


@pytest.mark.parametrize(
    ("with_parent", "index", "message"),
    [
        pytest.param(True, None, "set together", id="parent without index"),
        pytest.param(False, 3, "set together", id="index without parent"),
        pytest.param(True, -1, "must not be negative", id="negative index"),
    ],
)
def test_invalid_parent_coordinates_are_rejected(regional_tis, session, with_parent, index, message):
    run, _ = regional_tis
    outer = add_region(session, run)
    parent = {"parent_region_id": outer.id} if with_parent else {}

    with pytest.raises(ValueError, match=message), session.begin_nested():
        add_region(session, run, node_id="body", parent_region_index=index, **parent)


def test_regions_nest_at_most_the_maximum_depth(regional_tis, session):
    run, _ = regional_tis
    region = add_region(session, run)
    for _ in range(MAX_REGION_DEPTH):
        region = add_region(session, run, parent_region_id=region.id, parent_region_index=0)

    with pytest.raises(ValueError, match="nest at most"), session.begin_nested():
        add_region(session, run, parent_region_id=region.id, parent_region_index=0)


def test_coordinates_of_a_rolled_back_region_are_forgotten(regional_tis, session):
    run, _ = regional_tis
    with session.begin_nested() as savepoint:
        region_id = add_region(session, run).id
        savepoint.rollback()

    with pytest.raises(ValueError, match="fork source"), session.begin_nested():
        add_region(session, run, forked_from_region_id=region_id)


def test_coordinates_are_forgotten_after_the_flush(regional_tis, session):
    run, _ = regional_tis
    add_region(session, run)

    assert _COORDINATES_CACHE_KEY not in session.info


@pytest.fixture
def family_rows(producer_tis, session):
    """
    Task instances of ``mapped`` in the loop family of ``producer_tis``, in a loop of another node and outside.

    The family is three regions. Each has a row at the iteration it is named for, and the last has a second
    one. Nested regions hold rows at iterations 0 to 3, one of them two levels deep.
    """
    tis, regions, _ = producer_tis
    mapped = tis["mapped"]
    other_loop = add_region(session, mapped.dag_run, node_id="other_loop")

    def add_row(region: DynamicRegion, region_index: int = 0) -> TaskInstance:
        ti = TaskInstance(
            mapped.task,
            mapped.dag_version_id,
            run_id=mapped.run_id,
            map_index=region_index,
            region_id=region.id,
        )
        session.add(ti)
        return ti

    def add_nested(parent: DynamicRegion, iteration: int, node_id: str = "mapped") -> DynamicRegion:
        return add_region(
            session,
            mapped.dag_run,
            node_id=node_id,
            parent_region_id=parent.id,
            parent_region_index=iteration,
        )

    nested_in_first_pass = add_nested(regions[1], 0)
    rows = {
        "direct 0": add_row(regions[0], 0),
        "direct 1": add_row(regions[1], 1),
        "direct 2": add_row(regions[2], 2),
        "direct 3": add_row(regions[2], 3),
        "nested 0": add_row(nested_in_first_pass),
        "nested 2": add_row(add_nested(regions[0], 2)),
        "nested 3": add_row(add_nested(regions[2], 3)),
        "deep 0": add_row(add_nested(nested_in_first_pass, 0, "deep")),
        "other loop": add_row(add_nested(other_loop, 2)),
        "outside": add_row(add_region(session, mapped.dag_run, node_id="outside")),
    }
    session.flush()
    return regions, {name: ti.id for name, ti in rows.items()}


@pytest.mark.parametrize(
    ("iteration", "expected"),
    [
        pytest.param(
            None,
            ["direct 0", "direct 1", "direct 2", "direct 3", "nested 0", "nested 2", "nested 3", "deep 0"],
        ),
        pytest.param(0, ["direct 0", "nested 0", "deep 0"]),
        pytest.param(2, ["direct 2", "nested 2"]),
        pytest.param(3, ["direct 3", "nested 3"]),
    ],
)
def test_loop_family_legs_select_every_member_and_everything_nested_once(
    family_rows, session, iteration, expected
):
    regions, rows = family_rows

    query = select(TaskInstance.id).where(TaskInstance.task_id == "mapped")
    legs = build_loop_family_legs(query, regions[1], iteration=iteration)

    assert sorted(session.scalars(union_all(*legs))) == sorted(rows[name] for name in expected)


def test_get_or_create_returns_the_region_that_filled_the_slot(regional_tis, session):
    run, _ = regional_tis
    slot = {"dag_id": run.dag_id, "run_id": run.run_id, "node_id": "mapped", "session": session}
    first = DynamicRegion.get_or_create(**slot)
    assert DynamicRegion.get_or_create(**slot) is first
    assert DynamicRegion.get_or_create(**slot, parent_region_id=first.id, parent_region_index=0) is not first


def test_get_or_create_reuses_the_region_a_concurrent_writer_created(regional_tis, session, mocker):
    run, _ = regional_tis
    slot = {"dag_id": run.dag_id, "run_id": run.run_id, "node_id": "mapped", "session": session}
    winner = DynamicRegion.get_or_create(**slot)
    session.expunge(winner)
    mocker.patch.object(DynamicRegion, "_find_original", autospec=True, side_effect=[None, winner])

    assert DynamicRegion.get_or_create(**slot) is winner


def test_get_or_create_finds_a_region_created_before_slot_keys_existed(regional_tis, session):
    run, _ = regional_tis
    region_id = uuid4()
    session.execute(
        insert(DynamicRegion).values(
            id=region_id,
            dag_id=run.dag_id,
            run_id=run.run_id,
            node_id="legacy",
            family_root_id=region_id,
            path=b"",
            resumes_from_index=0,
            created_at=timezone.utcnow(),
        )
    )
    session.flush()

    found = DynamicRegion.get_or_create(
        dag_id=run.dag_id, run_id=run.run_id, node_id="legacy", session=session
    )

    assert found.id == region_id
    assert found.slot_key is None
    count = select(func.count()).select_from(DynamicRegion).where(DynamicRegion.node_id == "legacy")
    assert session.scalar(count) == 1


def test_gate_rerun_after_chained_forks_keeps_later_passes_in_later_regions(completed_loop, session):
    dr, loop, complete_pass = completed_loop

    def gate_at(index):
        return next(
            ti
            for ti in dr.get_task_instances(session=session)
            if ti.task_id == loop.gate_task_id and ti.region_index == index
        )

    clear_loop_task_instances([gate_at(3)], downstream=False, session=session)
    complete_pass(3, "continue")
    complete_pass(4, "continue")
    clear_loop_task_instances([gate_at(2)], downstream=False, session=session)
    complete_pass(2, "continue")
    complete_pass(3, "continue")
    before = {
        (ti.task_id, ti.region_index): (ti.id, ti.region_id) for ti in dr.get_task_instances(session=session)
    }
    clear_loop_task_instances([gate_at(2)], downstream=False, later_loop_iterations=False, session=session)

    complete_pass(2, "continue")

    current = {
        (ti.task_id, ti.region_index): (ti.id, ti.region_id) for ti in dr.get_task_instances(session=session)
    }
    assert {key: value for key, value in current.items() if key[1] >= 3} == {
        key: value for key, value in before.items() if key[1] >= 3
    }


def test_repeated_rewind_appends_empty_forks_and_generates_in_latest_region(completed_loop, session):
    dr, loop, complete_pass = completed_loop
    original_region = next(ti.region_id for ti in dr.get_task_instances(session=session))
    for index in (3, 2):
        gate = next(
            ti
            for ti in dr.get_task_instances(session=session)
            if ti.task_id == loop.gate_task_id and ti.region_index == index
        )
        clear_loop_task_instances([gate], downstream=False, session=session)
        complete_pass(index, "stop")
    regions = list(session.scalars(select(DynamicRegion).where(DynamicRegion.dag_id == dr.dag_id)))
    first = next(region for region in regions if region.forked_from_region_id == original_region)
    second = next(region for region in regions if region.forked_from_region_id == first.id)
    assert (first.resumes_from_index, second.resumes_from_index) == (4, 3)
    assert not session.scalar(
        select(TaskInstance.id).where(TaskInstance.region_id.in_([first.id, second.id]))
    )
    gate = next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == loop.gate_task_id and ti.region_index == 2
    )

    clear_loop_task_instances([gate], downstream=False, session=session)
    complete_pass(2, "continue")

    third = session.scalar(select(DynamicRegion).where(DynamicRegion.forked_from_region_id == second.id))
    generated = [ti for ti in dr.get_task_instances(session=session) if ti.region_index == 3]
    assert len(generated) == 4
    assert {ti.region_id for ti in generated} == {third.id}


def read_loop_sequence(session, dag_run, loop, task_id, *, is_mapped):
    ids = select_loop_producer_ids(
        dag_id=dag_run.dag_id,
        run_id=dag_run.run_id,
        loop_node_id=loop.group_id,
        task_id=task_id,
        is_mapped=is_mapped,
        session=session,
    )
    order = build_loop_sequence_order(TaskInstance.region_id, TaskInstance.region_index)
    return list(session.scalars(select(TaskInstance).where(TaskInstance.id.in_(ids)).order_by(*order)))


def test_loop_sequence_of_unmapped_task_is_ordered_by_iteration(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run

    sequence = read_loop_sequence(session, dr, loop, "body.prepare", is_mapped=False)

    assert [ti.region_index for ti in sequence] == [1, 2, 3, 4, 5]
    assert {ti.task_id for ti in sequence} == {"body.prepare"}


def test_loop_sequence_of_mapped_task_is_iteration_major_then_map_index(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run

    sequence = read_loop_sequence(session, dr, loop, "body.process", is_mapped=True)

    assert [(iteration(ti), ti.region_index) for ti in sequence] == [
        (index, slot) for index in range(1, 6) for slot in range(2)
    ]


def test_loop_sequence_skips_archived_and_other_regions(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    archived = next(ti for ti in tis if ti.task_id == "body.prepare" and ti.region_index == 2)
    archived.archive(reason="test", session=session)
    unrelated = DynamicRegion(dag_id=dr.dag_id, run_id=dr.run_id, node_id="other_loop")
    session.add(unrelated)
    session.flush()
    session.add(
        TaskInstance(
            dag.get_task("body.prepare"),
            dr.created_dag_version_id,
            run_id=dr.run_id,
            region_id=unrelated.id,
            region_index=7,
        )
    )
    session.flush()

    sequence = read_loop_sequence(session, dr, loop, "body.prepare", is_mapped=False)

    assert [ti.region_index for ti in sequence] == [1, 3, 4, 5]


def test_loop_sequence_without_a_loop_region_is_empty(dag_maker, session):
    with dag_maker(serialized=True):
        EmptyOperator(task_id="task")
    dr = dag_maker.create_dagrun()
    ids = select_loop_producer_ids(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        loop_node_id="loop",
        task_id="task",
        is_mapped=False,
        session=session,
    )

    assert session.scalars(ids).all() == []


def test_loop_sequence_rejects_a_loop_with_several_executions(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    session.add(
        DynamicRegion(
            dag_id=dr.dag_id,
            run_id=dr.run_id,
            node_id=loop.group_id,
            parent_region_id=root.id,
            parent_region_index=0,
        )
    )
    session.flush()

    with pytest.raises(ValueError, match="several executions"):
        read_loop_sequence(session, dr, loop, "body.prepare", is_mapped=False)


def test_loop_sequence_takes_each_pass_from_the_region_that_owns_it(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    first = add_region(
        session, dr, node_id=loop.group_id, forked_from_region_id=root.id, resumes_from_index=4
    )
    second = add_region(
        session, dr, node_id=loop.group_id, forked_from_region_id=first.id, resumes_from_index=3
    )
    prepare = dag.get_task("body.prepare")
    for region, index in [(first, 4), (first, 5), (second, 3)]:
        session.add(
            TaskInstance(
                prepare, dr.created_dag_version_id, run_id=dr.run_id, region_id=region.id, region_index=index
            )
        )
    session.flush()

    sequence = read_loop_sequence(session, dr, loop, "body.prepare", is_mapped=False)

    assert [(ti.region_id, ti.region_index) for ti in sequence] == [
        (root.id, 1),
        (root.id, 2),
        (second.id, 3),
    ]


def test_loop_sequence_of_mapped_task_skips_passes_a_fork_replaced(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    fork = add_region(session, dr, node_id=loop.group_id, forked_from_region_id=root.id, resumes_from_index=4)
    replacement = add_region(
        session,
        dr,
        node_id="body.process",
        parent_region_id=fork.id,
        parent_region_index=4,
    )
    process = dag.get_task("body.process")
    for slot in range(2):
        session.add(
            TaskInstance(
                process,
                dr.created_dag_version_id,
                run_id=dr.run_id,
                region_id=replacement.id,
                region_index=slot,
            )
        )
    session.flush()

    sequence = read_loop_sequence(session, dr, loop, "body.process", is_mapped=True)

    passes = {region.id: region.parent_region_index for region in session.scalars(select(DynamicRegion))}
    assert [(ti.region_id == replacement.id, passes[ti.region_id], ti.region_index) for ti in sequence] == [
        (False, 1, 0),
        (False, 1, 1),
        (False, 2, 0),
        (False, 2, 1),
        (False, 3, 0),
        (False, 3, 1),
        (True, 4, 0),
        (True, 4, 1),
    ]


@pytest.mark.parametrize("forks", [0, 1, 2])
def test_trajectory_counts_the_forks_a_region_descends_from(forks):
    dag_run = DagRun(dag_id="dag", run_id="run")
    region = make_region(dag_run, id=uuid4())
    regions = {region.id: region}
    for _ in range(forks):
        region = make_region(dag_run, id=uuid4(), forked_from_region_id=region.id)
        regions[region.id] = region

    assert get_trajectory(region, regions) == forks + 1
