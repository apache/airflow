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

from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
from sqlalchemy import delete, event, select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from airflow._shared.timezones import timezone
from airflow.api_fastapi.core_api.datamodels.task_instances import PatchTaskInstanceBody
from airflow.api_fastapi.core_api.services.public.dag_run import perform_clear_dag_run
from airflow.api_fastapi.core_api.services.public.task_instances import _patch_selected_task_state
from airflow.cli.commands.dag_command import _bulk_clear_runs
from airflow.exceptions import AirflowClearRunningTaskException
from airflow.models.dag_version import DagVersion
from airflow.models.dagbag import DBDagBag
from airflow.models.dagrun import DagRun, clear_partition_runs
from airflow.models.dynamic_region import LOOP_DECISION_KEY, DynamicRegion
from airflow.models.loop_clear import (
    LoopClearScope,
    apply_loop_clear_scope,
    clear_loop_task_instances,
    loop_gate_waits_for_archival,
    select_loop_clear_scope,
)
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.task_coordinates import TaskCoordinateResolver
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.models.xcom import XComModelV2
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.utils.session import create_session
from airflow.utils.state import State

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


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


@pytest.mark.parametrize("downstream", [False, True])
@pytest.mark.parametrize("later", [False, True])
def test_loop_clear_controls_select_exact_live_executions(loop_run, session, downstream, later):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(
        ti for ti in tis if ti.task_id == "body.process" and iteration(ti) == 2 and ti.region_index == 0
    )
    before = [(ti.id, ti.region_id, ti.region_index, ti.state, ti.working_set) for ti in tis]

    scope = select_loop_clear_scope(
        [selected], downstream=downstream, later_loop_iterations=later, session=session
    )

    expected_retry = {selected.id}
    if downstream:
        expected_retry.update(
            ti.id
            for ti in tis
            if ti.task_id == "outside"
            or (iteration(ti) == 2 and ti.task_id in {"body.consume", loop.gate_task_id})
        )
    assert scope.retry_ids == expected_retry
    assert scope.archive_ids == {
        ti.id for ti in tis if ti.task_id != "outside" and iteration(ti) > 2 and downstream and later
    }
    assert [(ti.id, ti.region_id, ti.region_index, ti.state, ti.working_set) for ti in tis] == before
    assert not session.new
    assert not session.deleted
    assert not session.dirty


@pytest.mark.parametrize("whole", [False, True])
def test_whole_mapped_task_selection_stays_in_selected_iteration(loop_run, session, whole):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(
        ti for ti in tis if ti.task_id == "body.process" and iteration(ti) == 2 and ti.region_index == 0
    )

    scope = select_loop_clear_scope(
        [selected, selected],
        whole_expansion_ids={selected.id} if whole else (),
        downstream=False,
        session=session,
    )

    assert scope.retry_ids == {
        ti.id
        for ti in tis
        if ti.task_id == selected.task_id and iteration(ti) == 2 and (whole or ti.region_index == 0)
    }
    assert not scope.archive_ids


def test_direct_gate_clear_archives_later_passes_without_downstream(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    gate = next(ti for ti in tis if ti.task_id == loop.gate_task_id and ti.region_index == 2)

    scope = select_loop_clear_scope([gate], downstream=False, session=session)

    assert scope.retry_ids == {gate.id}
    assert scope.archive_ids == {ti.id for ti in tis if ti.task_id != "outside" and iteration(ti) > 2}


def test_loop_clear_uses_execution_pinned_graph_after_latest_definition_changes(loop_run, dag_maker, session):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(ti for ti in tis if ti.task_id == "body.consume" and ti.region_index == 2)
    with dag_maker(serialized=True, session=session):
        EmptyOperator(task_id="replacement")

    scope = select_loop_clear_scope([selected], later_loop_iterations=False, session=session)

    assert scope.retry_ids == {
        ti.id
        for ti in tis
        if ti.task_id == "outside"
        or (iteration(ti) == 2 and ti.task_id in {selected.task_id, loop.gate_task_id})
    }
    assert not scope.archive_ids


@pytest.mark.parametrize("width", [1, 3])
@pytest.mark.parametrize("whole", [False, True])
def test_loop_clear_preserves_mapped_group_relevant_indexes(dag_maker, session, width, whole):
    @task_group
    def mapped(value):
        PythonOperator(task_id="first", python_callable=list, op_kwargs={"value": value}) >> EmptyOperator(
            task_id="last"
        )

    @task_group
    def body():
        mapped.expand(value=list(range(width)))

    with dag_maker(serialized=True):
        loop = create_loop(body, max_iterations=3)
    dr = dag_maker.create_dagrun()
    tis = dr.get_task_instances(session=session)
    selected = next(ti for ti in tis if ti.task_id == "body.mapped.first" and ti.region_index == 0)

    scope = select_loop_clear_scope(
        [selected], whole_expansion_ids={selected.id} if whole else (), session=session
    )

    assert scope.retry_ids == {
        ti.id for ti in tis if ti.task_id == loop.gate_task_id or whole or ti.region_index == 0
    }
    assert not scope.archive_ids


def test_loop_clear_does_not_resurrect_archived_passes_after_repeated_selection(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(ti for ti in tis if ti.task_id == "body.process" and iteration(ti) == 2)
    first = select_loop_clear_scope([selected], whole_expansion_ids={selected.id}, session=session)
    replacement = DynamicRegion(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        node_id=loop.group_id,
        forked_from_region_id=root.id,
        resumes_from_index=3,
    )
    session.add(replacement)
    session.flush()
    for ti in tis:
        if ti.id in first.archive_ids:
            ti.archive(reason="superseded", session=session)
    session.flush()
    replacement_gate = TaskInstance(
        task=dag.get_task(loop.gate_task_id),
        run_id=dr.run_id,
        dag_version_id=dr.created_dag_version_id,
        region_id=replacement.id,
        region_index=3,
    )
    session.add(replacement_gate)
    session.flush()
    consume = next(ti for ti in tis if ti.task_id == "body.consume" and ti.region_index == 2)

    second = select_loop_clear_scope([consume], later_loop_iterations=False, session=session)
    rewind = select_loop_clear_scope([consume], session=session)

    assert second.retry_ids == {
        ti.id
        for ti in tis
        if ti.task_id == "outside"
        or (iteration(ti) == 2 and ti.task_id in {consume.task_id, loop.gate_task_id})
    }
    assert not second.archive_ids
    assert rewind.archive_ids == {replacement_gate.id}
    assert not (first.archive_ids & (second.retry_ids | rewind.archive_ids))


def test_loop_clear_rejects_deleted_selected_execution(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(ti for ti in tis if ti.task_id == "body.consume")
    selected.archive(reason="superseded", session=session)

    with pytest.raises(ValueError, match="no longer live"):
        select_loop_clear_scope([selected], session=session)


def test_ordinary_seed_without_relatives_selects_only_itself(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(ti for ti in tis if ti.task_id == "outside")

    scope = select_loop_clear_scope([selected], downstream=False, session=session)
    assert scope.retry_ids == {selected.id}
    assert not scope.archive_ids


def test_mixed_clear_scope_applies_together_and_empty_scope_is_noop(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    outside = next(ti for ti in tis if ti.task_id == "outside")
    gate = next(ti for ti in tis if ti.task_id == loop.gate_task_id and ti.region_index == 2)
    scope = select_loop_clear_scope([outside, gate], downstream=False, session=session)
    outside.state = gate.state = State.SUCCESS
    session.flush()

    cleared = apply_loop_clear_scope(scope, session=session, dag_run_state=False)
    assert apply_loop_clear_scope(LoopClearScope(frozenset(), frozenset()), session=session) == []

    assert {ti.id for ti in cleared if ti.working_set is None} == scope.archive_ids
    assert {ti.id for ti in cleared if ti.working_set is True} == {
        ti.id for ti in dr.get_task_instances(session=session) if ti.id not in {t.id for t in tis}
    }
    assert {ti.archived_reason for ti in cleared if ti.working_set is None} == {"superseded"}
    assert outside.working_set is None
    assert gate.working_set is None
    assert {ti.id for ti in dr.get_task_instances(session=session)} - {t.id for t in tis} == {
        ti.id for ti in cleared if ti.working_set is True
    }


def test_running_clear_rejection_does_not_allocate_fork(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    gate = next(ti for ti in tis if ti.task_id == loop.gate_task_id and ti.region_index == 2)
    gate.state = State.RUNNING
    session.flush()
    scope = select_loop_clear_scope([gate], downstream=False, session=session)

    with pytest.raises(AirflowClearRunningTaskException):
        apply_loop_clear_scope(scope, session=session, prevent_running_task=True)

    assert not session.scalar(
        select(DynamicRegion.id).where(DynamicRegion.forked_from_region_id.is_not(None))
    )


@pytest.mark.parametrize("downstream", [False, True])
def test_loop_upstream_scope_stays_in_iteration_before_downstream_expansion(loop_run, session, downstream):
    dr, dag, loop, root, tis, iteration = loop_run
    consume = next(ti for ti in tis if ti.task_id == "body.consume" and ti.region_index == 2)

    scope = select_loop_clear_scope([consume], upstream=True, downstream=downstream, session=session)

    assert scope.retry_ids == {
        ti.id
        for ti in tis
        if (
            ti.task_id != "outside" and iteration(ti) == 2 and (downstream or ti.task_id != loop.gate_task_id)
        )
        or (downstream and ti.task_id == "outside")
    }
    assert scope.archive_ids == {
        ti.id for ti in tis if downstream and ti.task_id != "outside" and iteration(ti) > 2
    }


def test_outside_seed_upstream_selects_all_current_loop_occurrences(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    outside = next(ti for ti in tis if ti.task_id == "outside")

    scope = select_loop_clear_scope([outside], upstream=True, downstream=False, session=session)

    assert scope.retry_ids == {ti.id for ti in tis if ti.task_id == "outside" or iteration(ti) == 0}
    assert scope.archive_ids == {ti.id for ti in tis if ti.task_id != "outside" and iteration(ti) > 0}


@pytest.mark.parametrize("upstream", [False, True])
def test_outside_predecessor_selects_all_mapped_loop_occurrences(dag_maker, session, upstream):
    @task_group
    def body():
        PythonOperator.partial(task_id="process", python_callable=list).expand(op_kwargs=[{}, {}])

    with dag_maker(serialized=True):
        before = EmptyOperator(task_id="before")
        loop = create_loop(body, max_iterations=3)
        before >> loop
    dr = dag_maker.create_dagrun()
    root = session.scalar(select(DynamicRegion).where(DynamicRegion.node_id == loop.group_id))
    child = DynamicRegion(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        node_id="body.process",
        parent_region_id=root.id,
        parent_region_index=1,
    )
    session.add(child)
    session.flush()
    for task in loop.iter_tasks():
        mapped = task.task_id == "body.process"
        for index in range(2) if mapped else [1]:
            session.add(
                TaskInstance(
                    task=task,
                    run_id=dr.run_id,
                    dag_version_id=dr.created_dag_version_id,
                    region_id=child.id if mapped else root.id,
                    region_index=index,
                )
            )
    session.flush()
    tis = dr.get_task_instances(session=session)
    seed = next(
        ti
        for ti in tis
        if ti.task_id == ("body.process" if upstream else "before")
        and ti.region_index == (0 if upstream else -1)
    )

    scope = select_loop_clear_scope(
        [seed],
        upstream=upstream,
        downstream=True,
        later_loop_iterations=False,
        session=session,
    )

    assert scope.retry_ids == {ti.id for ti in tis}
    assert not scope.archive_ids


def test_loop_clear_refreshes_pending_execution_version_after_concurrent_clear(loop_run, dag_maker, session):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(ti for ti in tis if ti.task_id == "body.consume" and ti.region_index == 2)
    old_version = selected.dag_version_id
    selected_id = selected.id

    @task_group
    def body():
        prepare = EmptyOperator(task_id="prepare")
        process = PythonOperator.partial(task_id="process", python_callable=list).expand(op_kwargs=[{}, {}])
        consume = EmptyOperator(task_id="consume")
        prepare >> process >> consume >> EmptyOperator(task_id="new")

    with dag_maker(serialized=True, session=session):
        create_loop(body, max_iterations=5) >> EmptyOperator(task_id="outside")
    new_version = DagVersion.get_latest_version(dr.dag_id, session=session).id
    session.commit()
    assert selected.dag_version_id == old_version
    with create_session(scoped=False) as other_session:
        pending = other_session.get(TaskInstance, selected_id)
        clear_task_instances([pending], other_session, run_on_latest_version=True)
        assert pending.id == selected_id
        assert pending.dag_version_id == new_version
        pending.dag_run.verify_integrity(session=other_session, dag_version_id=new_version)
        new_id = other_session.scalar(
            select(TaskInstance.id).where(
                TaskInstance.dag_id == dr.dag_id,
                TaskInstance.run_id == dr.run_id,
                TaskInstance.task_id == "body.new",
                TaskInstance.region_index == 2,
            )
        )
        assert new_id is not None
    assert selected.dag_version_id == old_version

    scope = select_loop_clear_scope([selected], later_loop_iterations=False, session=session)

    assert scope.retry_ids == {
        ti.id
        for ti in tis
        if ti.task_id == "outside"
        or (iteration(ti) == 2 and ti.task_id in {selected.task_id, loop.gate_task_id})
    } | {new_id}
    assert selected.dag_version_id == new_version


def test_loop_clear_mixes_whole_and_single_index_for_same_task_in_different_passes(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    whole = next(
        ti for ti in tis if ti.task_id == "body.process" and iteration(ti) == 1 and ti.region_index == 0
    )
    single = next(
        ti for ti in tis if ti.task_id == "body.process" and iteration(ti) == 2 and ti.region_index == 0
    )

    scope = select_loop_clear_scope(
        [whole, single], whole_expansion_ids={whole.id}, downstream=False, session=session
    )

    assert scope.retry_ids == {ti.id for ti in tis if ti.task_id == whole.task_id and iteration(ti) == 1} | {
        single.id
    }
    assert not scope.archive_ids


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


def test_integrity_does_not_create_members_at_draining_gate_coordinates(completed_loop, dag_maker, session):
    dr, loop, complete_pass = completed_loop
    gates = {
        ti.region_index: ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == loop.gate_task_id
    }
    gates[4].state = State.RUNNING
    session.flush()
    clear_loop_task_instances([gates[2]], downstream=False, session=session)

    @task_group
    def body():
        (
            EmptyOperator(task_id="prepare")
            >> EmptyOperator(task_id="process")
            >> EmptyOperator(task_id="consume")
            >> EmptyOperator(task_id="new")
        )

    with dag_maker(serialized=True, session=session):
        create_loop(body, max_iterations=5, until=lambda loop: True)
    version = DagVersion.get_latest_version(dr.dag_id, session=session).id
    dr.dag = DBDagBag().get_dag(version_id=version, session=session)

    dr.verify_integrity(session=session, dag_version_id=version)

    assert gates[4].state == State.RESTARTING
    assert set(
        session.scalars(
            select(TaskInstance.region_index).where(
                TaskInstance.dag_id == dr.dag_id,
                TaskInstance.run_id == dr.run_id,
                TaskInstance.task_id == "body.new",
            )
        )
    ) == {0, 1, 2}


@pytest.mark.parametrize("decision", ["stop", "continue"])
def test_repeated_selective_loop_clear_preserves_unselected_work(completed_loop, session, decision):
    dr, loop, complete_pass = completed_loop
    original = {(ti.task_id, ti.region_index): ti.id for ti in dr.get_task_instances(session=session)}
    process = session.get(TaskInstance, original["body.process", 2])

    clear_loop_task_instances([process], session=session)
    complete_pass(2, "continue")
    complete_pass(3, "stop")
    first = {(ti.task_id, ti.region_index): ti.id for ti in dr.get_task_instances(session=session)}
    consume = session.get(TaskInstance, first["body.consume", 2])
    clear_loop_task_instances([consume], later_loop_iterations=False, session=session)
    complete_pass(2, decision)

    current = {(ti.task_id, ti.region_index): ti.id for ti in dr.get_task_instances(session=session)}
    assert current["body.prepare", 2] == original["body.prepare", 2]
    assert current["body.process", 2] == first["body.process", 2] != original["body.process", 2]
    assert current["body.consume", 2] != first["body.consume", 2]
    assert current[loop.gate_task_id, 2] != first[loop.gate_task_id, 2]
    assert {key: value for key, value in current.items() if key[1] == 3} == {
        key: value for key, value in first.items() if key[1] == 3
    }
    assert not any(index == 4 for _, index in current)
    assert session.get(TaskInstance, original["body.prepare", 2]).working_set is True
    original_four = session.get(TaskInstance, original[loop.gate_task_id, 4])
    assert (original_four.working_set, original_four.archived_reason) == (None, "superseded")
    assert session.get(TaskInstance, original["body.process", 2]).archived_reason == "retry"


def test_gate_rerun_after_chained_forks_keeps_later_passes_in_later_regions(completed_loop, session):
    dr, loop, complete_pass = completed_loop

    def gate_at(index):
        return next(
            ti
            for ti in dr.get_task_instances(session=session)
            if ti.task_id == loop.gate_task_id and ti.region_index == index
        )

    clear_loop_task_instances([gate_at(2)], downstream=False, session=session)
    complete_pass(2, "continue")
    complete_pass(3, "continue")
    clear_loop_task_instances([gate_at(1)], downstream=False, session=session)
    complete_pass(1, "continue")
    complete_pass(2, "continue")
    before = {
        (ti.task_id, ti.region_index): (ti.id, ti.region_id) for ti in dr.get_task_instances(session=session)
    }
    clear_loop_task_instances([gate_at(1)], downstream=False, later_loop_iterations=False, session=session)

    complete_pass(1, "continue")

    current = {
        (ti.task_id, ti.region_index): (ti.id, ti.region_id) for ti in dr.get_task_instances(session=session)
    }
    assert {key: value for key, value in current.items() if key[1] >= 2} == {
        key: value for key, value in before.items() if key[1] >= 2
    }


def clear_run_through_dag_clear(dr, session):
    dr.dag.clear(run_id=dr.run_id, session=session)


def clear_run_through_api_service(dr, session):
    perform_clear_dag_run(
        session=session,
        dag=dr.dag,
        dag_run=dr,
        dag_id=dr.dag_id,
        only_failed=False,
        only_new=False,
        run_on_latest_version=False,
        note=None,
        user=None,
    )


def clear_run_through_partition_clear(dr, session):
    clear_partition_runs(
        dag=dr.dag,
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        partition_key=None,
        partition_date_start=None,
        partition_date_end=None,
        clear_tis=True,
        dry_run=False,
        session=session,
    )


def clear_run_through_cli_bulk_clear(dr, session):
    _bulk_clear_runs(dr.dag_id, [dr.run_id], only_failed=False, only_running=False, session=session)


@pytest.mark.parametrize(
    "clear_run",
    [
        clear_run_through_dag_clear,
        clear_run_through_api_service,
        clear_run_through_partition_clear,
        clear_run_through_cli_bulk_clear,
    ],
)
class TestWholeRunClear:
    def test_archives_every_later_pass_and_regenerates_from_the_first(
        self, completed_loop, session, clear_run
    ):
        dr, loop, complete_pass = completed_loop
        original = {(ti.task_id, ti.region_index): ti.id for ti in dr.get_task_instances(session=session)}

        clear_run(dr, session)

        live = dr.get_task_instances(session=session)
        assert {ti.region_index for ti in live} == {0}
        for (task_id, index), ti_id in original.items():
            archived = session.get(TaskInstance, ti_id)
            if index == 0:
                assert (archived.working_set, archived.archived_reason) == (None, "retry"), task_id
            else:
                assert (archived.working_set, archived.archived_reason) == (None, "superseded"), task_id
        fork = session.scalar(select(DynamicRegion).where(DynamicRegion.forked_from_region_id.is_not(None)))
        assert fork.resumes_from_index == 1

        complete_pass(0, "continue")

        regenerated = [ti for ti in dr.get_task_instances(session=session) if ti.region_index == 1]
        assert len(regenerated) == 4
        assert {ti.region_id for ti in regenerated} == {fork.id}

    def test_rerun_can_shorten_the_loop(self, completed_loop, session, clear_run):
        dr, loop, complete_pass = completed_loop

        clear_run(dr, session)
        complete_pass(0, "stop")

        assert {ti.region_index for ti in dr.get_task_instances(session=session)} == {0}


def test_selected_state_patch_locks_dag_run_before_task_instances(completed_loop, session):
    dr, loop, _ = completed_loop
    selected = next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == "body.consume" and ti.region_index == 2
    )
    locked: list[str] = []

    @event.listens_for(session, "do_orm_execute")
    def record_locks(orm_execute_state):
        statement = orm_execute_state.statement
        if isinstance(statement, Select) and statement._for_update_arg is not None:
            locked.append(str(statement))

    try:
        _patch_selected_task_state(
            [selected],
            PatchTaskInstanceBody(new_state="failed"),
            {"new_state": State.FAILED},
            session=session,
            commit=True,
        )
    finally:
        event.remove(session, "do_orm_execute", record_locks)

    tables = ["dag_run" if "FROM dag_run" in sql else "task_instance" for sql in locked]
    assert "task_instance" in tables
    assert tables[0] == "dag_run"


@pytest.mark.backend("mysql", "postgres")
def test_selected_state_patch_and_clear_take_locks_in_the_same_order(completed_loop, session):
    dr, loop, _ = completed_loop
    selected = next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == "body.consume" and ti.region_index == 2
    )
    selected_id = selected.id
    next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == loop.gate_task_id and ti.region_index == 2
    ).state = State.UPSTREAM_FAILED
    session.commit()
    bind = session.get_bind()
    clear_holds_run_lock = Event()

    def patch_state():
        with Session(bind=bind) as patch_session:
            locks = 0
            waited = False

            @event.listens_for(patch_session, "do_orm_execute")
            def pause_after_first_lock(orm_execute_state):
                nonlocal locks, waited
                statement = orm_execute_state.statement
                if isinstance(statement, Select) and statement._for_update_arg is not None:
                    locks += 1
                elif locks and not waited:
                    waited = True
                    clear_holds_run_lock.wait(timeout=3)

            _patch_selected_task_state(
                [patch_session.get(TaskInstance, selected_id)],
                PatchTaskInstanceBody(new_state="failed"),
                {"new_state": State.FAILED},
                session=patch_session,
                commit=True,
            )
            patch_session.commit()

    def clear():
        with Session(bind=bind) as clear_session:
            run_locked = False

            @event.listens_for(clear_session, "do_orm_execute")
            def signal_after_run_lock(orm_execute_state):
                nonlocal run_locked
                statement = orm_execute_state.statement
                if run_locked:
                    clear_holds_run_lock.set()
                elif isinstance(statement, Select) and statement._for_update_arg is not None:
                    run_locked = True

            clear_task_instances([clear_session.get(TaskInstance, selected_id)], session=clear_session)
            clear_session.commit()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(patch_state), pool.submit(clear)]
        for future in futures:
            future.result(timeout=30)


def test_archive_decides_from_the_state_committed_after_the_lock(completed_loop, session):
    dr, loop, _ = completed_loop
    stale = next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == "body.consume" and ti.region_index == 2
    )
    stale.state, stale.start_date, stale.end_date = State.QUEUED, None, None
    session.flush()
    session.execute(
        update(TaskInstance)
        .where(TaskInstance.id == stale.id)
        .values(state=State.RUNNING, start_date=timezone.utcnow())
        .execution_options(synchronize_session=False)
    )

    stale.archive(reason="superseded", session=session)

    session.refresh(stale)
    assert (stale.working_set, stale.state) == (None, State.FAILED)
    assert stale.start_date is not None
    assert stale.end_date is not None


def test_archival_wait_needs_no_dag_definition_before_any_fork(completed_loop, session, mocker):
    dr, loop, _ = completed_loop
    gate = next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == loop.gate_task_id and ti.region_index == 2
    )
    dag_bag = mocker.patch("airflow.models.loop_clear.DBDagBag", autospec=True)
    statements = []

    @event.listens_for(session, "do_orm_execute")
    def count_statements(orm_execute_state):
        statements.append(orm_execute_state.statement)

    try:
        assert loop_gate_waits_for_archival(gate, session=session) is False
    finally:
        event.remove(session, "do_orm_execute", count_statements)

    assert len(statements) == 1
    dag_bag.assert_not_called()


def test_archival_wait_loads_no_task_instance_entities(completed_loop, session):
    dr, loop, _ = completed_loop
    gates = {
        ti.region_index: ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == loop.gate_task_id
    }
    gates[4].state = State.RUNNING
    session.flush()
    clear_loop_task_instances([gates[2]], downstream=False, session=session)
    gate_id = gates[2].id
    session.expire_all()
    session.expunge_all()
    loaded = []

    @event.listens_for(TaskInstance, "load")
    def count_loaded(target, context):
        loaded.append(target.id)

    try:
        assert loop_gate_waits_for_archival(session.get(TaskInstance, gate_id), session=session) is True
    finally:
        event.remove(TaskInstance, "load", count_loaded)

    assert len(loaded) == 1


def test_archival_wait_ignores_terminating_passes_at_or_before_the_gate(completed_loop, session):
    dr, loop, _ = completed_loop
    members = {(ti.task_id, ti.region_index): ti for ti in dr.get_task_instances(session=session)}
    members[loop.gate_task_id, 2].state = State.RUNNING
    session.flush()
    clear_loop_task_instances([members[loop.gate_task_id, 0]], downstream=False, session=session)
    fork = session.scalar(select(DynamicRegion).where(DynamicRegion.forked_from_region_id.is_not(None)))
    gate_task = next(task for task in loop.iter_tasks() if task.task_id == loop.gate_task_id)
    new_gates = {}
    for index in (1, 3):
        new_gates[index] = TaskInstance(
            task=gate_task,
            run_id=dr.run_id,
            dag_version_id=dr.created_dag_version_id,
            region_id=fork.id,
            region_index=index,
            state=State.RUNNING,
        )
        session.add(new_gates[index])
    session.flush()

    assert members[loop.gate_task_id, 2].state == State.RESTARTING
    assert loop_gate_waits_for_archival(new_gates[1], session=session) is True
    assert loop_gate_waits_for_archival(new_gates[3], session=session) is False


def test_repeated_rewind_appends_empty_forks_and_generates_in_latest_region(completed_loop, session):
    dr, loop, complete_pass = completed_loop
    original_region = next(ti.region_id for ti in dr.get_task_instances(session=session))
    for index in (2, 1):
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
    assert (first.resumes_from_index, second.resumes_from_index) == (3, 2)
    assert not session.scalar(
        select(TaskInstance.id).where(TaskInstance.region_id.in_([first.id, second.id]))
    )
    gate = next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == loop.gate_task_id and ti.region_index == 1
    )

    clear_loop_task_instances([gate], downstream=False, session=session)
    complete_pass(1, "continue")

    third = session.scalar(select(DynamicRegion).where(DynamicRegion.forked_from_region_id == second.id))
    generated = [ti for ti in dr.get_task_instances(session=session) if ti.region_index == 2]
    assert len(generated) == 4
    assert {ti.region_id for ti in generated} == {third.id}


def test_ordinary_loop_task_clear_does_not_allocate_fork(completed_loop, session):
    dr, loop, complete_pass = completed_loop
    selected = next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == "body.consume" and ti.region_index == 2
    )
    coordinate = selected.region_id, selected.region_index
    original_id = selected.id

    clear_loop_task_instances([selected], downstream=False, session=session)

    successor = session.scalars(
        select(TaskInstance).where(
            TaskInstance.dag_id == dr.dag_id,
            TaskInstance.task_id == "body.consume",
            TaskInstance.region_index == 2,
            TaskInstance.working_set.is_(True),
        )
    ).one()
    assert successor.id != original_id
    assert (successor.region_id, successor.region_index) == coordinate
    assert (selected.working_set, selected.archived_reason) == (None, "retry")
    assert len(session.scalars(select(DynamicRegion).where(DynamicRegion.dag_id == dr.dag_id)).all()) == 1


def test_mapped_archival_ack_uses_fork_facts_when_definition_is_missing(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    archiving = next(
        ti for ti in tis if ti.task_id == "body.process" and iteration(ti) == 3 and ti.region_index == 0
    )
    archiving.state = State.RUNNING
    session.flush()
    gate = next(ti for ti in tis if ti.task_id == loop.gate_task_id and ti.region_index == 2)
    clear_loop_task_instances([gate], downstream=False, session=session)
    session.execute(
        delete(SerializedDagModel).where(SerializedDagModel.dag_version_id == archiving.dag_version_id)
    )
    session.flush()
    session.expire_all()
    assert DBDagBag().get_dag(archiving.dag_version_id, session=session) is None
    archiving_id = archiving.id

    archiving.complete_restart(session=session)

    archived = session.get(TaskInstance, archiving_id)
    assert (archived.working_set, archived.archived_reason) == (None, "superseded")
