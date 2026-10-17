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

from airflow.models.dynamic_region import (
    SENTINEL_REGION_ID,
    AmbiguousProducerError,
    DynamicRegion,
    ProducerContext,
)
from airflow.models.expandinput import NotFullyPopulated
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XCOM_RETURN_KEY, XComModel
from airflow.sdk import task_group
from airflow.serialization.definitions.xcom_arg import (
    SchedulerPlainXComArg,
    get_task_map_length,
    prefetch_map_lengths,
)
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.mapping import expand_mapped_task_instances, push_mapped_length

pytestmark = pytest.mark.db_test


def _push_length(session, dag_run, task_id, length):
    ti = session.scalars(
        select(TaskInstance).where(
            TaskInstance.dag_id == dag_run.dag_id,
            TaskInstance.task_id == task_id,
            TaskInstance.run_id == dag_run.run_id,
            TaskInstance.map_index == -1,
        )
    ).one()
    push_mapped_length(ti, list(range(length)), session=session)


def _get_expand_input(dag_maker, task_id="show"):
    return dag_maker.serialized_dag.get_task(task_id)._get_specified_expand_input()


def test_map_lengths_over_unmapped_upstreams_use_a_single_query(dag_maker, session):
    with dag_maker(dag_id="unmapped_upstreams", session=session, serialized=True) as dag:

        @dag.task
        def emit_a(): ...

        @dag.task
        def emit_b(): ...

        @dag.task
        def emit_c(): ...

        @dag.task
        def show(a, b, c): ...

        show.expand(a=emit_a(), b=emit_b(), c=emit_c())

    dag_run = dag_maker.create_dagrun()
    for task_id, length in (("emit_a", 2), ("emit_b", 3), ("emit_c", 4)):
        _push_length(session, dag_run, task_id, length)
    session.commit()

    expand_input = _get_expand_input(dag_maker)
    with assert_queries_count(1, session=session):
        lengths = expand_input._get_map_lengths(dag_run.run_id, session=session)
    assert lengths == {"a": 2, "b": 3, "c": 4}


def test_map_lengths_resolve_nested_zip_leaves_in_one_batch(dag_maker, session):
    with dag_maker(dag_id="zipped_upstreams", session=session, serialized=True) as dag:

        @dag.task
        def emit_a(): ...

        @dag.task
        def emit_b(): ...

        @dag.task
        def show(a): ...

        show.expand(a=emit_a().zip(emit_b()))

    dag_run = dag_maker.create_dagrun()
    for task_id, length in (("emit_a", 2), ("emit_b", 3)):
        _push_length(session, dag_run, task_id, length)
    session.commit()

    expand_input = _get_expand_input(dag_maker)
    with assert_queries_count(1, session=session):
        lengths = expand_input._get_map_lengths(dag_run.run_id, session=session)
    assert lengths == {"a": 2}


def test_expand_kwargs_resolves_concatenated_upstreams_in_one_batch(dag_maker, session):
    with dag_maker(dag_id="expand_kwargs_upstreams", session=session, serialized=True) as dag:

        @dag.task
        def emit_a(): ...

        @dag.task
        def emit_b(): ...

        @dag.task
        def show(a): ...

        show.expand_kwargs(emit_a().concat(emit_b()))

    dag_run = dag_maker.create_dagrun()
    for task_id, length in (("emit_a", 2), ("emit_b", 3)):
        _push_length(session, dag_run, task_id, length)
    session.commit()

    expand_input = _get_expand_input(dag_maker)
    with assert_queries_count(1, session=session):
        assert expand_input.get_total_map_length(dag_run.run_id, session=session) == 5


def _make_mapped_upstream_dag(dag_maker, session):
    """Build ``show.expand(a=<mapped task>, b=<unmapped task>)`` with ``a`` expanded to 2 instances."""
    with dag_maker(dag_id="mapped_upstream", session=session, serialized=True) as dag:

        @dag.task
        def emit(): ...

        @dag.task
        def double(x): ...

        @dag.task
        def emit_other(): ...

        @dag.task
        def show(a, b): ...

        show.expand(a=double.expand(x=emit()), b=emit_other())

    dag_run = dag_maker.create_dagrun()
    _push_length(session, dag_run, "emit", 2)
    _push_length(session, dag_run, "emit_other", 4)
    session.flush()
    expand_mapped_task_instances(dag_maker.serialized_dag.get_task("double"), dag_run.run_id, session=session)
    session.flush()
    return dag_run


def _set_upstream_state(session, dag_run, task_id, state):
    for ti in session.scalars(
        select(TaskInstance).where(
            TaskInstance.dag_id == dag_run.dag_id,
            TaskInstance.run_id == dag_run.run_id,
            TaskInstance.task_id == task_id,
        )
    ):
        ti.state = state


@pytest.mark.parametrize(
    ("pushed_map_indexes", "expected"),
    [
        pytest.param([0, 1], {"a": 2, "b": 4}, id="counts-pushed-xcoms"),
        pytest.param([], {"a": 0, "b": 4}, id="finished-without-xcoms-is-zero"),
    ],
)
def test_map_lengths_over_a_finished_mapped_upstream(dag_maker, session, pushed_map_indexes, expected):
    dag_run = _make_mapped_upstream_dag(dag_maker, session)
    _set_upstream_state(session, dag_run, "double", TaskInstanceState.SUCCESS)
    for map_index in pushed_map_indexes:
        XComModel.set(
            key=XCOM_RETURN_KEY,
            value=map_index,
            dag_id=dag_run.dag_id,
            task_id="double",
            run_id=dag_run.run_id,
            map_index=map_index,
            session=session,
        )
    session.commit()

    expand_input = _get_expand_input(dag_maker)
    # One query per upstream kind: the unmapped lengths, the unfinished-instance
    # probe for the mapped upstream, and its pushed-XCom count.
    with assert_queries_count(3, session=session):
        lengths = expand_input._get_map_lengths(dag_run.run_id, session=session)
    assert lengths == expected


def test_map_lengths_over_an_unfinished_mapped_upstream(dag_maker, session):
    dag_run = _make_mapped_upstream_dag(dag_maker, session)
    _set_upstream_state(session, dag_run, "double", TaskInstanceState.RUNNING)
    session.commit()

    expand_input = _get_expand_input(dag_maker)
    with pytest.raises(NotFullyPopulated) as exc_info:
        expand_input._get_map_lengths(dag_run.run_id, session=session)
    assert exc_info.value.missing == {"a"}


def test_prefetch_map_lengths_without_references_runs_no_query(dag_maker, session):
    with dag_maker(dag_id="no_references", session=session, serialized=True):
        pass
    dag_run = dag_maker.create_dagrun()
    session.commit()

    with assert_queries_count(0, session=session):
        assert prefetch_map_lengths([], dag_run.run_id, session=session) == {}


def test_prefetch_leaves_loop_producers_to_the_per_call_lookup(dag_maker, session):
    with dag_maker(dag_id="loop_producer", session=session, serialized=True) as dag:

        @dag.task
        def emit_a(): ...

        @dag.task
        def emit_b(): ...

        @dag.task
        def show(a, b): ...

        show.expand(a=emit_a(), b=emit_b())

    dag_run = dag_maker.create_dagrun()
    for task_id, length in (("emit_a", 2), ("emit_b", 3)):
        _push_length(session, dag_run, task_id, length)
    session.commit()

    contexts = {"emit_a": ProducerContext(SENTINEL_REGION_ID, -1)}
    expand_input = _get_expand_input(dag_maker)
    lengths = prefetch_map_lengths(
        expand_input.value.values(), dag_run.run_id, producer_contexts=contexts, session=session
    )
    assert lengths == {(dag_run.dag_id, "emit_b"): 3}
    assert expand_input._get_map_lengths(dag_run.run_id, session=session, producer_contexts=contexts) == {
        "a": 2,
        "b": 3,
    }


def test_prefetch_records_a_mapped_group_member_as_unresolved(dag_maker, session):
    with dag_maker(dag_id="mapped_group_member", session=session, serialized=True) as dag:

        @dag.task
        def member(): ...

        @task_group
        def group(value):
            member()

        group.expand(value=[1, 2])

    dag_run = dag_maker.create_dagrun()
    session.commit()

    argument = SchedulerPlainXComArg(dag_maker.serialized_dag.get_task("group.member"), XCOM_RETURN_KEY)
    with assert_queries_count(0, session=session):
        lengths = prefetch_map_lengths([argument], dag_run.run_id, session=session)
    assert lengths == {(dag_run.dag_id, "group.member"): None}


def _add_region(session, dag_run, node_id, **kwargs):
    region = DynamicRegion(dag_id=dag_run.dag_id, run_id=dag_run.run_id, node_id=node_id, **kwargs)
    session.add(region)
    session.flush()
    return region


def _add_producer_row(
    session, dag_maker, dag_run, task_id, *, map_index, region_id, state=TaskInstanceState.RUNNING
):
    """Insert one more row of ``task_id`` that has pushed a return value, with a try number of its own."""
    sibling = session.scalars(
        select(TaskInstance).where(
            TaskInstance.dag_id == dag_run.dag_id, TaskInstance.run_id == dag_run.run_id
        )
    ).first()
    ti = TaskInstance(
        task=dag_maker.serialized_dag.get_task(task_id),
        dag_version_id=sibling.dag_version_id,
        run_id=dag_run.run_id,
        state=state,
        map_index=map_index,
        region_id=region_id,
    )
    ti.try_number = 7
    session.add(ti)
    session.flush()
    XComModel.set_for_attempt(
        task_instance_id=ti.id, key=XCOM_RETURN_KEY, value=list(range(9)), mapped_length=9, session=session
    )
    return ti


def _get_expansion_region_id(session, dag_run, task_id):
    return session.scalar(
        select(TaskInstance.region_id).where(
            TaskInstance.dag_id == dag_run.dag_id,
            TaskInstance.run_id == dag_run.run_id,
            TaskInstance.task_id == task_id,
        )
    )


@pytest.mark.parametrize("stray", ["archived-try", "nested-region"])
def test_map_lengths_ignore_rows_that_are_not_live_top_level_producers(dag_maker, session, stray):
    dag_run = _make_mapped_upstream_dag(dag_maker, session)
    _set_upstream_state(session, dag_run, "double", TaskInstanceState.SUCCESS)
    for map_index in (0, 1):
        XComModel.set(
            key=XCOM_RETURN_KEY,
            value=map_index,
            dag_id=dag_run.dag_id,
            task_id="double",
            run_id=dag_run.run_id,
            map_index=map_index,
            session=session,
        )
    if stray == "archived-try":
        earlier = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == dag_run.dag_id,
                TaskInstance.run_id == dag_run.run_id,
                TaskInstance.task_id == "double",
                TaskInstance.map_index == 0,
            )
        ).one()
        earlier.archive(reason="retried", session=session)
        _add_producer_row(
            session,
            dag_maker,
            dag_run,
            "double",
            map_index=0,
            region_id=earlier.region_id,
            state=TaskInstanceState.SUCCESS,
        )
    else:
        loop = _add_region(session, dag_run, "loop")
        nested = _add_region(session, dag_run, "double", parent_region_id=loop.id, parent_region_index=0)
        _add_producer_row(session, dag_maker, dag_run, "double", map_index=0, region_id=nested.id)
    session.commit()

    expand_input = _get_expand_input(dag_maker)
    assert expand_input._get_map_lengths(dag_run.run_id, session=session) == {"a": 2, "b": 4}


def test_map_lengths_read_an_unmapped_producer_only_from_the_sentinel_region(dag_maker, session):
    with dag_maker(dag_id="regional_unmapped", session=session, serialized=True) as dag:

        @dag.task
        def emit(): ...

        @dag.task
        def show(a): ...

        show.expand(a=emit())

    dag_run = dag_maker.create_dagrun()
    own = _add_region(session, dag_run, "emit")
    _add_producer_row(session, dag_maker, dag_run, "emit", map_index=-1, region_id=own.id)
    session.commit()

    expand_input = _get_expand_input(dag_maker)
    with pytest.raises(NotFullyPopulated) as exc_info:
        expand_input._get_map_lengths(dag_run.run_id, session=session)
    assert exc_info.value.missing == {"a"}


def test_map_lengths_reject_two_live_producers_at_one_index(dag_maker, session):
    dag_run = _make_mapped_upstream_dag(dag_maker, session)
    _set_upstream_state(session, dag_run, "double", TaskInstanceState.SUCCESS)
    expansion = _get_expansion_region_id(session, dag_run, "double")
    fork = _add_region(session, dag_run, "double", forked_from_region_id=expansion, resumes_from_index=0)
    _add_producer_row(
        session, dag_maker, dag_run, "double", map_index=0, region_id=fork.id, state=TaskInstanceState.SUCCESS
    )
    session.commit()

    expand_input = _get_expand_input(dag_maker)
    with pytest.raises(AmbiguousProducerError):
        expand_input._get_map_lengths(dag_run.run_id, session=session)
    with pytest.raises(AmbiguousProducerError):
        get_task_map_length(expand_input.value["a"], dag_run.run_id, session=session)
