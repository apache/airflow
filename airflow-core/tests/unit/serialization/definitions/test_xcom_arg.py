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

from airflow.models.expandinput import NotFullyPopulated
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskmap import TaskMap
from airflow.models.xcom import XCOM_RETURN_KEY, XComModel
from airflow.serialization.definitions.xcom_arg import prefetch_map_lengths
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.asserts import assert_queries_count

pytestmark = pytest.mark.db_test


def _add_task_map(session, dag_run, task_id, length):
    session.add(
        TaskMap(
            dag_id=dag_run.dag_id,
            task_id=task_id,
            run_id=dag_run.run_id,
            map_index=-1,
            length=length,
            keys=None,
        )
    )


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
        _add_task_map(session, dag_run, task_id, length)
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
        _add_task_map(session, dag_run, task_id, length)
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
        _add_task_map(session, dag_run, task_id, length)
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
    _add_task_map(session, dag_run, "emit", 2)
    _add_task_map(session, dag_run, "emit_other", 4)
    session.flush()
    TaskMap.expand_mapped_task(dag_maker.serialized_dag.get_task("double"), dag_run.run_id, session=session)
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
