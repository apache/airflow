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

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.utils.state import TaskInstanceState

pytestmark = pytest.mark.db_test


class _IterationCountingList(list):
    def __init__(self, iterable):
        super().__init__(iterable)
        self.iterations = 0

    def __iter__(self):
        self.iterations += 1
        return super().__iter__()


def _create_run_with_tasks(dag_maker, session):
    dr = dag_maker.create_dagrun()
    tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
    for ti in tis.values():
        ti.task = dag_maker.serialized_dag.get_task(ti.task_id)
    return dr, tis


@pytest.mark.parametrize("downstream_state", [None, TaskInstanceState.UP_FOR_RESCHEDULE])
def test_scheduling_pass_iterates_finished_tis_independently_of_task_instance_count(
    dag_maker, session, downstream_state
):
    """
    The dependency checks of a scheduling pass read the finished task instances of their own task
    ids, so the number of iterations of the run's finished list does not grow with the number of
    task instances the pass evaluates. ``UP_FOR_RESCHEDULE`` task instances are evaluated through the
    ``attrs.evolve`` copy that ``are_dependencies_met`` makes of the context.
    """
    with dag_maker(dag_id="finished_tis_index", session=session, serialized=True):
        EmptyOperator(task_id="upstream") >> [EmptyOperator(task_id=f"downstream_{k}") for k in range(20)]
    dr, tis = _create_run_with_tasks(dag_maker, session)
    upstream = tis.pop("upstream")
    upstream.state = TaskInstanceState.SUCCESS
    downstreams = list(tis.values())
    for ti in downstreams:
        ti.state = downstream_state
    session.flush()

    def count_iterations(schedulable_tis):
        finished_tis = _IterationCountingList([upstream])
        ready_tis, _, _ = dr._get_ready_tis(schedulable_tis, finished_tis, session)
        assert {ti.task_id for ti in ready_tis} == {ti.task_id for ti in schedulable_tis}
        return finished_tis.iterations

    assert count_iterations(downstreams[:2]) == count_iterations(downstreams)


def test_context_without_finished_tis_indexes_the_list_each_copy_fetches(dag_maker, session):
    """
    A context created without ``finished_tis`` fetches the list on first use, and each
    ``UP_FOR_RESCHEDULE`` copy that ``are_dependencies_met`` makes fetches a list of its own. An
    upstream that finished between two evaluations must therefore count for the later one.
    """
    with dag_maker(dag_id="finished_tis_index_lazy", session=session, serialized=True):
        EmptyOperator(task_id="upstream_1") >> EmptyOperator(task_id="downstream_1")
        EmptyOperator(task_id="upstream_2") >> EmptyOperator(task_id="downstream_2")
    _, tis = _create_run_with_tasks(dag_maker, session)
    tis["upstream_1"].state = TaskInstanceState.SUCCESS
    tis["downstream_1"].state = TaskInstanceState.UP_FOR_RESCHEDULE
    tis["downstream_2"].state = TaskInstanceState.UP_FOR_RESCHEDULE
    session.flush()
    dep_context = DepContext()

    assert tis["downstream_1"].are_dependencies_met(dep_context=dep_context, session=session)

    tis["upstream_2"].state = TaskInstanceState.SUCCESS
    session.flush()

    assert tis["downstream_2"].are_dependencies_met(dep_context=dep_context, session=session)
    assert "upstream_2" in dep_context.finished_tis_index.by_task_id
