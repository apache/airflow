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

from airflow.api_fastapi.core_api.services.public.execution import get_execution_members
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.asserts import assert_queries_count

pytestmark = pytest.mark.db_test


@pytest.mark.parametrize("task_count", [1, 50])
def test_members_load_notes_without_per_row_queries(dag_maker, session, task_count):
    with dag_maker(serialized=True):
        for index in range(task_count):
            EmptyOperator(task_id=f"task_{index:02}")
    run = dag_maker.create_dagrun()
    with assert_queries_count(2):
        members, total = get_execution_members(run, session=session)
        assert [ti.note for ti in members] == [None] * task_count
    assert total == task_count


def test_members_are_live_unless_a_try_number_is_selected(dag_maker, session):
    with dag_maker(serialized=True):
        EmptyOperator(task_id="task")
    run = dag_maker.create_dagrun()
    ti = run.task_instances[0]
    ti.state = TaskInstanceState.SUCCESS
    ti.try_number = 1
    archived_id = ti.id
    clear_task_instances([ti], session=session)
    session.flush()

    live_id = session.scalar(select(TaskInstance.id).where(TaskInstance.working_set.is_(True)))
    assert live_id != archived_id
    live, total = get_execution_members(run, session=session)
    assert (total, [member.id for member in live]) == (1, [live_id])
    archived, total = get_execution_members(run, session=session, try_number=1)
    assert (total, [member.id for member in archived]) == (1, [archived_id])


def test_members_page_in_a_stable_order(dag_maker, session):
    with dag_maker(serialized=True):
        for index in range(3):
            EmptyOperator(task_id=f"task_{index}")
    run = dag_maker.create_dagrun()

    pages = [get_execution_members(run, session=session, limit=1, offset=offset) for offset in range(3)]

    assert {total for _, total in pages} == {3}
    assert [member.task_id for members, _ in pages for member in members] == ["task_0", "task_1", "task_2"]
