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

import contextlib
import datetime

import pytest
from sqlalchemy import select

from airflow.exceptions import AirflowException
from airflow.models.taskinstance import clear_task_instances
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.state import State

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

pytestmark = [
    pytest.mark.db_test,
    pytest.mark.need_serialized_dag,
    pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="Tests the SDK task runner"),
]


@pytest.mark.parametrize("fails", [False, True])
def test_dag_maker_run_ti_allocates_first_try(dag_maker, session, fails):
    with dag_maker(session=session):
        BashOperator(
            task_id="task",
            bash_command="exit 1" if fails else "true",
            retries=1,
            retry_delay=datetime.timedelta(0),
        )
    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance("task", session=session)
    first_id = ti.id
    with contextlib.suppress(AirflowException):
        dag_maker.run_ti("task", dr)
    ti.refresh_from_db(session=session)
    if fails:
        assert ti.state == State.UP_FOR_RETRY
        assert ti.try_number == 2
        assert ti.id != first_id
        history = session.scalar(
            select(TaskInstanceHistory).where(TaskInstanceHistory.task_instance_id == first_id)
        )
        assert history.try_number == 1
    else:
        assert ti.state == State.SUCCESS
        assert ti.try_number == 1
        assert ti.id == first_id

    if not fails:
        clear_task_instances([ti], session=session)
        session.commit()
    pending_id = ti.id
    assert pending_id != first_id
    with contextlib.suppress(AirflowException):
        dag_maker.run_ti("task", dr)
    ti.refresh_from_db(session=session)
    assert ti.state == (State.FAILED if fails else State.SUCCESS)
    assert ti.try_number == 2
    assert ti.id == pending_id


@pytest.mark.parametrize(
    ("state", "next_method"),
    [(State.UP_FOR_RESCHEDULE, None), (State.QUEUED, "execute")],
)
def test_dag_maker_run_ti_preserves_resumed_try(dag_maker, session, state, next_method):
    with dag_maker(session=session):
        BashOperator(task_id="task", bash_command="true")
    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance("task", session=session)
    ti.try_number = 1
    ti.state = state
    ti.next_method = next_method
    ti.next_kwargs = {} if next_method else None
    session.commit()
    original_id = ti.id

    dag_maker.run_ti("task", dr)

    ti.refresh_from_db(session=session)
    assert ti.state == State.SUCCESS
    assert ti.try_number == 1
    assert ti.id == original_id
