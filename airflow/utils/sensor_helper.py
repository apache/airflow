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

from typing import TYPE_CHECKING, cast

from sqlalchemy import func, select

from airflow.models import DagBag, DagRun, TaskInstance
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import tuple_in_condition

if TYPE_CHECKING:
    from sqlalchemy.orm import Query, Session


@provide_session
def _get_count(
    dttm_filter,
    external_task_ids,
    external_task_group_id,
    external_dag_id,
    states,
    session: Session = NEW_SESSION,
) -> int:
    """
    Get the count of records against dttm filter and states.

    :param dttm_filter: date time filter for execution date
    :param external_task_ids: The list of task_ids
    :param external_task_group_id: The ID of the external task group
    :param external_dag_id: The ID of the external DAG.
    :param states: task or dag states
    :param session: airflow session object
    """
    TI = TaskInstance
    DR = DagRun
    if not dttm_filter:
        return 0

    if external_task_ids:
        count = (
            session.scalar(
                _count_query(TI, states, dttm_filter, external_dag_id, session).filter(
                    TI.task_id.in_(external_task_ids)
                )
            )
        ) / len(external_task_ids)
    elif external_task_group_id:
        external_task_group_task_ids = _get_external_task_group_task_ids(
            dttm_filter, external_task_group_id, external_dag_id, session
        )
        if not external_task_group_task_ids:
            count = 0
        else:
            count = (
                session.scalar(
                    _count_query(TI, states, dttm_filter, external_dag_id, session).filter(
                        tuple_in_condition((TI.task_id, TI.map_index), external_task_group_task_ids)
                    )
                )
            ) / len(external_task_group_task_ids)
    else:
        count = session.scalar(_count_query(DR, states, dttm_filter, external_dag_id, session))
    return cast(int, count)


def _count_query(model, states, dttm_filter, external_dag_id, session: Session) -> Query:
    """
    Get the count of records against dttm filter and states.

    :param model: The SQLAlchemy model representing the relevant table.
    :param states: task or dag states
    :param dttm_filter: date time filter for execution date
    :param external_dag_id: The ID of the external DAG.
    :param session: airflow session object
    """
    query = select(func.count()).filter(
        model.dag_id == external_dag_id, model.state.in_(states), model.execution_date.in_(dttm_filter)
    )
    return query


def _get_external_task_group_task_ids(dttm_filter, external_task_group_id, external_dag_id, session):
    """
    Get the count of records against dttm filter and states.

    :param dttm_filter: date time filter for execution date
    :param external_task_group_id: The ID of the external task group
    :param external_dag_id: The ID of the external DAG.
    :param session: airflow session object
    """
    refreshed_dag_info = DagBag(read_dags_from_db=True).get_dag(external_dag_id, session)
    task_group = refreshed_dag_info.task_group_dict.get(external_task_group_id)

    if task_group:
        group_tasks = session.scalars(
            select(TaskInstance).filter(
                TaskInstance.dag_id == external_dag_id,
                TaskInstance.task_id.in_(task.task_id for task in task_group),
                TaskInstance.execution_date.in_(dttm_filter),
            )
        )

        return [(t.task_id, t.map_index) for t in group_tasks]

    # returning default task_id as group_id itself, this will avoid any failure in case of
    # 'check_existence=False' and will fail on timeout
    return [(external_task_group_id, -1)]
