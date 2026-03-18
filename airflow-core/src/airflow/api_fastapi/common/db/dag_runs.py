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

from sqlalchemy import func, select
from sqlalchemy.orm import joinedload, selectinload
from sqlalchemy.orm.interfaces import LoaderOption

from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory

dagruns_select_with_state_count = (
    select(
        DagRun.__table__.c.dag_id,
        DagRun.__table__.c.state,
        DagModel.__table__.c.dag_display_name,
        func.count(DagRun.__table__.c.state).label("count"),
    )
    .join(DagModel, DagRun.__table__.c.dag_id == DagModel.__table__.c.dag_id)
    .group_by(DagRun.__table__.c.dag_id, DagRun.__table__.c.state, DagModel.__table__.c.dag_display_name)
    .order_by(DagRun.__table__.c.dag_id)
)


def eager_load_dag_run_for_validation() -> tuple[LoaderOption, ...]:
    """
    Construct the eager loading options necessary for a DagRunResponse object.

    For the list endpoint (get_dag_runs), loading all task instance columns is
    wasteful because we only need the dag_version_id FK to traverse to DagVersion.
    Using load_only() on TaskInstance and TaskInstanceHistory restricts the SELECT
    to just the identity columns and dag_version_id, avoiding large intermediate
    result sets caused by loading heavyweight columns (executor_config, etc.) for
    every task instance across every DAG run returned by the query.
    """
    return (
        joinedload(DagRun.dag_model),
        selectinload(DagRun.task_instances)
        .load_only(TaskInstance.dag_version_id)
        .joinedload(TaskInstance.dag_version)
        .joinedload(DagVersion.bundle),
        selectinload(DagRun.task_instances_histories)
        .load_only(TaskInstanceHistory.dag_version_id)
        .joinedload(TaskInstanceHistory.dag_version)
        .joinedload(DagVersion.bundle),
        joinedload(DagRun.dag_run_note),
    )
