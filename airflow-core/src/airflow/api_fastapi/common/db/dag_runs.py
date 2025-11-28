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
        DagRun.dag_id,
        DagRun.state,
        DagModel.dag_display_name,
        func.count(DagRun.state),
    )
    .join(DagModel, DagRun.dag_id == DagModel.dag_id)
    .group_by(DagRun.dag_id, DagRun.state, DagModel.dag_display_name)
    .order_by(DagRun.dag_id)
)


def eager_load_dag_run_for_validation() -> tuple[LoaderOption, ...]:
    """Construct the eager loading options necessary for a DagRunResponse object."""
    return (
        joinedload(DagRun.dag_model),
        selectinload(DagRun.task_instances)
        .joinedload(TaskInstance.dag_version)
        .joinedload(DagVersion.bundle),
        selectinload(DagRun.task_instances_histories)
        .joinedload(TaskInstanceHistory.dag_version)
        .joinedload(DagVersion.bundle),
        joinedload(DagRun.dag_run_note),
    )
