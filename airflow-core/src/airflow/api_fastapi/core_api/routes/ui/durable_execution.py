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

import json

from fastapi import Depends
from pydantic import ValidationError
from sqlalchemy import select

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.ui.durable_execution import DurableExecutionResponse
from airflow.api_fastapi.core_api.security import requires_access_dag
from airflow.api_fastapi.core_api.services.public.task_coordinates import TaskScopeDep
from airflow.models.task_state_store import TaskStateStoreModel

# Must match DURABLE_RECORD_KEY in the task SDK, which writes the record.
DURABLE_RECORD_KEY = "__airflow_durable__"

durable_execution_router = AirflowRouter(
    tags=["Durable Execution"],
    prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/durable",
)


@durable_execution_router.get(
    "",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_durable_execution(scope: TaskScopeDep, session: SessionDep) -> DurableExecutionResponse:
    """Get the durable execution history of a task instance; empty when the task is not durable."""
    value = session.scalar(
        select(TaskStateStoreModel.value).where(
            TaskStateStoreModel.dag_id == scope.dag_id,
            TaskStateStoreModel.run_id == scope.run_id,
            TaskStateStoreModel.task_id == scope.task_id,
            TaskStateStoreModel.region_id == scope.region_id,
            TaskStateStoreModel.region_index == scope.region_index,
            TaskStateStoreModel.key == DURABLE_RECORD_KEY,
        )
    )
    if value is None:
        return DurableExecutionResponse(tries=[])
    try:
        return DurableExecutionResponse.model_validate(json.loads(value))
    except ValidationError:
        # With [workers] state_store_backend set, the row holds only a reference to the offloaded value.
        return DurableExecutionResponse(tries=[])
