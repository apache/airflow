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

from sqlalchemy import Column, select
from sqlalchemy.orm import Query, selectinload

from airflow.models import DagRun, TaskInstance
from airflow.models.dag import DagModel
from airflow.task.task_querier_strategy import TaskQuerierStrategy
from airflow.utils.state import DagRunState, TaskInstanceState


class OptimisticTaskQuerierStrategy(TaskQuerierStrategy):
    def get_query(self, priority_order: list[Column], max_tis: int) -> Query:
        query = (
            select(TaskInstance)
            .with_hint(TaskInstance, "USE INDEX (ti_state)", dialect_name="mysql")
            .join(TaskInstance.dag_run)
            .where(DagRun.state == DagRunState.RUNNING)
            .join(TaskInstance.dag_model)
            .where(~DagModel.is_paused)
            .where(TaskInstance.state == TaskInstanceState.SCHEDULED)
            .where(DagModel.bundle_name.is_not(None))
            .order_by(priority_order)
            .options(selectinload(TaskInstance.dag_model))
        )

        return query.limit(max_tis)
