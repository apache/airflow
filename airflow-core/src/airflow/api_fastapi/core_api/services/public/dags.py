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

from fastapi import HTTPException, status
from sqlalchemy import select

from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionResponse,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.dags import BulkDAGBody, DAGPatchBody
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.models.dag import DagModel
from airflow.utils.state import DagSchedulingState


def get_scheduling_state(patch_body: DAGPatchBody) -> DagSchedulingState:
    """Resolve the target scheduling state from a legacy `is_paused` or a `scheduling_state` patch body."""
    if patch_body.scheduling_state is not None:
        return patch_body.scheduling_state
    if patch_body.is_paused is True:
        return DagSchedulingState.PAUSED
    return DagSchedulingState.ACTIVE


class BulkDagService(BulkService[BulkDAGBody]):
    """Service for handling bulk operations on Dags."""

    def handle_bulk_create(self, action: BulkCreateAction[BulkDAGBody], results: BulkActionResponse) -> None:
        results.errors.append(
            {
                "error": "Dags bulk create is not supported.",
                "status_code": status.HTTP_405_METHOD_NOT_ALLOWED,
            }
        )

    def handle_bulk_delete(self, action: BulkDeleteAction[BulkDAGBody], results: BulkActionResponse) -> None:
        results.errors.append(
            {
                "error": "Dags bulk delete is not supported. Use the delete Dag endpoint instead.",
                "status_code": status.HTTP_405_METHOD_NOT_ALLOWED,
            }
        )

    def handle_bulk_update(self, action: BulkUpdateAction[BulkDAGBody], results: BulkActionResponse) -> None:
        """Bulk update Dags (pause, resume, or drain)."""
        entities_by_id = {entity.dag_id: entity for entity in action.entities}
        if not entities_by_id:
            return

        dag_map = {
            dag.dag_id: dag
            for dag in self.session.scalars(
                select(DagModel).where(DagModel.dag_id.in_(entities_by_id.keys()))
            )
        }
        not_found_ids = set(entities_by_id) - set(dag_map)

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_ids:
                raise HTTPException(
                    status.HTTP_404_NOT_FOUND,
                    f"The Dags with these ids: {sorted(not_found_ids)} were not found",
                )
            update_ids = (
                set(dag_map)
                if action.action_on_non_existence == BulkActionNotOnExistence.SKIP
                else set(entities_by_id)
            )
            for dag_id in update_ids:
                dag = dag_map.get(dag_id)
                if dag is None:
                    continue
                dag.set_scheduling_state(get_scheduling_state(entities_by_id[dag_id]))
                results.success.append(dag_id)
        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})
