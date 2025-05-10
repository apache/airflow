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
from pydantic import ValidationError
from pytest import Session
from sqlalchemy import select

from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionResponse,
    BulkBody,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.task_instances import BulkTaskInstanceBody
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.models.connection import Connection
from airflow.models.taskinstance import TaskInstance


def update_orm_from_pydantic(
    orm_conn: Connection, pydantic_conn: BulkTaskInstanceBody, update_mask: list[str] | None = None
) -> None:
    """Update ORM object from Pydantic object."""
    # Not all fields match and some need setters, therefore copy partly manually via setters
    non_update_fields = {"task_id", "dag_id", "dag_run_id"}
    setter_fields = {"new_state", "note"}
    fields_set = pydantic_conn.model_fields_set
    if "schema_" in fields_set:  # Alias is not resolved correctly, need to patch
        fields_set.remove("schema_")
        fields_set.add("schema")
    fields_to_update = fields_set - non_update_fields - setter_fields
    if update_mask:
        fields_to_update = fields_to_update.intersection(update_mask)
    conn_data = pydantic_conn.model_dump(by_alias=True)
    for key, val in conn_data.items():
        if key in fields_to_update:
            setattr(orm_conn, key, val)


class BulkTaskInstanceService(BulkService[BulkTaskInstanceBody]):
    """Service for handling bulk operations on task instances."""

    def __init__(
        self, session: Session, request: BulkBody[BulkTaskInstanceBody], dag_id: str, dag_run_id: str
    ):
        super().__init__(session, request)
        self.dag_id = dag_id
        self.dag_run_id = dag_run_id

    def categorize_task_instances(self, task_ids: set) -> tuple[dict, set, set]:
        """
        Categorize the given task_ids into matched_task_ids and not_found_task_ids based on existing task_ids.

        Existed task instances are returned as a dict of {task_id : TaskInstance}.

        :param task_ids: set of task_ids
        :return: tuple of dict of existed task instances, set of matched task_ids, set of not found task_ids
        """
        existed_task_instances = self.session.execute(
            select(TaskInstance).filter(TaskInstance.task_id.in_(task_ids))
        ).scalars()
        existed_task_instances_dict = {
            task_instance.task_id: task_instance for task_instance in existed_task_instances
        }
        matched_task_ids = set(existed_task_instances_dict.keys())
        not_found_task_ids = task_ids - matched_task_ids
        return existed_task_instances_dict, matched_task_ids, not_found_task_ids

    def handle_bulk_create(
        self, action: BulkCreateAction[BulkTaskInstanceBody], results: BulkActionResponse
    ) -> None:
        raise HTTPException(
            status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
            detail="Bulk create is not allowed for task instances.",
        )

    def handle_bulk_update(
        self, action: BulkUpdateAction[BulkTaskInstanceBody], results: BulkActionResponse
    ) -> None:
        """Bulk Update Task Instances."""
        to_update_task_ids = {task_instance.task_id for task_instance in action.entities}
        _, matched_task_ids, not_found_task_ids = self.categorize_task_instances(to_update_task_ids)

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_task_ids:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The task instances with these task_ids: {not_found_task_ids} were not found.",
                )
            if action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                update_task_ids = matched_task_ids
            else:
                update_task_ids = to_update_task_ids

            for task_instance in action.entities:
                if task_instance.task_id in update_task_ids:
                    old_task_instance: TaskInstance = self.session.scalar(
                        select(TaskInstance).filter(TaskInstance.task_id == task_instance.task_id).limit(1)
                    )
                    if old_task_instance is None:
                        raise ValidationError(
                            f"The task instance with task_id: `{task_instance.task_id}` was not found"
                        )
                    BulkTaskInstanceBody(**task_instance.model_dump())

                    update_orm_from_pydantic(old_task_instance, task_instance)
                    results.success.append(task_instance.task_id)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

        except ValidationError as e:
            results.errors.append({"error": f"{e.errors()}"})

    def handle_bulk_delete(
        self, action: BulkDeleteAction[BulkTaskInstanceBody], results: BulkActionResponse
    ) -> None:
        """Bulk delete task instances."""
        to_delete_task_ids = set(action.entities)
        _, matched_task_ids, not_found_task_ids = self.categorize_task_instances(to_delete_task_ids)

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_task_ids:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The task instances with these task_ids: {not_found_task_ids} were not found.",
                )
            if action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                delete_task_ids = matched_task_ids
            else:
                delete_task_ids = to_delete_task_ids

            for task_id in delete_task_ids:
                existing_task_instance = self.session.scalar(
                    select(TaskInstance).where(TaskInstance.task_id == task_id).limit(1)
                )
                if existing_task_instance:
                    self.session.delete(existing_task_instance)
                    results.success.append(task_id)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})
