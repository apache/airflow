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

import structlog
from fastapi import HTTPException, Query, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.session import Session

from airflow.api_fastapi.common.dagbag import DagBagDep
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionResponse,
    BulkBody,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.task_instances import BulkTaskInstanceBody, PatchTaskInstanceBody
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.listeners.listener import get_listener_manager
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance as TI
from airflow.utils.state import TaskInstanceState

log = structlog.get_logger(__name__)


def _patch_ti_validate_request(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    dag_bag: DagBagDep,
    body: PatchTaskInstanceBody,
    session: SessionDep,
    map_index: int | None = -1,
    update_mask: list[str] | None = Query(None),
) -> tuple[DAG, list[TI], dict]:
    dag = dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"DAG {dag_id} not found")

    if not dag.has_task(task_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Task '{task_id}' not found in DAG '{dag_id}'")

    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .join(TI.dag_run)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    if map_index is not None:
        query = query.where(TI.map_index == map_index)
    else:
        query = query.order_by(TI.map_index)

    tis = session.scalars(query).all()

    err_msg_404 = (
        f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}` and map_index: `{map_index}` was not found",
    )
    if len(tis) == 0:
        raise HTTPException(status.HTTP_404_NOT_FOUND, err_msg_404)

    fields_to_update = body.model_fields_set
    if update_mask:
        fields_to_update = fields_to_update.intersection(update_mask)
    else:
        try:
            PatchTaskInstanceBody.model_validate(body)
        except ValidationError as e:
            raise RequestValidationError(errors=e.errors())

    return dag, list(tis), body.model_dump(include=fields_to_update, by_alias=True)


class BulkTaskInstanceService(BulkService[BulkTaskInstanceBody]):
    """Service for handling bulk operations on task instances."""

    def __init__(
        self,
        session: Session,
        request: BulkBody[BulkTaskInstanceBody],
        dag_id: str,
        dag_run_id: str,
        dag_bag: DagBagDep,
        user: GetUserDep,
    ):
        super().__init__(session, request)
        self.dag_id = dag_id
        self.dag_run_id = dag_run_id
        self.dag_bag = dag_bag
        self.user = user

    def categorize_task_instances(
        self, task_ids: set[tuple[str, int]]
    ) -> tuple[dict[tuple[str, int], TI], set[tuple[str, int]], set[tuple[str, int]]]:
        """
        Categorize the given task_ids into matched_task_keys and not_found_task_keys based on existing task_ids.

        :param task_ids: set of task_ids
        :return: tuple of (task_instances_map, matched_task_keys, not_found_task_keys)
        """
        query = select(TI).where(
            TI.dag_id == self.dag_id,
            TI.run_id == self.dag_run_id,
            TI.task_id.in_([task_id for task_id, _ in task_ids]),
        )
        task_instances = self.session.scalars(query).all()
        task_instances_map = {
            (ti.task_id, ti.map_index if ti.map_index is not None else -1): ti for ti in task_instances
        }
        matched_task_keys = {(task_id, map_index) for (task_id, map_index) in task_instances_map.keys()}
        not_found_task_keys = {(task_id, map_index) for task_id, map_index in task_ids} - matched_task_keys
        return task_instances_map, matched_task_keys, not_found_task_keys

    def _patch_task_instance_state(
        self,
        dag: DAG,
        task_instance_body: BulkTaskInstanceBody,
        data: dict,
    ) -> None:
        map_indexes = None if task_instance_body.map_index is None else [task_instance_body.map_index]

        updated_tis = dag.set_task_instance_state(
            task_id=task_instance_body.task_id,
            run_id=self.dag_run_id,
            map_indexes=map_indexes,
            state=data["new_state"],
            upstream=task_instance_body.include_upstream,
            downstream=task_instance_body.include_downstream,
            future=task_instance_body.include_future,
            past=task_instance_body.include_past,
            commit=True,
            session=self.session,
        )
        if not updated_tis:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                f"Task id {task_instance_body.task_id} is already in {data['new_state']} state",
            )
        for ti in updated_tis:
            try:
                if data["new_state"] == TaskInstanceState.SUCCESS:
                    get_listener_manager().hook.on_task_instance_success(
                        previous_state=None, task_instance=ti
                    )
                elif data["new_state"] == TaskInstanceState.FAILED:
                    get_listener_manager().hook.on_task_instance_failed(
                        previous_state=None,
                        task_instance=ti,
                        error=f"TaskInstance's state was manually set to `{TaskInstanceState.FAILED}`.",
                    )
            except Exception:
                log.exception("error calling listener")

    def _patch_task_instance_note(self, task_instance_body: BulkTaskInstanceBody, tis: list[TI]) -> None:
        for ti in tis:
            if task_instance_body.note is not None:
                if ti.task_instance_note is None:
                    ti.note = (task_instance_body.note, self.user.get_id())
                else:
                    ti.task_instance_note.content = task_instance_body.note
                    ti.task_instance_note.user_id = self.user.get_id()

    def handle_bulk_create(
        self, action: BulkCreateAction[BulkTaskInstanceBody], results: BulkActionResponse
    ) -> None:
        results.errors.append(
            {
                "error": "Task instances bulk create is not supported",
                "status_code": status.HTTP_405_METHOD_NOT_ALLOWED,
            }
        )

    def handle_bulk_update(
        self, action: BulkUpdateAction[BulkTaskInstanceBody], results: BulkActionResponse
    ) -> None:
        """Bulk Update Task Instances."""
        to_update_task_keys = {
            (task_instance.task_id, task_instance.map_index if task_instance.map_index is not None else -1)
            for task_instance in action.entities
        }
        _, _, not_found_task_keys = self.categorize_task_instances(to_update_task_keys)

        try:
            for task_instance_body in action.entities:
                task_key = (
                    task_instance_body.task_id,
                    task_instance_body.map_index if task_instance_body.map_index is not None else -1,
                )

                if task_key in not_found_task_keys:
                    if action.action_on_non_existence == BulkActionNotOnExistence.FAIL:
                        raise HTTPException(
                            status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"The Task Instance with dag_id: `{self.dag_id}`, run_id: `{self.dag_run_id}`, task_id: `{task_instance_body.task_id}` and map_index: `{task_instance_body.map_index}` was not found",
                        )
                    if action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                        continue

                dag, tis, data = _patch_ti_validate_request(
                    dag_id=self.dag_id,
                    dag_run_id=self.dag_run_id,
                    task_id=task_instance_body.task_id,
                    dag_bag=self.dag_bag,
                    body=task_instance_body,
                    session=self.session,
                    map_index=task_instance_body.map_index,
                    update_mask=None,
                )

                for key, _ in data.items():
                    if key == "new_state":
                        self._patch_task_instance_state(
                            dag=dag,
                            task_instance_body=task_instance_body,
                            data=data,
                        )
                    elif key == "note":
                        self._patch_task_instance_note(task_instance_body=task_instance_body, tis=tis)

                results.success.append(task_instance_body.task_id)
        except ValidationError as e:
            results.errors.append({"error": f"{e.errors()}"})
        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

    def handle_bulk_delete(
        self, action: BulkDeleteAction[BulkTaskInstanceBody], results: BulkActionResponse
    ) -> None:
        """Bulk delete task instances."""
        to_delete_task_keys = set((task_id, -1) for task_id in action.entities)
        _, matched_task_keys, not_found_task_keys = self.categorize_task_instances(to_delete_task_keys)
        not_found_task_ids = [task_id for task_id, _ in not_found_task_keys]

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_task_keys:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The task instances with these task_ids: {not_found_task_ids} were not found",
                )

            for task_id, _ in matched_task_keys:
                existing_task_instance = self.session.scalar(select(TI).where(TI.task_id == task_id).limit(1))
                if existing_task_instance:
                    self.session.delete(existing_task_instance)
                    results.success.append(task_id)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})
