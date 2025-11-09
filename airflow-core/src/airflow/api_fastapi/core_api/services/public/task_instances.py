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

from collections.abc import Sequence

import structlog
from fastapi import HTTPException, Query, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from sqlalchemy import select, tuple_
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.session import Session

from airflow.api_fastapi.common.dagbag import DagBagDep, get_latest_version_of_dag
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
from airflow.models.taskinstance import TaskInstance as TI
from airflow.serialization.serialized_objects import SerializedDAG
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
) -> tuple[SerializedDAG, list[TI], dict]:
    dag = get_latest_version_of_dag(dag_bag, dag_id, session)
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


def _patch_task_instance_state(
    task_id: str,
    dag_run_id: str,
    dag: SerializedDAG,
    task_instance_body: BulkTaskInstanceBody | PatchTaskInstanceBody,
    data: dict,
    session: Session,
) -> None:
    map_index = getattr(task_instance_body, "map_index", None)
    map_indexes = None if map_index is None else [map_index]

    updated_tis = dag.set_task_instance_state(
        task_id=task_id,
        run_id=dag_run_id,
        map_indexes=map_indexes,
        state=data["new_state"],
        upstream=task_instance_body.include_upstream,
        downstream=task_instance_body.include_downstream,
        future=task_instance_body.include_future,
        past=task_instance_body.include_past,
        commit=True,
        session=session,
    )
    if not updated_tis:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Task id {task_id} is already in {data['new_state']} state",
        )

    for ti in updated_tis:
        try:
            if data["new_state"] == TaskInstanceState.SUCCESS:
                get_listener_manager().hook.on_task_instance_success(previous_state=None, task_instance=ti)
            elif data["new_state"] == TaskInstanceState.FAILED:
                get_listener_manager().hook.on_task_instance_failed(
                    previous_state=None,
                    task_instance=ti,
                    error=f"TaskInstance's state was manually set to `{TaskInstanceState.FAILED}`.",
                )
        except Exception:
            log.exception("error calling listener")


def _patch_task_instance_note(
    task_instance_body: BulkTaskInstanceBody | PatchTaskInstanceBody,
    tis: list[TI],
    user: GetUserDep,
    update_mask: list[str] | None = Query(None),
) -> None:
    for ti in tis:
        if update_mask or task_instance_body.note is not None:
            if ti.task_instance_note is None:
                ti.note = (task_instance_body.note, user.get_id())
            else:
                ti.task_instance_note.content = task_instance_body.note
                ti.task_instance_note.user_id = user.get_id()


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

    def _extract_task_identifiers(
        self, entity: str | BulkTaskInstanceBody
    ) -> tuple[str, str, str, int | None]:
        """
        Extract task identifiers from an id or entity object.

        :param entity: Task identifier as string or BulkTaskInstanceBody object
        :return: tuple of (dag_id, dag_run_id, task_id, map_index)
        """
        if isinstance(entity, str):
            dag_id = self.dag_id
            dag_run_id = self.dag_run_id
            task_id = entity
            map_index = None
        else:
            dag_id = entity.dag_id if entity.dag_id else self.dag_id
            dag_run_id = entity.dag_run_id if entity.dag_run_id else self.dag_run_id
            task_id = entity.task_id
            map_index = entity.map_index

        return dag_id, dag_run_id, task_id, map_index

    def _categorize_entities(
        self,
        entities: Sequence[str | BulkTaskInstanceBody],
        results: BulkActionResponse,
    ) -> tuple[set[tuple[str, str, str, int]], set[tuple[str, str, str]]]:
        """
        Validate entities and categorize them into specific and all map index update sets.

        :param entities: Sequence of entities to validate
        :param results: BulkActionResponse object to track errors
        :return: tuple of (specific_map_index_task_keys, all_map_index_task_keys)
        """
        specific_map_index_task_keys = set()
        all_map_index_task_keys = set()

        for entity in entities:
            dag_id, dag_run_id, task_id, map_index = self._extract_task_identifiers(entity)

            # Validate that we have specific values, not wildcards
            if dag_id == "~" or dag_run_id == "~":
                if isinstance(entity, str):
                    error_msg = f"When using wildcard in path, dag_id and dag_run_id must be specified in BulkTaskInstanceBody object, not as string for task_id: {entity}"
                else:
                    error_msg = f"When using wildcard in path, dag_id and dag_run_id must be specified in request body for task_id: {entity.task_id}"
                results.errors.append(
                    {
                        "error": error_msg,
                        "status_code": status.HTTP_400_BAD_REQUEST,
                    }
                )
                continue

            # Separate logic for "update all" vs "update specific"
            if map_index is not None:
                specific_map_index_task_keys.add((dag_id, dag_run_id, task_id, map_index))
            else:
                all_map_index_task_keys.add((dag_id, dag_run_id, task_id))

        return specific_map_index_task_keys, all_map_index_task_keys

    def _categorize_task_instances(
        self, task_keys: set[tuple[str, str, str, int]]
    ) -> tuple[
        dict[tuple[str, str, str, int], TI], set[tuple[str, str, str, int]], set[tuple[str, str, str, int]]
    ]:
        """
        Categorize the given task_keys into matched and not_found based on existing task instances.

        :param task_keys: set of task_keys (tuple of dag_id, dag_run_id, task_id, and map_index)
        :return: tuple of (task_instances_map, matched_task_keys, not_found_task_keys)
        """
        # Filter at database level using exact tuple matching instead of fetching all combinations
        # and filtering in Python
        task_keys_list = list(task_keys)
        query = select(TI).where(tuple_(TI.dag_id, TI.run_id, TI.task_id, TI.map_index).in_(task_keys_list))

        task_instances = self.session.scalars(query).all()
        task_instances_map = {
            (ti.dag_id, ti.run_id, ti.task_id, ti.map_index if ti.map_index is not None else -1): ti
            for ti in task_instances
        }
        matched_task_keys = set(task_instances_map.keys())
        not_found_task_keys = task_keys - matched_task_keys
        return task_instances_map, matched_task_keys, not_found_task_keys

    def _perform_update(
        self,
        entity: BulkTaskInstanceBody,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        map_index: int,
        results: BulkActionResponse,
        update_mask: list[str] | None = Query(None),
    ) -> None:
        dag, tis, data = _patch_ti_validate_request(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            dag_bag=self.dag_bag,
            body=entity,
            session=self.session,
            update_mask=update_mask,
        )

        for key, _ in data.items():
            if key == "new_state":
                _patch_task_instance_state(
                    task_id=task_id,
                    dag_run_id=dag_run_id,
                    dag=dag,
                    task_instance_body=entity,
                    session=self.session,
                    data=data,
                )
            elif key == "note":
                _patch_task_instance_note(
                    task_instance_body=entity,
                    tis=tis,
                    user=self.user,
                )

        results.success.append(f"{task_id}[{map_index}]")

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
        # Validate and categorize entities into specific and all map index update sets
        update_specific_map_index_task_keys, update_all_map_index_task_keys = self._categorize_entities(
            action.entities, results
        )

        try:
            specific_entity_map = {
                (entity.dag_id, entity.dag_run_id, entity.task_id, entity.map_index): entity
                for entity in action.entities
                if entity.map_index is not None
            }
            all_map_entity_map = {
                (entity.dag_id, entity.dag_run_id, entity.task_id): entity
                for entity in action.entities
                if entity.map_index is None
            }

            # Handle updates for specific map_index task instances
            if update_specific_map_index_task_keys:
                _, matched_task_keys, not_found_task_keys = self._categorize_task_instances(
                    update_specific_map_index_task_keys
                )

                if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_task_keys:
                    not_found_task_ids = [
                        {"dag_id": dag_id, "dag_run_id": run_id, "task_id": task_id, "map_index": map_index}
                        for dag_id, run_id, task_id, map_index in not_found_task_keys
                    ]
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"The task instances with these identifiers: {not_found_task_ids} were not found",
                    )

                for dag_id, dag_run_id, task_id, map_index in matched_task_keys:
                    entity = specific_entity_map.get((dag_id, dag_run_id, task_id, map_index))

                    if entity is not None:
                        self._perform_update(
                            dag_id=dag_id,
                            dag_run_id=dag_run_id,
                            task_id=task_id,
                            map_index=map_index,
                            entity=entity,
                            results=results,
                            update_mask=action.update_mask,
                        )

            # Handle updates for all map indexes
            if update_all_map_index_task_keys:
                all_dag_ids = {dag_id for dag_id, _, _ in update_all_map_index_task_keys}
                all_run_ids = {run_id for _, run_id, _ in update_all_map_index_task_keys}
                all_task_ids = {task_id for _, _, task_id in update_all_map_index_task_keys}

                batch_task_instances = self.session.scalars(
                    select(TI).where(
                        TI.dag_id.in_(all_dag_ids),
                        TI.run_id.in_(all_run_ids),
                        TI.task_id.in_(all_task_ids),
                    )
                ).all()

                # Group task instances by (dag_id, run_id, task_id)
                task_instances_by_key: dict[tuple[str, str, str], list[TI]] = {}
                for ti in batch_task_instances:
                    key = (ti.dag_id, ti.run_id, ti.task_id)
                    task_instances_by_key.setdefault(key, []).append(ti)

                for dag_id, run_id, task_id in update_all_map_index_task_keys:
                    all_task_instances = task_instances_by_key.get((dag_id, run_id, task_id), [])

                    if (
                        not all_task_instances
                        and action.action_on_non_existence == BulkActionNotOnExistence.FAIL
                    ):
                        raise HTTPException(
                            status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"No task instances found for dag_id: {dag_id}, run_id: {run_id}, task_id: {task_id}",
                        )

                    entity = all_map_entity_map.get((dag_id, run_id, task_id))

                    if entity is not None:
                        for ti in all_task_instances:
                            self._perform_update(
                                dag_id=dag_id,
                                dag_run_id=run_id,
                                task_id=task_id,
                                map_index=ti.map_index if ti.map_index is not None else -1,
                                entity=entity,
                                results=results,
                                update_mask=action.update_mask,
                            )

        except ValidationError as e:
            results.errors.append({"error": f"{e.errors()}"})
        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

    def handle_bulk_delete(
        self, action: BulkDeleteAction[BulkTaskInstanceBody], results: BulkActionResponse
    ) -> None:
        """Bulk delete task instances."""
        # Validate and categorize entities into specific and all map index delete sets
        delete_specific_map_index_task_keys, delete_all_map_index_task_keys = self._categorize_entities(
            action.entities, results
        )

        try:
            # Handle deletion of specific (dag_id, dag_run_id, task_id, map_index) tuples
            if delete_specific_map_index_task_keys:
                _, matched_task_keys, not_found_task_keys = self._categorize_task_instances(
                    delete_specific_map_index_task_keys
                )
                not_found_task_ids = [
                    {"dag_id": dag_id, "dag_run_id": run_id, "task_id": task_id, "map_index": map_index}
                    for dag_id, run_id, task_id, map_index in not_found_task_keys
                ]

                if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_task_keys:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"The task instances with these identifiers: {not_found_task_ids} were not found",
                    )

                for dag_id, run_id, task_id, map_index in matched_task_keys:
                    ti = (
                        self.session.execute(
                            select(TI).where(
                                TI.dag_id == dag_id,
                                TI.run_id == run_id,
                                TI.task_id == task_id,
                                TI.map_index == map_index,
                            )
                        )
                        .scalars()
                        .one_or_none()
                    )

                    if ti:
                        self.session.delete(ti)
                        results.success.append(f"{task_id}[{map_index}]")

            # Handle deletion of all map indexes for certain (dag_id, dag_run_id, task_id) tuples
            if delete_all_map_index_task_keys:
                all_dag_ids = {dag_id for dag_id, _, _ in delete_all_map_index_task_keys}
                all_run_ids = {run_id for _, run_id, _ in delete_all_map_index_task_keys}
                all_task_ids = {task_id for _, _, task_id in delete_all_map_index_task_keys}

                batch_task_instances = self.session.scalars(
                    select(TI).where(
                        TI.dag_id.in_(all_dag_ids),
                        TI.run_id.in_(all_run_ids),
                        TI.task_id.in_(all_task_ids),
                    )
                ).all()

                # Group task instances by (dag_id, run_id, task_id) for efficient lookup
                task_instances_by_key: dict[tuple[str, str, str], list[TI]] = {}
                for ti in batch_task_instances:
                    key = (ti.dag_id, ti.run_id, ti.task_id)
                    task_instances_by_key.setdefault(key, []).append(ti)

                for dag_id, run_id, task_id in delete_all_map_index_task_keys:
                    all_task_instances = task_instances_by_key.get((dag_id, run_id, task_id), [])

                    if (
                        not all_task_instances
                        and action.action_on_non_existence == BulkActionNotOnExistence.FAIL
                    ):
                        raise HTTPException(
                            status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"No task instances found for dag_id: {dag_id}, run_id: {run_id}, task_id: {task_id}",
                        )

                    for ti in all_task_instances:
                        self.session.delete(ti)

                    if all_task_instances:
                        results.success.append(task_id)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})
