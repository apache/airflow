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
from typing import Literal, TypeVar
from uuid import UUID

import structlog
from fastapi import HTTPException, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from sqlalchemy import delete, select, tuple_
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.session import Session

from airflow._shared.state import TaskScope
from airflow.api.common.mark_tasks import get_run_ids
from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity, DagDetails
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
from airflow.api_fastapi.core_api.datamodels.task_instances import (
    BulkTaskInstanceBody,
    ClearTaskInstancesBody,
    PatchTaskInstanceBody,
)
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.api_fastapi.core_api.services.public.task_coordinates import resolve_task_scope
from airflow.configuration import conf
from airflow.listeners.listener import get_listener_manager
from airflow.models.dag import DagModel
from airflow.models.dagbag import DBDagBag
from airflow.models.dagrun import DagRun
from airflow.models.dynamic_region import (
    LOOP_DECISION_KEY,
    SENTINEL_REGION_ID,
    DynamicRegion,
    load_region_ancestry,
    loop_position,
)
from airflow.models.renderedtifields import load_legacy_rendered_fields
from airflow.models.task_coordinates import (
    LOOP_GATE_OPERATOR,
    TaskCoordinateResolver,
    enclosing_loop,
    public_map_index_expression,
)
from airflow.models.taskinstance import TaskInstance as TI, clear_task_instances, select_loop_clear_scope
from airflow.models.xcom import XComModelV2
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.state.metastore import _get_db_backend
from airflow.utils.state import TaskInstanceState

log = structlog.get_logger(__name__)
MutationAction = TypeVar(
    "MutationAction", BulkUpdateAction[BulkTaskInstanceBody], BulkDeleteAction[BulkTaskInstanceBody]
)


def _discard_task_state_store(tis: Sequence[TI], session: Session, *, event: str) -> None:
    """
    Discard the task state store entries of each task instance.

    A failure is logged and re-raised so the request fails and the session rolls back, rather than
    reporting success while some entries survive undiscarded.

    This only drops the metadata DB reference row via ``_get_db_backend()``; it does not go through
    ``get_state_backend()``. A custom ``[workers] state_store_backend`` payload is left orphaned
    with no reclaim path other than its own lifecycle/TTL policy, and a custom ``[state_store]
    backend`` is not touched at all: the worker still reads and writes there, so a clear reports
    success while the actual state survives and a later attempt can resume from it. Closing this
    gap needs a server-side path to the configured state backend and is tracked for a future
    change; today, this discard is only exact for the default metastore backend.

    :param event: what prompted the discard, used as the log event name.
    """
    backend = _get_db_backend()
    for ti in tis:
        scope = TaskScope(
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            task_id=ti.task_id,
            region_id=ti.region_id,
            region_index=ti.region_index,
        )
        try:
            backend.clear(scope=scope, session=session)
        except Exception:
            log.warning(
                "Failed to discard task state",
                discard_event=event,
                dag_id=ti.dag_id,
                run_id=ti.run_id,
                task_id=ti.task_id,
                map_index=ti.map_index,
                exc_info=True,
            )
            raise
    log.info(event, task_instance_count=len(tis))


def _clear_task_state_store_on_success(tis: Sequence[TI], session: Session) -> None:
    """Clear task state store rows for each TI if clear_on_success is enabled."""
    if not conf.getboolean("state_store", "clear_on_success", fallback=False):
        return
    backend = _get_db_backend()
    for ti in tis:
        scope = TaskScope(
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            task_id=ti.task_id,
            region_id=ti.region_id,
            region_index=ti.region_index,
        )
        try:
            backend.clear(scope=scope, session=session)
            log.info(
                "Cleared task state on success",
                dag_id=ti.dag_id,
                run_id=ti.run_id,
                task_id=ti.task_id,
                region_index=ti.region_index,
            )
        except Exception:
            log.warning(
                "Failed to clear task state on success",
                dag_id=ti.dag_id,
                run_id=ti.run_id,
                task_id=ti.task_id,
            )


def _validate_patch_task_instance_body(
    body: PatchTaskInstanceBody,
    update_mask: list[str] | None,
) -> dict:
    """Validate the patch body and return the fields to update as a dict."""
    fields_to_update = body.model_fields_set
    if update_mask:
        fields_to_update = fields_to_update.intersection(update_mask)
    else:
        try:
            PatchTaskInstanceBody.model_validate(body)
        except ValidationError as e:
            raise RequestValidationError(errors=e.errors())

    return body.model_dump(include=fields_to_update, by_alias=True)


def _emit_state_listener_hooks(updated_tis: list[TI], new_state: str | TaskInstanceState) -> None:
    """Fire listener hooks for the given TIs based on their new state. Listener errors are logged."""
    for ti in updated_tis:
        try:
            if new_state == TaskInstanceState.SUCCESS:
                get_listener_manager().hook.on_task_instance_success(previous_state=None, task_instance=ti)
            elif new_state == TaskInstanceState.FAILED:
                get_listener_manager().hook.on_task_instance_failed(
                    previous_state=None,
                    task_instance=ti,
                    error=f"TaskInstance's state was manually set to `{TaskInstanceState.FAILED}`.",
                )
            elif new_state == TaskInstanceState.SKIPPED:
                get_listener_manager().hook.on_task_instance_skipped(previous_state=None, task_instance=ti)
        except Exception:
            log.exception("error calling listener")


def _reload_tis_with_rendered_fields(tis: list[TI], session: Session) -> list[TI]:
    """
    Re-load TIs with ``rendered_task_instance_fields`` eagerly loaded.

    ``set_task_instance_state`` / ``set_task_group_state`` return TIs without this relationship
    loaded; we re-query so they can be serialized without lazy loads.
    ``populate_existing=True`` ensures the joinedload updates TIs already in the identity map.
    """
    if not tis:
        return tis
    reloaded = list(
        session.scalars(
            select(TI)
            .options(joinedload(TI.rendered_task_instance_fields))
            .where(TI.id.in_([ti.id for ti in tis]))
            .execution_options(populate_existing=True, include_all_attempts=True)
        ).all()
    )
    load_legacy_rendered_fields(reloaded, session=session)
    return reloaded


def _patch_ti_validate_request(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    dag_bag: DagBagDep,
    body: PatchTaskInstanceBody,
    session: SessionDep,
    map_index: int | None = -1,
    update_mask: list[str] | None = None,
    *,
    lock: bool = True,
) -> tuple[SerializedDAG, list[TI], dict]:
    _validate_region_selection(body)
    dag = get_latest_version_of_dag(dag_bag, dag_id, session)
    if lock:
        _lock_patch_runs(dag, dag_run_id, body, session)
    if body.region_id is None and not dag.has_task(task_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Task '{task_id}' not found in Dag '{dag_id}'")

    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    if body.region_id is None:
        resolver = TaskCoordinateResolver(dag_bag, session)
        for version in session.scalars(
            select(TI.dag_version_id)
            .where(
                TI.dag_id == dag_id,
                TI.run_id == dag_run_id,
                TI.task_id == task_id,
                TI.dag_version_id.is_not(None),
            )
            .distinct()
        ):
            if enclosing_loop(resolver.get_task(dag_id, dag_run_id, task_id, dag_version_id=version)):
                raise HTTPException(status.HTTP_409_CONFLICT, "Select a region and index for this loop task")
    scope = resolve_task_scope(
        dag_id=dag_id,
        run_id=dag_run_id,
        task_id=task_id,
        session=session,
        dag_bag=dag_bag,
        map_index=map_index if map_index is not None else -1,
        region_id=body.region_id,
        region_index=body.region_index,
        all_map_indices=map_index is None,
    )
    query = query.where(TI.region_id == scope.region_id)
    if body.region_id is not None or map_index is not None:
        query = query.where(TI.region_index == scope.region_index)
    query = query.order_by(TI.region_index).execution_options(populate_existing=True)
    if lock:
        query = query.with_for_update(of=TI)
    tis = session.scalars(query).all()

    err_msg_404 = (
        f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}` and map_index: `{map_index}` was not found",
    )
    if len(tis) == 0:
        raise HTTPException(status.HTTP_404_NOT_FOUND, err_msg_404)
    if body.region_id is not None and tis[0].dag_version_id is not None:
        pinned_dag = dag_bag.get_dag(tis[0].dag_version_id, session=session)
        if pinned_dag is not None:
            dag = pinned_dag

    data = _validate_patch_task_instance_body(body, update_mask)
    return dag, list(tis), data


def _validate_region_selection(body: PatchTaskInstanceBody) -> None:
    if (body.region_id is None) != (body.region_index is None):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "region_id and region_index must be supplied together"
        )
    if body.region_id is not None and (body.include_past or body.include_future):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Regional selection requires one explicit DagRun")


def _lock_patch_runs(dag: SerializedDAG, run_id: str, body: PatchTaskInstanceBody, session: Session) -> None:
    if body.include_future or body.include_past:
        run_ids = get_run_ids(dag, run_id, body.include_future, body.include_past, session=session)
    else:
        run_ids = [run_id]
    session.scalars(
        select(DagRun.id)
        .where(
            DagRun.dag_id == dag.dag_id,
            DagRun.run_id.in_(run_ids),
        )
        .order_by(DagRun.run_id)
        .with_for_update()
    ).all()


def patch_region_selection(
    body: PatchTaskInstanceBody, region_id: UUID | None, region_index: int | None
) -> PatchTaskInstanceBody:
    if region_id is None and region_index is None:
        _validate_region_selection(body)
        return body
    if region_id is None or region_index is None:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "region_id and region_index must be supplied together"
        )
    if body.region_id is not None or body.region_index is not None:
        if (body.region_id, body.region_index) != (region_id, region_index):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Body and query region coordinates conflict")
    body = body.model_copy(update={"region_id": region_id, "region_index": region_index})
    _validate_region_selection(body)
    return body


def _get_task_group_task_ids(dag_id: str, task_group_id: str, dag: SerializedDAG) -> list[str]:
    """Return the ids of every task in a task group, resolved from the dag structure."""
    task_group = dag.task_group_dict.get(task_group_id)
    if not task_group:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"Task group '{task_group_id}' not found in DAG '{dag_id}'"
        )
    return [task.task_id for task in task_group.iter_tasks()]


def _get_task_group_task_instances(
    dag_id: str,
    dag_run_id: str,
    task_group_id: str,
    dag: SerializedDAG,
    session: Session,
    body: PatchTaskInstanceBody | None = None,
    dag_bag: DBDagBag | None = None,
) -> list[TI]:
    """Get all task instances in a task group for a specific DAG run."""
    query = (
        select(TI)
        .where(
            TI.dag_id == dag_id,
            TI.run_id == dag_run_id,
        )
        .order_by(TI.task_id, TI.region_id, TI.region_index)
        .execution_options(populate_existing=True)
    )
    if body is None or body.region_id is None:
        task_ids = _get_task_group_task_ids(dag_id, task_group_id, dag)
        query = query.where(TI.task_id.in_(task_ids))

    group_tis = list(session.scalars(query).all())
    if body is not None:
        _validate_region_selection(body)
        if body.region_id is None:
            resolver = TaskCoordinateResolver(dag_bag or DBDagBag(), session)
            if any(
                ti.region_id != SENTINEL_REGION_ID
                and enclosing_loop(
                    resolver.get_task(dag_id, dag_run_id, ti.task_id, dag_version_id=ti.dag_version_id)
                )
                for ti in group_tis
            ):
                raise HTTPException(status.HTTP_409_CONFLICT, "Select a region and index for this loop group")
        elif body.region_index is not None:
            region = session.get(DynamicRegion, body.region_id)
            if region is None or (region.dag_id, region.run_id) != (dag_id, dag_run_id):
                raise HTTPException(status.HTTP_404_NOT_FOUND, "Selected loop region not found")
            regions = load_region_ancestry(
                {ti.region_id for ti in group_tis} | {body.region_id},
                dag_id=dag_id,
                run_id=dag_run_id,
                session=session,
            )
            position = loop_position(regions, body.region_id, body.region_index, region.node_id)
            group_tis = [
                ti
                for ti in group_tis
                if loop_position(regions, ti.region_id, ti.region_index, region.node_id) == position
            ]
            resolver = TaskCoordinateResolver(dag_bag or DBDagBag(), session)
            members = []
            for ti in group_tis:
                task = resolver.get_task(dag_id, dag_run_id, ti.task_id, dag_version_id=ti.dag_version_id)
                loop = enclosing_loop(task)
                if loop is None or loop.node_id != region.node_id:
                    raise HTTPException(
                        status.HTTP_400_BAD_REQUEST, "Group coordinates must identify an enclosing loop pass"
                    )
                group = task.task_group
                while group is not None:
                    if group.group_id == task_group_id:
                        members.append(ti)
                        break
                    group = group.parent_group
            group_tis = members
    if not group_tis:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"No task instances found for task group '{task_group_id}' in dag run '{dag_run_id}'",
        )

    return group_tis


def _patch_ti_group_validate_request(
    dag_id: str,
    dag_run_id: str,
    task_group_id: str,
    dag_bag: DagBagDep,
    body: PatchTaskInstanceBody,
    session: SessionDep,
    update_mask: list[str] | None = None,
    *,
    lock: bool = True,
) -> tuple[SerializedDAG, list[TI], dict]:
    """Validate and prepare data for task group patch request."""
    dag = get_latest_version_of_dag(dag_bag, dag_id, session)
    if lock:
        _lock_patch_runs(dag, dag_run_id, body, session)
    tis = _get_task_group_task_instances(dag_id, dag_run_id, task_group_id, dag, session, body, dag_bag)
    if lock:
        tis = list(
            session.scalars(
                select(TI)
                .where(TI.id.in_([ti.id for ti in tis]))
                .with_for_update(of=TI)
                .execution_options(populate_existing=True)
            )
        )

    data = _validate_patch_task_instance_body(body, update_mask)
    return dag, tis, data


def _patch_task_instance_state(
    task_id: str,
    dag_run_id: str,
    dag: SerializedDAG,
    task_instance_body: BulkTaskInstanceBody | PatchTaskInstanceBody,
    data: dict,
    session: Session,
    selected: list[TI] | None = None,
    commit: bool = True,
) -> list[TI]:
    if task_instance_body.region_id is not None:
        if selected is None:
            raise ValueError("Regional task instance updates require the selected task instances")
        return _patch_selected_task_state(selected, task_instance_body, data, session=session, commit=commit)
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

    if data["new_state"] == TaskInstanceState.SUCCESS:
        _clear_task_state_store_on_success(updated_tis, session)

    _emit_state_listener_hooks(updated_tis, data["new_state"])

    return updated_tis


def _patch_selected_task_state(
    selected: list[TI], body: PatchTaskInstanceBody, data: dict, *, session: Session, commit: bool
) -> list[TI]:
    if commit and selected:
        session.scalars(
            select(DagRun.id)
            .where(DagRun.dag_id == selected[0].dag_id, DagRun.run_id == selected[0].run_id)
            .with_for_update()
        ).all()
    scope = select_loop_clear_scope(
        selected,
        upstream=body.include_upstream,
        downstream=body.include_downstream,
        later_loop_iterations=False,
        session=session,
    )
    query = select(TI).where(TI.id.in_(scope.retry_ids))
    if commit:
        query = query.with_for_update(of=TI).execution_options(populate_existing=True)
    tis = list(session.scalars(query))
    changed = [ti for ti in tis if ti.state != data["new_state"]]
    if not commit:
        return changed
    if not changed:
        raise HTTPException(
            status.HTTP_409_CONFLICT, f"Selected task instances are already in {data['new_state']} state"
        )
    regions = {
        region.id: region
        for region in session.scalars(
            select(DynamicRegion).where(
                DynamicRegion.dag_id == tis[0].dag_id,
                DynamicRegion.run_id == tis[0].run_id,
            )
        )
    }
    superseded = {
        ti.id for ti in tis if DynamicRegion.is_coordinate_superseded(ti.region_id, ti.region_index, regions)
    }
    downstream = select_loop_clear_scope(
        [ti for ti in tis if ti.id not in superseded], later_loop_iterations=False, session=session
    )
    for ti in changed:
        if ti.operator == LOOP_GATE_OPERATOR:
            session.execute(
                delete(XComModelV2).where(
                    XComModelV2.task_instance_id == ti.id, XComModelV2.key == LOOP_DECISION_KEY
                )
            )
        if ti.state == TaskInstanceState.RESTARTING and ti.id in superseded:
            ti.complete_restart(session=session, terminal_outcome=data["new_state"])
        else:
            ti.set_state(data["new_state"], session=session)
    failures = list(
        session.scalars(
            select(TI).where(
                TI.id.in_(downstream.retry_ids - scope.retry_ids),
                TI.state.in_((TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED)),
            )
        )
    )
    if failures:
        clear_task_instances(failures, session=session)
    if data["new_state"] == TaskInstanceState.SUCCESS:
        _clear_task_state_store_on_success(changed, session)
    _emit_state_listener_hooks(changed, data["new_state"])
    return changed


def _patch_task_group_state(
    group_id: str,
    dag_run_id: str,
    dag: SerializedDAG,
    body: PatchTaskInstanceBody,
    data: dict,
    *,
    session: Session,
    selected: list[TI] | None = None,
    commit: bool = True,
) -> list[TI]:
    """Update the state of all task instances in a task group."""
    if body.region_id is not None:
        if selected is None:
            raise ValueError("Regional task group updates require the selected task instances")
        return _patch_selected_task_state(selected, body, data, session=session, commit=commit)
    updated_tis = dag.set_task_group_state(
        group_id=group_id,
        run_id=dag_run_id,
        state=data["new_state"],
        upstream=body.include_upstream,
        downstream=body.include_downstream,
        future=body.include_future,
        past=body.include_past,
        commit=True,
        session=session,
    )
    if not updated_tis:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"All task instances in the group are already in {data['new_state']} state",
        )

    if data["new_state"] == TaskInstanceState.SUCCESS:
        _clear_task_state_store_on_success(updated_tis, session)

    _emit_state_listener_hooks(updated_tis, data["new_state"])

    return updated_tis


def _patch_task_instance_note(
    task_instance_body: BulkTaskInstanceBody | ClearTaskInstancesBody | PatchTaskInstanceBody,
    tis: list[TI],
    user: GetUserDep,
    update_mask: list[str] | None = None,
) -> None:
    for ti in tis:
        if update_mask or task_instance_body.note is not None:
            if task_instance_body.note == "":
                ti.task_instance_note = None
            elif ti.task_instance_note is None:
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
        method: Literal["PUT", "DELETE"],
        action_name: str,
    ) -> tuple[set[tuple[str, str, str, int]], set[tuple[str, str, str]]]:
        """
        Validate entities and categorize them into specific and all map index update sets.

        :param entities: Sequence of entities to validate
        :param results: BulkActionResponse object to track errors
        :return: tuple of (specific_map_index_task_keys, all_map_index_task_keys)
        """
        specific_map_index_task_keys = set()
        all_map_index_task_keys = set()
        dag_authorization_cache: dict[str, bool] = {}

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

            if dag_id not in dag_authorization_cache:
                team_name = DagModel.get_team_name(dag_id, session=self.session)
                dag_authorization_cache[dag_id] = get_auth_manager().is_authorized_dag(
                    method=method,
                    access_entity=DagAccessEntity.TASK_INSTANCE,
                    details=DagDetails(id=dag_id, team_name=team_name),
                    user=self.user,
                )
            if not dag_authorization_cache[dag_id]:
                results.errors.append(
                    {
                        "error": f"User is not authorized to {action_name} task instances for DAG '{dag_id}'",
                        "status_code": status.HTTP_403_FORBIDDEN,
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
        public_index = public_map_index_expression(TI)
        query = select(TI, public_index).where(
            tuple_(TI.dag_id, TI.run_id, TI.task_id).in_({key[:3] for key in task_keys_list}),
            tuple_(TI.dag_id, TI.run_id, TI.task_id, public_index).in_(task_keys_list),
        )
        rows = self.session.execute(query).all()
        self._reject_unscoped_loop_tasks([ti for ti, _ in rows])
        task_instances_map = {}
        for ti, index in rows:
            key = (ti.dag_id, ti.run_id, ti.task_id, index)
            if key in task_instances_map:
                raise HTTPException(
                    status.HTTP_409_CONFLICT, "Select region coordinates for loop task instances"
                )
            task_instances_map[key] = ti
        matched_task_keys = set(task_instances_map.keys())
        not_found_task_keys = task_keys - matched_task_keys
        return task_instances_map, matched_task_keys, not_found_task_keys

    def _reject_unscoped_loop_tasks(self, tis: Sequence[TI]) -> None:
        resolver = TaskCoordinateResolver(self.dag_bag, self.session)
        for ti in tis:
            if ti.region_id == SENTINEL_REGION_ID:
                continue
            task = resolver.get_task(ti.dag_id, ti.run_id, ti.task_id, dag_version_id=ti.dag_version_id)
            if enclosing_loop(task) is not None:
                raise HTTPException(
                    status.HTTP_409_CONFLICT, "Select region coordinates for loop task instances"
                )

    def _perform_update(
        self,
        entity: BulkTaskInstanceBody,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        map_index: int,
        results: BulkActionResponse,
        update_mask: list[str] | None = None,
    ) -> None:
        dag, tis, data = _patch_ti_validate_request(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            dag_bag=self.dag_bag,
            body=entity,
            session=self.session,
            map_index=map_index,
            update_mask=update_mask,
        )

        # Apply "note" before "state" so listeners fired inside _patch_task_instance_state() see the updated note.
        if "note" in data:
            _patch_task_instance_note(
                task_instance_body=entity,
                tis=tis,
                user=self.user,
            )
        if "new_state" in data:
            _patch_task_instance_state(
                task_id=task_id,
                dag_run_id=dag_run_id,
                dag=dag,
                task_instance_body=entity,
                session=self.session,
                data=data,
            )

        results.success.append(f"{dag_id}.{dag_run_id}.{task_id}[{map_index}]")

    def handle_bulk_create(
        self, action: BulkCreateAction[BulkTaskInstanceBody], results: BulkActionResponse
    ) -> None:
        results.errors.append(
            {
                "error": "Task instances bulk create is not supported",
                "status_code": status.HTTP_405_METHOD_NOT_ALLOWED,
            }
        )

    def _handle_regional_bulk(
        self, action: MutationAction, results: BulkActionResponse
    ) -> tuple[MutationAction, set[tuple[str, str, str, int]], set[tuple[str, str, str]]]:
        regional = [
            entity
            for entity in action.entities
            if isinstance(entity, BulkTaskInstanceBody)
            and (entity.region_id is not None or entity.region_index is not None)
        ]
        deleting = isinstance(action, BulkDeleteAction)
        specific, whole = self._categorize_entities(
            action.entities, results, method="DELETE" if deleting else "PUT", action_name=action.action.value
        )
        keys = {key[:3] for key in specific} | whole
        run_keys = {key[:2] for key in keys}
        for entity in action.entities:
            if isinstance(entity, BulkTaskInstanceBody) and (entity.include_future or entity.include_past):
                dag_id, run_id, task_id, _ = self._extract_task_identifiers(entity)
                if (dag_id, run_id, task_id) in keys:
                    dag = get_latest_version_of_dag(self.dag_bag, dag_id, self.session)
                    run_keys.update(
                        (dag_id, selected_run)
                        for selected_run in get_run_ids(
                            dag, run_id, entity.include_future, entity.include_past, session=self.session
                        )
                    )
        self.session.scalars(
            select(DagRun)
            .where(
                tuple_(DagRun.dag_id, DagRun.run_id).in_(run_keys),
            )
            .order_by(DagRun.dag_id, DagRun.run_id)
            .with_for_update()
        ).all()
        for entity in regional:
            dag_id, run_id, task_id, map_index = self._extract_task_identifiers(entity)
            if (dag_id, run_id, task_id) not in keys:
                continue
            try:
                dag, tis, data = _patch_ti_validate_request(
                    dag_id,
                    run_id,
                    task_id,
                    self.dag_bag,
                    entity,
                    self.session,
                    map_index,
                    getattr(action, "update_mask", None),
                    lock=False,
                )
                if deleting:
                    for ti in tis:
                        self.session.delete(ti)
                else:
                    if "note" in data:
                        _patch_task_instance_note(entity, tis, self.user)
                    if "new_state" in data:
                        _patch_task_instance_state(
                            task_id, run_id, dag, entity, data, self.session, selected=tis
                        )
                results.success.extend(
                    str(ti.id)
                    if ti.region_id != SENTINEL_REGION_ID
                    else f"{dag_id}.{run_id}.{task_id}[{ti.region_index}]"
                    for ti in tis
                )
            except HTTPException as error:
                if (
                    error.status_code == status.HTTP_404_NOT_FOUND
                    and action.action_on_non_existence != BulkActionNotOnExistence.FAIL
                ):
                    continue
                results.errors.append({"error": str(error.detail), "status_code": error.status_code})
        remaining = [entity for entity in action.entities if entity not in regional]
        remaining_keys = {self._extract_task_identifiers(entity) for entity in remaining}
        return (
            action.model_copy(update={"entities": remaining}),
            specific & remaining_keys,
            whole & {key[:3] for key in remaining_keys if key[3] is None},
        )

    def handle_bulk_update(
        self, action: BulkUpdateAction[BulkTaskInstanceBody], results: BulkActionResponse
    ) -> None:
        """Bulk Update Task Instances."""
        action, update_specific_map_index_task_keys, update_all_map_index_task_keys = (
            self._handle_regional_bulk(action, results)
        )

        try:
            specific_entity_map = {
                self._extract_task_identifiers(entity): entity
                for entity in action.entities
                if entity.map_index is not None
            }
            all_map_entity_map = {
                self._extract_task_identifiers(entity)[:3]: entity
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
                self._reject_unscoped_loop_tasks(batch_task_instances)
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
                                map_index=ti.region_index,
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
        action, delete_specific_map_index_task_keys, delete_all_map_index_task_keys = (
            self._handle_regional_bulk(action, results)
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

                for task_key in matched_task_keys:
                    dag_id, run_id, task_id, map_index = task_key
                    TI.delete_attempts(
                        dag_id=dag_id,
                        run_id=run_id,
                        task_id=task_id,
                        map_index=map_index,
                        session=self.session,
                    )
                    results.success.append(f"{dag_id}.{run_id}.{task_id}[{map_index}]")

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
                self._reject_unscoped_loop_tasks(batch_task_instances)
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

                    if all_task_instances:
                        TI.delete_attempts(
                            dag_id=dag_id, run_id=run_id, task_id=task_id, session=self.session
                        )
                    for ti in all_task_instances:
                        results.success.append(f"{dag_id}.{run_id}.{task_id}[{ti.region_index}]")

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})
