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

import asyncio
import itertools
import json
import operator
from typing import TYPE_CHECKING, Any

import attrs
import structlog
from fastapi import HTTPException, status
from sqlalchemy import select, tuple_
from sqlalchemy.orm import Session, joinedload

from airflow.api.common.mark_tasks import (
    set_dag_run_state_to_failed,
    set_dag_run_state_to_queued,
    set_dag_run_state_to_success,
)
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.common.dagbag import (
    DagBagDep,
    get_dag_for_run,
    get_latest_version_of_dag,
    resolve_run_on_latest_version,
)
from airflow.api_fastapi.common.db.task_instances import eager_load_TI_and_TIH_for_validation
from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionResponse,
    BulkBody,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.dag_run import (
    BulkDAGRunBody,
    ClearPartitionsBody,
    DagRunMutableStates,
)
from airflow.api_fastapi.core_api.datamodels.task_instances import NewTaskResponse
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.listeners.listener import get_listener_manager
from airflow.models.dagrun import DagRun, clear_partition_runs
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XCOM_RETURN_KEY, XComModel
from airflow.utils.session import create_session_async
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

    from airflow.serialization.definitions.dag import SerializedDAG

log = structlog.get_logger(__name__)


def get_dag_run_and_dag_for_clear(
    *,
    session: Session,
    dag_bag: DagBagDep,
    dag_id: str,
    dag_run_id: str,
) -> tuple[DagRun, SerializedDAG]:
    dag_run = session.scalar(
        select(DagRun).filter_by(dag_id=dag_id, run_id=dag_run_id).options(joinedload(DagRun.dag_model))
    )
    if dag_run is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` was not found",
        )
    dag = dag_bag.get_dag_for_run(dag_run, session=session)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with id {dag_id} was not found")
    return dag_run, dag


def dry_run_clear_dag_run(
    *,
    session: Session,
    dag_bag: DagBagDep,
    dag_id: str,
    dag_run_id: str,
    only_failed: bool,
    only_new: bool,
) -> list[Any]:
    if only_new:
        # ``dag.clear(only_new=True, dry_run=True)`` returns nothing when
        # ``created_dag_version_id`` is None (e.g. LocalDagBundle), so derive new
        # tasks from TI existence instead.
        latest_dag = get_latest_version_of_dag(dag_bag, dag_id, session)
        existing_task_ids = set(
            session.scalars(
                select(TaskInstance.task_id).where(
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.run_id == dag_run_id,
                )
            ).all()
        )
        new_task_ids = sorted(set(latest_dag.task_ids) - existing_task_ids)
        return [NewTaskResponse(task_id=task_id, task_display_name=task_id) for task_id in new_task_ids]

    ti_query = eager_load_TI_and_TIH_for_validation(select(TaskInstance))
    ti_query = ti_query.where(
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id == dag_run_id,
    )
    if only_failed:
        ti_query = ti_query.where(
            TaskInstance.state.in_([TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED])
        )
    return list(session.scalars(ti_query))


def perform_clear_dag_run(
    *,
    session: Session,
    dag: SerializedDAG,
    dag_run: DagRun,
    dag_id: str,
    only_failed: bool,
    only_new: bool,
    run_on_latest_version: bool | None,
    note: str | None,
    user: BaseUser,
) -> DagRun:
    resolved_run_on_latest = resolve_run_on_latest_version(run_on_latest_version, dag_id, session)
    dag.clear(
        run_id=dag_run.run_id,
        task_ids=None,
        only_new=only_new,
        only_failed=only_failed,
        run_on_latest_version=resolved_run_on_latest,
        session=session,
    )
    dag_run_cleared = session.scalar(select(DagRun).where(DagRun.id == dag_run.id))
    if not dag_run_cleared:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Dag run not found after clearing")
    if note is not None:
        patch_dag_run_note(dag_run=dag_run_cleared, note=note, user=user)
    return dag_run_cleared


def clear_partition_fields(
    *,
    dag: SerializedDAG,
    body: ClearPartitionsBody,
    dag_id: str,
    session: Session,
) -> tuple[int, int]:
    """
    Reset partition_key and partition_date to None on matching runs.

    Returns (dag_runs_cleared, task_instances_cleared).
    """
    return clear_partition_runs(
        dag=dag,
        dag_id=dag_id,
        run_id=body.run_id,
        partition_key=body.partition_key,
        partition_date_start=body.partition_date_start,
        partition_date_end=body.partition_date_end,
        clear_tis=body.clear_task_instances,
        dry_run=body.dry_run,
        session=session,
    )


def patch_dag_run_state(
    *,
    dag: SerializedDAG,
    dag_run: DagRun,
    state: DagRunMutableStates,
    session: Session,
) -> None:
    """Set a Dag Run's state (success/queued/failed), firing the matching listener hooks."""
    if state == DagRunMutableStates.SUCCESS:
        set_dag_run_state_to_success(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
        try:
            get_listener_manager().hook.on_dag_run_success(dag_run=dag_run, msg="")
        except Exception:
            log.exception("error calling listener")
    elif state == DagRunMutableStates.QUEUED:
        # TODO AIP-103: https://github.com/apache/airflow/issues/66755
        # Handle clearing states for all task instances in a dagrun when cleared.
        # Not notifying on queued - only notifying on RUNNING, which happens in the scheduler.
        set_dag_run_state_to_queued(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
    elif state == DagRunMutableStates.FAILED:
        set_dag_run_state_to_failed(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
        try:
            get_listener_manager().hook.on_dag_run_failed(dag_run=dag_run, msg="")
        except Exception:
            log.exception("error calling listener")


def patch_dag_run_note(*, dag_run: DagRun, note: str | None, user: BaseUser) -> None:
    """Set, update, or clear a Dag Run's note. An empty note removes it so the run is left without a note."""
    if note == "":
        dag_run.dag_run_note = None
    elif dag_run.dag_run_note is None:
        dag_run.note = (note, user.get_id())
    else:
        dag_run.dag_run_note.content = note
        dag_run.dag_run_note.user_id = user.get_id()


@attrs.define
class DagRunWaiter:
    """Wait for the specified dag run to finish, and collect info from it."""

    dag_id: str
    run_id: str
    interval: float
    result_task_ids: list[str] | None

    async def _get_dag_run(self) -> DagRun:
        async with create_session_async() as session:
            return await session.scalar(select(DagRun).filter_by(dag_id=self.dag_id, run_id=self.run_id))

    async def _serialize_xcoms(self) -> dict[str, Any]:
        if self.result_task_ids is None:  # Return dag-author-specified results.
            xcom_query = XComModel.get_many(
                run_id=self.run_id,
                key=XCOM_RETURN_KEY,
                dag_ids=self.dag_id,
            )
            xcom_query = xcom_query.where(XComModel.dag_result.is_(True))
        else:  # Explicitly API user-specified results.
            xcom_query = XComModel.get_many(
                run_id=self.run_id,
                key=XCOM_RETURN_KEY,
                task_ids=self.result_task_ids,
                dag_ids=self.dag_id,
            )
        # XComModel.get_many() orders XCom by timestamp. Reset this to make
        # mapped task results stable since execution order is not guaranteed.
        xcom_query = xcom_query.order_by(None).order_by(XComModel.task_id, XComModel.map_index)
        async with create_session_async() as session:
            xcom_results = (await session.scalars(xcom_query)).all()

        def _group_xcoms(g: Iterator[XComModel | tuple[XComModel]]) -> Any:
            entries = [row[0] if isinstance(row, tuple) else row for row in g]
            if len(entries) == 1 and entries[0].map_index < 0:  # Unpack non-mapped task xcom.
                return entries[0].value
            return [entry.value for entry in entries]  # Task is mapped; return all xcoms in a list.

        return {
            task_id: _group_xcoms(g)
            for task_id, g in itertools.groupby(xcom_results, key=operator.attrgetter("task_id"))
        }

    async def _serialize_response(self, dag_run: DagRun) -> str:
        resp = {"state": dag_run.state}
        if dag_run.state not in State.finished_dr_states:
            return json.dumps(resp)
        if self.result_task_ids is None or self.result_task_ids:
            if result_xcoms := await self._serialize_xcoms():
                resp["results"] = result_xcoms
        return json.dumps(resp)

    async def wait(self) -> AsyncGenerator[str, None]:
        yield await self._serialize_response(dag_run := await self._get_dag_run())
        yield "\n"
        while dag_run.state not in State.finished_dr_states:
            await asyncio.sleep(self.interval)
            yield await self._serialize_response(dag_run := await self._get_dag_run())
            yield "\n"


class BulkDagRunService(BulkService[BulkDAGRunBody]):
    """Service for handling bulk operations on Dag Runs."""

    def __init__(
        self,
        session: Session,
        request: BulkBody[BulkDAGRunBody],
        dag_id: str,
        dag_bag: DagBagDep,
        user: BaseUser,
    ):
        super().__init__(session, request)
        self.dag_id = dag_id
        self.dag_bag = dag_bag
        self.user = user

    def handle_bulk_create(
        self, action: BulkCreateAction[BulkDAGRunBody], results: BulkActionResponse
    ) -> None:
        results.errors.append(
            {
                "error": "Dag Runs bulk create is not supported. Use the trigger Dag Run endpoint instead.",
                "status_code": status.HTTP_405_METHOD_NOT_ALLOWED,
            }
        )

    def _resolve_entity_key(
        self, entity: str | BulkDAGRunBody, results: BulkActionResponse
    ) -> tuple[str, str] | None:
        """
        Resolve the ``(dag_id, dag_run_id)`` for an entity.

        Records a 400 error and returns ``None`` when a wildcard ``~`` leaves the
        dag_id unresolved. Shared by the bulk update and delete handlers.
        """
        if isinstance(entity, str):
            dag_id, dag_run_id = self.dag_id, entity
        else:
            dag_id = entity.dag_id or self.dag_id
            dag_run_id = entity.dag_run_id

        if dag_id == "~" or dag_run_id == "~":
            if isinstance(entity, str):
                error_msg = (
                    "When using wildcard in path, dag_id must be specified in BulkDAGRunBody"
                    f" object, not as string for dag_run_id: {entity}"
                )
            else:
                error_msg = (
                    "When using wildcard in path, dag_id must be specified in request body for"
                    f" dag_run_id: {entity.dag_run_id}"
                )
            results.errors.append({"error": error_msg, "status_code": status.HTTP_400_BAD_REQUEST})
            return None

        return (dag_id, dag_run_id)

    def _categorize_dag_runs(
        self, keys: set[tuple[str, str]]
    ) -> tuple[dict[tuple[str, str], DagRun], set[tuple[str, str]], set[tuple[str, str]]]:
        """
        Split the requested ``(dag_id, dag_run_id)`` keys into existing and missing ones.

        :return: tuple of (dag_run_map, matched_keys, not_found_keys). Shared by the bulk
            update and delete handlers.
        """
        dag_run_map = {
            (dr.dag_id, dr.run_id): dr
            for dr in self.session.scalars(
                select(DagRun).where(tuple_(DagRun.dag_id, DagRun.run_id).in_(list(keys)))
            )
        }
        matched_keys = set(dag_run_map.keys())
        not_found_keys = keys - matched_keys
        return dag_run_map, matched_keys, not_found_keys

    def handle_bulk_update(
        self, action: BulkUpdateAction[BulkDAGRunBody], results: BulkActionResponse
    ) -> None:
        """Bulk update Dag Runs (mark as success/failed/queued and/or set a note)."""
        entities_by_key: dict[tuple[str, str], BulkDAGRunBody] = {}
        for entity in action.entities:
            if isinstance(entity, str):
                results.errors.append(
                    {
                        "error": (
                            "Bulk update requires a BulkDAGRunBody object,"
                            f" not a string for dag_run_id: {entity}"
                        ),
                        "status_code": status.HTTP_400_BAD_REQUEST,
                    }
                )
                continue
            key = self._resolve_entity_key(entity, results)
            if key is not None:
                entities_by_key[key] = entity

        if not entities_by_key:
            return

        to_update_keys = set(entities_by_key.keys())
        dag_run_map, matched_keys, not_found_keys = self._categorize_dag_runs(to_update_keys)

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_keys:
                raise HTTPException(
                    status.HTTP_404_NOT_FOUND,
                    f"The DagRuns with these identifiers: {sorted(not_found_keys)} were not found",
                )
            update_keys = (
                matched_keys
                if action.action_on_non_existence == BulkActionNotOnExistence.SKIP
                else to_update_keys
            )

            for key, entity in entities_by_key.items():
                if key not in update_keys:
                    continue
                dag_id, run_id = key
                dag_run = dag_run_map[key]
                if entity.state is not None:
                    dag = get_dag_for_run(self.dag_bag, dag_run, session=self.session)
                    patch_dag_run_state(dag=dag, dag_run=dag_run, state=entity.state, session=self.session)
                if entity.note is not None:
                    patch_dag_run_note(dag_run=dag_run, note=entity.note, user=self.user)
                results.success.append(f"{dag_id}.{run_id}")
        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

    def handle_bulk_delete(
        self, action: BulkDeleteAction[BulkDAGRunBody], results: BulkActionResponse
    ) -> None:
        """Bulk delete Dag Runs."""
        to_delete_keys: set[tuple[str, str]] = set()
        for entity in action.entities:
            key = self._resolve_entity_key(entity, results)
            if key is not None:
                to_delete_keys.add(key)

        if not to_delete_keys:
            return

        dag_run_map, matched_keys, not_found_keys = self._categorize_dag_runs(to_delete_keys)
        deletable_states = {s.value for s in DagRunMutableStates}

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_keys:
                raise HTTPException(
                    status.HTTP_404_NOT_FOUND,
                    f"The DagRuns with these identifiers: {sorted(not_found_keys)} were not found",
                )
            delete_keys = (
                matched_keys
                if action.action_on_non_existence == BulkActionNotOnExistence.SKIP
                else to_delete_keys
            )

            for dag_id, run_id in sorted(delete_keys):
                dag_run = dag_run_map[(dag_id, run_id)]
                if dag_run.state not in deletable_states:
                    results.errors.append(
                        {
                            "error": (
                                f"The DagRun with dag_id: `{dag_id}` and run_id: `{run_id}` "
                                f"cannot be deleted in {dag_run.state} state"
                            ),
                            "status_code": status.HTTP_409_CONFLICT,
                        }
                    )
                    continue
                self.session.delete(dag_run)
                results.success.append(f"{dag_id}.{run_id}")
        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})
