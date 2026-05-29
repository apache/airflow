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

from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.common.dagbag import DagBagDep, get_latest_version_of_dag
from airflow.api_fastapi.common.db.task_instances import eager_load_TI_and_TIH_for_validation
from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionResponse,
    BulkBody,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.dag_run import BulkDAGRunBody, DagRunMutableStates
from airflow.api_fastapi.core_api.datamodels.task_instances import NewTaskResponse
from airflow.api_fastapi.core_api.services.public.common import BulkService, resolve_run_on_latest_version
from airflow.models.dagrun import DagRun
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
        if dag_run_cleared.dag_run_note is None:
            dag_run_cleared.note = (note, user.get_id())
        else:
            dag_run_cleared.dag_run_note.content = note
            dag_run_cleared.dag_run_note.user_id = user.get_id()
    return dag_run_cleared


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
        xcom_query = XComModel.get_many(
            run_id=self.run_id,
            key=XCOM_RETURN_KEY,
            task_ids=self.result_task_ids,
            dag_ids=self.dag_id,
        )
        async with create_session_async() as session:
            xcom_results = (
                await session.scalars(xcom_query.order_by(XComModel.task_id, XComModel.map_index))
            ).all()

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
        if self.result_task_ids:
            resp["results"] = await self._serialize_xcoms()
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
    ):
        super().__init__(session, request)
        self.dag_id = dag_id

    def handle_bulk_create(
        self, action: BulkCreateAction[BulkDAGRunBody], results: BulkActionResponse
    ) -> None:
        results.errors.append(
            {
                "error": "Dag Runs bulk create is not supported. Use the trigger Dag Run endpoint instead.",
                "status_code": status.HTTP_405_METHOD_NOT_ALLOWED,
            }
        )

    def handle_bulk_update(
        self, action: BulkUpdateAction[BulkDAGRunBody], results: BulkActionResponse
    ) -> None:
        results.errors.append(
            {
                "error": "Dag Runs bulk update is not supported yet. Use the patch Dag Run endpoint per run instead.",
                "status_code": status.HTTP_405_METHOD_NOT_ALLOWED,
            }
        )

    def handle_bulk_delete(
        self, action: BulkDeleteAction[BulkDAGRunBody], results: BulkActionResponse
    ) -> None:
        """Bulk delete Dag Runs."""
        keys: set[tuple[str, str]] = set()

        for entity in action.entities:
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
                results.errors.append(
                    {"error": error_msg, "status_code": status.HTTP_400_BAD_REQUEST},
                )
                continue

            keys.add((dag_id, dag_run_id))

        if not keys:
            return

        dag_runs = self.session.scalars(
            select(DagRun).where(tuple_(DagRun.dag_id, DagRun.run_id).in_(list(keys)))
        ).all()
        dag_run_map = {(dr.dag_id, dr.run_id): dr for dr in dag_runs}
        not_found = keys - dag_run_map.keys()

        if action.action_on_non_existence == BulkActionNotOnExistence.FAIL:
            for dag_id, run_id in sorted(not_found):
                results.errors.append(
                    {
                        "error": (f"The DagRun with dag_id: `{dag_id}` and run_id: `{run_id}` was not found"),
                        "status_code": status.HTTP_404_NOT_FOUND,
                    }
                )

        deletable_states = {s.value for s in DagRunMutableStates}
        for (dag_id, run_id), dag_run in dag_run_map.items():
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
