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
from typing import TYPE_CHECKING, Any, Literal

import attrs
import structlog
from fastapi import status
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionResponse,
    BulkBody,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.dag_run import BulkDAGRunBody, DAGRunPatchStates
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.xcom import XCOM_RETURN_KEY, XComModel
from airflow.utils.session import create_session_async
from airflow.utils.state import State

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

log = structlog.get_logger(__name__)


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
        user: GetUserDep,
    ):
        super().__init__(session, request)
        self.dag_id = dag_id
        self.user = user

    def _resolve_dag_id(self, entity: str | BulkDAGRunBody) -> tuple[str, str]:
        """Resolve the (dag_id, dag_run_id) pair for an entity, falling back to the path ``dag_id``."""
        if isinstance(entity, str):
            return self.dag_id, entity
        return entity.dag_id or self.dag_id, entity.dag_run_id

    def _check_dag_authorization(
        self,
        dag_id: str,
        method: Literal["GET", "POST", "PUT", "DELETE"],
        action_name: str,
        cache: dict[str, bool],
        results: BulkActionResponse,
    ) -> bool:
        """Cache and enforce per-Dag authorization for a bulk action."""
        if dag_id not in cache:
            team_name = DagModel.get_team_name(dag_id, session=self.session)
            cache[dag_id] = get_auth_manager().is_authorized_dag(
                method=method,
                access_entity=DagAccessEntity.RUN,
                details=DagDetails(id=dag_id, team_name=team_name),
                user=self.user,
            )
        if not cache[dag_id]:
            results.errors.append(
                {
                    "error": f"User is not authorized to {action_name} Dag Runs for DAG '{dag_id}'",
                    "status_code": status.HTTP_403_FORBIDDEN,
                }
            )
            return False
        return True

    @staticmethod
    def _result_key(dag_id: str, dag_run_id: str) -> str:
        return f"{dag_id}.{dag_run_id}"

    def _fetch_dag_runs(
        self,
        keys: set[tuple[str, str]],
    ) -> tuple[dict[tuple[str, str], DagRun], set[tuple[str, str]]]:
        if not keys:
            return {}, set()
        dag_ids = {dag_id for dag_id, _ in keys}
        run_ids = {run_id for _, run_id in keys}
        dag_runs = self.session.scalars(
            select(DagRun).where(DagRun.dag_id.in_(dag_ids), DagRun.run_id.in_(run_ids))
        ).all()
        dag_run_map = {(dr.dag_id, dr.run_id): dr for dr in dag_runs if (dr.dag_id, dr.run_id) in keys}
        not_found = keys - dag_run_map.keys()
        return dag_run_map, not_found

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
        authorization_cache: dict[str, bool] = {}
        keys: set[tuple[str, str]] = set()

        for entity in action.entities:
            dag_id, dag_run_id = self._resolve_dag_id(entity)
            if dag_id == "~":
                results.errors.append(
                    {
                        "error": (
                            "When using wildcard in path, dag_id must be specified "
                            f"in the request body for dag_run_id: {dag_run_id}"
                        ),
                        "status_code": status.HTTP_400_BAD_REQUEST,
                    }
                )
                continue
            if not self._check_dag_authorization(
                dag_id, method="DELETE", action_name="delete", cache=authorization_cache, results=results
            ):
                continue
            keys.add((dag_id, dag_run_id))

        if not keys:
            return

        dag_run_map, not_found = self._fetch_dag_runs(keys)

        if not_found and action.action_on_non_existence == BulkActionNotOnExistence.FAIL:
            not_found_ids = [{"dag_id": dag_id, "dag_run_id": run_id} for dag_id, run_id in sorted(not_found)]
            results.errors.append(
                {
                    "error": f"The Dag Runs with these identifiers: {not_found_ids} were not found",
                    "status_code": status.HTTP_404_NOT_FOUND,
                }
            )
            return

        deletable_states = {s.value for s in DAGRunPatchStates}
        for key, dag_run in dag_run_map.items():
            if dag_run.state not in deletable_states:
                results.errors.append(
                    {
                        "error": (
                            f"The DagRun with dag_id: `{key[0]}` and run_id: `{key[1]}` "
                            f"cannot be deleted in {dag_run.state} state"
                        ),
                        "status_code": status.HTTP_409_CONFLICT,
                    }
                )
                continue
            self.session.delete(dag_run)
            results.success.append(self._result_key(key[0], key[1]))
