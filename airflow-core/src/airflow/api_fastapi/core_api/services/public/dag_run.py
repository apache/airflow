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
from typing import TYPE_CHECKING, Any, TypeAlias

import attrs
from fastapi import HTTPException, status
from sqlalchemy import select, tuple_

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionResponse,
    BulkBody,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.dag_run import BulkDagRunBody, DAGRunPatchStates
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.models.dagrun import DagRun
from airflow.models.xcom import XCOM_RETURN_KEY, XComModel
from airflow.utils.session import create_session_async
from airflow.utils.state import State

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

    from sqlalchemy import ScalarResult


DagRunKey: TypeAlias = tuple[str, str]
"""Unique identifier for a DagRun as (dag_id, dag_run_id)."""


@attrs.define
class DagRunWaiter:
    """Wait for the specified dag run to finish, and collect info from it."""

    dag_id: str
    run_id: str
    interval: float
    result_task_ids: list[str] | None
    session: SessionDep

    async def _get_dag_run(self) -> DagRun:
        async with create_session_async() as session:
            return await session.scalar(select(DagRun).filter_by(dag_id=self.dag_id, run_id=self.run_id))

    def _serialize_xcoms(self) -> dict[str, Any]:
        xcom_query = XComModel.get_many(
            run_id=self.run_id,
            key=XCOM_RETURN_KEY,
            task_ids=self.result_task_ids,
            dag_ids=self.dag_id,
        )
        xcom_results: ScalarResult[tuple[XComModel]] = self.session.scalars(
            xcom_query.order_by(XComModel.task_id, XComModel.map_index)
        )

        def _group_xcoms(g: Iterator[XComModel | tuple[XComModel]]) -> Any:
            entries = [row[0] if isinstance(row, tuple) else row for row in g]
            if len(entries) == 1 and entries[0].map_index < 0:  # Unpack non-mapped task xcom.
                return entries[0].value
            return [entry.value for entry in entries]  # Task is mapped; return all xcoms in a list.

        return {
            task_id: _group_xcoms(g)
            for task_id, g in itertools.groupby(xcom_results, key=operator.attrgetter("task_id"))
        }

    def _serialize_response(self, dag_run: DagRun) -> str:
        resp = {"state": dag_run.state}
        if dag_run.state not in State.finished_dr_states:
            return json.dumps(resp)
        if self.result_task_ids:
            resp["results"] = self._serialize_xcoms()
        return json.dumps(resp)

    async def wait(self) -> AsyncGenerator[str, None]:
        yield self._serialize_response(dag_run := await self._get_dag_run())
        yield "\n"
        while dag_run.state not in State.finished_dr_states:
            await asyncio.sleep(self.interval)
            yield self._serialize_response(dag_run := await self._get_dag_run())
            yield "\n"


class BulkDagRunService(BulkService[BulkDagRunBody]):
    """Service for handling bulk operations on dag runs."""

    def __init__(
        self,
        session: SessionDep,
        request: BulkBody[BulkDagRunBody],
        dag_id: str,
    ):
        super().__init__(session, request)
        self.dag_id = dag_id

    def _extract_dag_run_identifiers(self, entity: str | BulkDagRunBody) -> DagRunKey | None:
        if isinstance(entity, str):
            if self.dag_id == "~":
                return None
            return self.dag_id, entity

        return entity.dag_id, entity.dag_run_id

    def categorize_dag_runs(
        self, dag_run_keys: set[DagRunKey]
    ) -> tuple[dict[DagRunKey, DagRun], set[DagRunKey], set[DagRunKey]]:
        dag_runs = self.session.scalars(
            select(DagRun).where(tuple_(DagRun.dag_id, DagRun.run_id).in_(list(dag_run_keys)))
        ).all()
        dag_runs_map = {(run.dag_id, run.run_id): run for run in dag_runs}
        matched_keys = set(dag_runs_map.keys())
        not_found_keys = dag_run_keys - matched_keys
        return dag_runs_map, matched_keys, not_found_keys

    def handle_bulk_create(
        self, action: BulkCreateAction[BulkDagRunBody], results: BulkActionResponse
    ) -> None:
        results.errors.append(
            {
                "error": "Dag runs bulk create is not implemented",
                "status_code": status.HTTP_501_NOT_IMPLEMENTED,
            }
        )

    def handle_bulk_update(
        self, action: BulkUpdateAction[BulkDagRunBody], results: BulkActionResponse
    ) -> None:
        results.errors.append(
            {
                "error": "Dag runs bulk update is not implemented",
                "status_code": status.HTTP_501_NOT_IMPLEMENTED,
            }
        )

    def handle_bulk_delete(
        self, action: BulkDeleteAction[BulkDagRunBody], results: BulkActionResponse
    ) -> None:
        dag_run_keys: set[DagRunKey] = set()
        for dag_run_entity in action.entities:
            dag_run_key = self._extract_dag_run_identifiers(dag_run_entity)
            if dag_run_key is None:
                results.errors.append(
                    {
                        "error": (
                            "When using wildcard in path, dag_id must be specified in BulkDagRunBody object, "
                            f"not as string for dag_run_id: {dag_run_entity}"
                        ),
                        "status_code": status.HTTP_400_BAD_REQUEST,
                    }
                )
                continue

            dag_run_keys.add(dag_run_key)

        dag_runs_map, matched_keys, not_found_keys = self.categorize_dag_runs(dag_run_keys)

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_keys:
                not_found_list = [
                    {"dag_id": dag_id, "dag_run_id": dag_run_id}
                    for dag_id, dag_run_id in sorted(not_found_keys)
                ]
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The DagRuns with these identifiers were not found: {not_found_list}",
                )

            deletable_states = {state.value for state in DAGRunPatchStates}

            for dag_id, dag_run_id in matched_keys:
                dag_run = dag_runs_map[(dag_id, dag_run_id)]
                if dag_run.state not in deletable_states:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=(
                            f"The DagRun with dag_id: `{dag_id}` and run_id: `{dag_run_id}` "
                            f"cannot be deleted in {dag_run.state} state"
                        ),
                    )
                self.session.delete(dag_run)
                results.success.append(f"{dag_id}.{dag_run_id}")

        except HTTPException as exc:
            results.errors.append({"error": f"{exc.detail}", "status_code": exc.status_code})
