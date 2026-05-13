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
import logging
import operator
from typing import TYPE_CHECKING, Any, Literal

import attrs
from fastapi import HTTPException, status
from sqlalchemy import select, tuple_
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.session import Session

from airflow.api.common.mark_tasks import (
    set_dag_run_state_to_failed,
    set_dag_run_state_to_queued,
    set_dag_run_state_to_success,
)
from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.api_fastapi.common.dagbag import DagBagDep, get_dag_for_run
from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionResponse,
    BulkBody,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.dag_run import (
    BulkClearDagRunsBody,
    BulkDagRunBody,
    DAGRunPatchStates,
)
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.listeners.listener import get_listener_manager
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.xcom import XCOM_RETURN_KEY, XComModel
from airflow.utils.session import create_session_async
from airflow.utils.state import State

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Iterator

    from airflow.serialization.definitions.dag import SerializedDAG


AuthMethod = Literal["GET", "PUT", "POST", "DELETE"]


log = logging.getLogger(__name__)


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


def _format_dag_run_key(dag_id: str, dag_run_id: str) -> str:
    return f"{dag_id}.{dag_run_id}"


def _authorize_dag_run(
    *,
    session: Session,
    user,
    dag_id: str,
    method: AuthMethod,
    cache: dict[str, bool],
) -> bool:
    """
    Return whether ``user`` may perform ``method`` on Dag runs of ``dag_id``.

    The result is memoised in ``cache`` so a bulk request that touches many
    runs of the same Dag only pays for one ``is_authorized_dag`` call per Dag.
    """
    if dag_id not in cache:
        team_name = DagModel.get_team_name(dag_id, session=session)
        cache[dag_id] = get_auth_manager().is_authorized_dag(
            method=method,
            access_entity=DagAccessEntity.RUN,
            details=DagDetails(id=dag_id, team_name=team_name),
            user=user,
        )
    return cache[dag_id]


def _cached_dag_for_run(
    dag_bag: DagBagDep,
    dag_run: DagRun,
    session: Session,
    cache: dict[str, SerializedDAG],
) -> SerializedDAG:
    """Return the SerializedDAG for ``dag_run``, memoising lookups by ``dag_id``."""
    dag_id = dag_run.dag_id
    if dag_id not in cache:
        cache[dag_id] = get_dag_for_run(dag_bag, dag_run, session=session)
    return cache[dag_id]


def _apply_state_change(
    dag_run: DagRun,
    new_state: DAGRunPatchStates,
    dag: SerializedDAG,
    session: Session,
) -> None:
    """Apply ``new_state`` to ``dag_run`` and fire the matching listener hook."""
    if new_state == DAGRunPatchStates.SUCCESS:
        set_dag_run_state_to_success(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
        try:
            get_listener_manager().hook.on_dag_run_success(dag_run=dag_run, msg="")
        except Exception:
            log.exception("error calling listener")
    elif new_state == DAGRunPatchStates.QUEUED:
        # Notification on queued is intentionally skipped; the scheduler emits
        # the RUNNING notification instead.
        set_dag_run_state_to_queued(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
    elif new_state == DAGRunPatchStates.FAILED:
        set_dag_run_state_to_failed(dag=dag, run_id=dag_run.run_id, commit=True, session=session)
        try:
            get_listener_manager().hook.on_dag_run_failed(dag_run=dag_run, msg="")
        except Exception:
            log.exception("error calling listener")


def _apply_note(dag_run: DagRun, note: str | None, user_id: str) -> None:
    if dag_run.dag_run_note is None:
        dag_run.note = (note, user_id)
    else:
        dag_run.dag_run_note.content = note
        dag_run.dag_run_note.user_id = user_id


def _validate_no_wildcard_in_resolved(
    *,
    dag_id: str,
    dag_run_id: str,
    results: BulkActionResponse,
) -> bool:
    if dag_id == "~" or dag_run_id == "~":
        results.errors.append(
            {
                "error": (
                    "When the path uses the ``~`` wildcard, ``dag_id`` and ``dag_run_id`` must be "
                    "specified in the body for each entity."
                ),
                "status_code": status.HTTP_400_BAD_REQUEST,
            }
        )
        return False
    return True


def _validate_path_dag_id_match(
    *,
    path_dag_id: str,
    entity_dag_id: str | None,
    dag_run_id: str,
    results: BulkActionResponse,
) -> bool:
    if path_dag_id != "~" and entity_dag_id is not None and entity_dag_id != path_dag_id:
        results.errors.append(
            {
                "error": (
                    f"Entity dag_id '{entity_dag_id}' does not match path dag_id '{path_dag_id}'. "
                    "Use ``~`` in the path for cross-DAG bulk operations."
                ),
                "status_code": status.HTTP_400_BAD_REQUEST,
                "dag_id": entity_dag_id,
                "dag_run_id": dag_run_id,
            }
        )
        return False
    return True


def _fetch_dag_runs(
    session: Session,
    keys: set[tuple[str, str]],
) -> tuple[dict[tuple[str, str], DagRun], set[tuple[str, str]]]:
    """Batch-fetch Dag runs by ``(dag_id, dag_run_id)`` pairs in a single query."""
    if not keys:
        return {}, set()
    dag_runs = session.scalars(
        select(DagRun)
        .options(joinedload(DagRun.dag_model))
        .where(tuple_(DagRun.dag_id, DagRun.run_id).in_(keys))
    ).all()
    found = {(dr.dag_id, dr.run_id): dr for dr in dag_runs}
    not_found = keys - set(found.keys())
    return found, not_found


class BulkDagRunService(BulkService[BulkDagRunBody]):
    """Service for handling bulk operations on Dag runs."""

    def __init__(
        self,
        session: Session,
        request: BulkBody[BulkDagRunBody],
        dag_id: str,
        dag_bag: DagBagDep,
        user: GetUserDep,
    ):
        super().__init__(session, request)
        self.dag_id = dag_id
        self.dag_bag = dag_bag
        self.user = user

    def _resolve_identifiers(self, entity: str | BulkDagRunBody) -> tuple[str, str]:
        """Return ``(dag_id, dag_run_id)`` for an entity, falling back to the path's ``dag_id``."""
        if isinstance(entity, str):
            return self.dag_id, entity
        dag_id = entity.dag_id or self.dag_id
        return dag_id, entity.dag_run_id

    def _check_dag_authorization(
        self,
        dag_id: str,
        method: AuthMethod,
        action_name: str,
        results: BulkActionResponse,
        cache: dict[str, bool],
    ) -> bool:
        if not _authorize_dag_run(
            session=self.session,
            user=self.user,
            dag_id=dag_id,
            method=method,
            cache=cache,
        ):
            results.errors.append(
                {
                    "error": f"User is not authorized to {action_name} Dag runs for DAG '{dag_id}'",
                    "status_code": status.HTTP_403_FORBIDDEN,
                }
            )
            return False
        return True

    def handle_bulk_create(
        self, action: BulkCreateAction[BulkDagRunBody], results: BulkActionResponse
    ) -> None:
        results.errors.append(
            {
                "error": "Dag runs bulk create is not supported via this endpoint; use the trigger Dag run endpoint instead.",
                "status_code": status.HTTP_405_METHOD_NOT_ALLOWED,
            }
        )

    def handle_bulk_update(
        self, action: BulkUpdateAction[BulkDagRunBody], results: BulkActionResponse
    ) -> None:
        """Bulk update Dag runs (state and/or note)."""
        update_mask = action.update_mask
        auth_cache: dict[str, bool] = {}
        dag_cache: dict[str, SerializedDAG] = {}
        keys: set[tuple[str, str]] = set()
        entity_map: dict[tuple[str, str], BulkDagRunBody] = {}

        for entity in action.entities:
            if isinstance(entity, str):
                results.errors.append(
                    {
                        "error": "Bulk update requires entities as objects, not strings.",
                        "status_code": status.HTTP_400_BAD_REQUEST,
                    }
                )
                continue
            dag_id, dag_run_id = self._resolve_identifiers(entity)
            if not _validate_no_wildcard_in_resolved(dag_id=dag_id, dag_run_id=dag_run_id, results=results):
                continue
            if not _validate_path_dag_id_match(
                path_dag_id=self.dag_id,
                entity_dag_id=entity.dag_id,
                dag_run_id=dag_run_id,
                results=results,
            ):
                continue
            if not self._check_dag_authorization(dag_id, "PUT", action.action.value, results, auth_cache):
                continue
            keys.add((dag_id, dag_run_id))
            entity_map[(dag_id, dag_run_id)] = entity

        try:
            found, not_found = _fetch_dag_runs(self.session, keys)

            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found:
                missing = [{"dag_id": d, "dag_run_id": r} for d, r in not_found]
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The Dag runs with these identifiers were not found: {missing}",
                )

            for key, dag_run in found.items():
                entity = entity_map[key]
                fields_to_update = entity.model_fields_set
                if update_mask:
                    fields_to_update = fields_to_update.intersection(update_mask)
                fields_to_update = fields_to_update - {"dag_id", "dag_run_id"}
                if not fields_to_update:
                    continue

                try:
                    with self.session.begin_nested():
                        dag = _cached_dag_for_run(self.dag_bag, dag_run, self.session, dag_cache)
                        if "state" in fields_to_update and entity.state is not None:
                            _apply_state_change(dag_run, entity.state, dag, self.session)
                        if "note" in fields_to_update:
                            refreshed = self.session.get(DagRun, dag_run.id)
                            if refreshed is not None:
                                _apply_note(refreshed, entity.note, self.user.get_id())
                except HTTPException as exc:
                    results.errors.append(
                        {
                            "error": str(exc.detail),
                            "status_code": exc.status_code,
                            "dag_id": key[0],
                            "dag_run_id": key[1],
                        }
                    )
                    continue
                except Exception as exc:
                    results.errors.append(
                        {
                            "error": str(exc),
                            "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
                            "dag_id": key[0],
                            "dag_run_id": key[1],
                        }
                    )
                    continue

                results.success.append(_format_dag_run_key(*key))
        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

    def handle_bulk_delete(
        self, action: BulkDeleteAction[BulkDagRunBody], results: BulkActionResponse
    ) -> None:
        """Bulk delete Dag runs."""
        auth_cache: dict[str, bool] = {}
        keys: set[tuple[str, str]] = set()

        for entity in action.entities:
            dag_id, dag_run_id = self._resolve_identifiers(entity)
            entity_dag_id = entity.dag_id if isinstance(entity, BulkDagRunBody) else None
            if not _validate_no_wildcard_in_resolved(dag_id=dag_id, dag_run_id=dag_run_id, results=results):
                continue
            if not _validate_path_dag_id_match(
                path_dag_id=self.dag_id,
                entity_dag_id=entity_dag_id,
                dag_run_id=dag_run_id,
                results=results,
            ):
                continue
            if not self._check_dag_authorization(dag_id, "DELETE", action.action.value, results, auth_cache):
                continue
            keys.add((dag_id, dag_run_id))

        try:
            found, not_found = _fetch_dag_runs(self.session, keys)

            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found:
                missing = [{"dag_id": d, "dag_run_id": r} for d, r in not_found]
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The Dag runs with these identifiers were not found: {missing}",
                )

            deletable_states = {s.value for s in DAGRunPatchStates}
            for key, dag_run in found.items():
                if dag_run.state not in deletable_states:
                    results.errors.append(
                        {
                            "error": (
                                f"The DagRun with dag_id: `{dag_run.dag_id}` and run_id: `{dag_run.run_id}` "
                                f"cannot be deleted in {dag_run.state} state"
                            ),
                            "status_code": status.HTTP_409_CONFLICT,
                            "dag_id": dag_run.dag_id,
                            "dag_run_id": dag_run.run_id,
                        }
                    )
                    continue
                self.session.delete(dag_run)
                results.success.append(_format_dag_run_key(*key))
        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})


def bulk_clear_dag_runs(
    body: BulkClearDagRunsBody,
    dag_id: str,
    dag_bag: DagBagDep,
    session: Session,
    user: GetUserDep,
) -> BulkActionResponse:
    """
    Run ``dag.clear()`` for each ``(dag_id, dag_run_id)`` in ``body.runs`` within a single transaction.

    Returns ``BulkActionResponse`` with per-run success keys and per-run failure entries so that a partial
    failure does not abort the entire batch.
    """
    results = BulkActionResponse()
    auth_cache: dict[str, bool] = {}
    dag_cache: dict[str, SerializedDAG] = {}
    keys_to_fetch: list[tuple[str, str]] = []

    for identifier in body.runs:
        run_dag_id = identifier.dag_id or dag_id
        run_id = identifier.dag_run_id

        if not _validate_no_wildcard_in_resolved(dag_id=run_dag_id, dag_run_id=run_id, results=results):
            continue
        if not _validate_path_dag_id_match(
            path_dag_id=dag_id,
            entity_dag_id=identifier.dag_id,
            dag_run_id=run_id,
            results=results,
        ):
            continue
        if not _authorize_dag_run(
            session=session, user=user, dag_id=run_dag_id, method="PUT", cache=auth_cache
        ):
            results.errors.append(
                {
                    "error": f"User is not authorized to clear Dag runs for DAG '{run_dag_id}'",
                    "status_code": status.HTTP_403_FORBIDDEN,
                    "dag_id": run_dag_id,
                    "dag_run_id": run_id,
                }
            )
            continue

        keys_to_fetch.append((run_dag_id, run_id))

    found, _ = _fetch_dag_runs(session, set(keys_to_fetch))

    for key in keys_to_fetch:
        run_dag_id, run_id = key
        dag_run = found.get(key)
        if dag_run is None:
            results.errors.append(
                {
                    "error": f"Dag run not found for dag_id '{run_dag_id}', dag_run_id '{run_id}'",
                    "status_code": status.HTTP_404_NOT_FOUND,
                    "dag_id": run_dag_id,
                    "dag_run_id": run_id,
                }
            )
            continue

        try:
            with session.begin_nested():
                dag = _cached_dag_for_run(dag_bag, dag_run, session, dag_cache)
                if body.dry_run:
                    dag.clear(
                        run_id=run_id,
                        task_ids=None,
                        only_failed=body.only_failed,
                        only_new=body.only_new,
                        run_on_latest_version=body.run_on_latest_version,
                        dry_run=True,
                        session=session,
                    )
                else:
                    dag.clear(
                        run_id=run_id,
                        task_ids=None,
                        only_failed=body.only_failed,
                        only_new=body.only_new,
                        run_on_latest_version=body.run_on_latest_version,
                        session=session,
                    )
                    if body.note is not None:
                        refreshed = session.get(DagRun, dag_run.id)
                        if refreshed is not None:
                            _apply_note(refreshed, body.note, user.get_id())
        except HTTPException as exc:
            results.errors.append(
                {
                    "error": str(exc.detail),
                    "status_code": exc.status_code,
                    "dag_id": run_dag_id,
                    "dag_run_id": run_id,
                }
            )
            continue
        except Exception as exc:
            results.errors.append(
                {
                    "error": str(exc),
                    "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
                    "dag_id": run_dag_id,
                    "dag_run_id": run_id,
                }
            )
            continue

        results.success.append(_format_dag_run_key(run_dag_id, run_id))

    return results
