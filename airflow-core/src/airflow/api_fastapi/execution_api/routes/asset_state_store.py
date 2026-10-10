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
"""
Execution API routes for asset state store.

Routes are split into ``/by-name`` and ``/by-uri`` sub-prefixes mirroring the
existing ``/assets/by-name`` and ``/assets/by-uri`` pattern.  Callers pass
whichever identifier their inlet type carries: ``Asset``/``AssetNameRef`` use
the name routes, ``AssetUriRef`` uses the URI routes.

Per-task asset registration checks are intentionally not implemented here
(deferred to AIP-93 — see TODO comment below).
"""

from __future__ import annotations

import json
from typing import Annotated
from uuid import UUID

from cadwyn import VersionedAPIRouter
from fastapi import HTTPException, Query, status
from sqlalchemy import select

from airflow._shared.state import AssetScope, AssetStateStoreWriterKind
from airflow.api_fastapi.common.db.common import AsyncSessionDep
from airflow.api_fastapi.execution_api.datamodels.asset_state_store import (
    AssetStateStorePutBody,
    AssetStateStoreResponse,
)
from airflow.api_fastapi.execution_api.datamodels.token import TIToken
from airflow.api_fastapi.execution_api.security import CurrentTIToken, ExecutionAPIRoute
from airflow.models.asset import AssetModel
from airflow.models.taskinstance import TaskInstance
from airflow.state import get_state_backend
from airflow.state.metastore import MetastoreBackend

_TIWriterFields = tuple[str, str, str, int]
NULL_UUID = UUID(int=0)


async def _fetch_ti_writer_fields(token: TIToken, session: AsyncSessionDep) -> _TIWriterFields:
    """Return (dag_id, run_id, task_id, map_index) for the TI identified by the token."""
    result = await session.execute(
        select(
            TaskInstance.dag_id,
            TaskInstance.run_id,
            TaskInstance.task_id,
            TaskInstance.map_index,
        ).where(TaskInstance.id == token.id)
    )
    row = result.one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Task instance {token.id!r} not found"},
        )
    return row.dag_id, row.run_id, row.task_id, row.map_index


# TODO(AIP-103): enforce that the requesting task is registered with the asset
# (via task_inlet_asset_reference or task_outlet_asset_reference) before
# allowing reads/writes. Currently any task with a valid execution token can
# access any asset's state store — the same gap exists in /assets and /asset-events.
# Proper fix is a unified asset-registration check across all asset routes,
# not just here.
router = VersionedAPIRouter(
    route_class=ExecutionAPIRoute,
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"},
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
    },
)


async def _resolve_asset_id_by_name(name: str, session: AsyncSessionDep) -> int:
    asset_id = await session.scalar(
        select(AssetModel.id).where(AssetModel.name == name, AssetModel.active.has())
    )
    if asset_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Asset with name={name!r} not found"},
        )
    return asset_id


async def _resolve_asset_id_by_uri(uri: str, session: AsyncSessionDep) -> int:
    asset_id = await session.scalar(
        select(AssetModel.id).where(AssetModel.uri == uri, AssetModel.active.has())
    )
    if asset_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Asset with uri={uri!r} not found"},
        )
    return asset_id


@router.get("/by-name/value")
async def get_asset_state_store_by_name(
    name: Annotated[str, Query(min_length=1)],
    key: Annotated[str, Query(min_length=1)],
    session: AsyncSessionDep,
) -> AssetStateStoreResponse:
    """Get an asset state store value by asset name."""
    asset_id = await _resolve_asset_id_by_name(name, session)
    value = await get_state_backend().aget(AssetScope(asset_id=asset_id), key, session=session)
    if value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Asset state store key {key!r} not found"},
        )
    return AssetStateStoreResponse(value=json.loads(value))


async def _put_asset_state_store(
    scope: AssetScope,
    key: str,
    body: AssetStateStorePutBody,
    token: TIToken,
    session: AsyncSessionDep,
) -> None:
    backend = get_state_backend()
    if isinstance(backend, MetastoreBackend):
        if token.id == NULL_UUID:
            # Since the asset state store routes do not have `task_instance_id` in their path params, the default kicks in which is"00000000-0000-0000-0000-000000000000"
            await backend.aset_asset_state_store(
                scope,
                key,
                json.dumps(body.value),
                kind=AssetStateStoreWriterKind.WATCHER,
                session=session,
            )
        else:
            ti_fields = await _fetch_ti_writer_fields(token, session)
            dag_id, run_id, task_id, map_index = ti_fields

            await backend.aset_asset_state_store(
                scope,
                key,
                json.dumps(body.value),
                kind=AssetStateStoreWriterKind.TASK,
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                map_index=map_index,
                session=session,
            )
    else:
        await backend.aset(scope, key, json.dumps(body.value), session=session)


@router.put("/by-name/value", status_code=status.HTTP_204_NO_CONTENT)
async def set_asset_state_store_by_name(
    name: Annotated[str, Query(min_length=1)],
    key: Annotated[str, Query(min_length=1)],
    body: AssetStateStorePutBody,
    session: AsyncSessionDep,
    token: TIToken = CurrentTIToken,
) -> None:
    """Set an asset state store value by asset name."""
    await _put_asset_state_store(
        AssetScope(asset_id=await _resolve_asset_id_by_name(name, session)), key, body, token, session
    )


@router.delete("/by-name/value", status_code=status.HTTP_204_NO_CONTENT)
async def delete_asset_state_store_by_name(
    name: Annotated[str, Query(min_length=1)],
    key: Annotated[str, Query(min_length=1)],
    session: AsyncSessionDep,
) -> None:
    """Delete a single asset state store key by asset name."""
    asset_id = await _resolve_asset_id_by_name(name, session)
    await get_state_backend().adelete(AssetScope(asset_id=asset_id), key, session=session)


@router.delete("/by-name/clear", status_code=status.HTTP_204_NO_CONTENT)
async def clear_asset_state_store_by_name(
    name: Annotated[str, Query(min_length=1)],
    session: AsyncSessionDep,
) -> None:
    """Delete all state store keys for an asset by asset name."""
    asset_id = await _resolve_asset_id_by_name(name, session)
    await get_state_backend().aclear(AssetScope(asset_id=asset_id), session=session)


@router.get("/by-uri/value")
async def get_asset_state_store_by_uri(
    uri: Annotated[str, Query(min_length=1)],
    key: Annotated[str, Query(min_length=1)],
    session: AsyncSessionDep,
) -> AssetStateStoreResponse:
    """Get an asset state store value by asset URI."""
    asset_id = await _resolve_asset_id_by_uri(uri, session)
    value = await get_state_backend().aget(AssetScope(asset_id=asset_id), key, session=session)
    if value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Asset state store key {key!r} not found"},
        )
    return AssetStateStoreResponse(value=json.loads(value))


@router.put("/by-uri/value", status_code=status.HTTP_204_NO_CONTENT)
async def set_asset_state_store_by_uri(
    uri: Annotated[str, Query(min_length=1)],
    key: Annotated[str, Query(min_length=1)],
    body: AssetStateStorePutBody,
    session: AsyncSessionDep,
    token: TIToken = CurrentTIToken,
) -> None:
    """Set an asset state store value by asset URI."""
    asset_id = await _resolve_asset_id_by_uri(uri, session)
    await _put_asset_state_store(AssetScope(asset_id=asset_id), key, body, token, session)


@router.delete("/by-uri/value", status_code=status.HTTP_204_NO_CONTENT)
async def delete_asset_state_store_by_uri(
    uri: Annotated[str, Query(min_length=1)],
    key: Annotated[str, Query(min_length=1)],
    session: AsyncSessionDep,
) -> None:
    """Delete a single asset state store key by asset URI."""
    asset_id = await _resolve_asset_id_by_uri(uri, session)
    await get_state_backend().adelete(AssetScope(asset_id=asset_id), key, session=session)


@router.delete("/by-uri/clear", status_code=status.HTTP_204_NO_CONTENT)
async def clear_asset_state_store_by_uri(
    uri: Annotated[str, Query(min_length=1)],
    session: AsyncSessionDep,
) -> None:
    """Delete all state store keys for an asset by asset URI."""
    asset_id = await _resolve_asset_id_by_uri(uri, session)
    await get_state_backend().aclear(AssetScope(asset_id=asset_id), session=session)
