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
from airflow.api_fastapi.common.db.common import SessionDep
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


def _fetch_ti_writer_fields(token: TIToken, session: SessionDep) -> _TIWriterFields:
    """Return (dag_id, run_id, task_id, map_index) for the TI identified by the token."""
    row = session.execute(
        select(
            TaskInstance.dag_id,
            TaskInstance.run_id,
            TaskInstance.task_id,
            TaskInstance.map_index,
        ).where(TaskInstance.id == token.id)
    ).one_or_none()
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


def _resolve_asset_id_by_name(name: str, session: SessionDep) -> int:
    asset_id = session.scalar(select(AssetModel.id).where(AssetModel.name == name, AssetModel.active.has()))
    if asset_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Asset with name={name!r} not found"},
        )
    return asset_id


def _resolve_asset_id_by_uri(uri: str, session: SessionDep) -> int:
    asset_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == uri, AssetModel.active.has()))
    if asset_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Asset with uri={uri!r} not found"},
        )
    return asset_id


@router.get("/by-name/value")
def get_asset_state_store_by_name(
    name: Annotated[str, Query(min_length=1)],
    key: Annotated[str, Query(min_length=1)],
    session: SessionDep,
) -> AssetStateStoreResponse:
    """Get an asset state store value by asset name."""
    asset_id = _resolve_asset_id_by_name(name, session)
    value = get_state_backend().get(AssetScope(asset_id=asset_id), key, session=session)
    if value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Asset state store key {key!r} not found"},
        )
    return AssetStateStoreResponse(value=json.loads(value))


def _put_asset_state_store(
    scope: AssetScope,
    key: str,
    body: AssetStateStorePutBody,
    token: TIToken,
    session: SessionDep,
) -> None:
    backend = get_state_backend()
    if isinstance(backend, MetastoreBackend):
        if token.id == NULL_UUID:
            # Since the asset state store routes do not have `task_instance_id` in their path params, the default kicks in which is"00000000-0000-0000-0000-000000000000"
            backend.set_asset_state_store(
                scope,
                key,
                json.dumps(body.value),
                kind=AssetStateStoreWriterKind.WATCHER,
                session=session,
            )
        else:
            ti_fields = _fetch_ti_writer_fields(token, session)
            dag_id, run_id, task_id, map_index = ti_fields

            backend.set_asset_state_store(
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
        backend.set(scope, key, json.dumps(body.value), session=session)


@router.put("/by-name/value", status_code=status.HTTP_204_NO_CONTENT)
def set_asset_state_store_by_name(
    name: Annotated[str, Query(min_length=1)],
    key: Annotated[str, Query(min_length=1)],
    body: AssetStateStorePutBody,
    session: SessionDep,
    token: TIToken = CurrentTIToken,
) -> None:
    """Set an asset state store value by asset name."""
    _put_asset_state_store(
        AssetScope(asset_id=_resolve_asset_id_by_name(name, session)), key, body, token, session
    )


@router.delete("/by-name/value", status_code=status.HTTP_204_NO_CONTENT)
def delete_asset_state_store_by_name(
    name: Annotated[str, Query(min_length=1)],
    key: Annotated[str, Query(min_length=1)],
    session: SessionDep,
) -> None:
    """Delete a single asset state store key by asset name."""
    asset_id = _resolve_asset_id_by_name(name, session)
    get_state_backend().delete(AssetScope(asset_id=asset_id), key, session=session)


@router.delete("/by-name/clear", status_code=status.HTTP_204_NO_CONTENT)
def clear_asset_state_store_by_name(
    name: Annotated[str, Query(min_length=1)],
    session: SessionDep,
) -> None:
    """Delete all state store keys for an asset by asset name."""
    asset_id = _resolve_asset_id_by_name(name, session)
    get_state_backend().clear(AssetScope(asset_id=asset_id), session=session)


@router.get("/by-uri/value")
def get_asset_state_store_by_uri(
    uri: Annotated[str, Query(min_length=1)],
    key: Annotated[str, Query(min_length=1)],
    session: SessionDep,
) -> AssetStateStoreResponse:
    """Get an asset state store value by asset URI."""
    asset_id = _resolve_asset_id_by_uri(uri, session)
    value = get_state_backend().get(AssetScope(asset_id=asset_id), key, session=session)
    if value is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Asset state store key {key!r} not found"},
        )
    return AssetStateStoreResponse(value=json.loads(value))


@router.put("/by-uri/value", status_code=status.HTTP_204_NO_CONTENT)
def set_asset_state_store_by_uri(
    uri: Annotated[str, Query(min_length=1)],
    key: Annotated[str, Query(min_length=1)],
    body: AssetStateStorePutBody,
    session: SessionDep,
    token: TIToken = CurrentTIToken,
) -> None:
    """Set an asset state store value by asset URI."""
    _put_asset_state_store(
        AssetScope(asset_id=_resolve_asset_id_by_uri(uri, session)), key, body, token, session
    )


@router.delete("/by-uri/value", status_code=status.HTTP_204_NO_CONTENT)
def delete_asset_state_store_by_uri(
    uri: Annotated[str, Query(min_length=1)],
    key: Annotated[str, Query(min_length=1)],
    session: SessionDep,
) -> None:
    """Delete a single asset state store key by asset URI."""
    asset_id = _resolve_asset_id_by_uri(uri, session)
    get_state_backend().delete(AssetScope(asset_id=asset_id), key, session=session)


@router.delete("/by-uri/clear", status_code=status.HTTP_204_NO_CONTENT)
def clear_asset_state_store_by_uri(
    uri: Annotated[str, Query(min_length=1)],
    session: SessionDep,
) -> None:
    """Delete all state store keys for an asset by asset URI."""
    asset_id = _resolve_asset_id_by_uri(uri, session)
    get_state_backend().clear(AssetScope(asset_id=asset_id), session=session)
