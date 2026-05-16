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

from fastapi import Depends, HTTPException, status
from sqlalchemy import select

from airflow._shared.state import AssetScope
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.asset_state import (
    AssetStateBody,
    AssetStateCollectionResponse,
    AssetStateEntry,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_asset
from airflow.models.asset_state import AssetStateModel
from airflow.state.metastore import MetastoreStateBackend

asset_state_router = AirflowRouter(
    tags=["Asset State"],
    prefix="/assets/{asset_id}/state",
)


@asset_state_router.get(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="GET"))],
)
def list_asset_state(
    asset_id: int,
    session: SessionDep,
) -> AssetStateCollectionResponse:
    """List all state entries for an asset."""
    rows = session.execute(
        select(
            AssetStateModel.key,
            AssetStateModel.value,
            AssetStateModel.updated_at,
        ).where(AssetStateModel.asset_id == asset_id)
    ).all()
    entries = [AssetStateEntry(key=r.key, value=r.value, updated_at=r.updated_at) for r in rows]
    return AssetStateCollectionResponse(asset_states=entries, total_entries=len(entries))


@asset_state_router.get(
    "/{key}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="GET"))],
)
def get_asset_state(
    asset_id: int,
    key: str,
    session: SessionDep,
) -> AssetStateEntry:
    """Get a single asset state entry."""
    row = session.execute(
        select(
            AssetStateModel.key,
            AssetStateModel.value,
            AssetStateModel.updated_at,
        ).where(
            AssetStateModel.asset_id == asset_id,
            AssetStateModel.key == key,
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"reason": "not_found", "message": f"Asset state key {key!r} not found"},
        )
    return AssetStateEntry(key=row.key, value=row.value, updated_at=row.updated_at)


@asset_state_router.put(
    "/{key}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="PUT"))],
)
def set_asset_state(
    asset_id: int,
    key: str,
    body: AssetStateBody,
    session: SessionDep,
) -> None:
    """Set an asset state value. Creates or overwrites the key."""
    MetastoreStateBackend().set(AssetScope(asset_id=asset_id), key, body.value, session=session)


@asset_state_router.delete(
    "/{key}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="DELETE"))],
)
def delete_asset_state(
    asset_id: int,
    key: str,
    session: SessionDep,
) -> None:
    """Delete a single asset state key. No-op if the key does not exist."""
    MetastoreStateBackend().delete(AssetScope(asset_id=asset_id), key, session=session)


@asset_state_router.delete(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="DELETE"))],
)
def clear_asset_state(
    asset_id: int,
    session: SessionDep,
) -> None:
    """Delete all state keys for an asset."""
    MetastoreStateBackend().clear(AssetScope(asset_id=asset_id), session=session)
