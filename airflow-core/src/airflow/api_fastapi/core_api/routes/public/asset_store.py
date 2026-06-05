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

import json
from typing import Annotated

from fastapi import Depends, HTTPException, status
from sqlalchemy import select

from airflow._shared.state import AssetScope, AssetStoreWriterKind
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.asset_store import (
    AssetStoreBody,
    AssetStoreCollectionResponse,
    AssetStoreLastUpdatedBy,
    AssetStoreResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_asset
from airflow.models.asset import AssetModel
from airflow.models.asset_store import AssetStoreModel
from airflow.state.metastore import _get_db_backend

asset_store_router = AirflowRouter(
    tags=["Asset Store"],
    prefix="/assets/{asset_id}/store",
)


def _get_asset_or_404(asset_id: int, session: SessionDep) -> int:
    exists = session.scalar(select(AssetModel.id).where(AssetModel.id == asset_id))
    if exists is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Asset with id {asset_id!r} not found",
        )
    return asset_id


AssetIdDep = Annotated[int, Depends(_get_asset_or_404)]


@asset_store_router.get(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="GET"))],
)
def list_asset_store(
    asset_id: AssetIdDep,
    limit: QueryLimit,
    offset: QueryOffset,
    session: SessionDep,
) -> AssetStoreCollectionResponse:
    """List all store entries for an asset."""
    base = (
        select(
            AssetStoreModel.key,
            AssetStoreModel.value,
            AssetStoreModel.updated_at,
            AssetStoreModel.last_updated_by_kind,
            AssetStoreModel.last_updated_by_dag_id,
            AssetStoreModel.last_updated_by_run_id,
            AssetStoreModel.last_updated_by_task_id,
            AssetStoreModel.last_updated_by_map_index,
        )
        .where(AssetStoreModel.asset_id == asset_id)
        .order_by(AssetStoreModel.key.asc())
    )
    paginated, total_entries = paginated_select(
        statement=base,
        filters=None,
        order_by=None,
        offset=offset,
        limit=limit,
        session=session,
    )
    rows = session.execute(paginated).all()
    entries = [
        AssetStoreResponse(
            key=r.key,
            value=json.loads(r.value),
            updated_at=r.updated_at,
            last_updated_by=AssetStoreLastUpdatedBy(
                kind=r.last_updated_by_kind,
                dag_id=r.last_updated_by_dag_id,
                run_id=r.last_updated_by_run_id,
                task_id=r.last_updated_by_task_id,
                map_index=r.last_updated_by_map_index,
            )
            if r.last_updated_by_kind is not None
            else None,
        )
        for r in rows
    ]
    return AssetStoreCollectionResponse(asset_store=entries, total_entries=total_entries)


@asset_store_router.get(
    "/{key:path}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="GET"))],
)
def get_asset_store(
    asset_id: AssetIdDep,
    key: str,
    session: SessionDep,
) -> AssetStoreResponse:
    """Get a single asset store entry."""
    row = session.execute(
        select(
            AssetStoreModel.key,
            AssetStoreModel.value,
            AssetStoreModel.updated_at,
            AssetStoreModel.last_updated_by_kind,
            AssetStoreModel.last_updated_by_dag_id,
            AssetStoreModel.last_updated_by_run_id,
            AssetStoreModel.last_updated_by_task_id,
            AssetStoreModel.last_updated_by_map_index,
        ).where(
            AssetStoreModel.asset_id == asset_id,
            AssetStoreModel.key == key,
        )
    ).one_or_none()
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Asset store key {key!r} not found",
        )
    return AssetStoreResponse(
        key=row.key,
        value=json.loads(row.value),
        updated_at=row.updated_at,
        last_updated_by=AssetStoreLastUpdatedBy(
            kind=row.last_updated_by_kind,
            dag_id=row.last_updated_by_dag_id,
            run_id=row.last_updated_by_run_id,
            task_id=row.last_updated_by_task_id,
            map_index=row.last_updated_by_map_index,
        )
        if row.last_updated_by_kind is not None
        else None,
    )


@asset_store_router.put(
    "/{key:path}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="PUT"))],
)
def set_asset_store(
    asset_id: AssetIdDep,
    key: str,
    body: AssetStoreBody,
    session: SessionDep,
) -> None:
    """Set an asset store value. Creates or overwrites the key."""
    _get_db_backend().set_asset_store(
        AssetScope(asset_id=asset_id),
        key,
        json.dumps(body.value),
        kind=AssetStoreWriterKind.API,
        session=session,
    )


@asset_store_router.delete(
    "/{key:path}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="DELETE"))],
)
def delete_asset_store(
    asset_id: AssetIdDep,
    key: str,
    session: SessionDep,
) -> None:
    """Delete a single asset store key. No-op if the key does not exist."""
    _get_db_backend().delete(AssetScope(asset_id=asset_id), key, session=session)


@asset_store_router.delete(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_asset(method="DELETE"))],
)
def clear_asset_store(
    asset_id: AssetIdDep,
    session: SessionDep,
) -> None:
    """Delete all store keys for an asset."""
    _get_db_backend().clear(AssetScope(asset_id=asset_id), session=session)
