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

from typing import Annotated

from fastapi import Depends, HTTPException, Query, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from sqlalchemy import delete, select

from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryLimit,
    QueryOffset,
    QueryPoolNamePatternSearch,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.common import BulkBody, BulkResponse
from airflow.api_fastapi.core_api.datamodels.pools import (
    BasePool,
    PoolBody,
    PoolCollectionResponse,
    PoolPatchBody,
    PoolResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import requires_access_pool
from airflow.api_fastapi.core_api.services.public.pools import BulkPoolService
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.models.pool import Pool

pools_router = AirflowRouter(tags=["Pool"], prefix="/pools")


@pools_router.delete(
    "/{pool_name}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_pool(method="DELETE")), Depends(action_logging())],
)
def delete_pool(
    pool_name: str,
    session: SessionDep,
):
    """Delete a pool entry."""
    if pool_name == "default_pool":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Default Pool can't be deleted")

    affected_count = session.execute(delete(Pool).where(Pool.pool == pool_name)).rowcount

    if affected_count == 0:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Pool with name: `{pool_name}` was not found")


@pools_router.get(
    "/{pool_name}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_pool(method="GET"))],
)
def get_pool(
    pool_name: str,
    session: SessionDep,
) -> PoolResponse:
    """Get a pool."""
    pool = session.scalar(select(Pool).where(Pool.pool == pool_name))
    if pool is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"The Pool with name: `{pool_name}` was not found")

    return pool


@pools_router.get(
    "",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_pool(method="GET"))],
)
def get_pools(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id", "pool"], Pool, to_replace={"name": "pool"}).dynamic_depends()),
    ],
    pool_name_pattern: QueryPoolNamePatternSearch,
    session: SessionDep,
) -> PoolCollectionResponse:
    """Get all pools entries."""
    pools_select, total_entries = paginated_select(
        statement=select(Pool),
        filters=[pool_name_pattern],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    pools = session.scalars(pools_select)

    return PoolCollectionResponse(
        pools=pools,
        total_entries=total_entries,
    )


@pools_router.patch(
    "/{pool_name}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_pool(method="PUT")), Depends(action_logging())],
)
def patch_pool(
    pool_name: str,
    patch_body: PoolPatchBody,
    session: SessionDep,
    update_mask: list[str] | None = Query(None),
) -> PoolResponse:
    """Update a Pool."""
    if patch_body.name and patch_body.name != pool_name:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Invalid body, pool name from request body doesn't match uri parameter",
        )
    # Only slots and include_deferred can be modified in 'default_pool'
    if pool_name == Pool.DEFAULT_POOL_NAME:
        if update_mask and all(mask.strip() in {"slots", "include_deferred"} for mask in update_mask):
            pass
        else:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "Only slots and included_deferred can be modified on Default Pool",
            )
    pool = session.scalar(select(Pool).where(Pool.pool == pool_name).limit(1))
    if not pool:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, detail=f"The Pool with name: `{pool_name}` was not found"
        )

    fields_to_update = patch_body.model_fields_set
    if update_mask:
        fields_to_update = fields_to_update.intersection(update_mask)
        data = patch_body.model_dump(include=fields_to_update, by_alias=True)
    else:
        data = patch_body.model_dump(include=fields_to_update, by_alias=True)
        try:
            BasePool.model_validate(data)
        except ValidationError as e:
            raise RequestValidationError(errors=e.errors())

    for key, value in data.items():
        setattr(pool, key, value)

    return pool


@pools_router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [status.HTTP_409_CONFLICT]
    ),  # handled by global exception handler
    dependencies=[Depends(requires_access_pool(method="POST")), Depends(action_logging())],
)
def post_pool(
    body: PoolBody,
    session: SessionDep,
) -> PoolResponse:
    """Create a Pool."""
    pool = Pool(**body.model_dump())
    session.add(pool)
    return pool


@pools_router.patch(
    "",
    dependencies=[Depends(requires_access_pool(method="PUT")), Depends(action_logging())],
)
def bulk_pools(
    request: BulkBody[PoolBody],
    session: SessionDep,
) -> BulkResponse:
    """Bulk create, update, and delete pools."""
    return BulkPoolService(session=session, request=request).handle_request()
