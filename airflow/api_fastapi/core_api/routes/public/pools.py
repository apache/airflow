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

from fastapi import Depends, HTTPException, Query
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import get_session, paginated_select
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset, SortParam
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import (
    create_openapi_http_exception_doc,
)
from airflow.api_fastapi.core_api.serializers.pools import (
    BasePool,
    PoolCollectionResponse,
    PoolPatchBody,
    PoolPostBody,
    PoolResponse,
)
from airflow.models.pool import Pool

pools_router = AirflowRouter(tags=["Pool"], prefix="/pools")


@pools_router.delete(
    "/{pool_name}",
    status_code=204,
    responses=create_openapi_http_exception_doc([400, 401, 403, 404]),
)
async def delete_pool(
    pool_name: str,
    session: Annotated[Session, Depends(get_session)],
):
    """Delete a pool entry."""
    if pool_name == "default_pool":
        raise HTTPException(400, "Default Pool can't be deleted")

    affected_count = session.execute(delete(Pool).where(Pool.pool == pool_name)).rowcount

    if affected_count == 0:
        raise HTTPException(404, f"The Pool with name: `{pool_name}` was not found")


@pools_router.get(
    "/{pool_name}",
    responses=create_openapi_http_exception_doc([401, 403, 404]),
)
async def get_pool(
    pool_name: str,
    session: Annotated[Session, Depends(get_session)],
) -> PoolResponse:
    """Get a pool."""
    pool = session.scalar(select(Pool).where(Pool.pool == pool_name))
    if pool is None:
        raise HTTPException(404, f"The Pool with name: `{pool_name}` was not found")

    return PoolResponse.model_validate(pool, from_attributes=True)


@pools_router.get(
    "/",
    responses=create_openapi_http_exception_doc([401, 403, 404]),
)
async def get_pools(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["id", "name"], Pool).dynamic_depends()),
    ],
    session: Annotated[Session, Depends(get_session)],
) -> PoolCollectionResponse:
    """Get all pools entries."""
    pools_select, total_entries = paginated_select(
        select(Pool),
        [],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    pools = session.scalars(pools_select).all()

    return PoolCollectionResponse(
        pools=[PoolResponse.model_validate(pool, from_attributes=True) for pool in pools],
        total_entries=total_entries,
    )


@pools_router.patch(
    "/{pool_name}", responses=create_openapi_http_exception_doc([400, 401, 403, 404])
)
async def patch_pool(
    pool_name: str,
    patch_body: PoolPatchBody,
    session: Annotated[Session, Depends(get_session)],
    update_mask: list[str] | None = Query(None),
) -> PoolResponse:
    """Update a Pool."""
    # Only slots and include_deferred can be modified in 'default_pool'
    if pool_name == Pool.DEFAULT_POOL_NAME:
        if update_mask and all(
            mask.strip() in {"slots", "include_deferred"} for mask in update_mask
        ):
            pass
        else:
            raise HTTPException(
                400, "Only slots and included_deferred can be modified on Default Pool"
            )

    pool = session.scalar(select(Pool).where(Pool.pool == pool_name).limit(1))
    if not pool:
        raise HTTPException(
            404, detail=f"The Pool with name: `{pool_name}` was not found"
        )

    if update_mask:
        data = patch_body.model_dump(include=set(update_mask), by_alias=True)
    else:
        data = patch_body.model_dump(by_alias=True)
        try:
            BasePool.model_validate(data)
        except ValidationError as e:
            raise RequestValidationError(errors=e.errors())

    for key, value in data.items():
        setattr(pool, key, value)

    return PoolResponse.model_validate(pool, from_attributes=True)


@pools_router.post(
    "/", status_code=201, responses=create_openapi_http_exception_doc([401, 403])
)
async def post_pool(
    post_body: PoolPostBody,
    session: Annotated[Session, Depends(get_session)],
) -> PoolResponse:
    """Create a Pool."""
    pool = Pool(**post_body.model_dump())

    session.add(pool)

    return PoolResponse.model_validate(pool, from_attributes=True)
