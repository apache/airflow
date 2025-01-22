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

from fastapi import HTTPException, status
from pydantic import ValidationError
from sqlalchemy import select

from airflow.api_fastapi.core_api.datamodels.common import BulkActionNotOnExistence, BulkActionOnExistence
from airflow.api_fastapi.core_api.datamodels.pools import (
    PoolBulkActionResponse,
    PoolBulkCreateAction,
    PoolBulkDeleteAction,
    PoolBulkUpdateAction,
    PoolPatchBody,
)
from airflow.models.pool import Pool


def categorize_pools(session, pool_names: set) -> tuple[dict, set, set]:
    """
    Categorize the given pool_names into matched_pool_names and not_found_pool_names based on existing pool_names.

    Existed pools are returned as a dict of {pool_names : Pool}.

    :param session: SQLAlchemy session
    :param pool_names: set of pool_names
    :return: tuple of dict of existed pools, set of matched pool_names, set of not found pool_names
    """
    existed_pools = session.execute(select(Pool).filter(Pool.pool.in_(pool_names))).scalars()
    existed_pools_dict = {pool.pool: pool for pool in existed_pools}
    matched_pool_names = set(existed_pools_dict.keys())
    not_found_pool_names = pool_names - matched_pool_names
    return existed_pools_dict, matched_pool_names, not_found_pool_names


def handle_bulk_create(session, action: PoolBulkCreateAction, results: PoolBulkActionResponse) -> None:
    """Bulk create pools."""
    to_create_pool_names = {pool.pool for pool in action.pools}
    existed_pools_dict, matched_pool_names, not_found_pool_names = categorize_pools(
        session, to_create_pool_names
    )
    try:
        if action.action_on_existence == BulkActionOnExistence.FAIL and matched_pool_names:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"The pools with these pool names: {matched_pool_names} already exist.",
            )
        elif action.action_on_existence == BulkActionOnExistence.SKIP:
            create_pool_names = not_found_pool_names
        else:
            create_pool_names = to_create_pool_names

        for pool in action.pools:
            if pool.pool in create_pool_names:
                if pool.pool in matched_pool_names:
                    existed_pool = existed_pools_dict[pool.pool]
                    for key, val in pool.model_dump().items():
                        setattr(existed_pool, key, val)
                else:
                    Pool(**pool.model_dump())
                results.success.append(pool.pool)

    except HTTPException as e:
        results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})


def handle_bulk_update(session, action: PoolBulkUpdateAction, results: PoolBulkActionResponse) -> None:
    """Bulk Update pools."""
    to_update_pool_names = {pool.name for pool in action.pools}
    _, matched_pool_names, not_found_pool_names = categorize_pools(session, to_update_pool_names)

    try:
        if action.action_not_on_existence == BulkActionNotOnExistence.FAIL and not_found_pool_names:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"The pools with these pool names: {not_found_pool_names} were not found.",
            )
        elif action.action_not_on_existence == BulkActionNotOnExistence.SKIP:
            update_pool_names = matched_pool_names
        else:
            update_pool_names = to_update_pool_names

        for pool in action.pools:
            if pool.name in update_pool_names:
                old_pool = session.scalar(select(Pool).filter(Pool.pool == pool.name).limit(1))
                PoolPatchBody(**pool.model_dump())
                for key, val in pool.model_dump().items():
                    setattr(old_pool, key, val)
                results.success.append(str(pool.name))

    except HTTPException as e:
        results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

    except ValidationError as e:
        results.errors.append({"error": f"{e.errors()}"})


def handle_bulk_delete(session, action: PoolBulkDeleteAction, results: PoolBulkActionResponse) -> None:
    """Bulk delete pools."""
    to_delete_pool_names = set(action.pool_names)
    _, matched_pool_names, not_found_pool_names = categorize_pools(session, to_delete_pool_names)

    try:
        if action.action_not_on_existence == BulkActionNotOnExistence.FAIL and not_found_pool_names:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"The pools with these pool names: {not_found_pool_names} were not found.",
            )
        elif action.action_not_on_existence == BulkActionNotOnExistence.SKIP:
            delete_pool_names = matched_pool_names
        else:
            delete_pool_names = to_delete_pool_names

        for pool_name in delete_pool_names:
            existing_pool = session.scalar(select(Pool).where(Pool.pool == pool_name).limit(1))
            if existing_pool:
                session.delete(existing_pool)
                results.success.append(pool_name)

    except HTTPException as e:
        results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})
