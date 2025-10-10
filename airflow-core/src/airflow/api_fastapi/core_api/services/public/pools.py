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

from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionOnExistence,
    BulkActionResponse,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.pools import (
    PoolBody,
)
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.models.pool import Pool


class BulkPoolService(BulkService[PoolBody]):
    """Service for handling bulk operations on pools."""

    def categorize_pools(self, pool_names: set) -> tuple[dict, set, set]:
        """
        Categorize the given pool_names into matched_pool_names and not_found_pool_names based on existing pool_names.

        Existing pools are returned as a dict of {pool_name : Pool}.

        :param pool_names: set of pool_names
        :return: tuple of dict of existed pools, set of matched pool_names, set of not found pool_names
        """
        existed_pools = self.session.execute(select(Pool).filter(Pool.pool.in_(pool_names))).scalars()
        existing_pools_dict = {pool.pool: pool for pool in existed_pools}
        matched_pool_names = set(existing_pools_dict.keys())
        not_found_pool_names = pool_names - matched_pool_names
        return existing_pools_dict, matched_pool_names, not_found_pool_names

    def handle_bulk_create(self, action: BulkCreateAction[PoolBody], results: BulkActionResponse) -> None:
        """Bulk create pools."""
        to_create_pool_names = {pool.pool for pool in action.entities}
        existing_pools_dict, matched_pool_names, not_found_pool_names = self.categorize_pools(
            to_create_pool_names
        )
        try:
            if action.action_on_existence == BulkActionOnExistence.FAIL and matched_pool_names:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"The pools with these pool names: {matched_pool_names} already exist.",
                )
            if action.action_on_existence == BulkActionOnExistence.SKIP:
                create_pool_names = not_found_pool_names
            else:
                create_pool_names = to_create_pool_names

            for pool in action.entities:
                if pool.pool in create_pool_names:
                    if pool.pool in matched_pool_names:
                        existed_pool = existing_pools_dict[pool.pool]
                        for key, val in pool.model_dump().items():
                            setattr(existed_pool, key, val)
                    else:
                        self.session.add(Pool(**pool.model_dump()))
                    results.success.append(pool.pool)

            self.session.flush()

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

    def handle_bulk_update(self, action: BulkUpdateAction[PoolBody], results: BulkActionResponse) -> None:
        """Bulk Update pools."""
        to_update_pool_names = {pool.pool for pool in action.entities}
        _, matched_pool_names, not_found_pool_names = self.categorize_pools(to_update_pool_names)

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_pool_names:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The pools with these pool names: {not_found_pool_names} were not found.",
                )
            if action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                update_pool_names = matched_pool_names
            else:
                update_pool_names = to_update_pool_names

            for pool in action.entities:
                if pool.pool in update_pool_names:
                    old_pool = self.session.scalar(select(Pool).filter(Pool.pool == pool.pool).limit(1))

                    data = {
                        key: val for key, val in pool.model_dump(by_alias=True).items() if val is not None
                    }
                    try:
                        PoolBody(**data)

                        for key, val in data.items():
                            setattr(old_pool, key, val)

                        results.success.append(str(pool.pool))

                    except ValidationError as e:
                        results.errors.append({"error": f"{e.errors()}"})

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

    def handle_bulk_delete(self, action: BulkDeleteAction[PoolBody], results: BulkActionResponse) -> None:
        """Bulk delete pools."""
        to_delete_pool_names = set(action.entities)
        _, matched_pool_names, not_found_pool_names = self.categorize_pools(to_delete_pool_names)

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_pool_names:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The pools with these pool names: {not_found_pool_names} were not found.",
                )
            if action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                delete_pool_names = matched_pool_names
            else:
                delete_pool_names = to_delete_pool_names

            for pool_name in delete_pool_names:
                existing_pool = self.session.scalar(select(Pool).where(Pool.pool == pool_name).limit(1))
                if existing_pool:
                    self.session.delete(existing_pool)
                    results.success.append(pool_name)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})
