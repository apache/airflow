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

from typing import cast

from fastapi import HTTPException, status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.datamodels.common import (
    BulkActionNotOnExistence,
    BulkActionOnExistence,
    BulkActionResponse,
    BulkCreateAction,
    BulkDeleteAction,
    BulkUpdateAction,
)
from airflow.api_fastapi.core_api.datamodels.pools import (
    BasePool,
    PoolBody,
    PoolPatchBody,
)
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.models.pool import Pool


def update_orm_from_pydantic(
    pool_name: str,
    patch_body: PoolBody | PoolPatchBody,
    update_mask: list[str] | None,
    session: SessionDep,
) -> Pool:
    """
    Update an existing pool.

    :param pool_name: The name of the existing Pool to be updated.
    :param patch_body: Pydantic model containing the fields to update.
    :param update_mask: Specific fields to update. If None, all provided fields will be considered.
    :param session: The database session dependency.
    :return: The updated Pool instance.
    :raises HTTPException: If attempting to update disallowed fields on ``default_pool``.
    """
    # Special restriction: default pool only allows limited fields to be patched
    pool = session.scalar(select(Pool).where(Pool.pool == pool_name).limit(1))
    if not pool:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, detail=f"The Pool with name: `{pool_name}` was not found"
        )
    if pool_name == Pool.DEFAULT_POOL_NAME:
        if update_mask and all(mask.strip() in {"slots", "include_deferred"} for mask in update_mask):
            # Validate only slots/include_deferred
            try:
                patch_body_subset = patch_body.model_dump(
                    include={"slots", "include_deferred"}, exclude_unset=True, by_alias=True
                )
                # Re-run validation with BasePool but only on allowed fields
                PoolPatchBody.model_validate(patch_body_subset)
            except ValidationError as e:
                raise RequestValidationError(errors=e.errors())
        else:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "Only slots and included_deferred can be modified on Default Pool",
            )
    else:
        fields_to_update = patch_body.model_fields_set
        try:
            # Dump with both input + output aliases handled
            body_dict = patch_body.model_dump(
                include=fields_to_update,
                by_alias=True,  # ensures we get the API-facing alias keys
            )

            # Normalize keys for BasePool (expects "pool")
            if "name" in body_dict and "pool" not in body_dict:
                body_dict["pool"] = body_dict.pop("name")

            BasePool.model_validate(body_dict)

        except ValidationError as e:
            raise RequestValidationError(errors=e.errors())

    # Delegate patch application to the common utility
    return cast(
        "Pool",
        BulkService.apply_patch_with_update_mask(
            model=pool,
            patch_body=patch_body,
            update_mask=update_mask,
            non_update_fields=None,
        ),
    )


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
                if pool.pool not in update_pool_names:
                    continue

                updated_pool = update_orm_from_pydantic(pool.pool, pool, action.update_mask, self.session)

                results.success.append(str(updated_pool.pool))  # use request field, always consistent

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
