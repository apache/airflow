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
from airflow.api_fastapi.core_api.datamodels.variables import (
    VariableBody,
)
from airflow.api_fastapi.core_api.services.public.common import BulkService
from airflow.models.variable import Variable


def update_orm_from_pydantic(
    variable_key: str, patch_body: VariableBody, update_mask: list[str] | None, session: SessionDep
) -> Variable:
    """
    Update an existing Variable.

    :param variable_key: The name of the existing Variable_key to update.
    :param patch_body: The patch request body containing fields to update.
    :param update_mask: List of fields to update. If None, all provided fields will be updated.
    :param session: The database session dependency.
    :return: The updated Variable object.
    :raises HTTPException: If attempting to update restricted fields (e.g., ``key``).
    """
    # Key field is immutable → cannot be patched

    if patch_body.key != variable_key:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "Invalid body, key from request body doesn't match uri parameter"
        )
    old_variable = session.scalar(select(Variable).filter_by(key=variable_key).limit(1))
    if not old_variable:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"The Variable with key: `{variable_key}` was not found"
        )

    try:
        VariableBody(**patch_body.model_dump())
    except ValidationError as e:
        raise RequestValidationError(errors=e.errors())
    non_update_fields = {"key"}

    if patch_body.key != old_variable.key:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Invalid body, key from request body doesn't match uri parameter",
        )

    # Apply patch via utility
    return cast(
        "Variable",
        BulkService.apply_patch_with_update_mask(
            model=old_variable,
            patch_body=patch_body,
            update_mask=update_mask,
            non_update_fields=non_update_fields,
        ),
    )


class BulkVariableService(BulkService[VariableBody]):
    """Service for handling bulk operations on variables."""

    def categorize_keys(self, keys: set) -> tuple[set, set]:
        """Categorize the given keys into matched_keys and not_found_keys based on existing keys."""
        existing_keys = {variable for variable in self.session.execute(select(Variable.key)).scalars()}
        matched_keys = existing_keys & keys
        not_found_keys = keys - existing_keys
        return matched_keys, not_found_keys

    def handle_bulk_create(self, action: BulkCreateAction, results: BulkActionResponse) -> None:
        """Bulk create variables."""
        to_create_keys = {variable.key for variable in action.entities}
        matched_keys, not_found_keys = self.categorize_keys(to_create_keys)

        try:
            if action.action_on_existence == BulkActionOnExistence.FAIL and matched_keys:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"The variables with these keys: {matched_keys} already exist.",
                )
            if action.action_on_existence == BulkActionOnExistence.SKIP:
                create_keys = not_found_keys
            else:
                create_keys = to_create_keys

            for variable in action.entities:
                if variable.key in create_keys:
                    should_serialize_json = isinstance(variable.value, (dict, list))
                    Variable.set(
                        key=variable.key,
                        value=variable.value,
                        description=variable.description,
                        session=self.session,
                        serialize_json=should_serialize_json,
                    )
                    results.success.append(variable.key)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

    def handle_bulk_update(self, action: BulkUpdateAction, results: BulkActionResponse) -> None:
        """Bulk Update variables."""
        to_update_keys = {variable.key for variable in action.entities}
        matched_keys, not_found_keys = self.categorize_keys(to_update_keys)
        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_keys:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The variables with these keys: {not_found_keys} were not found.",
                )
            if action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                update_keys = matched_keys
            else:
                update_keys = to_update_keys

            for variable in action.entities:
                if variable.key not in update_keys:
                    continue
                updated_variable = update_orm_from_pydantic(
                    variable.key, variable, action.update_mask, self.session
                )

                results.success.append(updated_variable.key)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})

        except ValidationError as e:
            results.errors.append({"error": f"{e.errors()}"})

    def handle_bulk_delete(self, action: BulkDeleteAction, results: BulkActionResponse) -> None:
        """Bulk delete variables."""
        to_delete_keys = set(action.entities)
        matched_keys, not_found_keys = self.categorize_keys(to_delete_keys)

        try:
            if action.action_on_non_existence == BulkActionNotOnExistence.FAIL and not_found_keys:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"The variables with these keys: {not_found_keys} were not found.",
                )
            if action.action_on_non_existence == BulkActionNotOnExistence.SKIP:
                delete_keys = matched_keys
            else:
                delete_keys = to_delete_keys

            for key in delete_keys:
                existing_variable = self.session.scalar(select(Variable).where(Variable.key == key).limit(1))
                if existing_variable:
                    self.session.delete(existing_variable)
                    results.success.append(key)

        except HTTPException as e:
            results.errors.append({"error": f"{e.detail}", "status_code": e.status_code})
