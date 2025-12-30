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
from sqlalchemy import delete, select

from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryLimit,
    QueryOffset,
    QueryVariableKeyPatternSearch,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.common import BulkBody, BulkResponse
from airflow.api_fastapi.core_api.datamodels.variables import (
    VariableBody,
    VariableCollectionResponse,
    VariableResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    ReadableVariablesFilterDep,
    requires_access_variable,
    requires_access_variable_bulk,
)
from airflow.api_fastapi.core_api.services.public.variables import (
    BulkVariableService,
    update_orm_from_pydantic,
)
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.models.variable import Variable
MAX_PUBLIC_API_LIMIT = 100

variables_router = AirflowRouter(tags=["Variable"], prefix="/variables")


@variables_router.delete(
    "/{variable_key:path}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(action_logging()), Depends(requires_access_variable("DELETE"))],
)
def delete_variable(
    variable_key: str,
    session: SessionDep,
):
    """Delete a variable entry."""
    # Like the other endpoints (get, patch), we do not use Variable.delete/get/set here because these methods
    # are intended to be used in task execution environment (execution API)
    result = session.execute(delete(Variable).where(Variable.key == variable_key))
    rows = getattr(result, "rowcount", 0) or 0
    if rows == 0:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"The Variable with key: `{variable_key}` was not found"
        )


@variables_router.get(
    "/{variable_key:path}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_variable("GET"))],
)
def get_variable(
    variable_key: str,
    session: SessionDep,
) -> VariableResponse:
    """Get a variable entry."""
    variable = session.scalar(select(Variable).where(Variable.key == variable_key).limit(1))

    if variable is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"The Variable with key: `{variable_key}` was not found"
        )

    return variable


@variables_router.get(
    "",
    dependencies=[Depends(requires_access_variable("GET"))],
)
def get_variables(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["key", "id", "_val", "description", "is_encrypted"],
                Variable,
            ).dynamic_depends()
        ),
    ],
    readable_variables_filter: ReadableVariablesFilterDep,
    session: SessionDep,
    variable_key_pattern: QueryVariableKeyPatternSearch,
) -> VariableCollectionResponse:
    """Get all Variables entries."""
    
    if limit.value is not None and limit.value > MAX_PUBLIC_API_LIMIT:
        limit.value = MAX_PUBLIC_API_LIMIT


    variable_select, total_entries = paginated_select(
        statement=select(Variable),
        filters=[variable_key_pattern, readable_variables_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    variables = session.scalars(variable_select)

    return VariableCollectionResponse(
        variables=variables,
        total_entries=total_entries,
    )


@variables_router.patch(
    "/{variable_key:path}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(action_logging()), Depends(requires_access_variable("PUT"))],
)
def patch_variable(
    variable_key: str,
    patch_body: VariableBody,
    session: SessionDep,
    update_mask: list[str] | None = Query(None),
) -> VariableResponse:
    """Update a variable by key."""
    variable = update_orm_from_pydantic(variable_key, patch_body, update_mask, session)
    return variable


@variables_router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc([status.HTTP_409_CONFLICT]),
    dependencies=[Depends(action_logging()), Depends(requires_access_variable("POST"))],
)
def post_variable(
    post_body: VariableBody,
    session: SessionDep,
) -> VariableResponse:
    """Create a variable."""
    # Check if the key already exists
    existing_variable = session.scalar(select(Variable).where(Variable.key == post_body.key).limit(1))
    if existing_variable:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"The Variable with key: `{post_body.key}` already exists",
        )

    Variable.set(**post_body.model_dump(), session=session)

    variable = session.scalar(select(Variable).where(Variable.key == post_body.key).limit(1))
    if variable is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Variable with key: `{post_body.key}` was not found",
        )

    return variable


@variables_router.patch(
    "", dependencies=[Depends(action_logging()), Depends(requires_access_variable_bulk())]
)
def bulk_variables(
    request: BulkBody[VariableBody],
    session: SessionDep,
) -> BulkResponse:
    """Bulk create, update, and delete variables."""
    return BulkVariableService(session=session, request=request).handle_request()
