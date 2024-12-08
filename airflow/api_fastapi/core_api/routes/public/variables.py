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
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset, SortParam
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.variables import (
    VariableBody,
    VariableCollectionResponse,
    VariableResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.models.variable import Variable

variables_router = AirflowRouter(tags=["Variable"], prefix="/variables")


@variables_router.delete(
    "/{variable_key}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def delete_variable(
    variable_key: str,
    session: SessionDep,
):
    """Delete a variable entry."""
    if Variable.delete(variable_key, session) == 0:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"The Variable with key: `{variable_key}` was not found"
        )


@variables_router.get(
    "/{variable_key}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
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
)
def get_variables(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["key", "id", "_val"],
                Variable,
            ).dynamic_depends()
        ),
    ],
    session: SessionDep,
) -> VariableCollectionResponse:
    """Get all Variables entries."""
    variable_select, total_entries = paginated_select(
        statement=select(Variable),
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
    "/{variable_key}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def patch_variable(
    variable_key: str,
    patch_body: VariableBody,
    session: SessionDep,
    update_mask: list[str] | None = Query(None),
) -> VariableResponse:
    """Update a variable by key."""
    if patch_body.key != variable_key:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "Invalid body, key from request body doesn't match uri parameter"
        )
    non_update_fields = {"key"}
    variable = session.scalar(select(Variable).filter_by(key=variable_key).limit(1))
    if not variable:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"The Variable with key: `{variable_key}` was not found"
        )
    if update_mask:
        data = patch_body.model_dump(
            include=set(update_mask) - non_update_fields, by_alias=True, exclude_none=True
        )
    else:
        data = patch_body.model_dump(exclude=non_update_fields, by_alias=True, exclude_none=True)
    for key, val in data.items():
        setattr(variable, key, val)
    return variable


@variables_router.post(
    "",
    status_code=status.HTTP_201_CREATED,
)
def post_variable(
    post_body: VariableBody,
    session: SessionDep,
) -> VariableResponse:
    """Create a variable."""
    Variable.set(**post_body.model_dump(), session=session)

    variable = session.scalar(select(Variable).where(Variable.key == post_body.key).limit(1))

    return variable
