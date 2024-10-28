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
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import get_session, paginated_select
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset, SortParam
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import (
    create_openapi_http_exception_doc,
)
from airflow.api_fastapi.core_api.serializers.variables import (
    VariableBody,
    VariableCollectionResponse,
    VariableResponse,
)
from airflow.models.variable import Variable

variables_router = AirflowRouter(tags=["Variable"], prefix="/variables")


@variables_router.delete(
    "/{variable_key}",
    status_code=204,
    responses=create_openapi_http_exception_doc([401, 403, 404]),
)
async def delete_variable(
    variable_key: str,
    session: Annotated[Session, Depends(get_session)],
):
    """Delete a variable entry."""
    if Variable.delete(variable_key, session) == 0:
        raise HTTPException(404, f"The Variable with key: `{variable_key}` was not found")


@variables_router.get(
    "/{variable_key}", responses=create_openapi_http_exception_doc([401, 403, 404])
)
async def get_variable(
    variable_key: str,
    session: Annotated[Session, Depends(get_session)],
) -> VariableResponse:
    """Get a variable entry."""
    variable = session.scalar(
        select(Variable).where(Variable.key == variable_key).limit(1)
    )

    if variable is None:
        raise HTTPException(404, f"The Variable with key: `{variable_key}` was not found")

    return VariableResponse.model_validate(variable, from_attributes=True)


@variables_router.get(
    "/",
    responses=create_openapi_http_exception_doc([401, 403]),
)
async def get_variables(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["key", "id"],
                Variable,
            ).dynamic_depends()
        ),
    ],
    session: Annotated[Session, Depends(get_session)],
) -> VariableCollectionResponse:
    """Get all Variables entries."""
    variable_select, total_entries = paginated_select(
        select(Variable),
        [],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    variables = session.scalars(variable_select).all()

    return VariableCollectionResponse(
        variables=[
            VariableResponse.model_validate(variable, from_attributes=True)
            for variable in variables
        ],
        total_entries=total_entries,
    )


@variables_router.patch(
    "/{variable_key}", responses=create_openapi_http_exception_doc([400, 401, 403, 404])
)
async def patch_variable(
    variable_key: str,
    patch_body: VariableBody,
    session: Annotated[Session, Depends(get_session)],
    update_mask: list[str] | None = Query(None),
) -> VariableResponse:
    """Update a variable by key."""
    if patch_body.key != variable_key:
        raise HTTPException(
            400, "Invalid body, key from request body doesn't match uri parameter"
        )
    non_update_fields = {"key"}
    variable = session.scalar(select(Variable).filter_by(key=variable_key).limit(1))
    if not variable:
        raise HTTPException(404, f"The Variable with key: `{variable_key}` was not found")
    if update_mask:
        data = patch_body.model_dump(include=set(update_mask) - non_update_fields)
    else:
        data = patch_body.model_dump(exclude=non_update_fields)
    for key, val in data.items():
        setattr(variable, key, val)
    return variable


@variables_router.post(
    "/", status_code=201, responses=create_openapi_http_exception_doc([401, 403])
)
async def post_variable(
    post_body: VariableBody,
    session: Annotated[Session, Depends(get_session)],
) -> VariableResponse:
    """Create a variable."""
    Variable.set(**post_body.model_dump(), session=session)

    variable = session.scalar(
        select(Variable).where(Variable.key == post_body.key).limit(1)
    )

    return VariableResponse.model_validate(variable, from_attributes=True)
