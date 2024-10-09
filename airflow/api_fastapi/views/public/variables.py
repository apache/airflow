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

from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.db.common import get_session
from airflow.api_fastapi.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.views.router import AirflowRouter
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
