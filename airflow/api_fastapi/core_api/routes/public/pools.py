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
from sqlalchemy import delete
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
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
