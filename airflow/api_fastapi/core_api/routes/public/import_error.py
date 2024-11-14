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

from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow.api_fastapi.common.db.common import (
    get_session,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import (
    QueryLimit,
    QueryOffset,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.import_error import (
    ImportErrorCollectionResponse,
    ImportErrorResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.models.errors import ParseImportError

import_error_router = AirflowRouter(tags=["Import Error"], prefix="/importErrors")


@import_error_router.get(
    "/{import_error_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
)
def get_import_error(
    import_error_id: int,
    session: Annotated[Session, Depends(get_session)],
) -> ImportErrorResponse:
    """Get an import error."""
    error = session.scalar(select(ParseImportError).where(ParseImportError.id == import_error_id))
    if error is None:
        raise HTTPException(404, f"The ImportError with import_error_id: `{import_error_id}` was not found")

    return ImportErrorResponse.model_validate(
        error,
        from_attributes=True,
    )


@import_error_router.get(
    "/",
)
def get_import_errors(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "id",
                    "import_error_id",
                    "timestamp",
                    "filename",
                    "stacktrace",
                ],
                ParseImportError,
            ).dynamic_depends()
        ),
    ],
    session: Annotated[Session, Depends(get_session)],
) -> ImportErrorCollectionResponse:
    """Get all import errors."""
    import_errors_select, total_entries = paginated_select(
        select(ParseImportError),
        [],
        order_by,
        offset,
        limit,
        session,
    )
    import_errors = session.scalars(import_errors_select).all()

    return ImportErrorCollectionResponse(
        import_errors=[
            ImportErrorResponse.model_validate(error, from_attributes=True) for error in import_errors
        ],
        total_entries=total_entries,
    )
