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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Annotated

from fastapi import Depends, HTTPException, status
from sqlalchemy import select

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.batch_apis import IsAuthorizedDagRequest
from airflow.api_fastapi.auth.managers.models.resource_details import (
    DagDetails,
)
from airflow.api_fastapi.common.db.common import (
    SessionDep,
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
from airflow.api_fastapi.core_api.security import (
    AccessView,
    GetUserDep,
    requires_access_dag,
    requires_access_view,
)
from airflow.models import DagModel
from airflow.models.errors import ParseImportError

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

import_error_router = AirflowRouter(tags=["Import Error"], prefix="/importErrors")


def get_file_dag_ids(session: Session, filename: str) -> set[str]:
    return {dag_id for dag_id in session.scalars(select(DagModel.dag_id).where(DagModel.fileloc == filename))}


@import_error_router.get(
    "/{import_error_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_403_FORBIDDEN, status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_view(AccessView.IMPORT_ERRORS)),
        Depends(requires_access_dag("GET")),
    ],
)
def get_import_error(
    import_error_id: int,
    session: SessionDep,
    user: GetUserDep,
) -> ImportErrorResponse:
    """Get an import error."""
    error = session.scalar(select(ParseImportError).where(ParseImportError.id == import_error_id))
    if error is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The ImportError with import_error_id: `{import_error_id}` was not found",
        )
    session.expunge(error)

    auth_manager = get_auth_manager()
    can_read_all_dags = auth_manager.is_authorized_dag(method="GET", user=user)
    if not can_read_all_dags:
        readable_dag_ids = auth_manager.get_permitted_dag_ids(user=user)
        file_dag_ids = get_file_dag_ids(session, error.filename)

        # Can the user read any DAGs in the file?
        if not readable_dag_ids.intersection(file_dag_ids):
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                "You do not have read permission on any of the DAGs in the file",
            )

        # Check if user has read access to all the DAGs defined in the file
        if not file_dag_ids.issubset(readable_dag_ids):
            error.stacktrace = "REDACTED - you do not have read permission on all DAGs in the file"

    return error


@import_error_router.get(
    "",
    dependencies=[
        Depends(requires_access_view(AccessView.IMPORT_ERRORS)),
        Depends(requires_access_dag("GET")),
    ],
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
                    "timestamp",
                    "filename",
                    "bundle_name",
                    "stacktrace",
                ],
                ParseImportError,
                {"import_error_id": "id"},
            ).dynamic_depends()
        ),
    ],
    session: SessionDep,
    user: GetUserDep,
) -> ImportErrorCollectionResponse:
    """Get all import errors."""
    statement = select(ParseImportError)
    import_errors_select, total_entries = paginated_select(
        statement=statement,
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    auth_manager = get_auth_manager()
    can_read_all_dags = auth_manager.is_authorized_dag(method="GET", user=user)
    if not can_read_all_dags:
        # if the user doesn't have access to all DAGs, only display errors from visible DAGs
        readable_dag_ids = auth_manager.get_permitted_dag_ids(method="GET", user=user)
        dagfiles_stmt = select(DagModel.fileloc).distinct().where(DagModel.dag_id.in_(readable_dag_ids))
        statement = statement.where(ParseImportError.filename.in_(dagfiles_stmt))
        import_errors_select, total_entries = paginated_select(
            statement=statement,
            order_by=order_by,
            offset=offset,
            limit=limit,
            session=session,
        )

    import_errors = session.scalars(import_errors_select)
    if not can_read_all_dags:
        for import_error in import_errors:
            # Check if user has read access to all the DAGs defined in the file
            file_dag_ids = get_file_dag_ids(session, import_error.filename)
            requests: Sequence[IsAuthorizedDagRequest] = [
                {
                    "method": "GET",
                    "details": DagDetails(id=dag_id),
                }
                for dag_id in file_dag_ids
            ]
            if not auth_manager.batch_is_authorized_dag(requests, user=user):
                session.expunge(import_error)
                import_error.stacktrace = "REDACTED - you do not have read permission on all DAGs in the file"

    return ImportErrorCollectionResponse(
        import_errors=import_errors,
        total_entries=total_entries,
    )
