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

from collections.abc import Iterable, Sequence
from itertools import groupby
from operator import itemgetter
from typing import Annotated

from fastapi import Depends, HTTPException, status
from sqlalchemy import and_, or_, select

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
    QueryParseImportErrorFilenamePatternSearch,
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
    requires_access_view,
)
from airflow.models import DagModel
from airflow.models.errors import ParseImportError

REDACTED_STACKTRACE = "REDACTED - you do not have read permission on all DAGs in the file"
import_error_router = AirflowRouter(tags=["Import Error"], prefix="/importErrors")


@import_error_router.get(
    "/{import_error_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[
        Depends(requires_access_view(AccessView.IMPORT_ERRORS)),
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
    readable_dag_ids = auth_manager.get_authorized_dag_ids(user=user)
    # We need file_dag_ids as a set for intersection, issubset operations
    file_dag_ids = set(
        session.scalars(select(DagModel.dag_id).where(DagModel.fileloc == error.filename)).all()
    )

    # No DAGs in the file (failed to parse), nothing to check permissions against
    if not file_dag_ids:
        return error

    # Can the user read any DAGs in the file?
    if not readable_dag_ids.intersection(file_dag_ids):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "You do not have read permission on any of the DAGs in the file",
        )

    # Check if user has read access to all the DAGs defined in the file
    if not file_dag_ids.issubset(readable_dag_ids):
        error.stacktrace = REDACTED_STACKTRACE
    return error


@import_error_router.get(
    "",
    dependencies=[
        Depends(requires_access_view(AccessView.IMPORT_ERRORS)),
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
    filename_pattern: QueryParseImportErrorFilenamePatternSearch,
    session: SessionDep,
    user: GetUserDep,
) -> ImportErrorCollectionResponse:
    """Get all import errors."""
    auth_manager = get_auth_manager()
    readable_dag_ids = auth_manager.get_authorized_dag_ids(method="GET", user=user)

    # Subquery for files that have any DAGs
    files_with_any_dags = select(DagModel.relative_fileloc).distinct().subquery()

    # CTE for DAGs the user can read
    visible_files_cte = (
        select(DagModel.relative_fileloc, DagModel.dag_id, DagModel.bundle_name)
        .where(DagModel.dag_id.in_(readable_dag_ids))
        .cte()
    )

    # Prepare the import errors query by joining with the cte.
    # Each returned row will be a tuple: (ParseImportError, dag_id)
    import_errors_stmt = (
        select(ParseImportError, visible_files_cte.c.dag_id)
        .outerjoin(
            files_with_any_dags,
            ParseImportError.filename == files_with_any_dags.c.relative_fileloc,
        )
        .outerjoin(
            visible_files_cte,
            and_(
                ParseImportError.filename == visible_files_cte.c.relative_fileloc,
                ParseImportError.bundle_name == visible_files_cte.c.bundle_name,
            ),
        )
        .where(
            or_(
                files_with_any_dags.c.relative_fileloc.is_(None),
                visible_files_cte.c.dag_id.isnot(None),
            )
        )
        .order_by(ParseImportError.id)
    )

    # Paginate the import errors query
    import_errors_select, total_entries = paginated_select(
        statement=import_errors_stmt,
        filters=[filename_pattern],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    import_errors_result: Iterable[tuple[ParseImportError, Iterable]] = groupby(
        session.execute(import_errors_select), itemgetter(0)
    )

    import_errors = []
    for import_error, file_dag_ids_iter in import_errors_result:
        dag_ids = [dag_id for _, dag_id in file_dag_ids_iter if dag_id is not None]

        # No DAGs in the file, nothing to check permissions against
        if not dag_ids:
            import_errors.append(import_error)
            continue

        dag_id_to_team = DagModel.get_dag_id_to_team_name_mapping(dag_ids, session=session)
        # Check if user has read access to all the DAGs defined in the file
        requests: Sequence[IsAuthorizedDagRequest] = [
            {
                "method": "GET",
                "details": DagDetails(id=dag_id, team_name=dag_id_to_team.get(dag_id)),
            }
            for dag_id in dag_ids
        ]
        if not auth_manager.batch_is_authorized_dag(requests, user=user):
            session.expunge(import_error)
            import_error.stacktrace = REDACTED_STACKTRACE
        import_errors.append(import_error)

    return ImportErrorCollectionResponse(
        import_errors=import_errors,
        total_entries=total_entries,
    )
