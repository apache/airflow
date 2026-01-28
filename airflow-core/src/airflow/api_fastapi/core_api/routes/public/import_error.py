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
from typing import Annotated

from fastapi import Depends, HTTPException, status
from sqlalchemy import and_, select

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

    if error.bundle_name is None or error.filename is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The ImportError with import_error_id: `{import_error_id}` has invalid bundle_name or filename",
        )

    # We need file_dag_ids as a set for intersection, issubset operations
    # Check DAGs in the file using relative_fileloc and bundle_name
    file_dag_ids = set(
        session.scalars(
            select(DagModel.dag_id).where(
                and_(
                    DagModel.relative_fileloc == error.filename,
                    DagModel.bundle_name == error.bundle_name,
                )
            )
        ).all()
    )

    # If no DAGs exist for this file, check if user has access to any DAG in the bundle
    if not file_dag_ids:
        bundle_dag_ids = set(
            session.scalars(select(DagModel.dag_id).where(DagModel.bundle_name == error.bundle_name)).all()
        )
        readable_bundle_dag_ids = readable_dag_ids.intersection(bundle_dag_ids)
        # Can the user read any DAGs in the bundle?
        if not readable_bundle_dag_ids:
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                "You do not have read permission on any of the DAGs in the bundle",
            )
        # User has access to bundle, return the error
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

    # First, get all import errors (without filtering by DagModel)
    # This ensures we include import errors even when no DAG was created from the file
    import_errors_stmt = select(ParseImportError).order_by(ParseImportError.id)

    # Paginate the import errors query
    import_errors_select, total_entries = paginated_select(
        statement=import_errors_stmt,
        filters=[filename_pattern],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )

    # Get all import errors
    all_import_errors = session.scalars(import_errors_select).all()

    # Build mappings for permission checks in a single query
    # Get all DAGs the user can read, grouped by (bundle_name, relative_fileloc) and bundle_name
    visible_dags = session.execute(
        select(
            DagModel.relative_fileloc,
            DagModel.dag_id,
            DagModel.bundle_name,
        ).where(DagModel.dag_id.in_(readable_dag_ids))
    ).all()

    # Group dag_ids by (bundle_name, relative_fileloc) for file-level checks
    file_dag_map: dict[tuple[str, str], list[str]] = {}
    # Group dag_ids by bundle_name for bundle-level checks (when file has no DAGs)
    bundle_dag_map: dict[str, list[str]] = {}
    for relative_fileloc, dag_id, bundle_name in visible_dags:
        # File-level mapping
        key = (bundle_name, relative_fileloc)
        if key not in file_dag_map:
            file_dag_map[key] = []
        file_dag_map[key].append(dag_id)
        # Bundle-level mapping
        if bundle_name not in bundle_dag_map:
            bundle_dag_map[bundle_name] = []
        bundle_dag_map[bundle_name].append(dag_id)

    import_errors = []
    for import_error in all_import_errors:
        if import_error.bundle_name is None or import_error.filename is None:
            continue

        key = (import_error.bundle_name, import_error.filename)
        dag_ids = file_dag_map.get(key, [])

        # If no DAGs exist for this file, check if user has access to any DAG in the bundle
        if not dag_ids:
            bundle_dag_ids = bundle_dag_map.get(import_error.bundle_name, [])
            # If user has no access to any DAG in the bundle, skip this import error
            if not bundle_dag_ids:
                continue
            # If user has access to bundle, show the error (but we can't check full access)
            session.expunge(import_error)
            import_errors.append(import_error)
            continue

        # Check if user has read access to all the DAGs defined in the file
        dag_id_to_team = DagModel.get_dag_id_to_team_name_mapping(dag_ids, session=session)
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

    # Return filtered count as total_entries to match expected behavior
    return ImportErrorCollectionResponse(
        import_errors=import_errors,
        total_entries=len(import_errors),
    )
