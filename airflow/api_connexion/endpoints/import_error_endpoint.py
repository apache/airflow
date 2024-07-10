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

from typing import TYPE_CHECKING, Sequence

from sqlalchemy import func, select

from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound, PermissionDenied
from airflow.api_connexion.parameters import apply_sorting, check_limit, format_parameters
from airflow.api_connexion.schemas.error_schema import (
    ImportErrorCollection,
    import_error_collection_schema,
    import_error_schema,
)
from airflow.auth.managers.models.resource_details import AccessView, DagDetails
from airflow.models.dag import DagModel
from airflow.models.errors import ParseImportError
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.www.extensions.init_auth_manager import get_auth_manager

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_connexion.types import APIResponse
    from airflow.auth.managers.models.batch_apis import IsAuthorizedDagRequest


@security.requires_access_view(AccessView.IMPORT_ERRORS)
@provide_session
def get_import_error(*, import_error_id: int, session: Session = NEW_SESSION) -> APIResponse:
    """Get an import error."""
    error = session.get(ParseImportError, import_error_id)
    if error is None:
        raise NotFound(
            "Import error not found",
            detail=f"The ImportError with import_error_id: `{import_error_id}` was not found",
        )
    session.expunge(error)

    can_read_all_dags = get_auth_manager().is_authorized_dag(method="GET")
    if not can_read_all_dags:
        readable_dag_ids = security.get_readable_dags()
        file_dag_ids = {
            dag_id[0]
            for dag_id in session.query(DagModel.dag_id).filter(DagModel.fileloc == error.filename).all()
        }

        # Can the user read any DAGs in the file?
        if not readable_dag_ids.intersection(file_dag_ids):
            raise PermissionDenied(detail="You do not have read permission on any of the DAGs in the file")

        # Check if user has read access to all the DAGs defined in the file
        if not file_dag_ids.issubset(readable_dag_ids):
            error.stacktrace = "REDACTED - you do not have read permission on all DAGs in the file"

    return import_error_schema.dump(error)


@security.requires_access_view(AccessView.IMPORT_ERRORS)
@format_parameters({"limit": check_limit})
@provide_session
def get_import_errors(
    *,
    limit: int,
    offset: int | None = None,
    order_by: str = "import_error_id",
    session: Session = NEW_SESSION,
) -> APIResponse:
    """Get all import errors."""
    to_replace = {"import_error_id": "id"}
    allowed_sort_attrs = ["import_error_id", "timestamp", "filename"]
    count_query = select(func.count(ParseImportError.id))
    query = select(ParseImportError)
    query = apply_sorting(query, order_by, to_replace, allowed_sort_attrs)

    can_read_all_dags = get_auth_manager().is_authorized_dag(method="GET")

    if not can_read_all_dags:
        # if the user doesn't have access to all DAGs, only display errors from visible DAGs
        readable_dag_ids = security.get_readable_dags()
        dagfiles_stmt = select(DagModel.fileloc).distinct().where(DagModel.dag_id.in_(readable_dag_ids))
        query = query.where(ParseImportError.filename.in_(dagfiles_stmt))
        count_query = count_query.where(ParseImportError.filename.in_(dagfiles_stmt))

    total_entries = session.scalars(count_query).one()
    import_errors = session.scalars(query.offset(offset).limit(limit)).all()

    if not can_read_all_dags:
        for import_error in import_errors:
            # Check if user has read access to all the DAGs defined in the file
            file_dag_ids = (
                session.query(DagModel.dag_id).filter(DagModel.fileloc == import_error.filename).all()
            )
            requests: Sequence[IsAuthorizedDagRequest] = [
                {
                    "method": "GET",
                    "details": DagDetails(id=dag_id[0]),
                }
                for dag_id in file_dag_ids
            ]
            if not get_auth_manager().batch_is_authorized_dag(requests):
                session.expunge(import_error)
                import_error.stacktrace = "REDACTED - you do not have read permission on all DAGs in the file"

    return import_error_collection_schema.dump(
        ImportErrorCollection(import_errors=import_errors, total_entries=total_entries)
    )
