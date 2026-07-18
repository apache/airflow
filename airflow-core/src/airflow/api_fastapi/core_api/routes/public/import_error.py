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
from sqlalchemy import and_, func, or_, select

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.batch_apis import IsAuthorizedDagRequest
from airflow.api_fastapi.auth.managers.models.resource_details import (
    DagDetails,
)
from airflow.api_fastapi.common.db.common import (
    SessionDep,
    apply_filters_to_select,
    paginated_select,
)
from airflow.api_fastapi.common.parameters import (
    QueryLimit,
    QueryOffset,
    QueryParseImportErrorBundleNameFilter,
    QueryParseImportErrorFilenameFilter,
    QueryParseImportErrorFilenamePatternSearch,
    QueryParseImportErrorFilenamePrefixPatternSearch,
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
from airflow.models.dagbundle import DagBundleModel
from airflow.models.errors import ParseImportError

REDACTED_STACKTRACE = "REDACTED - you do not have read permission on all Dags in the file"
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
    # ``ParseImportError.filename`` is a repository-relative path and
    # ``DagModel.fileloc`` is typically the absolute path those files were
    # loaded from, so matching on ``fileloc == filename`` would come back
    # empty in most real deployments. Match on ``relative_fileloc`` (and the
    # bundle name that scopes it) instead, which is the same key the list
    # endpoint already uses for the join below.
    file_dag_ids = set(
        session.scalars(
            select(DagModel.dag_id).where(
                DagModel.relative_fileloc == error.filename,
                DagModel.bundle_name == error.bundle_name,
            )
        ).all()
    )

    # No Dags matched for this file -- either the file genuinely contains
    # no Dags (parse failed before any Dag was defined), or the name keys
    # did not resolve. There is no per-Dag key to authorize on, so gate
    # visibility on the dedicated ``IMPORT_ERRORS_ALL`` view (admin by
    # default), scoped to the file's team via its bundle. Deny access
    # entirely for callers without it rather than failing open -- returning
    # the row would leak the file's existence (and, for team-scoped bundles,
    # cross-team repository structure).
    if not file_dag_ids:
        team_name = (
            DagBundleModel.get_team_name(error.bundle_name, session=session) if error.bundle_name else None
        )
        if not auth_manager.is_authorized_view_for_team(
            access_view=AccessView.IMPORT_ERRORS_ALL, user=user, team_name=team_name
        ):
            raise HTTPException(
                status.HTTP_403_FORBIDDEN,
                "You do not have permission to view import errors for files with no registered Dag",
            )
        return error

    # Can the user read any Dags in the file?
    if not readable_dag_ids.intersection(file_dag_ids):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "You do not have read permission on any of the Dags in the file",
        )

    # Check if user has read access to all the Dags defined in the file
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
    filename_prefix_pattern: QueryParseImportErrorFilenamePrefixPatternSearch,
    filename: QueryParseImportErrorFilenameFilter,
    bundle_name: QueryParseImportErrorBundleNameFilter,
    session: SessionDep,
    user: GetUserDep,
) -> ImportErrorCollectionResponse:
    """Get all import errors."""
    auth_manager = get_auth_manager()
    readable_dag_ids = auth_manager.get_authorized_dag_ids(method="GET", user=user)

    # Subquery for files that have any Dags
    files_with_any_dags = select(DagModel.relative_fileloc, DagModel.bundle_name).distinct().subquery()

    # Import errors for files that have **no** registered Dag have no per-Dag
    # key to authorize on. Authorize them on the dedicated ``IMPORT_ERRORS_ALL``
    # view (admin by default), scoped to the file's team via its bundle, and
    # keep only the ones the caller may see. Filtering here (rather than
    # redacting in the loop) also excludes unauthorized rows from the count and
    # pagination, so their existence -- and, for team-scoped bundles, the
    # cross-team file/bundle names -- does not leak.
    unregistered_errors = session.execute(
        select(ParseImportError.id, ParseImportError.bundle_name)
        .outerjoin(
            files_with_any_dags,
            (ParseImportError.filename == files_with_any_dags.c.relative_fileloc)
            & (ParseImportError.bundle_name == files_with_any_dags.c.bundle_name),
        )
        .where(files_with_any_dags.c.relative_fileloc.is_(None))
    ).all()
    team_name_by_bundle = DagBundleModel.get_team_names(
        {bundle_name for _, bundle_name in unregistered_errors if bundle_name}, session=session
    )
    authorized_unregistered_ids = {
        error_id
        for error_id, bundle_name in unregistered_errors
        if auth_manager.is_authorized_view_for_team(
            access_view=AccessView.IMPORT_ERRORS_ALL,
            user=user,
            team_name=team_name_by_bundle.get(bundle_name) if bundle_name else None,
        )
    }

    # Files (identified by ``(relative_fileloc, bundle_name)``) where the
    # user can read at least one Dag. Used to decide which import errors
    # the user is allowed to see at all.
    readable_files_cte = (
        select(DagModel.relative_fileloc, DagModel.bundle_name)
        .where(DagModel.dag_id.in_(readable_dag_ids))
        .distinct()
        .cte()
    )

    # Full ``(relative_fileloc, dag_id, bundle_name)`` set for every file
    # the user can see at least one Dag of. Crucially this is **not**
    # filtered by ``readable_dag_ids`` -- the per-file authorization
    # check in the loop below needs the complete Dag set so it can
    # detect co-located Dags that the caller is not authorized to read
    # and redact the stacktrace accordingly.
    file_dags_cte = (
        select(DagModel.relative_fileloc, DagModel.dag_id, DagModel.bundle_name)
        .join(
            readable_files_cte,
            and_(
                DagModel.relative_fileloc == readable_files_cte.c.relative_fileloc,
                DagModel.bundle_name == readable_files_cte.c.bundle_name,
            ),
        )
        .cte()
    )

    # Prepare the import errors query by joining with the CTE above.
    # Each returned row will be a tuple: (ParseImportError, dag_id).
    # ``dag_id`` is NULL for import errors whose file has no Dags at all
    # in ``DagModel`` (parse failed before any Dag was defined).
    import_errors_stmt = (
        select(ParseImportError, file_dags_cte.c.dag_id)
        .outerjoin(
            files_with_any_dags,
            ParseImportError.filename == files_with_any_dags.c.relative_fileloc,
        )
        .outerjoin(
            file_dags_cte,
            and_(
                ParseImportError.filename == file_dags_cte.c.relative_fileloc,
                ParseImportError.bundle_name == file_dags_cte.c.bundle_name,
            ),
        )
        .where(
            or_(
                # Unregistered-file errors the caller is authorized to see.
                ParseImportError.id.in_(authorized_unregistered_ids),
                # Files where the caller can read at least one Dag.
                file_dags_cte.c.dag_id.isnot(None),
            )
        )
        .order_by(ParseImportError.id)
    )

    filtered_import_errors_stmt = apply_filters_to_select(
        statement=import_errors_stmt,
        filters=[filename_pattern, filename_prefix_pattern, filename, bundle_name],
    )
    import_error_ids_stmt = (
        filtered_import_errors_stmt.with_only_columns(
            ParseImportError.id,
            ParseImportError.timestamp,
            ParseImportError.filename,
            ParseImportError.bundle_name,
            ParseImportError.stacktrace,
        )
        .distinct()
        .order_by(None)
    )
    total_entries = session.scalar(select(func.count()).select_from(import_error_ids_stmt.subquery())) or 0

    # Paginate distinct import error IDs first so limit/offset apply to
    # import error objects, not to the joined Dag rows.
    paginated_import_error_ids_select, _ = paginated_select(
        statement=import_error_ids_stmt,
        filters=[],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
        return_total_entries=False,
    )
    paginated_import_error_ids = paginated_import_error_ids_select.subquery()

    # Fetch all joined Dag rows for the paginated import error IDs before
    # grouping, so each returned import error still has the full Dag set.
    import_errors_select = apply_filters_to_select(
        statement=filtered_import_errors_stmt.where(
            ParseImportError.id.in_(select(paginated_import_error_ids.c.id))
        ),
        filters=[order_by],
    )
    import_errors_result: Iterable[tuple[ParseImportError, Iterable]] = groupby(
        session.execute(import_errors_select), itemgetter(0)
    )

    import_errors = []
    for import_error, file_dag_ids_iter in import_errors_result:
        dag_ids = [dag_id for _, dag_id in file_dag_ids_iter if dag_id is not None]

        # No Dags matched for this file. Only unregistered-file errors the
        # caller is authorized to see reach this point (unauthorized ones are
        # excluded by the ``authorized_unregistered_ids`` filter above), so
        # return the raw stacktrace.
        if not dag_ids:
            import_errors.append(import_error)
            continue

        dag_id_to_team = DagModel.get_dag_id_to_team_name_mapping(dag_ids, session=session)
        # Check if user has read access to all the Dags defined in the file
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
