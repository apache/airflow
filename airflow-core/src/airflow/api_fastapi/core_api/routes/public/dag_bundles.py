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

from operator import itemgetter
from typing import TYPE_CHECKING, Annotated, Any, TypeVar

from fastapi import Depends, HTTPException, status
from sqlalchemy import and_, case, false, func, select

from airflow.api_fastapi.auth.managers.models.resource_details import AccessView, DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset, SortParam
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_bundles import (
    DagBundleCollectionResponse,
    DagBundleDetailResponse,
    DagBundleFileCollectionResponse,
    DagBundleFileResponse,
    DagBundleResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    AuthManagerDep,
    GetUserDep,
    PermittedDagBundleFilter,
    ReadableDagBundlesFilterDep,
    requires_access_dag,
)
from airflow.configuration import conf
from airflow.models import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.errors import ParseImportError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

    from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager
    from airflow.api_fastapi.auth.managers.models.base_user import BaseUser

dag_bundles_router = AirflowRouter(tags=["Dag Bundle"], prefix="/dagBundles")

_BundleResponseT = TypeVar("_BundleResponseT", bound=DagBundleResponse)


def _import_error_counts(
    *,
    bundle_names: Sequence[str],
    readable_dag_ids: set[str],
    auth_manager: BaseAuthManager,
    user: BaseUser,
    session: Session,
    team_names: dict[str, str | None] | None = None,
) -> dict[str, int] | None:
    """
    Count the import errors per bundle that this caller is allowed to know about.

    Reproduces the two-part authorization of ``GET /importErrors`` rather than counting every row
    for the bundle: an error in a file the caller can read no Dag in stays hidden, and an error in
    a file with no registered Dag needs the admin-by-default ``IMPORT_ERRORS_ALL``, since the
    file's existence would otherwise leak. Returns ``None`` when the caller may not read import
    errors at all.
    """
    if not auth_manager.authorize_view(access_view=AccessView.IMPORT_ERRORS, user=user):
        return None
    if not bundle_names:
        return {}

    # Files -- keyed ``(relative_fileloc, bundle_name)`` -- in which the caller can read a Dag.
    readable_files = (
        select(DagModel.relative_fileloc, DagModel.bundle_name)
        .where(
            DagModel.dag_id.in_(readable_dag_ids),
            DagModel.bundle_name.in_(bundle_names),
        )
        .distinct()
        .subquery()
    )
    counts: dict[str, int] = {
        bundle_name: count
        for bundle_name, count in session.execute(
            select(ParseImportError.bundle_name, func.count())
            .join(
                readable_files,
                and_(
                    ParseImportError.filename == readable_files.c.relative_fileloc,
                    ParseImportError.bundle_name == readable_files.c.bundle_name,
                ),
            )
            .group_by(ParseImportError.bundle_name)
        ).all()
        if bundle_name is not None
    }

    # Errors for files that never registered a Dag, added only where the caller holds the
    # admin-by-default view for that bundle's team. Narrowed to this page's bundles, unlike the
    # equivalent in ``import_error.py``: this endpoint polls, so an unbounded ``SELECT DISTINCT``
    # over ``dag`` every few seconds would be a real cost on a large deployment.
    files_with_any_dags = (
        select(DagModel.relative_fileloc, DagModel.bundle_name)
        .where(DagModel.bundle_name.in_(bundle_names))
        .distinct()
        .subquery()
    )
    unregistered = session.execute(
        select(ParseImportError.bundle_name, func.count())
        .outerjoin(
            files_with_any_dags,
            and_(
                ParseImportError.filename == files_with_any_dags.c.relative_fileloc,
                ParseImportError.bundle_name == files_with_any_dags.c.bundle_name,
            ),
        )
        .where(
            files_with_any_dags.c.relative_fileloc.is_(None),
            ParseImportError.bundle_name.in_(bundle_names),
        )
        .group_by(ParseImportError.bundle_name)
    ).all()
    if unregistered:
        if team_names is None:
            team_names = DagBundleModel.get_team_names(
                [bundle_name for bundle_name, _ in unregistered if bundle_name], session=session
            )
        for bundle_name, count in unregistered:
            if bundle_name is None:
                continue
            if auth_manager.authorize_view(
                access_view=AccessView.IMPORT_ERRORS_ALL,
                user=user,
                team_name=team_names.get(bundle_name),
            ):
                counts[bundle_name] = counts.get(bundle_name, 0) + count

    return counts


def _resolve_readable_bundle(
    bundle_name: str,
    readable_dag_bundles_filter: PermittedDagBundleFilter,
    session: Session,
) -> DagBundleModel:
    """Load a bundle the caller is allowed to see, or raise 404 if there is no such bundle."""
    # 404 rather than 403 for a bundle that exists but holds no readable Dag, so that the response
    # does not confirm which bundle names a deployment has.
    bundle = session.scalar(
        readable_dag_bundles_filter.to_orm(select(DagBundleModel).where(DagBundleModel.name == bundle_name))
    )
    if bundle is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag bundle with name `{bundle_name}` was not found")
    return bundle


def _serialize_bundle(
    response_class: type[_BundleResponseT],
    bundle: DagBundleModel,
    *,
    team_name: str | None,
    import_error_count: int | None,
    show_bundle_url: bool,
    **extra: Any,
) -> _BundleResponseT:
    """Build a bundle response, so the column-to-field mapping lives in one place."""
    return response_class(
        name=bundle.name,
        active=bundle.active,
        version=bundle.version,
        last_refreshed=bundle.last_refreshed,
        bundle_url=bundle.render_url(bundle.version) if show_bundle_url else None,
        team_name=team_name,
        import_error_count=import_error_count,
        **extra,
    )


def _may_show_bundle_url(auth_manager: BaseAuthManager, user: BaseUser) -> bool:
    # A rendered bundle url is otherwise only reachable through ``GET /dags/{dag_id}/dagVersions``,
    # which additionally requires Dag *version* read, so withhold it from a role without that.
    return auth_manager.is_authorized_dag(method="GET", access_entity=DagAccessEntity.VERSION, user=user)


@dag_bundles_router.get("", dependencies=[Depends(requires_access_dag(method="GET"))])
def get_dag_bundles(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["name", "version", "last_refreshed", "active"], DagBundleModel).dynamic_depends()),
    ],
    readable_dag_bundles_filter: ReadableDagBundlesFilterDep,
    auth_manager: AuthManagerDep,
    session: SessionDep,
    user: GetUserDep,
) -> DagBundleCollectionResponse:
    """
    List the Dag bundles Airflow knows about.

    Reports the version of each bundle Airflow currently holds and when a Dag processor last
    refreshed it, so whoever deployed the code can tell whether it has been picked up yet.

    A bundle is visible to a user who can read at least one Dag recorded against it, so one from
    which no Dag has ever parsed successfully is not listed at all.
    """
    bundles_select, total_entries = paginated_select(
        statement=select(DagBundleModel),
        filters=[readable_dag_bundles_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    bundles = session.scalars(bundles_select).all()
    bundle_names = [bundle.name for bundle in bundles]

    # ``DagBundleModel.team_name`` walks a lazily loaded relationship, so read them in one query.
    team_names = (
        DagBundleModel.get_team_names(bundle_names, session=session)
        if bundle_names and conf.getboolean("core", "multi_team")
        else {}
    )

    import_error_counts = _import_error_counts(
        bundle_names=bundle_names,
        readable_dag_ids=readable_dag_bundles_filter.value or set(),
        auth_manager=auth_manager,
        user=user,
        session=session,
    )

    show_bundle_url = _may_show_bundle_url(auth_manager, user)

    return DagBundleCollectionResponse(
        dag_bundles=[
            _serialize_bundle(
                DagBundleResponse,
                bundle,
                team_name=team_names.get(bundle.name),
                import_error_count=(
                    None if import_error_counts is None else import_error_counts.get(bundle.name, 0)
                ),
                show_bundle_url=show_bundle_url,
            )
            for bundle in bundles
        ],
        total_entries=total_entries,
    )


@dag_bundles_router.get(
    "/{bundle_name}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET"))],
)
def get_dag_bundle(
    bundle_name: str,
    readable_dag_bundles_filter: ReadableDagBundlesFilterDep,
    auth_manager: AuthManagerDep,
    session: SessionDep,
    user: GetUserDep,
) -> DagBundleDetailResponse:
    """Get a Dag bundle."""
    bundle = _resolve_readable_bundle(bundle_name, readable_dag_bundles_filter, session)
    readable_dag_ids = readable_dag_bundles_filter.value or set()

    # Stale rows are excluded, matching ``GET /dags``; see ``get_dag_bundle_files`` for why.
    dag_count = session.scalar(
        select(func.count(DagModel.dag_id)).where(
            DagModel.bundle_name == bundle_name,
            DagModel.dag_id.in_(readable_dag_ids),
            DagModel.is_stale == false(),
        )
    )
    # Resolved once and threaded through: ``_import_error_counts`` needs the team to authorize
    # the admin-gated view, and the response needs it again.
    team_names = (
        DagBundleModel.get_team_names([bundle_name], session=session)
        if conf.getboolean("core", "multi_team")
        else {}
    )
    import_error_counts = _import_error_counts(
        bundle_names=[bundle_name],
        readable_dag_ids=readable_dag_ids,
        auth_manager=auth_manager,
        user=user,
        session=session,
        team_names=team_names,
    )

    return _serialize_bundle(
        DagBundleDetailResponse,
        bundle,
        team_name=team_names.get(bundle_name),
        import_error_count=(None if import_error_counts is None else import_error_counts.get(bundle_name, 0)),
        show_bundle_url=_may_show_bundle_url(auth_manager, user),
        dag_count=dag_count or 0,
    )


@dag_bundles_router.get(
    "/{bundle_name}/files",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET"))],
)
def get_dag_bundle_files(
    bundle_name: str,
    limit: QueryLimit,
    offset: QueryOffset,
    readable_dag_bundles_filter: ReadableDagBundlesFilterDep,
    auth_manager: AuthManagerDep,
    session: SessionDep,
    user: GetUserDep,
) -> DagBundleFileCollectionResponse:
    """
    List the files in a Dag bundle, ordered by path.

    A file is listed when the caller can read at least one Dag it defines. A file that recorded an
    import error without registering any Dag is listed too, on the same admin-by-default terms as
    ``GET /importErrors`` -- a file that fails before defining a Dag has no Dag to authorize on,
    and it is the case this page most needs to show.

    ``dag_count`` counts live Dags only. Dag rows are never deleted: a file that fails to import
    has every Dag in it marked stale, and so does a file dropped from the bundle. A file therefore
    stays listed on the strength of a live Dag *or* an import error, so a file that has just broken
    does not vanish from the page someone opened to find out why, while a file deleted long ago
    drops out instead of lingering forever.

    Parse times come from the Dags in the file rather than the file itself, which is the only
    record Airflow keeps: a file whose every Dag was removed keeps no parse time of its own.
    """
    _resolve_readable_bundle(bundle_name, readable_dag_bundles_filter, session)
    readable_dag_ids = readable_dag_bundles_filter.value or set()

    # Rows are merged in the api-server rather than by a union in the database: the two halves
    # differ in both shape and authorization, and the set being merged is one bundle's files.
    #
    # ``last_parsed_time`` is the file's last known parse and survives the file breaking, so a
    # broken file still says when it last worked. ``last_parse_duration`` is taken from live rows
    # only: a Dag removed from a file keeps its old row, frozen at an older parse, and an
    # unrestricted ``MAX`` would pair that stale duration with the newer timestamp. Whenever a live
    # row exists it is from the most recent parse, so the pair always describes one parse; once
    # none does, the duration is null rather than wrong.
    files: dict[str, dict] = {
        fileloc: {
            "relative_fileloc": fileloc,
            "dag_count": live_dag_count,
            "last_parsed_time": last_parsed_time,
            "last_parse_duration": last_parse_duration,
            # Zero would read as "nothing wrong with this file"; the caller's permission is
            # decided below, and until then the count is unknown rather than clean.
            "import_error_count": None,
        }
        for fileloc, live_dag_count, last_parsed_time, last_parse_duration in session.execute(
            select(
                DagModel.relative_fileloc,
                func.count(case((DagModel.is_stale == false(), DagModel.dag_id))),
                func.max(DagModel.last_parsed_time),
                func.max(case((DagModel.is_stale == false(), DagModel.last_parse_duration))),
            )
            .where(
                DagModel.bundle_name == bundle_name,
                DagModel.dag_id.in_(readable_dag_ids),
                DagModel.relative_fileloc.is_not(None),
            )
            .group_by(DagModel.relative_fileloc)
        )
    }

    error_filenames: set[str] = set()
    if auth_manager.authorize_view(access_view=AccessView.IMPORT_ERRORS, user=user):
        for row in files.values():
            row["import_error_count"] = 0
        error_filenames = {
            filename
            for filename in session.scalars(
                select(ParseImportError.filename).where(ParseImportError.bundle_name == bundle_name)
            )
            if filename is not None
        }
        for filename in error_filenames & files.keys():
            # ``import_error`` holds one row per file, so this is 0 or 1.
            files[filename]["import_error_count"] = 1

        unregistered = error_filenames - files.keys()
        if unregistered and auth_manager.authorize_view(
            access_view=AccessView.IMPORT_ERRORS_ALL,
            user=user,
            team_name=(
                DagBundleModel.get_team_names([bundle_name], session=session).get(bundle_name)
                if conf.getboolean("core", "multi_team")
                else None
            ),
        ):
            # Queried only behind the permission that consumes it. A file with no Dag row at all
            # has no Dag to authorize on, so it needs the admin-by-default view; a file whose Dags
            # exist but are unreadable is excluded by being in this set.
            files_with_any_dags = set(
                session.scalars(
                    select(DagModel.relative_fileloc).where(DagModel.bundle_name == bundle_name).distinct()
                )
            )
            for filename in unregistered - files_with_any_dags:
                files[filename] = {
                    "relative_fileloc": filename,
                    "dag_count": 0,
                    "last_parsed_time": None,
                    "last_parse_duration": None,
                    "import_error_count": 1,
                }

    # A file with no live Dag and no error the caller can see is one the bundle no longer has.
    ordered = sorted(
        (row for path, row in files.items() if row["dag_count"] > 0 or path in error_filenames),
        key=itemgetter("relative_fileloc"),
    )
    start = offset.value or 0
    # ``limit`` is a non-negative int, so zero is a legal value meaning "no rows" -- as
    # ``Select.limit(0)`` gives every other endpoint. Testing it for truthiness would read it as
    # "unlimited" and return the whole list.
    end = None if limit.value is None else start + limit.value

    return DagBundleFileCollectionResponse(
        dag_bundle_files=[DagBundleFileResponse(**row) for row in ordered[start:end]],
        total_entries=len(ordered),
    )
