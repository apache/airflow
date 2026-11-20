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
from sqlalchemy import case, false, func, select

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
    GetUserDep,
    PermittedDagBundleFilter,
    ReadableDagBundlesFilterDep,
    requires_access_dag_bundles,
)
from airflow.api_fastapi.core_api.services.public.import_errors import get_readable_import_error_files
from airflow.configuration import conf
from airflow.models import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.errors import ParseImportError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

dag_bundles_router = AirflowRouter(tags=["Dag Bundle"], prefix="/dagBundles")

_BundleResponseT = TypeVar("_BundleResponseT", bound=DagBundleResponse)


def _count_import_errors(bundle_names: Sequence[str], *, session: Session) -> dict[str, int]:
    if not bundle_names:
        return {}
    return {
        bundle_name: count
        for bundle_name, count in session.execute(
            select(ParseImportError.bundle_name, func.count())
            .where(ParseImportError.bundle_name.in_(bundle_names))
            .group_by(ParseImportError.bundle_name)
        )
    }


def _get_team_names(bundle_names: Sequence[str], *, session: Session) -> dict[str, str | None]:
    # ``DagBundleModel.team_name`` walks a lazily loaded relationship, so read them in one query.
    if not bundle_names or not conf.getboolean("core", "multi_team"):
        return {}
    return DagBundleModel.get_team_names(bundle_names, session=session)


def _resolve_readable_bundle(
    bundle_name: str,
    readable_dag_bundles_filter: PermittedDagBundleFilter,
    session: Session,
) -> DagBundleModel:
    """Load a bundle the caller is allowed to see, or raise 404 if there is no such bundle."""
    # 404 rather than 403 for a bundle of a team the caller cannot see, so that the response does
    # not confirm which bundle names a deployment has.
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
    import_error_count: int,
    **extra: Any,
) -> _BundleResponseT:
    """Build a bundle response, so the column-to-field mapping lives in one place."""
    return response_class(
        name=bundle.name,
        active=bundle.active,
        version=bundle.version,
        last_refreshed=bundle.last_refreshed,
        bundle_url=bundle.render_url(bundle.version),
        team_name=team_name,
        import_error_count=import_error_count,
        **extra,
    )


@dag_bundles_router.get("", dependencies=[Depends(requires_access_dag_bundles())])
def get_dag_bundles(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(SortParam(["name", "version", "last_refreshed", "active"], DagBundleModel).dynamic_depends()),
    ],
    readable_dag_bundles_filter: ReadableDagBundlesFilterDep,
    session: SessionDep,
) -> DagBundleCollectionResponse:
    """
    List the Dag bundles Airflow knows about.

    Reports the version of each bundle Airflow currently holds and when a Dag processor last
    refreshed it, so whoever deployed the code can tell whether it has been picked up yet.

    Needs ``AccessView.DAG_BUNDLES``. In a multi-team deployment it is checked against each
    bundle's team, and bundles of a team the caller cannot see are left out.
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
    team_names = _get_team_names(bundle_names, session=session)
    import_error_counts = _count_import_errors(bundle_names, session=session)

    return DagBundleCollectionResponse(
        dag_bundles=[
            _serialize_bundle(
                DagBundleResponse,
                bundle,
                team_name=team_names.get(bundle.name),
                import_error_count=import_error_counts.get(bundle.name, 0),
            )
            for bundle in bundles
        ],
        total_entries=total_entries,
    )


@dag_bundles_router.get(
    "/{bundle_name}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag_bundles())],
)
def get_dag_bundle(
    bundle_name: str,
    readable_dag_bundles_filter: ReadableDagBundlesFilterDep,
    session: SessionDep,
) -> DagBundleDetailResponse:
    """Get a Dag bundle."""
    bundle = _resolve_readable_bundle(bundle_name, readable_dag_bundles_filter, session)

    # Stale rows are excluded, matching ``GET /dags``; see ``get_dag_bundle_files`` for why.
    dag_count = session.scalar(
        select(func.count(DagModel.dag_id)).where(
            DagModel.bundle_name == bundle_name,
            DagModel.is_stale == false(),
        )
    )

    return _serialize_bundle(
        DagBundleDetailResponse,
        bundle,
        team_name=_get_team_names([bundle_name], session=session).get(bundle_name),
        import_error_count=_count_import_errors([bundle_name], session=session).get(bundle_name, 0),
        dag_count=dag_count or 0,
    )


@dag_bundles_router.get(
    "/{bundle_name}/files",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag_bundles())],
)
def get_dag_bundle_files(
    bundle_name: str,
    limit: QueryLimit,
    offset: QueryOffset,
    readable_dag_bundles_filter: ReadableDagBundlesFilterDep,
    session: SessionDep,
    user: GetUserDep,
) -> DagBundleFileCollectionResponse:
    """
    List the files in a Dag bundle, ordered by path.

    Every file the bundle holds is listed, with counts covering the whole file: the bundle is
    authorized as a unit, so its contents are not narrowed to the Dags the caller may read. What a
    file's Dags contain, and the text of its import error, stay behind their own permissions.

    ``can_read_import_error`` is per caller, unlike the counts: it says whether ``GET /importErrors``
    would return the file's error to them.

    ``dag_count`` counts live Dags only. Dag rows are never deleted: a file that fails to import
    has every Dag in it marked stale, and so does a file dropped from the bundle. A file therefore
    stays listed on the strength of a live Dag *or* an import error, so a file that has just broken
    does not vanish from the page someone opened to find out why, while a file deleted long ago
    drops out instead of lingering forever.

    Parse times come from the Dags in the file rather than the file itself, which is the only
    record Airflow keeps: a file whose every Dag was removed keeps no parse time of its own.
    """
    _resolve_readable_bundle(bundle_name, readable_dag_bundles_filter, session)

    # The duration is aggregated over live rows only. A Dag removed from a file keeps its row,
    # frozen at an older parse, so an unrestricted ``MAX`` would pair that stale duration with the
    # newer timestamp and report a parse that never happened.
    files: dict[str, dict] = {
        fileloc: {
            "relative_fileloc": fileloc,
            "dag_count": live_dag_count,
            "last_parsed_time": last_parsed_time,
            "last_parse_duration": last_parse_duration,
            "import_error_count": 0,
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
                DagModel.relative_fileloc.is_not(None),
            )
            .group_by(DagModel.relative_fileloc)
        )
    }

    error_filenames = {
        filename
        for filename in session.scalars(
            select(ParseImportError.filename).where(ParseImportError.bundle_name == bundle_name)
        )
        if filename is not None
    }
    for filename in error_filenames:
        # ``import_error`` holds one row per file, so this is 0 or 1.
        files.setdefault(
            filename,
            {
                "relative_fileloc": filename,
                "dag_count": 0,
                "last_parsed_time": None,
                "last_parse_duration": None,
            },
        )["import_error_count"] = 1

    # A file with no live Dag and no error is one the bundle no longer has.
    ordered = sorted(
        (row for path, row in files.items() if row["dag_count"] > 0 or path in error_filenames),
        key=itemgetter("relative_fileloc"),
    )
    start = offset.value or 0
    # ``limit`` is a non-negative int, so zero is a legal value meaning "no rows" -- as
    # ``Select.limit(0)`` gives every other endpoint. Testing it for truthiness would read it as
    # "unlimited" and return the whole list.
    end = None if limit.value is None else start + limit.value

    page = ordered[start:end]
    readable_error_files = get_readable_import_error_files(
        bundle_name,
        [row["relative_fileloc"] for row in page if row["import_error_count"]],
        user=user,
        session=session,
    )
    return DagBundleFileCollectionResponse(
        dag_bundle_files=[
            DagBundleFileResponse(
                **row, can_read_import_error=row["relative_fileloc"] in readable_error_files
            )
            for row in page
        ],
        total_entries=len(ordered),
    )
