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

from typing import TYPE_CHECKING, Annotated

from fastapi import Depends
from sqlalchemy import and_, func, select

from airflow.api_fastapi.auth.managers.models.resource_details import AccessView, DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset, SortParam
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_bundles import (
    DagBundleCollectionResponse,
    DagBundleResponse,
)
from airflow.api_fastapi.core_api.security import (
    AuthManagerDep,
    GetUserDep,
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


def _import_error_counts(
    *,
    bundle_names: Sequence[str],
    readable_dag_ids: set[str],
    auth_manager: BaseAuthManager,
    user: BaseUser,
    session: Session,
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

    # A rendered bundle url is otherwise only reachable through ``GET /dags/{dag_id}/dagVersions``,
    # which additionally requires Dag *version* read, so withhold it from a role without that.
    show_bundle_url = auth_manager.is_authorized_dag(
        method="GET", access_entity=DagAccessEntity.VERSION, user=user
    )

    return DagBundleCollectionResponse(
        dag_bundles=[
            DagBundleResponse(
                name=bundle.name,
                active=bundle.active,
                version=bundle.version,
                last_refreshed=bundle.last_refreshed,
                bundle_url=bundle.render_url(bundle.version) if show_bundle_url else None,
                team_name=team_names.get(bundle.name),
                import_error_count=(
                    None if import_error_counts is None else import_error_counts.get(bundle.name, 0)
                ),
            )
            for bundle in bundles
        ],
        total_entries=total_entries,
    )
