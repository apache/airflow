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

from typing import TYPE_CHECKING, Annotated, Literal

from fastapi import Depends, HTTPException, Path, Query, status
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.api_fastapi.common.dagbag import DagBagDep, get_latest_version_of_dag
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterParam,
    QueryLimit,
    QueryOffset,
    SortParam,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.dag_versions import (
    DAGVersionCollectionResponse,
    DagVersionDiffResponse,
    DagVersionResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import (
    GetUserDep,
    ReadableDagVersionsFilterDep,
    requires_access_dag,
)
from airflow.models.dag_version import DagVersion
from airflow.models.dag_version_diff import (
    MAX_ALLOWED_CHANGES,
    DagVersionNotFoundError,
    SourceStatus,
    ValuesStatus,
    get_dag_version_diff as build_dag_version_diff,
)
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagcode import DagCode
from airflow.models.team import Team

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.api_fastapi.auth.managers.models.base_user import BaseUser

dag_versions_router = AirflowRouter(tags=["DagVersion"], prefix="/dags/{dag_id}/dagVersions")

RawDataAuthorizationStatus = Literal["available", "redacted", "unavailable"]


@dag_versions_router.get(
    "/{version_number}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.VERSION))],
)
def get_dag_version(
    dag_id: str,
    version_number: int,
    session: SessionDep,
) -> DagVersionResponse:
    """Get one Dag Version."""
    dag_version = session.scalar(
        select(DagVersion)
        .filter_by(dag_id=dag_id, version_number=version_number)
        .options(joinedload(DagVersion.dag_model))
    )

    if dag_version is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The DagVersion with dag_id: `{dag_id}` and version_number: `{version_number}` was not found",
        )

    return dag_version


@dag_versions_router.get(
    "/{base_version_number}/diff/{target_version_number}",
    response_model_exclude_unset=True,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.VERSION))],
)
def get_dag_version_diff(
    dag_id: str,
    base_version_number: Annotated[int, Path(ge=1)],
    target_version_number: Annotated[int, Path(ge=1)],
    session: SessionDep,
    user: GetUserDep,
    include_values: Annotated[bool, Query()] = False,
    include_source: Annotated[bool, Query()] = False,
    max_changes: Annotated[int, Query(gt=0, le=MAX_ALLOWED_CHANGES)] = 500,
) -> DagVersionDiffResponse:
    """Compare two currently stored Dag versions."""
    source_status: SourceStatus | None = None
    values_status: ValuesStatus | None = None
    if include_values or include_source:
        versions_by_number = _get_requested_versions(
            dag_id,
            base_version_number,
            target_version_number,
            session=session,
        )
        raw_data_authorization_status = _get_raw_data_authorization_status(
            dag_id,
            base_version_number,
            target_version_number,
            versions_by_number=versions_by_number,
            user=user,
            session=session,
        )
        if include_values:
            values_status = "available" if raw_data_authorization_status == "available" else "unavailable"
        if include_source:
            source_status = _get_source_status(
                base_version_number,
                target_version_number,
                versions_by_number=versions_by_number,
                authorization_status=raw_data_authorization_status,
                session=session,
                user=user,
            )

    try:
        result = build_dag_version_diff(
            dag_id,
            base_version_number,
            target_version_number,
            include_values=include_values,
            include_source=include_source,
            max_changes=max_changes,
            source_status=source_status,
            values_status=values_status,
            session=session,
        )
    except DagVersionNotFoundError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(error)) from error

    return DagVersionDiffResponse.model_validate(result)


def _get_requested_versions(
    dag_id: str,
    base_version_number: int,
    target_version_number: int,
    *,
    session: Session,
) -> dict[int, DagVersion]:
    versions = session.scalars(
        select(DagVersion).where(
            DagVersion.dag_id == dag_id,
            DagVersion.version_number.in_((base_version_number, target_version_number)),
        )
    ).all()
    return {version.version_number: version for version in versions}


def _get_raw_data_authorization_status(
    dag_id: str,
    base_version_number: int,
    target_version_number: int,
    *,
    versions_by_number: dict[int, DagVersion],
    session: Session,
    user: BaseUser,
) -> RawDataAuthorizationStatus:
    """Authorize raw values and source against each stored version's bundle."""
    requested_versions = []
    for version_number in (base_version_number, target_version_number):
        if version := versions_by_number.get(version_number):
            requested_versions.append(version)
        else:
            return "unavailable"

    bundle_names = {version.bundle_name for version in requested_versions}
    if None in bundle_names:
        return "unavailable"
    concrete_bundle_names = {bundle_name for bundle_name in bundle_names if bundle_name is not None}
    team_names_by_bundle = _get_bundle_team_names(concrete_bundle_names, session=session)
    if concrete_bundle_names != set(team_names_by_bundle):
        return "unavailable"

    auth_manager = get_auth_manager()
    for version in requested_versions:
        bundle_name = version.bundle_name
        if bundle_name is None:
            return "unavailable"
        if not auth_manager.is_authorized_dag(
            method="GET",
            access_entity=DagAccessEntity.CODE,
            details=DagDetails(id=dag_id, team_name=team_names_by_bundle[bundle_name]),
            user=user,
        ):
            return "redacted"
    return "available"


def _get_bundle_team_names(bundle_names: set[str], *, session: Session) -> dict[str, str | None]:
    rows = session.execute(
        select(DagBundleModel.name, Team.name)
        .outerjoin(DagBundleModel.teams)
        .where(DagBundleModel.name.in_(bundle_names))
    ).all()
    team_names: dict[str, str | None] = {}
    for bundle_name, team_name in rows:
        team_names[bundle_name] = team_name
    return team_names


def _get_source_status(
    base_version_number: int,
    target_version_number: int,
    *,
    versions_by_number: dict[int, DagVersion],
    authorization_status: RawDataAuthorizationStatus,
    session: Session,
    user: BaseUser,
) -> SourceStatus:
    """Return the source visibility state after checking both versions and historical co-location."""
    if authorization_status != "available":
        return "redacted" if authorization_status == "redacted" else "unavailable"

    requested_versions = []
    for version_number in (base_version_number, target_version_number):
        if version := versions_by_number.get(version_number):
            requested_versions.append(version)
        else:
            return "unavailable"

    dag_codes = session.scalars(
        select(DagCode).where(DagCode.dag_version_id.in_([version.id for version in requested_versions]))
    ).all()
    dag_codes_by_version_id = {dag_code.dag_version_id: dag_code for dag_code in dag_codes}
    if any(version.id not in dag_codes_by_version_id for version in requested_versions):
        return "unavailable"

    auth_manager = get_auth_manager()
    readable_dag_ids = auth_manager.get_authorized_dag_ids(user=user)
    filelocs = {dag_codes_by_version_id[version.id].fileloc for version in requested_versions}
    colocated_dag_ids = set(
        session.scalars(select(DagCode.dag_id).where(DagCode.fileloc.in_(filelocs))).all()
    )
    if not colocated_dag_ids.issubset(readable_dag_ids):
        return "redacted"
    return "current_stored_code"


@dag_versions_router.get(
    "",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
        ],
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.VERSION))],
)
def get_dag_versions(
    dag_id: str,
    session: SessionDep,
    limit: QueryLimit,
    offset: QueryOffset,
    version_number: Annotated[
        FilterParam[int], Depends(filter_param_factory(DagVersion.version_number, int))
    ],
    bundle_name: Annotated[FilterParam[str], Depends(filter_param_factory(DagVersion.bundle_name, str))],
    bundle_version: Annotated[
        FilterParam[str | None], Depends(filter_param_factory(DagVersion.bundle_version, str | None))
    ],
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(["id", "version_number", "bundle_name", "bundle_version"], DagVersion).dynamic_depends()
        ),
    ],
    dag_bag: DagBagDep,
    readable_dag_versions_filter: ReadableDagVersionsFilterDep,
) -> DAGVersionCollectionResponse:
    """
    Get all Dag Versions.

    This endpoint allows specifying `~` as the dag_id to retrieve Dag Versions for all Dags.
    """
    query = select(DagVersion).options(joinedload(DagVersion.dag_model), joinedload(DagVersion.bundle))

    if dag_id != "~":
        get_latest_version_of_dag(dag_bag, dag_id, session)
        query = query.filter(DagVersion.dag_id == dag_id)

    dag_versions_select, total_entries = paginated_select(
        statement=query,
        filters=[version_number, bundle_name, bundle_version, readable_dag_versions_filter],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    dag_versions = session.scalars(dag_versions_select)

    return DAGVersionCollectionResponse(
        dag_versions=dag_versions,
        total_entries=total_entries,
    )
